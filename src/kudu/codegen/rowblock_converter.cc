// Copyright 2014 Cloudera inc.

#include "kudu/codegen/rowblock_converter.h"

#include <string>
#include <vector>

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/Argument.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Type.h>

#include "kudu/codegen/jit_wrapper.h"
#include "kudu/codegen/module_builder.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/faststring.h"

namespace llvm {
class LLVMContext;
class TargetMachine;
} // namespace llvm

using boost::assign::list_of;
using llvm::Argument;
using llvm::ArrayType;
using llvm::BasicBlock;
using llvm::Constant;
using llvm::ConstantArray;
using llvm::ConstantExpr;
using llvm::ExecutionEngine;
using llvm::Function;
using llvm::FunctionType;
using llvm::Instruction;
using llvm::IntegerType;
using llvm::LLVMContext;
using llvm::PointerType;
using llvm::Type;
using llvm::Value;
using std::string;
using std::ostream;
using std::vector;
using strings::Substitute;

DECLARE_bool(codegen_dump_functions);

namespace kudu {

namespace codegen {

typedef RowBlockConverterFunction RBCF;

namespace {

vector<size_t> GatherSrcIndices(const Schema& src, const Schema& dst) {
  vector<size_t> indices;
    for (int i = 0; i < src.num_columns(); ++i) {
      size_t idx = dst.find_column(src.column(i).name());
      if (idx != -1) {
        indices.push_back(i);
      }
    }
  return indices;
}

vector<size_t> GatherDstIndices(const Schema& src, const Schema& dst) {
  vector<size_t> indices;
  BOOST_FOREACH(const ColumnSchema& col, src.columns()) {
    size_t idx = dst.find_column(col.name());
    if (idx != -1) {
      indices.push_back(idx);
    }
  }
  return indices;
}

vector<size_t> GatherOffsets(const Schema& src, const Schema& dst) {
  vector<size_t> offsets;
  BOOST_FOREACH(const ColumnSchema& col, src.columns()) {
    size_t idx = dst.find_column(col.name());
    if (idx != -1) {
      offsets.push_back(dst.column_offset(idx));
    }
  }
  return offsets;
}

// Immutable type representing subset of a schema, requires the
// 'src' schema is valid for its lifetime. Also contains the
// 'dst' schema row size, its selected column offsets and indices, and the
// offset to the null bitmap.
//
// This type can be thought of as the abstraction
// which defines the information that is relevant to RowBlock conversion.
struct SchemaSubset {
  SchemaSubset(const Schema& src, const Schema& dst)
    : src_schema_(src),
      src_indices_(GatherSrcIndices(src, dst)),
      dst_indices_(GatherDstIndices(dst, dst)),
      offsets_(GatherOffsets(src, dst)),
      offset_to_null_bitmap_(dst.byte_size()),
      row_stride_(ContiguousRowHelper::row_size(dst)) {}
  const Schema& src_schema_;
  const vector<size_t> src_indices_;
  const vector<size_t> dst_indices_;
  const vector<size_t> offsets_;
  const size_t offset_to_null_bitmap_;
  const size_t row_stride_;
};

// Generates a schema-to-schema rowblock-to-rowwise-rowblock-pb conversion function
// of the form:
// void(RowBlock* %rb, i8* %dst_base, faststring* %indirect).
// Where 'dst_base' is a pointer to the (already-resized) rowblock buffer and
// and 'indirect' is its indirect data store.
Function* MakeConversionFunction(ModuleBuilder* mbuilder, const SchemaSubset& subset) {
  ModuleBuilder::LLVMBuilder* builder = mbuilder->builder();
  LLVMContext& context = builder->getContext();

  vector<Type*> argtypes = list_of<Type*>
    (PointerType::getUnqual(mbuilder->GetType("class.kudu::RowBlock")))
    (Type::getInt8PtrTy(context))
    (PointerType::getUnqual(mbuilder->GetType("class.kudu::faststring")));
  FunctionType* fty =
    FunctionType::get(Type::getVoidTy(context), argtypes, false);
  Function* f = mbuilder->Create(fty, "RowBlockToRowwisePBConverter");

  Function::arg_iterator it = f->arg_begin();
  Argument* rb = &*it++;
  Argument* dst_base = &*it++;
  Argument* indirect = &*it++;
  DCHECK(it == f->arg_end());

  rb->setName("rb");
  dst_base->setName("dst_base");
  indirect->setName("indirect");

  // Set arguments to not alias to reduce redundant loads.
  // Note 1-indexing.
  // TODO this doesn't give us noalias for the RowBlock's column
  // pointers themselves. We need an alias pass for this.
  f->setDoesNotAlias(1);
  f->setDoesNotAlias(2);
  f->setDoesNotAlias(3);

  // define void @SerializeRowBlock2(
  //           %"class.kudu::RowBlock"* %block,
  //           i8* %dst_base,
  //           %"class.kudu::faststring"* %indirect,
  //           i32* %src_col_indexes,
  //           i32* %proj_col_sizes,
  //           i8* %proj_col_nullable,
  //           i8* %proj_cols_string,
  //           i32 %num_proj_cols,
  //           i32 %row_stride,
  //           i32 %offset_to_null_bitmap) #1 {
  //   ret void
  builder->SetInsertPoint(BasicBlock::Create(context, "entry", f));

  // This is a large method, and its inlining needs to be forced.
  Function* precomp_function =
    mbuilder->GetFunction("_SerializeRowBlock2");
  precomp_function->addFnAttr(llvm::Attribute::AlwaysInline);

  // Make the constant arrays describing the columns.
  vector<Constant*> src_col_indexes;
  vector<Constant*> proj_col_sizes;
  vector<Constant*> proj_col_nullable;
  vector<Constant*> proj_cols_string;
  int num_cols = subset.src_indices_.size();
  for (int i = 0; i < num_cols; i++) {
    int src_idx = subset.src_indices_[i];
    const ColumnSchema& col = subset.src_schema_.column(src_idx);

    src_col_indexes.push_back(builder->getInt32(src_idx));
    proj_col_sizes.push_back(builder->getInt32(col.type_info()->size()));
    proj_col_nullable.push_back(builder->getInt8(col.is_nullable()));
    proj_cols_string.push_back(builder->getInt8(col.type_info()->physical_type() == BINARY));
  }

  ArrayType* int32_array = ArrayType::get(IntegerType::get(context, 32), num_cols);
  ArrayType* int8_array = ArrayType::get(IntegerType::get(context, 8), num_cols);

  // src_col_indexes
  Value* src_col_indexes_ptr = builder->CreateAlloca(IntegerType::get(context, 32),
                                                     builder->getInt32(num_cols));
  Value* proj_col_sizes_ptr = builder->CreateAlloca(IntegerType::get(context, 32),
                                                    builder->getInt32(num_cols));
  Value* proj_col_nullable_ptr = builder->CreateAlloca(IntegerType::get(context, 8),
                                                    builder->getInt32(num_cols));
  Value* proj_cols_string_ptr = builder->CreateAlloca(IntegerType::get(context, 8),
                                                    builder->getInt32(num_cols));

  for (int i = 0; i < num_cols; i++) {
    builder->CreateStore(src_col_indexes[i], builder->CreateConstGEP1_32(src_col_indexes_ptr, i));
    builder->CreateStore(proj_col_sizes[i], builder->CreateConstGEP1_32(proj_col_sizes_ptr, i));
    builder->CreateStore(proj_col_nullable[i], builder->CreateConstGEP1_32(proj_col_nullable_ptr, i));
    builder->CreateStore(proj_cols_string[i], builder->CreateConstGEP1_32(proj_cols_string_ptr, i));
  }


  vector<Value*> args;
  args.push_back(rb);
  args.push_back(dst_base);
  args.push_back(indirect);
  args.push_back(src_col_indexes_ptr);
  args.push_back(proj_col_sizes_ptr);
  args.push_back(proj_col_nullable_ptr);
  args.push_back(proj_cols_string_ptr);

  args.push_back(builder->getInt32(num_cols));
  args.push_back(builder->getInt32(subset.row_stride_));
  args.push_back(builder->getInt32(subset.offset_to_null_bitmap_));
  builder->CreateCall(precomp_function, args);
  builder->CreateRetVoid();

  if (FLAGS_codegen_dump_functions) {
    LOG(INFO) << "Dumping ConvertRowBlockToPB IR:";
    f->dump();
  }

  return f;
}

} // anonymous namespace

Status RBCF::Create(const Schema& src_schema, const Schema& dst_schema,
                    scoped_refptr<RBCF>* out, llvm::TargetMachine** tm) {
  ModuleBuilder builder;
  RETURN_NOT_OK(builder.Init());

  SchemaSubset subset(src_schema, dst_schema);

  Function* converter = MakeConversionFunction(&builder, subset);

  ConversionFunction converter_f;
  builder.AddJITPromise(converter, &converter_f);

  gscoped_ptr<JITCodeOwner> owner;
  RETURN_NOT_OK(builder.Compile(&owner));

  if (tm) {
    *tm = builder.GetTargetMachine();
  }
  out->reset(new RowBlockConverterFunction(src_schema, dst_schema, converter_f,
                                           owner.Pass()));
  return Status::OK();
}

RBCF::RowBlockConverterFunction(const Schema& src_schema,
                                const Schema& dst_schema,
                                ConversionFunction converter,
                                gscoped_ptr<JITCodeOwner> owner)
  : JITWrapper(owner.Pass()),
    src_schema_(src_schema),
    dst_schema_(dst_schema),
    converter_(converter) {
  CHECK(converter != NULL)
    << "Promise to compile RowBlock to rowwise PB not fulfilled by ModuleBuilder";
}

namespace {

template<class T>
bool ContainerEquals(const T& t1, const T& t2) {
  if (t1.size() != t2.size()) return false;
  if (!std::equal(t1.begin(), t1.end(), t2.begin())) return false;
  return true;
}

} // anonymous namespace

// In order for two pairs of schemas to be compatible, we require that the
// subsets of selected columns, the indices of the selected columns,
// destination schema offsets from the null bitmap, and the row strides
// of the destination columns are the same.
//
// The set of projected columns only needs to be equal in the sense
// that the types are the same.
Status RBCF::IsCompatible(const Schema& src_schema, const Schema& dst_schema) {
  SchemaSubset subset1(src_schema_, dst_schema_);
  SchemaSubset subset2(src_schema, dst_schema);
  if (subset1.row_stride_ != subset2.row_stride_) {
    return Status::IllegalState(
      Substitute("Row strides $0 and $1 for destination schemas don't match.",
                 subset1.row_stride_, subset2.row_stride_));
  }
  if (subset1.offset_to_null_bitmap_ != subset2.offset_to_null_bitmap_) {
    return Status::IllegalState(
      Substitute("Offsets to null bitmaps for destination schemas don't match"
                 " ($0 and $1)",
                 subset1.offset_to_null_bitmap_,
                 subset2.offset_to_null_bitmap_));
  }
  if (!ContainerEquals(subset1.src_indices_, subset2.src_indices_)) {
    return Status::IllegalState("The selected src_schema column indices to"
                                " convert do not match");
  }
  if (!ContainerEquals(subset1.dst_indices_, subset2.dst_indices_)) {
    return Status::IllegalState("The selected dst_schema column indices "
                                "convert do not match");
  }
  if (!ContainerEquals(subset1.offsets_, subset2.offsets_)) {
    return Status::IllegalState("Destination schema column offsets are unequal");
  }
  BOOST_FOREACH(size_t idx, subset1.src_indices_) {
    const ColumnSchema& cs1 = subset1.src_schema_.column(idx);
    const ColumnSchema& cs2 = subset2.src_schema_.column(idx);
    if (!cs1.EqualsType(cs2)) {
      return Status::IllegalState(
        Substitute("Columns at idx $0 for src_schema do not match", idx));
    }
  }
  return Status::OK();
}

namespace {
template<class T>
void AddNext(faststring* fs, const T& t) {
  fs->append(&t, sizeof(T));
}
} // anonymous namespace

// Defines key encoding that respects the equivalence relation defined by
// IsCompatible(). To do so, we encode:
//
// (1 byte) Unique type identifier for RBCF
// (8 bytes) Destination null bitmap offset
// (8 bytes) Destination row size
// (13 bytes each) Source schema column info
//   8 bytes for its index
//   4 bytes for the type of every selected column.
//   1 byte for the column's nullability.
// (8 bytes each) Destination schema column indices
// (8 bytes each) Destination schema column offsets
Status RBCF::EncodeKey(const Schema& src_schema, const Schema& dst_schema,
                       faststring* out) {
  SchemaSubset subset(src_schema, dst_schema);

  AddNext(out, ROWBLOCK_CONVERTER);
  AddNext(out, subset.offset_to_null_bitmap_);
  AddNext(out, subset.row_stride_);
  BOOST_FOREACH(size_t idx, subset.src_indices_) {
    AddNext(out, idx);
    AddNext(out, src_schema.column(idx).type_info()->type());
    AddNext(out, src_schema.column(idx).is_nullable());
  }
  BOOST_FOREACH(size_t idx, subset.dst_indices_) {
    AddNext(out, idx);
  }
  BOOST_FOREACH(size_t offset, subset.offsets_) {
    AddNext(out, offset);
  }

  return Status::OK();
}

Status RowBlockConverter::Init() {
#ifndef NDEBUG
  RETURN_NOT_OK(function_->IsCompatible(*src_schema_, *dst_schema_));
#endif
  return Status::OK();
}

} // namespace codegen
} // namespace kudu
