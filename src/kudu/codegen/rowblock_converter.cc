// Copyright 2014 Cloudera inc.

#include "kudu/codegen/rowblock_converter.h"

#include <iostream>
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
#include <llvm/IR/Verifier.h>
#include <llvm/Support/raw_os_ostream.h>

#include "kudu/codegen/codegen_params_generated.h"
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
using llvm::BasicBlock;
using llvm::ExecutionEngine;
using llvm::Function;
using llvm::FunctionType;
using llvm::LLVMContext;
using llvm::PointerType;
using llvm::Type;
using llvm::Value;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(codegen_dump_functions);

namespace kudu {

namespace codegen {

typedef RowBlockConverterFunction RBCF;

namespace {

vector<fbs::CellInfo> GetCellInfoFBs(const Schema& schema) {
  vector<fbs::CellInfo> ret;
  for (int i = 0; i < schema.num_columns(); i++) {
    const auto& col = schema.column(i);
    ret.emplace_back(
        col.type_info()->physical_type(),
        schema.column_offset(i),
        col.type_info()->size(),
        col.is_nullable(),
        reinterpret_cast<uint64_t>(col.read_default_value()),
        reinterpret_cast<uint64_t>(col.write_default_value()));
  }
  return ret;
}


void AppendFlatBuf(const Schema& src, const Schema& dst,
                   faststring* buf) {
  flatbuffers::FlatBufferBuilder fb_builder;

  vector<uint32_t> dst_to_src;
  for (int src_idx = 0; src_idx < src.num_columns(); src_idx++) {
    int dst_idx = dst.find_column(src.column(src_idx).name());
    if (dst_idx != -1) {
      dst_to_src.push_back(src_idx);
    }
  }
  CHECK_EQ(dst_to_src.size(), dst.num_columns());
  auto dst_to_src_fb = fb_builder.CreateVector(dst_to_src);
  auto dst_cols = GetCellInfoFBs(dst);
  auto dst_cols_fb = fb_builder.CreateVectorOfStructs(dst_cols);

  fbs::SerializeRowBlockParamBuilder param(fb_builder);
  param.add_dst_to_src_idx_mapping(dst_to_src_fb);
  param.add_dst_cols(dst_cols_fb);
  param.add_dst_row_stride(ContiguousRowHelper::row_size(dst));
  param.add_offset_to_null_bitmap(dst.byte_size());

  fb_builder.Finish(param.Finish());

  buf->append(fb_builder.GetBufferPointer(), fb_builder.GetSize());
}


// Generates a schema-to-schema rowblock-to-rowwise-rowblock-pb conversion function
// of the form:
// void(RowBlock* %rb, RowwiseRowBlockPB* %pb,
//      faststring* %direct_buf, faststring* %indirect_buf).
Function* MakeConversionFunction(ModuleBuilder* mbuilder,
                                 const Schema& src_schema,
                                 const Schema& dst_schema) {
  ModuleBuilder::LLVMBuilder* builder = mbuilder->builder();
  LLVMContext& context = builder->getContext();

  faststring flatbuf;
  AppendFlatBuf(src_schema, dst_schema, &flatbuf);
  auto* fb_val = mbuilder->GetPointerToConstantArray(
      flatbuf.data(), flatbuf.size());
  auto fb_len = builder->getInt64(flatbuf.size());


  vector<Type*> argtypes = {
    PointerType::getUnqual(mbuilder->GetType("class.kudu::RowBlock")),
    PointerType::getUnqual(mbuilder->GetType("class.kudu::RowwiseRowBlockPB")),
    PointerType::getUnqual(mbuilder->GetType("class.kudu::faststring")),
    PointerType::getUnqual(mbuilder->GetType("class.kudu::faststring")) };


  FunctionType* fty =
    FunctionType::get(Type::getVoidTy(context), argtypes, false);
  Function* f = mbuilder->Create(fty, "RowBlockToRowwisePBConverter");

  Function::arg_iterator it = f->arg_begin();
  Argument* rb = &*it++;
  Argument* pb = &*it++;
  Argument* direct_buf = &*it++;
  Argument* indirect_buf = &*it++;
  DCHECK(it == f->arg_end());

  rb->setName("rb");
  pb->setName("pb");
  direct_buf->setName("direct_buf");
  indirect_buf->setName("indirect");

  builder->SetInsertPoint(BasicBlock::Create(context, "entry", f));
  builder->CreateCall(
      mbuilder->GetFunction("_PrecompiledSerializeRowBlock"),
      { rb, pb, direct_buf, indirect_buf, fb_val, fb_len });
  builder->CreateRetVoid();

  if (FLAGS_codegen_dump_functions) {
    LOG(INFO) << "Dumping ConvertRowBlockToPB IR:";
    f->dump();
  }

  llvm::raw_os_ostream os(std::cerr);
  DCHECK(!llvm::verifyFunction(*f, &os));

  return f;
}

} // anonymous namespace

Status RBCF::Create(const Schema& src_schema, const Schema& dst_schema,
                    scoped_refptr<RBCF>* out, llvm::TargetMachine** tm) {
  ModuleBuilder builder;
  RETURN_NOT_OK(builder.Init());

  Function* converter = MakeConversionFunction(&builder, src_schema, dst_schema);

  ConversionFunction converter_f;
  builder.AddJITPromise(converter, &converter_f);

  unique_ptr<JITCodeOwner> owner;
  RETURN_NOT_OK(builder.Compile(&owner));

  if (tm) {
    *tm = builder.GetTargetMachine();
  }
  out->reset(new RowBlockConverterFunction(src_schema, dst_schema, converter_f,
                                           std::move(owner)));
  return Status::OK();
}

RBCF::RowBlockConverterFunction(const Schema& src_schema,
                                const Schema& dst_schema,
                                ConversionFunction converter,
                                unique_ptr<JITCodeOwner> owner)
    : JITWrapper(std::move(owner)),
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
  /*
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

  TODO
  */
  return Status::OK();
}

namespace {
template<class T>
void AddNext(faststring* fs, const T& t) {
  fs->append(&t, sizeof(T));
}
} // anonymous namespace

// Defines key encoding that respects the equivalence relation defined by
// IsCompatible(). To do so, we just use the flatbuf that represents all
// propagated constants.
Status RBCF::EncodeKey(const Schema& src_schema, const Schema& dst_schema,
                       faststring* out) {
  AddNext(out, ROWBLOCK_CONVERTER);
  AppendFlatBuf(src_schema, dst_schema, out);
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
