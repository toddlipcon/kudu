// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_ROWBLOCK_CONVERTER_H
#define KUDU_CODEGEN_ROWBLOCK_CONVERTER_H

#include "kudu/codegen/jit_wrapper.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"

namespace llvm {
class TargetMachine;
} // namespace llvm

namespace kudu {

class faststring;

namespace codegen {

// A RowBlockConverterFunction is an immutable JITWrapper that contains
// the function used for converting a (columnwise) rowblock of a given schema
// to a protobuf (as a rowwise rowblock).
class RowBlockConverterFunction : public JITWrapper {
 public:
  // Compiles the parameter schema's rowblock to pb conversion function and
  // writes to 'out' upon success. 'dst_schema' should contain a subset of
  // 'src_schema' columns, which are mapped to the destination protobuf.
  // 'out' is not initialized until builder->Compile() is called and
  // fulfills its JITPromises.
  // Writes to 'tm' with the target machine used upon success.
  static Status Create(const Schema& src_schema, const Schema& dst_schema,
                       scoped_refptr<RowBlockConverterFunction>* out,
                       llvm::TargetMachine** tm = NULL);

  // Defines the equivalence relation for which RowBlockConverterFunctions
  // have functionally equivalent code. Status::OK() corresponds to true
  // in the relation, and a failing status corresponds to false, its
  // message explaining the reason.
  Status IsCompatible(const Schema& src_schema, const Schema& dst_schema);

  typedef void(*ConversionFunction)(const RowBlock* block,
                                    RowwiseRowBlockPB* pb,
                                    faststring* direct,
                                    faststring* indirect);
  ConversionFunction converter() const { return converter_; }

  virtual Status EncodeOwnKey(faststring* out) OVERRIDE {
    return EncodeKey(src_schema_, dst_schema_, out);
  }

  // Encoding respects IsCompatible() in that equivalent keys imply
  // IsCompatible(). Writes to 'out' upon success.
  static Status EncodeKey(const Schema& src_schema, const Schema& dst_schema,
                          faststring* out);

 private:
  RowBlockConverterFunction(const Schema& src_schema, const Schema& dst_schema,
                            ConversionFunction converter,
                            std::unique_ptr<JITCodeOwner> owner);

  const Schema src_schema_;
  const Schema dst_schema_;
  const ConversionFunction converter_;
};

// Container class for a RowBlockConverterFunction.
class RowBlockConverter {
 public:
  // Generates a RowBlockConverter that maps rowblocks from the src_schema
  // onto dst_schema. Requires that the schema pair (src_schema, dst_schema)
  // are "compatible" with the schema pair used to create 'function'.
  // Requires that 'src_schema' and 'dst_schema' remain valid for lifetime
  // of this instance.
  RowBlockConverter(const Schema* src_schema, const Schema* dst_schema,
                    const scoped_refptr<RowBlockConverterFunction>& function)
    : src_schema_(src_schema),
      dst_schema_(dst_schema),
      row_stride_(ContiguousRowHelper::row_size(*dst_schema_)),
      function_(function) {}

  // Checks for schema compatibility ifndef NDEBUG
  Status Init() WARN_UNUSED_RESULT;

  // Converts a rowblock which is assumed to have the same schema as the one
  // used to construct the ConversionFunction using ConversionFunction::Create
  // above. Writes output to 'pb' and appends to 'row_data' and 'indirect_data'
  // as appropriate.
  // Requires that block.nrows() > 0
  void ConvertRowBlockToPB(const RowBlock& block, RowwiseRowBlockPB* pb,
                           faststring* row_data, faststring* indirect_data) {
    DCHECK_SCHEMA_EQ(block.schema(), *src_schema_);
    DCHECK_GT(block.nrows(), 0);
    function_->converter()(&block, pb, row_data, indirect_data);
  }

 private:
  const Schema* const src_schema_;
  const Schema* const dst_schema_;
  const size_t row_stride_;
  const scoped_refptr<RowBlockConverterFunction> function_;

  DISALLOW_COPY_AND_ASSIGN(RowBlockConverter);
};

} // namespace codegen
} // namespace kudu

#endif
