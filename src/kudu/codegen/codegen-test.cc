// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <algorithm>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>

#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/compilation_manager.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/codegen/rowblock_converter.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

DECLARE_bool(codegen_dump_mc);
DECLARE_int32(codegen_cache_capacity);

namespace kudu {

typedef RowProjector NoCodegenRP;
typedef codegen::RowProjector CodegenRP;

using codegen::CompilationManager;
using codegen::RowBlockConverterFunction;
using codegen::RowBlockConverter;

class CodegenTest : public KuduTest {
 public:
  CodegenTest()
    : random_(SeedRandom()),
      // Set the arena size as small as possible to catch errors during relocation,
      // for its initial size and its eventual max size.
      projections_arena_(16, kIndirectPerProjection * 2) {
    // Create the base schema.
    vector<ColumnSchema> cols = { ColumnSchema("key           ", UINT64, false),
                                  ColumnSchema("int32         ",  INT32, false),
                                  ColumnSchema("int32-null-val",  INT32,  true),
                                  ColumnSchema("int32-null    ",  INT32,  true),
                                  ColumnSchema("str32         ", STRING, false),
                                  ColumnSchema("str32-null-val", STRING,  true),
                                  ColumnSchema("str32-null    ", STRING,  true) };
    base_.Reset(cols, 1);
    base_ = SchemaBuilder(base_).Build(); // add IDs

    // Create an extended default schema
    cols.push_back(ColumnSchema("int32-R ",  INT32, false, kI32R,  nullptr));
    cols.push_back(ColumnSchema("int32-RW",  INT32, false, kI32R, kI32W));
    cols.push_back(ColumnSchema("str32-R ", STRING, false, kStrR,  nullptr));
    cols.push_back(ColumnSchema("str32-RW", STRING, false, kStrR, kStrW));
    defaults_.Reset(cols, 1);
    defaults_ = SchemaBuilder(defaults_).Build(); // add IDs

    test_rows_arena_.reset(new Arena(2 * 1024, 1024 * 1024));
    RowBuilder rb(base_);
    for (int i = 0; i < kNumTestRows; ++i) {
      rb.AddUint64(i);
      rb.AddInt32(random_.Next32());
      rb.AddInt32(random_.Next32());
      rb.AddNull();
      AddRandomString(&rb);
      AddRandomString(&rb);
      rb.AddNull();

      void* arena_data = test_rows_arena_->AllocateBytes(
        ContiguousRowHelper::row_size(base_));
      ContiguousRow dst(&base_, static_cast<uint8_t*>(arena_data));
      CHECK_OK(CopyRow(rb.row(), &dst, test_rows_arena_.get()));
      test_rows_rowwise_[i].reset(new ConstContiguousRow(dst));
      rb.Reset();
    }
    test_rows_columnwise_.reset(new RowBlock(base_, kNumTestRows,
                                             test_rows_arena_.get()));
    test_rows_columnwise_->selection_vector()->SetAllTrue();
    NoCodegenRP rp(&base_, &base_);
    CHECK_OK(rp.Init());
    ProjectTestRows<true>(&rp, test_rows_columnwise_.get(), test_rows_arena_.get());
  }

 protected:
  Schema base_;
  Schema defaults_;

  // Compares the projection-for-read and projection-for-write results
  // of the codegen projection and the non-codegen projection
  template<bool READ>
  void TestProjection(const Schema* proj);
  // Generates a new row projector for the given projection schema.
  Status Generate(const Schema* proj, gscoped_ptr<CodegenRP>* out);

  void TestRoundtripRBToPB(const Schema& dst_schema);
  Status GenerateRBConverter(const Schema* dst_schema,
                             gscoped_ptr<RowBlockConverter>* out);

  Schema EmptyProjection() const {
    return SubschemaProjection({});
  }
  Schema IntProjection() const {
    return SubschemaProjection({ kI32Col, kI32NullValCol, kI32NullCol });
  }
  Schema StringProjection() const {
    return SubschemaProjection(
        { kStrCol, kStrNullValCol, kStrNullCol });
  }
  Schema NonNullablesProjection() const {
    return SubschemaProjection(
        { kKeyCol, kI32Col, kStrCol });
  }
  Schema NullablesProjection() const {
    return SubschemaProjection(
        { kI32NullValCol, kI32NullCol, kStrNullValCol, kStrNullCol });
  }

  Schema SubschemaProjection(const vector<int>& part_cols) const {
    Schema ret;
    CHECK_OK(CreatePartialSchema(part_cols, &ret));
    return ret;
  }

  enum {
    // Base schema column indices
    kKeyCol,
    kI32Col,
    kI32NullValCol,
    kI32NullCol,
    kStrCol,
    kStrNullValCol,
    kStrNullCol,
    // Extended default projection schema column indices
    kI32RCol,
    kI32RWCol,
    kStrRCol,
    kStrRWCol
  };

  Status CreatePartialSchema(const vector<int>& col_indexes,
                             Schema* out) const;

 private:
  // Projects the test rows into parameter rowblock using the projector and
  // parameter arena (which is not reset in the method).
  template<bool READ, class RowProjectorType>
  void ProjectTestRows(RowProjectorType* rp, RowBlock* rb,
                       Arena* arena);
  void AddRandomString(RowBuilder* rb);

  static const int kRandomStringMaxLength = 32;
  static const int kNumTestRows = 10;
  static const size_t kIndirectPerRow = 4 * kRandomStringMaxLength;
  static const size_t kIndirectPerProjection = kIndirectPerRow * kNumTestRows;
  typedef const void* DefaultValueType;
  static const DefaultValueType kI32R, kI32W, kStrR, kStrW;

  codegen::CodeGenerator generator_;
  Random random_;
  gscoped_ptr<ConstContiguousRow> test_rows_rowwise_[kNumTestRows];
  gscoped_ptr<RowBlock> test_rows_columnwise_;
  Arena projections_arena_;
  gscoped_ptr<Arena> test_rows_arena_;
};

namespace {

const int32_t kI32RValue = 0xFFFF0000;
const int32_t kI32WValue = 0x0000FFFF;
const   Slice kStrRValue = "RRRRR STRING DEFAULT READ";
const   Slice kStrWValue = "WWWWW STRING DEFAULT WRITE";

// Assumes all rows are selected
// Also assumes schemas are the same.
void CheckRowBlocksEqual(const RowBlock* rb1, const RowBlock* rb2,
                         const string& name1, const string& name2) {
  CHECK_EQ(rb1->nrows(), rb2->nrows());
  const Schema& schema = rb1->schema();
  for (int i = 0; i < rb1->nrows(); ++i) {
    RowBlockRow row1 = rb1->row(i);
    RowBlockRow row2 = rb2->row(i);
    CHECK_EQ(schema.Compare(row1, row2), 0)
      << "Rows unequal (failed at row " << i << "):\n"
      << "\t(" << name1 << ") = " << schema.DebugRow(row1) << "\n"
      << "\t(" << name2 << ") = " << schema.DebugRow(row2);
  }
}

} // anonymous namespace

const CodegenTest::DefaultValueType CodegenTest::kI32R = &kI32RValue;
const CodegenTest::DefaultValueType CodegenTest::kI32W = &kI32WValue;
const CodegenTest::DefaultValueType CodegenTest::kStrR = &kStrRValue;
const CodegenTest::DefaultValueType CodegenTest::kStrW = &kStrWValue;

void CodegenTest::AddRandomString(RowBuilder* rb) {
  static char buf[kRandomStringMaxLength];
  int size = random_.Uniform(kRandomStringMaxLength);
  RandomString(buf, size, &random_);
  rb->AddString(Slice(buf, size));
}

template<bool READ, class RowProjectorType>
void CodegenTest::ProjectTestRows(RowProjectorType* rp, RowBlock* rb,
                                  Arena* arena) {
  // Even though we can test two rows at a time, without using up the
  // extra memory for keeping an entire row block around, this tests
  // what the actual use case will be.
  for (int i = 0; i < kNumTestRows; ++i) {
    ConstContiguousRow src = *test_rows_rowwise_[i];
    RowBlockRow dst = rb->row(i);
    if (READ) {
      CHECK_OK(rp->ProjectRowForRead(src, &dst, arena));
    } else {
      CHECK_OK(rp->ProjectRowForWrite(src, &dst, arena));
    }
  }
}

template<bool READ>
void CodegenTest::TestProjection(const Schema* proj) {
  gscoped_ptr<CodegenRP> with;
  ASSERT_OK(Generate(proj, &with));
  NoCodegenRP without(&base_, proj);
  ASSERT_OK(without.Init());

  CHECK_EQ(with->base_schema(), &base_);
  CHECK_EQ(with->projection(), proj);

  RowBlock rb_with(*proj, kNumTestRows, &projections_arena_);
  RowBlock rb_without(*proj, kNumTestRows, &projections_arena_);

  projections_arena_.Reset();
  ProjectTestRows<READ>(with.get(), &rb_with, &projections_arena_);
  ProjectTestRows<READ>(&without, &rb_without, &projections_arena_);
  CheckRowBlocksEqual(&rb_with, &rb_without, "Codegen", "Expected");
}

Status CodegenTest::Generate(const Schema* proj, gscoped_ptr<CodegenRP>* out) {
  scoped_refptr<codegen::RowProjectorFunctions> functions;
  RETURN_NOT_OK(generator_.CompileRowProjector(base_, *proj, &functions));
  out->reset(new CodegenRP(&base_, proj, functions));
  return Status::OK();
}

Status CodegenTest::CreatePartialSchema(const vector<int>& col_indexes,
                                        Schema* out) const {
  vector<ColumnId> col_ids;
  for (auto col_idx : col_indexes) {
    col_ids.push_back(defaults_.column_id(col_idx));
  }
  return defaults_.CreateProjectionByIdsIgnoreMissing(col_ids, out);
}

TEST_F(CodegenTest, TestProjectObservables) {
  // Test when not identity
  Schema proj = base_.CreateKeyProjection();
  gscoped_ptr<CodegenRP> with;
  CHECK_OK(Generate(&proj, &with));
  NoCodegenRP without(&base_, &proj);
  ASSERT_OK(without.Init());
  ASSERT_EQ(with->base_schema(), without.base_schema());
  ASSERT_EQ(with->projection(), without.projection());
  ASSERT_EQ(with->is_identity(), without.is_identity());
  ASSERT_FALSE(with->is_identity());

  // Test when identity
  Schema iproj = *&base_;
  gscoped_ptr<CodegenRP> iwith;
  CHECK_OK(Generate(&iproj, &iwith))
  NoCodegenRP iwithout(&base_, &iproj);
  ASSERT_OK(iwithout.Init());
  ASSERT_EQ(iwith->base_schema(), iwithout.base_schema());
  ASSERT_EQ(iwith->projection(), iwithout.projection());
  ASSERT_EQ(iwith->is_identity(), iwithout.is_identity());
  ASSERT_TRUE(iwith->is_identity());
}

TEST_F(CodegenTest, TestProjectEmpty) {
  Schema empty = EmptyProjection();
  TestProjection<true>(&empty);
  TestProjection<false>(&empty);
}

// Test key projection
TEST_F(CodegenTest, TestProjectKey) {
  Schema key = base_.CreateKeyProjection();
  TestProjection<true>(&key);
  TestProjection<false>(&key);
}

// Test int projection
TEST_F(CodegenTest, TestProjectInts) {
  Schema ints = IntProjection();
  TestProjection<true>(&ints);
  TestProjection<false>(&ints);
}

// Test string projection
TEST_F(CodegenTest, TestProjectStrings) {
  Schema strs = StringProjection();
  TestProjection<true>(&strs);
  TestProjection<false>(&strs);
}

// Tests the projection of every non-nullable column
TEST_F(CodegenTest, TestProjectNonNullables) {
  Schema non_null = NonNullablesProjection();
  TestProjection<true>(&non_null);
  TestProjection<false>(&non_null);
}

// Tests the projection of every nullable column
TEST_F(CodegenTest, TestProjectNullables) {
  Schema nullables = NullablesProjection();
  TestProjection<true>(&nullables);
  TestProjection<false>(&nullables);
}

// Test full schema projection
TEST_F(CodegenTest, TestProjectFullSchema) {
  TestProjection<true>(&base_);
  TestProjection<false>(&base_);
}

// Tests just the default projection
TEST_F(CodegenTest, TestProjectDefaultsOnly) {
  Schema pure_defaults;

  // Default read projections
  vector<int> part_cols = { kI32RCol, kI32RWCol, kStrRCol, kStrRWCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &pure_defaults));

  TestProjection<true>(&pure_defaults);

  // Default write projections
  part_cols = { kI32RWCol, kStrRWCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &pure_defaults));

  TestProjection<false>(&pure_defaults);
}

// Test full defaults projection
TEST_F(CodegenTest, TestProjectFullSchemaWithDefaults) {
  TestProjection<true>(&defaults_);

  // Default write projection
  Schema full_write;
  vector<int> part_cols = { kKeyCol,
                            kI32Col,
                            kI32NullValCol,
                            kI32NullCol,
                            kStrCol,
                            kStrNullValCol,
                            kStrNullCol,
                            kI32RWCol,
                            kStrRWCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &full_write));

  TestProjection<false>(&full_write);
}

// Test the codegen_dump_mc flag works properly.
TEST_F(CodegenTest, TestDumpMC) {
  FLAGS_codegen_dump_mc = true;

  StringVectorSink sink;
  ScopedRegisterSink srs(&sink);

  Schema ints;
  vector<int> part_cols = { kI32Col, kI32NullValCol, kI32NullCol, kStrCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &ints));
  TestProjection<true>(&ints);

  const vector<string>& msgs = sink.logged_msgs();
  ASSERT_EQ(msgs.size(), 1);
  EXPECT_THAT(msgs[0], testing::ContainsRegex("retq"));
}

// Basic test for the CompilationManager code cache.
// This runs a bunch of compilation tasks and ensures that the cache
// sometimes hits on the second attempt for the same projection.
TEST_F(CodegenTest, TestCodeCache) {
  Singleton<CompilationManager>::UnsafeReset();
  FLAGS_codegen_cache_capacity = 10;
  CompilationManager* cm = CompilationManager::GetSingleton();

  for (int pass = 0; pass < 2; pass++) {
    int num_hits = 0;

    // Generate all permutations of the first four columns (24 permutations).
    // For each such permutation, we'll create a projection and request code generation.
    vector<int> perm = { 0, 1, 2, 3 };
    do {
      SCOPED_TRACE(perm);
      Schema projection;
      ASSERT_OK(CreatePartialSchema(perm, &projection));

      gscoped_ptr<CodegenRP> projector;
      if (cm->RequestRowProjector(&base_, &projection, &projector)) {
        num_hits++;
      }
      cm->Wait();
    } while (std::next_permutation(perm.begin(), perm.end()));

    if (pass == 0) {
      // On the first pass, the cache should have been empty and gotten 0 hits.
      ASSERT_EQ(0, num_hits);
    } else {
      // Otherwise, we expect to have gotten some hits.
      // If our cache were a perfect LRU implementation, then we would actually
      // expect 0 hits here as well, since we are accessing the entries in
      // exactly the same order as we inserted them, and thus would evict
      // an entry before we look for it again. But, our LRU cache is sharded
      // so we expect to get some hits on the second time.
      ASSERT_GT(num_hits, 0);
      ASSERT_LT(num_hits, 24);
    }
  }
}

Status CodegenTest::GenerateRBConverter(const Schema* dst_schema,
                                        gscoped_ptr<RowBlockConverter>* out) {
  scoped_refptr<RowBlockConverterFunction> function;
  RETURN_NOT_OK(generator_.CompileRowBlockConverter(base_, *dst_schema, &function));
  out->reset(new RowBlockConverter(&base_, dst_schema, function));
  RETURN_NOT_OK((*out)->Init());
  return Status::OK();
}

void CodegenTest::TestRoundtripRBToPB(const Schema& dst_schema) {
  gscoped_ptr<RowBlockConverter> converter;
  CHECK_OK(GenerateRBConverter(&dst_schema, &converter));

  // Convert to PB.
  RowwiseRowBlockPB no_codegen_pb;
  RowwiseRowBlockPB codegen_pb;
  faststring direct, indirect, codegen_direct, codegen_indirect;
  SerializeRowBlock(*test_rows_columnwise_, &no_codegen_pb, &dst_schema,
                    &direct, &indirect);
  converter->ConvertRowBlockToPB(*test_rows_columnwise_, &codegen_pb,
                                 &codegen_direct, &codegen_indirect);

  // Convert back to a row, ensure that the resulting row is the same
  // as the one we put in.
  vector<const uint8_t*> no_codegen_row_ptrs;
  vector<const uint8_t*> codegen_row_ptrs;
  Slice direct_slice = direct, codegen_direct_slice = codegen_direct;
  CHECK_OK(ExtractRowsFromRowBlockPB(dst_schema, no_codegen_pb,
                                     indirect, &direct_slice,
                                     &no_codegen_row_ptrs));
  CHECK_OK(ExtractRowsFromRowBlockPB(dst_schema, codegen_pb,
                                     codegen_indirect, &codegen_direct_slice,
                                     &codegen_row_ptrs));
  CHECK_EQ(codegen_row_ptrs.size(), no_codegen_row_ptrs.size());
  for (int i = 0; i < codegen_row_ptrs.size(); ++i) {
    SCOPED_TRACE(i);
    ConstContiguousRow no_codegen_row(&dst_schema, no_codegen_row_ptrs[i]);
    ConstContiguousRow codegen_row(&dst_schema, codegen_row_ptrs[i]);
    CHECK_EQ(dst_schema.DebugRow(no_codegen_row),
             dst_schema.DebugRow(codegen_row));
  }
}

TEST_F(CodegenTest, TestRowBlockToPBEmpty) {
  TestRoundtripRBToPB(EmptyProjection());
}

TEST_F(CodegenTest, TestRowBlockToPBKey) {
  TestRoundtripRBToPB(base_.CreateKeyProjection());
}

TEST_F(CodegenTest, TestRowBlockToPBInt) {
  TestRoundtripRBToPB(IntProjection());
}

TEST_F(CodegenTest, TestRowBlockToPBString) {
  TestRoundtripRBToPB(StringProjection());
}

TEST_F(CodegenTest, TestRowBlockToPBNonNullables) {
  TestRoundtripRBToPB(NonNullablesProjection());
}

TEST_F(CodegenTest, TestRowBlockToPBNullables) {
  TestRoundtripRBToPB(NullablesProjection());
}

TEST_F(CodegenTest, TestRowBlockToPBFull) {
  TestRoundtripRBToPB(base_);
}

} // namespace kudu
