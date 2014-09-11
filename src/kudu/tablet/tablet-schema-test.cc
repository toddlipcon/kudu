// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/assign/list_of.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using strings::Substitute;

namespace kudu {
namespace tablet {

class TestTabletSchema : public KuduTabletTest {
 public:
  TestTabletSchema()
    : KuduTabletTest(CreateBaseSchema()) {
  }

  void InsertRows(const Schema& schema, size_t first_key, size_t nrows) {
    for (size_t i = first_key; i < nrows; ++i) {
      InsertRow(schema, i);

      // Half of the rows will be on disk
      // and the other half in the MemRowSet
      if (i == (nrows / 2)) {
        ASSERT_STATUS_OK(tablet()->Flush());
      }
    }
  }

  void InsertRow(const Schema& schema, size_t key) {
    LocalTabletWriter writer(tablet().get(), &schema);
    KuduPartialRow row(&schema);
    CHECK_OK(row.SetUInt32(0, key));
    CHECK_OK(row.SetUInt32(1, key));
    ASSERT_STATUS_OK(writer.Insert(row));
  }

  void DeleteRow(const Schema& schema, size_t key) {
    LocalTabletWriter writer(tablet().get(), &schema);
    KuduPartialRow row(&schema);
    CHECK_OK(row.SetUInt32(0, key));
    ASSERT_STATUS_OK(writer.Delete(row));
  }

  void MutateRow(const Schema& schema, size_t key, size_t col_idx, uint32_t new_val) {
    LocalTabletWriter writer(tablet().get(), &schema);
    KuduPartialRow row(&schema);
    CHECK_OK(row.SetUInt32(0, key));
    CHECK_OK(row.SetUInt32(col_idx, new_val));
    ASSERT_STATUS_OK(writer.Update(row));
  }

  void VerifyTabletRows(const Schema& projection,
                        const std::vector<std::pair<string, string> >& keys) {
    typedef std::pair<string, string> StringPair;

    vector<string> rows;
    ASSERT_STATUS_OK(DumpTablet(*tablet(), projection, &rows));
    BOOST_FOREACH(const string& row, rows) {
      bool found = false;
      BOOST_FOREACH(const StringPair& k, keys) {
        if (row.find(k.first) != string::npos) {
          ASSERT_STR_CONTAINS(row, k.second);
          found = true;
          break;
        }
      }
      ASSERT_TRUE(found);
    }
  }

 private:
  friend class MutateAfterSwap;
  Schema CreateBaseSchema() {
    return Schema(boost::assign::list_of
                   (ColumnSchema("key", UINT32))
                   (ColumnSchema("c1", UINT32)),
                   1);
  }
};

// Read from a tablet using a projection schema with columns not present in
// the original schema. Verify that the server reject the request.
TEST_F(TestTabletSchema, TestRead) {
  const size_t kNumRows = 10;
  Schema projection(boost::assign::list_of
                    (ColumnSchema("key", UINT32))
                    (ColumnSchema("c2", UINT64))
                    (ColumnSchema("c3", STRING)),
                    1);

  InsertRows(client_schema_, 0, kNumRows);

  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(tablet()->NewRowIterator(projection, &iter));

  Status s = iter->Init(NULL);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.message().ToString(),
                      "Some columns are not present in the current schema: c2, c3");
}

// Write to the tablet using different schemas,
// and verifies that the read and write defauls are respected.
TEST_F(TestTabletSchema, TestWrite) {
  const size_t kNumBaseRows = 10;

  // Insert some rows with the base schema
  InsertRows(client_schema_, 0, kNumBaseRows);

  // Add one column with a default value
  const uint32_t c2_write_default = 5;
  const uint32_t c2_read_default = 7;

  SchemaBuilder builder(tablet()->metadata()->schema());
  ASSERT_STATUS_OK(builder.AddColumn("c2", UINT32, false, &c2_read_default, &c2_write_default));
  AlterSchema(builder.Build());
  Schema s2 = builder.BuildWithoutIds();

  // Insert with base/old schema
  size_t s2Key = kNumBaseRows + 1;
  InsertRow(client_schema_, s2Key);

  // Verify the default value
  std::vector<std::pair<string, string> > keys;
  keys.push_back(std::pair<string, string>(Substitute("key=$0", s2Key),
                                           Substitute("c2=$0", c2_write_default)));
  keys.push_back(std::pair<string, string>("", Substitute("c2=$0", c2_read_default)));
  VerifyTabletRows(s2, keys);

  // Delete the row
  DeleteRow(s2, s2Key);

  // Verify the default value
  VerifyTabletRows(s2, keys);

  // Re-Insert with base/old schema
  InsertRow(client_schema_, s2Key);
  VerifyTabletRows(s2, keys);

  // Try compact all (different schemas)
  ASSERT_STATUS_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  VerifyTabletRows(s2, keys);
}

// Verify that the RowChangeList projection works for reinsert mutation
TEST_F(TestTabletSchema, TestReInsert) {
  // Insert some rows with the base schema
  size_t s1Key = 0;
  InsertRow(client_schema_, s1Key);
  DeleteRow(client_schema_, s1Key);
  InsertRow(client_schema_, s1Key);

  // Add one column with a default value
  const uint32_t c2_write_default = 5;
  const uint32_t c2_read_default = 7;

  SchemaBuilder builder(tablet()->metadata()->schema());
  ASSERT_STATUS_OK(builder.AddColumn("c2", UINT32, false, &c2_read_default, &c2_write_default));
  AlterSchema(builder.Build());
  Schema s2 = builder.BuildWithoutIds();

  // Insert with base/old schema
  size_t s2Key = 1;
  InsertRow(client_schema_, s2Key);

  // Verify the default value
  std::vector<std::pair<string, string> > keys;
  keys.push_back(std::pair<string, string>(Substitute("key=$0", s1Key),
                                           Substitute("c2=$0", c2_read_default)));
  keys.push_back(std::pair<string, string>(Substitute("key=$0", s2Key),
                                           Substitute("c2=$0", c2_write_default)));
  VerifyTabletRows(s2, keys);

  // Try compact all (different schemas)
  ASSERT_STATUS_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  VerifyTabletRows(s2, keys);
}

// Write to the table using a projection schema with a renamed field.
TEST_F(TestTabletSchema, TestRenameProjection) {
  std::vector<std::pair<string, string> > keys;

  // Insert with the base schema
  InsertRow(client_schema_, 1);

  // Switch schema to s2
  SchemaBuilder builder(tablet()->metadata()->schema());
  ASSERT_STATUS_OK(builder.RenameColumn("c1", "c1_renamed"));
  AlterSchema(builder.Build());
  Schema s2 = builder.BuildWithoutIds();

  // Insert with the s2 schema after AlterSchema(s2)
  InsertRow(s2, 2);

  // Read and verify using the s2 schema
  keys.clear();
  for (int i = 1; i <= 4; ++i) {
    keys.push_back(std::pair<string, string>(Substitute("key=$0", i),
                                             Substitute("c1_renamed=$0", i)));
  }
  VerifyTabletRows(s2, keys);

  // Delete the first two rows
  DeleteRow(s2, /* key= */ 1);

  // Alter the remaining row
  MutateRow(s2,      /* key= */ 2, /* col_idx= */ 1, /* new_val= */ 6);

  // Read and verify using the s2 schema
  keys.clear();
  keys.push_back(std::pair<string, string>("key=2", "c1_renamed=6"));
  VerifyTabletRows(s2, keys);
}

// Verify that removing a column and re-adding it will not result in making old data visible
TEST_F(TestTabletSchema, TestDeleteAndReAddColumn) {
  std::vector<std::pair<string, string> > keys;

  // Insert and Mutate with the base schema
  InsertRow(client_schema_, 1);
  MutateRow(client_schema_, /* key= */ 1, /* col_idx= */ 1, /* new_val= */ 2);

  keys.clear();
  keys.push_back(std::pair<string, string>("key=1", "c1=2"));
  VerifyTabletRows(client_schema_, keys);

  // Switch schema to s2
  SchemaBuilder builder(tablet()->metadata()->schema());
  ASSERT_STATUS_OK(builder.RemoveColumn("c1"));
  // NOTE this new 'c1' will have a different id from the previous one
  //      so the data added to the previous 'c1' will not be visible.
  ASSERT_STATUS_OK(builder.AddNullableColumn("c1", UINT32));
  AlterSchema(builder.Build());
  Schema s2 = builder.BuildWithoutIds();

  // Verify that the new 'c1' have the default value
  keys.clear();
  keys.push_back(std::pair<string, string>("key=1", "c1=NULL"));
  VerifyTabletRows(s2, keys);
}

// Verify modifying an empty MemRowSet
TEST_F(TestTabletSchema, TestModifyEmptyMemRowSet) {
  std::vector<std::pair<string, string> > keys;

  // Switch schema to s2
  SchemaBuilder builder(tablet()->metadata()->schema());
  ASSERT_STATUS_OK(builder.AddNullableColumn("c2", UINT32));
  AlterSchema(builder.Build());
  Schema s2 = builder.BuildWithoutIds();

  // Verify we can insert some new data.
  // Inserts the row "(2, 2, 2)"
  LocalTabletWriter writer(tablet().get(), &s2);
  KuduPartialRow row(&s2);
  CHECK_OK(row.SetUInt32(0, 2));
  CHECK_OK(row.SetUInt32(1, 2));
  CHECK_OK(row.SetUInt32(2, 2));
  ASSERT_STATUS_OK(writer.Insert(row));

  vector<string> rows;
  ASSERT_STATUS_OK(DumpTablet(*tablet(), s2, &rows));
  EXPECT_EQ("(uint32 key=2, uint32 c1=2, uint32 c2=2)", rows[0]);

  // Update some columns.
  MutateRow(s2, /* key= */ 2, /* col_idx= */ 2, /* new_val= */ 3);
  ASSERT_STATUS_OK(DumpTablet(*tablet(), s2, &rows));
  EXPECT_EQ("(uint32 key=2, uint32 c1=2, uint32 c2=3)", rows[0]);

  MutateRow(s2, /* key= */ 2, /* col_idx= */ 1, /* new_val= */ 4);
  ASSERT_STATUS_OK(DumpTablet(*tablet(), s2, &rows));
  EXPECT_EQ("(uint32 key=2, uint32 c1=4, uint32 c2=3)", rows[0]);
}

class MutateAfterSwap : public Tablet::FlushCompactCommonHooks {
 public:
  explicit MutateAfterSwap(TestTabletSchema* test)
    : test_(test) {
  }

  virtual Status PostWriteSnapshot() {
    test_->MutateRow(test_->client_schema_,
                     /* key= */ 0, /* col_idx= */ 1, /* new_val= */ 1);
    return Status::OK();
  }

  virtual Status PostSwapInDuplicatingRowSet() {
    const uint32_t c2_write_default = 5;
    const uint32_t c2_read_default = 7;
    SchemaBuilder builder(test_->tablet()->metadata()->schema());
    CHECK_OK(builder.AddColumn("c2", UINT32, false, &c2_read_default, &c2_write_default));
    test_->AlterSchema(builder.Build());

    test_->MutateRow(test_->client_schema_,
                     /* key= */ 0, /* col_idx= */ 1, /* new_val= */ 2);

    return Status::OK();
  };

 private:
  TestTabletSchema* test_;
};

TEST_F(TestTabletSchema, TestAlterDuringCompaction) {
  InsertRow(client_schema_, 0);
  ASSERT_STATUS_OK(tablet()->Flush());

  InsertRow(client_schema_, 1);
  ASSERT_STATUS_OK(tablet()->Flush());


  vector<string> rows;
  ASSERT_STATUS_OK(DumpTablet(*tablet(), client_schema_, &rows));
  EXPECT_EQ("(uint32 key=0, uint32 c1=0)", rows[0]);
  EXPECT_EQ("(uint32 key=1, uint32 c1=1)", rows[1]);


  shared_ptr<Tablet::FlushCompactCommonHooks> my_hooks(new MutateAfterSwap(this));
  tablet()->SetFlushCompactCommonHooksForTests(my_hooks);

  LOG(INFO) << "----------------\nCompacting\n------------------";
  ASSERT_STATUS_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Reset hooks.
  tablet()->SetFlushCompactCommonHooksForTests(
    shared_ptr<Tablet::FlushCompactCommonHooks>());

  // Flush another RS so that the Compact() below isn't a no-op.
  InsertRow(client_schema_, 3);
  LOG(INFO) << "----------------\nFlushing a new row\n------------------";
  ASSERT_STATUS_OK(tablet()->Flush());

  // Compact to make sure that all of the data is sorted right.
  // This is a regression test -- previously, we ended up with a case
  // where updates got re-ordered relative to the delta files in which
  // they were contained, which wouldn't fail immediately, but would fail
  // on the next compaction.
  LOG(INFO) << "----------------\nCompacting again\n------------------";
  ASSERT_STATUS_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
}

} // namespace tablet
} // namespace kudu
