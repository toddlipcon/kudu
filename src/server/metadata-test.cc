// Copyright (c) 2014, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "common/schema.h"
#include "server/metadata.h"
#include "gutil/strings/join.h"
#include "util/status.h"
#include "util/test_util.h"

using boost::assign::list_of;
using std::vector;
using std::string;

namespace kudu {
namespace metadata {

class MetadataTest : public KuduTest {
 public:
  MetadataTest() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("key", STRING));
    CHECK_OK(builder.AddColumn("val", UINT32));
    schema_ = builder.Build();

    CHECK_OK(RowSetMetadata::CreateNew(NULL, 0, schema_, &meta_));
    CHECK_OK(meta_->CommitRedoDeltaDataBlock(1, BlockId("delta_001")));
    CHECK_OK(meta_->CommitRedoDeltaDataBlock(2, BlockId("delta_002")));
    CHECK_OK(meta_->CommitRedoDeltaDataBlock(3, BlockId("delta_003")));
    CHECK_OK(meta_->CommitRedoDeltaDataBlock(4, BlockId("delta_004")));
    CHECK_EQ(4, meta_->redo_delta_blocks().size());
  }

 protected:
  gscoped_ptr<RowSetMetadata> meta_;
  Schema schema_;
};

// Swap out some deltas from the middle of the list
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_1) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_002"));
  to_replace.push_back(BlockId("delta_003"));

  ASSERT_OK(meta_->CommitUpdate(
              RowSetMetadataUpdate()
              .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block")))));
  EXPECT_EQ("delta_001,new_block,delta_004",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
}

// Swap out some deltas from the beginning of the list
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_2) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_001"));
  to_replace.push_back(BlockId("delta_002"));

  ASSERT_OK(meta_->CommitUpdate(
              RowSetMetadataUpdate()
              .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block")))));
  EXPECT_EQ("new_block,delta_003,delta_004",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
}

// Swap out some deltas from the end of the list
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_3) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_003"));
  to_replace.push_back(BlockId("delta_004"));

  ASSERT_OK(meta_->CommitUpdate(
              RowSetMetadataUpdate()
              .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block")))));
  ASSERT_EQ("delta_001,delta_002,new_block",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
}

// Swap out a non-contiguous list, check error.
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_Bad_NonContiguous) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_002"));
  to_replace.push_back(BlockId("delta_004"));

  Status s = meta_->CommitUpdate(
    RowSetMetadataUpdate()
    .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block"))));
  EXPECT_EQ("Invalid argument: Cannot find subsequence <delta_002,delta_004> "
            "in <delta_001,delta_002,delta_003,delta_004>",
            s.ToString());

  // Should be unchanged
  EXPECT_EQ("delta_001,delta_002,delta_003,delta_004",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
}

// Swap out a list which contains an invalid element, check error.
TEST_F(MetadataTest, RSMD_TestReplaceDeltas_Bad_DoesntExist) {
  vector<BlockId> to_replace;
  to_replace.push_back(BlockId("delta_noexist"));

  Status s = meta_->CommitUpdate(
    RowSetMetadataUpdate()
    .ReplaceRedoDeltaBlocks(to_replace, list_of(BlockId("new_block"))));
  EXPECT_EQ("Invalid argument: Cannot find subsequence <delta_noexist> "
            "in <delta_001,delta_002,delta_003,delta_004>",
            s.ToString());

  // Should be unchanged
  ASSERT_EQ("delta_001,delta_002,delta_003,delta_004",
            BlockId::JoinStrings(meta_->redo_delta_blocks()));
}

} // namespace metadata
} // namespace kudu
