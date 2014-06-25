// Copyright (c) 2014, Cloudera, inc.

#include "consensus/log-test-base.h"

#include <vector>

#include "common/iterator.h"
#include "consensus/log_util.h"
#include "tablet/tablet_bootstrap.h"
#include "tablet/tablet-test-util.h"
#include "server/logical_clock.h"

namespace kudu {

namespace log {

extern const char* kTestTable;
extern const char* kTestTablet;

} // namespace log

namespace tablet {

using std::vector;
using std::string;

using consensus::ConsensusBootstrapInfo;
using log::Log;
using log::LogTestBase;
using log::OpIdAnchorRegistry;
using log::ReadableLogSegmentMap;
using log::TabletMasterBlockPB;
using server::Clock;
using server::LogicalClock;

class BootstrapTest : public LogTestBase {
 protected:

  void SetUp() OVERRIDE {
    LogTestBase::SetUp();
    master_block_.set_table_id(log::kTestTable);
    master_block_.set_tablet_id(log::kTestTablet);
    master_block_.set_block_a(fs_manager_->GenerateBlockId().ToString());
    master_block_.set_block_b(fs_manager_->GenerateBlockId().ToString());
  }

  Status BootstrapTestTablet(int mrs_id,
                             int delta_id,
                             shared_ptr<Tablet>* tablet,
                             ConsensusBootstrapInfo* boot_info) {
    metadata::QuorumPB quorum;
    quorum.set_seqno(0);

    scoped_refptr<metadata::TabletMetadata> meta;
    scoped_refptr<OpIdAnchorRegistry> new_anchor_registry;

    RETURN_NOT_OK(metadata::TabletMetadata::LoadOrCreate(fs_manager_.get(),
                                                         master_block_,
                                                         log::kTestTable,
                                                         // We need a schema with ids for
                                                         // TabletMetadata::LoadOrCreate()
                                                         SchemaBuilder(schema_).Build(),
                                                         quorum,
                                                         "",
                                                         "",
                                                         &meta));
    meta->SetLastDurableMrsIdForTests(mrs_id);
    if (meta->GetRowSetForTests(0) != NULL) {
      meta->GetRowSetForTests(0)->SetLastDurableRedoDmsIdForTests(delta_id);
    }
    meta->Flush();

    gscoped_ptr<Log> new_log;
    gscoped_ptr<TabletStatusListener> listener(new TabletStatusListener(meta));

    // Now attempt to recover the log
    RETURN_NOT_OK(BootstrapTablet(
        meta,
        scoped_refptr<Clock>(LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp)),
        NULL,
        listener.get(),
        tablet,
        &new_log,
        &new_anchor_registry,
        boot_info));
    log_.reset(new_log.release());

    return Status::OK();
  }

  void IterateTabletRows(const Tablet* tablet,
                         vector<string>* results) {
    gscoped_ptr<RowwiseIterator> iter;
    // TODO: there seems to be something funny with timestamps in this test.
    // Unless we explicitly scan at a snapshot including all timestamps, we don't
    // see the bootstrapped operation. This is likely due to KUDU-138 -- perhaps
    // we aren't properly setting up the clock after bootstrap.
    MvccSnapshot snap = MvccSnapshot::CreateSnapshotIncludingAllTransactions();
    ASSERT_STATUS_OK(tablet->NewRowIterator(schema_, snap, &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));
    ASSERT_STATUS_OK(IterateToStringList(iter.get(), results));
    BOOST_FOREACH(const string& result, *results) {
      VLOG(1) << result;
    }
  }

  TabletMasterBlockPB master_block_;
};

// Tests a normal bootstrap scenario
TEST_F(BootstrapTest, TestBootstrap) {
  BuildLog();

  AppendReplicateBatch(current_id_);
  ASSERT_STATUS_OK(RollLog());

  AppendCommit(current_id_ + 1, current_id_);

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  ASSERT_STATUS_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));

  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());
}

// Tests the KUDU-141 scenario: bootstrap when there is
// an orphaned commit after a log roll.
// The test simulates the following scenario:
//
// 1) 'Replicate A' is written to Segment_1, which is anchored
// on MemRowSet_1.
// 2) Segment_1 is rolled, 'Commit A' is written to Segment_2.
// 3) MemRowSet_1 is flushed, releasing all anchors.
// 4) Segment_1 is garbage collected.
// 5) We crash, requiring a recovery of Segment_2 which now contains
// the orphan 'Commit A'.
TEST_F(BootstrapTest, TestOrphanCommit) {
  BuildLog();

  // Step 1) Write a REPLICATE to the log, and roll it.
  AppendReplicateBatch(current_id_);
  ASSERT_STATUS_OK(RollLog());

  // Step 2) Write the corresponding COMMIT in the second segment.
  AppendCommit(current_id_ + 1, current_id_);

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;

  // Step 3) Apply the operations in the log to the tablet and flush
  // the tablet to disk.
  ASSERT_STATUS_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));
  ASSERT_STATUS_OK(tablet->Flush());

  // Create a new log segment.
  ASSERT_STATUS_OK(RollLog());

  // Step 4) Create an orphanned commit by first adding a commit to
  // the newly rolled logfile, and then by removing the previous
  // commits.
  AppendCommit(current_id_ + 1, current_id_);
  ReadableLogSegmentMap segments;
  log_->GetReadableLogSegments(&segments);
  fs_manager_->env()->DeleteFile(segments.begin()->second->path());

  // Note: when GLOG_v=1, the test logs should include 'Ignoring
  // orphan commit: op_type: WRITE_OP...' line.
  ASSERT_STATUS_OK(BootstrapTestTablet(2, 1, &tablet, &boot_info));

  // Confirm that the legitimate data (from Step 3) is still there.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());
  ASSERT_EQ("(uint32 key=0, uint32 int_val=0, string string_val=this is a test insert)",
            results[0]);
  ASSERT_EQ(2, tablet->metadata()->last_durable_mrs_id());
}

// Tests this scenario:
// Orphan COMMIT with id <= current mrs id, followed by a REPLICATE
// message with mrs_id > current mrs_id, and a COMMIT message for that
// REPLICATE message.
//
// This should result in the orphan COMMIT being ignored, but the last
// REPLICATE/COMMIT messages ending up in the tablet.
TEST_F(BootstrapTest, TestNonOrphansAfterOrphanCommit) {
  BuildLog();

  AppendReplicateBatch(current_id_);
  ASSERT_STATUS_OK(RollLog());

  AppendCommit(current_id_ + 1, current_id_);

  ReadableLogSegmentMap segments;
  log_->GetReadableLogSegments(&segments);
  fs_manager_->env()->DeleteFile(segments.begin()->second->path());

  current_id_ += 2;

  AppendReplicateBatch(current_id_);
  AppendCommit(current_id_ + 1, current_id_, 2, 1, 0);

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  ASSERT_STATUS_OK(BootstrapTestTablet(1, 0, &tablet, &boot_info));

  // Confirm that the legitimate data is there.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());

  // 'key=2' means the REPLICATE message was inserted when current_id_ was 2, meaning
  // that only the non-orphan commit went in.
  ASSERT_EQ("(uint32 key=2, uint32 int_val=0, string string_val=this is a test insert)",
            results[0]);
}

// Test for where the server crashes in between REPLICATE and COMMIT.
// Bootstrap should not replay the operation, but should return it in
// the ConsensusBootstrapInfo
TEST_F(BootstrapTest, TestOrphanedReplicate) {
  BuildLog();

  // Append a REPLICATE with no commit
  int replicate_index = current_id_++;
  AppendReplicateBatch(replicate_index);

  // Bootstrap the tablet. It shouldn't replay anything.
  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_STATUS_OK(BootstrapTestTablet(0, 0, &tablet, &boot_info));

  // Table should be empty because we didn't replay the REPLICATE
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(0, results.size());

  // The consensus bootstrap info should include the orphaned REPLICATE.
  ASSERT_EQ(1, boot_info.orphaned_replicates.size());
  ASSERT_STR_CONTAINS(boot_info.orphaned_replicates[0]->ShortDebugString(),
                      "this is a test mutate");

  // And it should also include the latest opids.
  EXPECT_EQ("", boot_info.last_commit_id.ShortDebugString())
    << "Commit ID should be uninitialized";
  EXPECT_EQ("term: 0 index: 0", boot_info.last_replicate_id.ShortDebugString());
  EXPECT_EQ("term: 0 index: 0", boot_info.last_id.ShortDebugString());
}

} // namespace tablet
} // namespace kudu
