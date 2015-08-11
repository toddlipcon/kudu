// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/log-test-base.h"

#include <tr1/unordered_map>
#include <vector>

#include "kudu/common/iterator.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/server/logical_clock.h"
#include "kudu/server/metadata.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_metadata.h"

namespace kudu {

namespace log {

extern const char* kTestTable;
extern const char* kTestTablet;

} // namespace log

namespace tablet {

using std::vector;
using std::string;
using std::tr1::unordered_map;

using consensus::ConsensusBootstrapInfo;
using consensus::ConsensusMetadata;
using consensus::kMinimumTerm;
using consensus::MakeOpId;
using consensus::OpId;
using consensus::ReplicateMsg;
using consensus::ReplicateRefPtr;
using consensus::make_scoped_refptr_replicate;
using log::Log;
using log::LogAnchorRegistry;
using log::LogTestBase;
using log::ReadableLogSegment;
using server::Clock;
using server::LogicalClock;
using tserver::WriteRequestPB;

class BootstrapTest : public LogTestBase {
 protected:

  void SetUp() OVERRIDE {
    LogTestBase::SetUp();
  }

  // Struct containing fields that we'll plug into the tablet metadata
  // before triggering a bootstrap.
  struct TestMetadataSetup {
    TestMetadataSetup()
      : durable_mrs_id(-1),
        durable_drs_id(-1) {
    }
    // The last MRS ID that the bootstrap should consider flushed.
    int durable_mrs_id;

    // The last DRS ID that was successfully written to disk.
    int durable_drs_id;

    // For each DRS ID (key) the DMS ID (value) that should be considered flushed.
    unordered_map<int, int> durable_dms_by_drs;
  };

  Status LoadTestTabletMetadata(const TestMetadataSetup& setup,
                                scoped_refptr<TabletMetadata>* meta) {
    RETURN_NOT_OK(TabletMetadata::LoadOrCreate(fs_manager_.get(),
                                               log::kTestTablet,
                                               log::kTestTable,
                                               // We need a schema with ids for
                                               // TabletMetadata::LoadOrCreate()
                                               SchemaBuilder(schema_).Build(),
                                               "",
                                               "",
                                               TABLET_DATA_READY,
                                               meta));
    (*meta)->SetLastDurableMrsIdForTests(setup.durable_mrs_id);
    (*meta)->SetLastDurableDrsIdForTests(setup.durable_drs_id);
    typedef std::pair<int, int> entry;
    BOOST_FOREACH(const entry& e, setup.durable_dms_by_drs) {
      (*meta)->GetRowSetForTests(e.first)->SetLastDurableRedoDmsIdForTests(e.second);
    }
    return (*meta)->Flush();
  }

  Status PersistTestTabletMetadataState(TabletDataState state) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK(LoadTestTabletMetadata(TestMetadataSetup(), &meta));
    meta->set_tablet_data_state(state);
    RETURN_NOT_OK(meta->Flush());
    return Status::OK();
  }

  Status RunBootstrapOnTestTablet(const scoped_refptr<TabletMetadata>& meta,
                                  shared_ptr<Tablet>* tablet,
                                  ConsensusBootstrapInfo* boot_info) {
    gscoped_ptr<TabletStatusListener> listener(new TabletStatusListener(meta));
    scoped_refptr<LogAnchorRegistry> log_anchor_registry(new LogAnchorRegistry());
    // Now attempt to recover the log
    RETURN_NOT_OK(BootstrapTablet(
        meta,
        scoped_refptr<Clock>(LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp)),
        shared_ptr<MemTracker>(),
        NULL,
        listener.get(),
        tablet,
        &log_,
        log_anchor_registry,
        boot_info));

    return Status::OK();
  }

  Status BootstrapTestTablet(const TestMetadataSetup& setup,
                             shared_ptr<Tablet>* tablet,
                             ConsensusBootstrapInfo* boot_info) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(LoadTestTabletMetadata(setup, &meta),
                          "Unable to load test tablet metadata");

    consensus::RaftConfigPB config;
    config.set_local(true);
    config.add_peers()->set_permanent_uuid(meta->fs_manager()->uuid());
    config.set_opid_index(consensus::kInvalidOpIdIndex);

    gscoped_ptr<ConsensusMetadata> cmeta;
    RETURN_NOT_OK_PREPEND(ConsensusMetadata::Create(meta->fs_manager(), meta->tablet_id(),
                                                    meta->fs_manager()->uuid(),
                                                    config, kMinimumTerm, &cmeta),
                          "Unable to create consensus metadata");

    RETURN_NOT_OK_PREPEND(RunBootstrapOnTestTablet(meta, tablet, boot_info),
                          "Unable to bootstrap test tablet");
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
    ASSERT_OK(tablet->NewRowIterator(schema_, snap, Tablet::UNORDERED, &iter));
    ASSERT_OK(iter->Init(NULL));
    ASSERT_OK(IterateToStringList(iter.get(), results));
    BOOST_FOREACH(const string& result, *results) {
      VLOG(1) << result;
    }
  }
};

// Tests a simple bootstrap scenario: some INSERTs which are already flushed,
// some INSERTs which are not yet flushed.
TEST_F(BootstrapTest, TestSimpleBootstrap) {
  BuildLog();

  AppendReplicateInsert(MakeOpId(1, 1), 1, "hello 1");
  AppendReplicateInsert(MakeOpId(1, 2), 2, "hello 2");

  AppendCommitInsert(MakeOpId(1, 1), 1);
  AppendCommitInsert(MakeOpId(1, 2), 2);

  AppendReplicateInsert(MakeOpId(1, 3), 3, "hello 3");
  AppendReplicateInsert(MakeOpId(1, 4), 4, "hello 4");

  // Some out-of-order commits
  AppendCommitInsert(MakeOpId(1, 4), 4);
  AppendCommitInsert(MakeOpId(1, 3), 3);

  // Run a bootstrap with metadata that says MRS through 2 already been flushed.
  // We should end up with only rows 3 and 4.
  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  TestMetadataSetup s;
  s.durable_mrs_id = 2;
  ASSERT_OK(BootstrapTestTablet(s, &tablet, &boot_info));

  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(2, results.size());
  EXPECT_EQ("(int32 key=3, int32 int_val=0, string string_val=hello 3)", results[0]);
  EXPECT_EQ("(int32 key=4, int32 int_val=0, string string_val=hello 4)", results[1]);
}

// Tests attempting a local bootstrap of a tablet that was in the middle of a
// remote bootstrap before "crashing".
TEST_F(BootstrapTest, TestIncompleteRemoteBootstrap) {
  BuildLog();

  ASSERT_OK(PersistTestTabletMetadataState(TABLET_DATA_COPYING));
  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  Status s = BootstrapTestTablet(TestMetadataSetup(), &tablet, &boot_info);
  ASSERT_TRUE(s.IsCorruption()) << "Expected corruption: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "TabletMetadata bootstrap state is TABLET_DATA_COPYING");
  LOG(INFO) << "State is still TABLET_DATA_COPYING, as expected: " << s.ToString();
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
  AppendReplicateInsert(MakeOpId(1, 1), 1, "replicate A");
  ASSERT_OK(RollLog());

  // Step 2) Write the corresponding COMMIT in the second segment.
  AppendCommitInsert(MakeOpId(1, 1), 1);

  {
    shared_ptr<Tablet> tablet;
    ConsensusBootstrapInfo boot_info;

    // Step 3) Apply the operations in the log to the tablet and flush
    // the tablet to disk.
    ASSERT_OK(BootstrapTestTablet(TestMetadataSetup(), &tablet, &boot_info));
    ASSERT_OK(tablet->Flush());

    // Check that the data we expect is there.
    {
      vector<string> results;
      IterateTabletRows(tablet.get(), &results);
      ASSERT_EQ(1, results.size());
      ASSERT_EQ("(int32 key=1, int32 int_val=0, string string_val=replicate A)",
                results[0]);
    }

    // Create a new log segment.
    ASSERT_OK(RollLog());

    // Step 4) Create an orphaned commit by first adding a commit to
    // the newly rolled logfile, and then by removing the previous
    // commits.
    AppendCommitInsert(MakeOpId(1, 2), 1);
    log::SegmentSequence segments;
    ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
    fs_manager_->env()->DeleteFile(segments[0]->path());
  }
  {
    shared_ptr<Tablet> tablet;
    ConsensusBootstrapInfo boot_info;

    // Note: when GLOG_v=1, the test logs should include 'Ignoring
    // orphan commit: op_type: WRITE_OP...' line.
    TestMetadataSetup setup;
    setup.durable_mrs_id = 2;
    setup.durable_drs_id = 0;
    setup.durable_dms_by_drs[0] = 1;
    ASSERT_OK(BootstrapTestTablet(setup, &tablet, &boot_info));

    // Confirm that the legitimate data (from Step 3) is still there.
    vector<string> results;
    IterateTabletRows(tablet.get(), &results);
    ASSERT_EQ(1, results.size());
    ASSERT_EQ("(int32 key=1, int32 int_val=0, string string_val=replicate A)",
              results[0]);
    ASSERT_EQ(2, tablet->metadata()->last_durable_mrs_id());
  }
}

// Tests this scenario:
// Orphan COMMIT with mrs id <= current mrs id, followed by a REPLICATE
// message with mrs_id > current mrs_id, and a COMMIT message for that
// REPLICATE message.
//
// This should result in the orphan COMMIT being ignored, but the last
// REPLICATE/COMMIT messages ending up in the tablet.
TEST_F(BootstrapTest, TestNonOrphansAfterOrphanCommit) {
  BuildLog();

  const int kMrsIdForOriginalInsert = 1;
  const int kMrsIdForSecondInsert = 2;

  OpId opid = MakeOpId(1, current_index_++);

  AppendReplicateInsert(opid, 1, "original insert");
  ASSERT_OK(RollLog());
  AppendCommitInsert(opid, kMrsIdForOriginalInsert);

  log::SegmentSequence segments;
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
  fs_manager_->env()->DeleteFile(segments[0]->path());

  opid = MakeOpId(1, current_index_++);
  AppendReplicateInsert(opid, 2, "next insert");
  AppendCommitInsert(opid, kMrsIdForSecondInsert);

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  TestMetadataSetup setup;
  setup.durable_mrs_id = 1;
  ASSERT_OK(BootstrapTestTablet(setup, &tablet, &boot_info));

  // Confirm that the legitimate data is there.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());

  // Verify that we bootstrapped OK and only the second commit shows up.
  ASSERT_EQ("(int32 key=2, int32 int_val=0, string string_val=next insert)",
            results[0]);
}

// Test for where the server crashes in between REPLICATE and COMMIT.
// Bootstrap should not replay the operation, but should return it in
// the ConsensusBootstrapInfo
TEST_F(BootstrapTest, TestOrphanedReplicate) {
  BuildLog();

  // Append a REPLICATE with no commit
  OpId opid = MakeOpId(1, 3);
  AppendReplicateInsert(opid, 1, "replicate with no commit");

  // Bootstrap the tablet. It shouldn't replay anything.
  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(TestMetadataSetup(), &tablet, &boot_info));

  // Table should be empty because we didn't replay the REPLICATE
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(0, results.size());

  // The consensus bootstrap info should include the orphaned REPLICATE.
  ASSERT_EQ(1, boot_info.orphaned_replicates.size());
  ASSERT_STR_CONTAINS(boot_info.orphaned_replicates[0]->ShortDebugString(),
                      "replicate with no commit");

  // And it should also include the latest opids.
  EXPECT_EQ("term: 1 index: 3", boot_info.last_id.ShortDebugString());
}

// Bootstrap should fail if no ConsensusMetadata file exists.
TEST_F(BootstrapTest, TestMissingConsensusMetadata) {
  BuildLog();

  scoped_refptr<TabletMetadata> meta;
  ASSERT_OK(LoadTestTabletMetadata(TestMetadataSetup(), &meta));

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  Status s = RunBootstrapOnTestTablet(meta, &tablet, &boot_info);

  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "Unable to load Consensus metadata");
}

TEST_F(BootstrapTest, TestOperationOverwriting) {
  const int kUnflushedMrs = 1;

  BuildLog();

  OpId opid = MakeOpId(1, 1);

  // Append a replicate in term 1
  AppendReplicateInsert(opid, 1, "insert 1.1");

  // Append a commit for op 1.1
  AppendCommitInsert(opid, kUnflushedMrs);

  // Now append replicates for 4.2 and 4.3
  AppendReplicateInsert(MakeOpId(4, 2), 2, "insert 4.2");
  AppendReplicateInsert(MakeOpId(4, 3), 3, "insert 4.3");

  ASSERT_OK(RollLog());
  // And overwrite with 3.2
  AppendReplicateInsert(MakeOpId(3, 2), 4, "insert 3.2");

  // When bootstrapping we should apply ops 1.1 and get 3.2 as pending.
  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(TestMetadataSetup(), &tablet, &boot_info));

  ASSERT_EQ(boot_info.orphaned_replicates.size(), 1);
  ASSERT_OPID_EQ(boot_info.orphaned_replicates[0]->id(), MakeOpId(3, 2));

  // Confirm that the legitimate data is there.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());

  ASSERT_EQ("(int32 key=1, int32 int_val=0, string string_val=insert 1.1)",
            results[0]);
}

// Tests that when we have out-of-order commits that touch the same rows, operations are
// still applied and in the correct order.
TEST_F(BootstrapTest, TestOutOfOrderCommits) {
  const int kUnflushedMrs = 1;
  BuildLog();

  // Append an INSERT with opid 10.10
  AppendReplicateInsert(MakeOpId(10, 10), 1, "insert 10.10");

  // Append UPDATE of the same row with op 10.11
  AppendReplicateUpdate(MakeOpId(10, 11), 1, "update 10.11");

  // Now write the commit entry for the mutate before the one for the insert
  gscoped_ptr<consensus::CommitMsg> mutate_commit(new consensus::CommitMsg);
  mutate_commit->set_op_type(consensus::WRITE_OP);
  mutate_commit->mutable_commited_op_id()->CopyFrom(MakeOpId(10, 11));
  mutate_commit->mutable_result()->add_ops()->add_mutated_stores()->set_mrs_id(kUnflushedMrs);
  AppendCommit(mutate_commit.Pass());

  gscoped_ptr<consensus::CommitMsg> insert_commit(new consensus::CommitMsg);
  insert_commit->set_op_type(consensus::WRITE_OP);
  insert_commit->mutable_commited_op_id()->CopyFrom(MakeOpId(10, 10));
  insert_commit->mutable_result()->add_ops()->add_mutated_stores()->set_mrs_id(kUnflushedMrs);
  AppendCommit(insert_commit.Pass());

  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(TestMetadataSetup(), &tablet, &boot_info));

  // Confirm that both operations were applied.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());

  ASSERT_EQ("(int32 key=1, int32 int_val=0, string string_val=update 10.11)",
            results[0]);
}

// Tests that when we have two consecutive replicates but the commit message for the
// first one is missing, both appear as pending in ConsensusInfo.
TEST_F(BootstrapTest, TestMissingCommitMessage) {
  const int kUnflushedMrs = 1;
  BuildLog();

  AppendReplicateInsert(MakeOpId(10, 10), 1, "insert");
  AppendReplicateInsert(MakeOpId(10, 11), 1, "update");

  // Now commit the second one, but not the first.
  AppendCommitInsert(MakeOpId(10, 11), kUnflushedMrs);

  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(TestMetadataSetup(), &tablet, &boot_info));

  // Both operations should be considered orphaned.
  ASSERT_EQ(boot_info.orphaned_replicates.size(), 2);

  // OpId 10.11 is known to be committed (even though we didn't have a commit for 10.10)
  ASSERT_OPID_EQ(boot_info.last_committed_id, MakeOpId(10, 11));

  // Confirm that no operation was applied.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(0, results.size());
}

// Test that we do not crash when a consensus-only operation has a timestamp
// that is higher than a timestamp assigned to a write operation that follows
// it in the log.
TEST_F(BootstrapTest, TestConsensusOnlyOperationOutOfOrderTimestamp) {
  BuildLog();

  // Append NO_OP.
  ReplicateRefPtr noop_replicate = make_scoped_refptr_replicate(new ReplicateMsg());
  noop_replicate->get()->set_op_type(consensus::NO_OP);
  *noop_replicate->get()->mutable_id() = MakeOpId(1, 1);
  noop_replicate->get()->set_timestamp(2);

  AppendReplicateBatch(noop_replicate, true);

  // Append WRITE_OP with higher OpId and lower timestamp.
  ReplicateRefPtr write_replicate = make_scoped_refptr_replicate(new ReplicateMsg());
  write_replicate->get()->set_op_type(consensus::WRITE_OP);
  WriteRequestPB* batch_request = write_replicate->get()->mutable_write_request();
  ASSERT_OK(SchemaToPB(schema_, batch_request->mutable_schema()));
  batch_request->set_tablet_id(log::kTestTablet);
  *write_replicate->get()->mutable_id() = MakeOpId(1, 2);
  write_replicate->get()->set_timestamp(1);
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1, 1, "foo",
                 batch_request->mutable_row_operations());

  AppendReplicateBatch(write_replicate, true);

  // Now commit in OpId order.
  // NO_OP...
  gscoped_ptr<consensus::CommitMsg> mutate_commit(new consensus::CommitMsg);
  mutate_commit->set_op_type(consensus::NO_OP);
  *mutate_commit->mutable_commited_op_id() = noop_replicate->get()->id();

  AppendCommit(mutate_commit.Pass());

  // ...and WRITE_OP...
  mutate_commit.reset(new consensus::CommitMsg);
  mutate_commit->set_op_type(consensus::WRITE_OP);
  *mutate_commit->mutable_commited_op_id() = write_replicate->get()->id();
  TxResultPB* result = mutate_commit->mutable_result();
  OperationResultPB* mutate = result->add_ops();
  MemStoreTargetPB* target = mutate->add_mutated_stores();
  target->set_mrs_id(1);

  AppendCommit(mutate_commit.Pass());

  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(TestMetadataSetup(), &tablet, &boot_info));
  ASSERT_EQ(boot_info.orphaned_replicates.size(), 0);
  ASSERT_OPID_EQ(boot_info.last_committed_id, write_replicate->get()->id());

  // Confirm that the insert op was applied.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());
}

} // namespace tablet
} // namespace kudu
