// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <tr1/memory>

#include "common/schema.h"
#include "tablet/deltafile.h"
#include "tablet/delta_tracker.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/memenv/memenv.h"
#include "util/test_macros.h"

DECLARE_int32(deltafile_block_size);
DEFINE_int32(first_row_to_update, 10000, "the first row to update");
DEFINE_int32(last_row_to_update, 100000, "the last row to update");
DEFINE_int32(n_verify, 1, "number of times to verify the updates"
             "(useful for benchmarks");

namespace kudu {
namespace tablet {

using std::tr1::shared_ptr;

// Test path to write delta file to (in in-memory environment)
const char kTestPath[] = "/tmp/test";

class TestDeltaFile : public ::testing::Test {
 public:
  TestDeltaFile() :
    env_(NewMemEnv(Env::Default())),
    schema_(CreateSchema()),
    arena_(1024, 1024),
    kTestBlock("test-block-id")
  {}

  static Schema CreateSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddColumn("val", UINT32));
    return builder.Build();
  }

  void WriteTestFile(int min_timestamp = 0, int max_timestamp = 0) {
    shared_ptr<WritableFile> file;
    ASSERT_STATUS_OK(env_util::OpenFileForWrite(env_.get(), kTestPath, &file));

    DeltaFileWriter dfw(schema_, file);
    ASSERT_STATUS_OK(dfw.Start());

    // Update even numbered rows.
    faststring buf;

    DeltaStats stats(schema_.num_columns());
    for (int i = FLAGS_first_row_to_update; i <= FLAGS_last_row_to_update; i += 2) {
      for (int timestamp = min_timestamp; timestamp <= max_timestamp; timestamp++) {
        buf.clear();
        RowChangeListEncoder update(schema_, &buf);
        uint32_t new_val = timestamp + i;
        update.AddColumnUpdate(0, &new_val);
        DeltaKey key(i, Timestamp(timestamp));
        RowChangeList rcl(buf);
        ASSERT_STATUS_OK_FAST(dfw.AppendDelta<REDO>(key, rcl));
        ASSERT_STATUS_OK_FAST(stats.UpdateStats(key.timestamp(), schema_, rcl));
      }
    }
    ASSERT_STATUS_OK(dfw.WriteDeltaStats(stats));
    ASSERT_STATUS_OK(dfw.Finish());
  }


  void DoTestRoundTrip() {
    // First write the file.
    WriteTestFile();

    // Then iterate back over it, applying deltas to a fake row block.
    for (int i = 0; i < FLAGS_n_verify; i++) {
      VerifyTestFile();
    }
  }

  void VerifyTestFile() {
    shared_ptr<DeltaFileReader> reader;
    ASSERT_STATUS_OK(DeltaFileReader::Open(env_.get(), kTestPath, kTestBlock, &reader, REDO));
    ASSERT_EQ(((FLAGS_last_row_to_update - FLAGS_first_row_to_update) / 2) + 1,
              reader->delta_stats().update_count(0));
    ASSERT_EQ(0, reader->delta_stats().delete_count());
    MvccSnapshot snap = MvccSnapshot::CreateSnapshotIncludingAllTransactions();

    DeltaIterator* raw_iter;
    Status s =  reader->NewDeltaIterator(&schema_, snap, &raw_iter);
    if (s.IsNotFound()) {
      FAIL() << "Iterator fell outside of the range of an include-all snapshot";
    }
    ASSERT_STATUS_OK(s);
    gscoped_ptr<DeltaIterator> it(raw_iter);
    ASSERT_STATUS_OK(it->Init());

    RowBlock block(schema_, 100, &arena_);

    // Iterate through the faked table, starting with batches that
    // come before all of the updates, and extending a bit further
    // past the updates, to ensure that nothing breaks on the boundaries.
    ASSERT_STATUS_OK(it->SeekToOrdinal(0));

    int start_row = 0;
    while (start_row < FLAGS_last_row_to_update + 10000) {
      block.ZeroMemory();
      arena_.Reset();

      ASSERT_STATUS_OK_FAST(it->PrepareBatch(block.nrows()));
      ColumnBlock dst_col = block.column_block(0);
      ASSERT_STATUS_OK_FAST(it->ApplyUpdates(0, &dst_col));

      for (int i = 0; i < block.nrows(); i++) {
        uint32_t row = start_row + i;
        bool should_be_updated = (row >= FLAGS_first_row_to_update) &&
          (row <= FLAGS_last_row_to_update) &&
          (row % 2 == 0);

        DCHECK_EQ(block.row(i).cell_ptr(0), dst_col.cell_ptr(i));
        uint32_t updated_val = *schema_.ExtractColumnFromRow<UINT32>(block.row(i), 0);
        VLOG(2) << "row " << row << ": " << updated_val;
        uint32_t expected_val = should_be_updated ? row : 0;
        // Don't use ASSERT_EQ, since it's slow (records positive results, not just negative)
        if (updated_val != expected_val) {
          FAIL() << "failed on row " << row <<
            ": expected " << expected_val << ", got " << updated_val;
        }
      }

      start_row += block.nrows();
    }
  }

 protected:
  gscoped_ptr<Env> env_;
  Schema schema_;
  Arena arena_;
  const BlockId kTestBlock;
};

TEST_F(TestDeltaFile, TestRoundTripTinyDeltaBlocks) {
  // Set block size small, so that we get good coverage
  // of the case where multiple delta blocks correspond to a
  // single underlying data block.
  google::FlagSaver saver;
  FLAGS_deltafile_block_size = 256;
  DoTestRoundTrip();
}

TEST_F(TestDeltaFile, TestRoundTrip) {
  DoTestRoundTrip();
}

TEST_F(TestDeltaFile, TestCollectMutations) {
  WriteTestFile();

  {
    shared_ptr<DeltaFileReader> reader;
    ASSERT_STATUS_OK(DeltaFileReader::Open(env_.get(), kTestPath, kTestBlock, &reader, REDO));

    MvccSnapshot snap = MvccSnapshot::CreateSnapshotIncludingAllTransactions();

    DeltaIterator* raw_iter;
    Status s =  reader->NewDeltaIterator(&schema_, snap, &raw_iter);
    if (s.IsNotFound()) {
      FAIL() << "Iterator fell outside of the range of an include-all snapshot";
    }
    ASSERT_STATUS_OK(s);
    gscoped_ptr<DeltaIterator> it(raw_iter);

    ASSERT_STATUS_OK(it->Init());
    ASSERT_STATUS_OK(it->SeekToOrdinal(0));

    vector<Mutation *> mutations;
    mutations.resize(100);

    int start_row = 0;
    while (start_row < FLAGS_last_row_to_update + 10000) {
      std::fill(mutations.begin(), mutations.end(), reinterpret_cast<Mutation *>(NULL));

      arena_.Reset();
      ASSERT_STATUS_OK_FAST(it->PrepareBatch(mutations.size()));
      ASSERT_STATUS_OK(it->CollectMutations(&mutations, &arena_));

      for (int i = 0; i < mutations.size(); i++) {
        Mutation *mut_head = mutations[i];
        if (mut_head != NULL) {
          rowid_t row = start_row + i;
          string str = Mutation::StringifyMutationList(schema_, mut_head);
          VLOG(1) << "Mutation on row " << row << ": " << str;
        }
      }

      start_row += mutations.size();
    }
  }

}

TEST_F(TestDeltaFile, TestSkipsDeltasOutOfRange) {
  WriteTestFile(10, 20);
  shared_ptr<DeltaFileReader> reader;
  ASSERT_STATUS_OK(DeltaFileReader::Open(env_.get(), kTestPath, kTestBlock, &reader, REDO));

  gscoped_ptr<DeltaIterator> iter;

  // should skip
  MvccSnapshot snap1(Timestamp(9));
  ASSERT_FALSE(snap1.MayHaveCommittedTransactionsAtOrAfter(Timestamp(10)));
  DeltaIterator* raw_iter = NULL;
  Status s = reader->NewDeltaIterator(&schema_, snap1, &raw_iter);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(raw_iter == NULL);

  // should include
  raw_iter = NULL;
  MvccSnapshot snap2(Timestamp(15));
  ASSERT_STATUS_OK(reader->NewDeltaIterator(&schema_, snap2, &raw_iter));
  ASSERT_TRUE(raw_iter != NULL);
  iter.reset(raw_iter);

  // should include
  raw_iter = NULL;
  MvccSnapshot snap3(Timestamp(21));
  ASSERT_STATUS_OK(reader->NewDeltaIterator(&schema_, snap3, &raw_iter));
  ASSERT_TRUE(raw_iter != NULL);
  iter.reset(raw_iter);
}

} // namespace tablet
} // namespace kudu
