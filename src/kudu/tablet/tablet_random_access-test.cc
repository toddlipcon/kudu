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

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/gutil/casts.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/util/stopwatch.h"

DEFINE_int32(keyspace_size, 3000, "number of unique row keys to insert/mutate");
DEFINE_int32(runtime_seconds, 1, "number of seconds to run the test");
DEFINE_int32(sleep_between_background_ops_ms, 100,
             "number of milliseconds to sleep between flushing or compacting");
DEFINE_int32(update_delete_ratio, 4, "ratio of update:delete when mutating existing rows");

DECLARE_int32(deltafile_default_block_size);

using std::string;
using std::vector;

namespace kudu {
namespace tablet {

// Test which does only random operations against a tablet, including update and random
// get (ie scans with equal lower and upper bounds).
//
// The test maintains an in-memory copy of the expected state of the tablet, and uses only
// a single thread, so that it's easy to verify that the tablet always matches the expected
// state.
class TestRandomAccess : public KuduTabletTest {
 public:
  TestRandomAccess()
    : KuduTabletTest(Schema({ ColumnSchema("key", INT32),
                              ColumnSchema("val", INT32, true) }, 1)),
      done_(1) {
    OverrideFlagForSlowTests("keyspace_size", "30000");
    OverrideFlagForSlowTests("runtime_seconds", "10");
    OverrideFlagForSlowTests("sleep_between_background_ops_ms", "1000");

    // Set a small block size to increase chances that a single update will span
    // multiple delta blocks.
    FLAGS_deltafile_default_block_size = 1024;
    expected_tablet_state_.resize(FLAGS_keyspace_size);
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();
    writer_.reset(new LocalTabletWriter(tablet().get(), &client_schema_));
  }

  // Pick a random row of the table, verify its current state, and then
  // modify it in some way. The modifications may include multiple mutations
  // to the same row in a single batch (eg insert/update/delete).
  //
  // The mutations are always valid. For example:
  // - inserting if it doesn't exist yet
  // - perform an update or delete the row if it does exist.
  //
  // TODO: should add a version of this test which also tries invalid operations
  // and validates the correct errors.
  void DoRandomBatch() {
    int key = rand() % expected_tablet_state_.size();
    string& cur_val = expected_tablet_state_[key];

    // Check that a read yields what we expect.
    string val_in_table = GetRow(key);
    ASSERT_EQ("(" + cur_val + ")", val_in_table);

    vector<LocalTabletWriter::Op> pending;
    for (int i = 0; i < 3; i++) {
      int new_val = rand();
      if (cur_val.empty()) {
        // If there is no row, then insert one.
        cur_val = InsertRow(key, new_val, &pending);
      } else {
        if (new_val % (FLAGS_update_delete_ratio + 1) == 0) {
          cur_val = DeleteRow(key, &pending);
        } else {
          cur_val = MutateRow(key, new_val, &pending);
        }
      }
    }
    CHECK_OK(writer_->WriteBatch(pending));
    for (LocalTabletWriter::Op op : pending) {
      delete op.row;
    }
  }

  void DoRandomBatches() {
    int op_count = 0;
    Stopwatch s;
    s.start();
    while (s.elapsed().wall_seconds() < FLAGS_runtime_seconds) {
      for (int i = 0; i < 100; i++) {
        ASSERT_NO_FATAL_FAILURE(DoRandomBatch());
        op_count++;
      }
    }
    LOG(INFO) << "Ran " << op_count << " ops "
              << "(" << (op_count / s.elapsed().wall_seconds()) << " ops/sec)";
  }

  // Wakes up periodically to perform a flush or compaction.
  void BackgroundOpThread() {
    int n_flushes = 0;
    while (!done_.WaitFor(MonoDelta::FromMilliseconds(FLAGS_sleep_between_background_ops_ms))) {
      CHECK_OK(tablet()->Flush());
      ++n_flushes;
      switch (n_flushes % 3) {
        case 0:
          CHECK_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
          break;
        case 1:
          CHECK_OK(tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
          break;
        case 2:
          CHECK_OK(tablet()->CompactWorstDeltas(RowSet::MINOR_DELTA_COMPACTION));
          break;
      }
    }
  }

  // Adds an insert for the given key/value pair to 'ops', returning the new stringified
  // value of the row.
  string InsertRow(int key, int val, vector<LocalTabletWriter::Op>* ops) {
    gscoped_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    CHECK_OK(row->SetInt32(0, key));
    if (val & 1) {
      CHECK_OK(row->SetNull(1));
    } else {
      CHECK_OK(row->SetInt32(1, val));
    }
    string ret = row->ToString();
    ops->push_back(LocalTabletWriter::Op(RowOperationsPB::INSERT, row.release()));
    return ret;
  }

  // Adds an update of the given key/value pair to 'ops', returning the new stringified
  // value of the row.
  string MutateRow(int key, uint32_t new_val, vector<LocalTabletWriter::Op>* ops) {
    gscoped_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    CHECK_OK(row->SetInt32(0, key));
    if (new_val & 1) {
      CHECK_OK(row->SetNull(1));
    } else {
      CHECK_OK(row->SetInt32(1, new_val));
    }
    string ret = row->ToString();
    ops->push_back(LocalTabletWriter::Op(RowOperationsPB::UPDATE, row.release()));
    return ret;
  }

  // Adds a delete of the given row to 'ops', returning an empty string (indicating that
  // the row no longer exists).
  string DeleteRow(int key, vector<LocalTabletWriter::Op>* ops) {
    gscoped_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    CHECK_OK(row->SetInt32(0, key));
    ops->push_back(LocalTabletWriter::Op(RowOperationsPB::DELETE, row.release()));
    return "";
  }

  // Random-read the given row, returning its current value.
  // If the row doesn't exist, returns "()".
  string GetRow(int key) {
    ScanSpec spec;
    const Schema& schema = this->client_schema_;
    gscoped_ptr<RowwiseIterator> iter;
    CHECK_OK(this->tablet()->NewRowIterator(schema, &iter));
    ColumnRangePredicate pred_one(schema.column(0), &key, &key);
    spec.AddPredicate(pred_one);
    CHECK_OK(iter->Init(&spec));

    string ret = "()";
    int n_results = 0;

    Arena arena(1024, 4*1024*1024);
    RowBlock block(schema, 100, &arena);
    while (iter->HasNext()) {
      arena.Reset();
      CHECK_OK(iter->NextBlock(&block));
      for (int i = 0; i < block.nrows(); i++) {
        if (!block.selection_vector()->IsRowSelected(i)) {
          continue;
        }
        // We expect to only get exactly one result per read.
        CHECK_EQ(n_results, 0)
          << "Already got result when looking up row "
          << key << ": " << ret
          << " and now have new matching row: "
          << schema.DebugRow(block.row(i))
          << "  iterator: " << iter->ToString();
        ret = schema.DebugRow(block.row(i));
        n_results++;
      }
    }
    return ret;
  }

 protected:
  // The current expected state of the tablet.
  vector<string> expected_tablet_state_;

  // Latch triggered when the main thread is finished performing
  // operations. This stops the compact/flush thread.
  CountDownLatch done_;

  gscoped_ptr<LocalTabletWriter> writer_;
};

TEST_F(TestRandomAccess, Test) {
  scoped_refptr<Thread> flush_thread;
  CHECK_OK(Thread::Create("test", "flush",
                          boost::bind(&TestRandomAccess::BackgroundOpThread, this),
                          &flush_thread));

  DoRandomBatches();
  done_.CountDown();
  flush_thread->Join();
}



} // namespace tablet
} // namespace kudu
