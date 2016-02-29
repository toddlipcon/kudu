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

#include "kudu/client/client.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/row_result.h"
#include "kudu/client/schema.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

DEFINE_int32(keyspace_size, 2,  "number of distinct primary keys to test with");
DECLARE_bool(enable_maintenance_manager);
DECLARE_bool(use_hybrid_clock);

using std::string;
using std::vector;
using std::unique_ptr;

enum TestOpType {
  TEST_INSERT,
  TEST_UPDATE,
  TEST_DELETE,
  TEST_FLUSH_OPS,
  TEST_FLUSH_TABLET,
  TEST_FLUSH_DELTAS,
  TEST_MINOR_COMPACT_DELTAS,
  TEST_MAJOR_COMPACT_DELTAS,
  TEST_COMPACT_TABLET,
  TEST_RESTART_TS,
  TEST_NUM_OP_TYPES // max value for enum
};
MAKE_ENUM_LIMITS(TestOpType, TEST_INSERT, TEST_NUM_OP_TYPES);


const char* kTableName = "table";

namespace kudu {

using namespace client; // TODO NOLINT
using sp::shared_ptr;

namespace tablet {

const char* TestOpType_names[] = {
  "TEST_INSERT",
  "TEST_UPDATE",
  "TEST_DELETE",
  "TEST_FLUSH_OPS",
  "TEST_FLUSH_TABLET",
  "TEST_FLUSH_DELTAS",
  "TEST_MINOR_COMPACT_DELTAS",
  "TEST_MAJOR_COMPACT_DELTAS",
  "TEST_COMPACT_TABLET",
  "TEST_RESTART_TS"
};

struct TestOp {
  TestOpType type;
  int arg;

  string ToString() const {
    return strings::Substitute("{$0, $1}", TestOpType_names[type], arg);
  }
};


// Test which does only random operations against a tablet, including update and random
// get (ie scans with equal lower and upper bounds).
//
// The test maintains an in-memory copy of the expected state of the tablet, and uses only
// a single thread, so that it's easy to verify that the tablet always matches the expected
// state.
class FuzzTest : public KuduTest {
 public:
  FuzzTest() {
    FLAGS_enable_maintenance_manager = false;
    FLAGS_use_hybrid_clock = false;

    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(KuduColumnSchema::INT32);
    CHECK_OK(b.Build(&schema_));

  }

  void SetUp() override {
    KuduTest::SetUp();

    MiniClusterOptions opts;
    cluster_.reset(new MiniCluster(env_.get(), opts));
    ASSERT_OK(cluster_->Start());
    CHECK_OK(KuduClientBuilder()
             .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr_str())
             .default_admin_operation_timeout(MonoDelta::FromSeconds(60))
             .Build(&client_));
    // Add a table, make sure it reports itself.
    gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    CHECK_OK(table_creator->table_name(kTableName)
             .schema(&schema_)
             .num_replicas(1)
             .Create());

    // Find the peer.
    tablet_peer_ = LookupTabletPeer();

    // Setup session and table.
    session_ = client_->NewSession();
    CHECK_OK(session_->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session_->SetTimeoutMillis(15 * 1000);
    CHECK_OK(client_->OpenTable(kTableName, &table_));
  }

  void TearDown() override {
    tablet_peer_.reset();
    cluster_->Shutdown();
  }

  scoped_refptr<TabletPeer> LookupTabletPeer() {
    vector<scoped_refptr<TabletPeer> > peers;
    cluster_->mini_tablet_server(0)->server()->tablet_manager()->GetTabletPeers(&peers);
    CHECK_EQ(1, peers.size());
    return peers[0];
  }

  void RestartTabletServer() {
    tablet_peer_.reset();
    auto ts = cluster_->mini_tablet_server(0);
    if (ts->server()) {
      ASSERT_OK(ts->Restart());
    } else {
      ASSERT_OK(ts->Start());
    }
    ASSERT_OK(ts->server()->WaitInited());

    tablet_peer_ = LookupTabletPeer();
  }

  Tablet* tablet() const {
    return tablet_peer_->tablet();
  }

  // Adds an insert for the given key/value pair to 'ops', returning the new stringified
  // value of the row.
  string InsertRow(int key, int val) {
    unique_ptr<KuduInsert> ins(table_->NewInsert());
    KuduPartialRow* row = ins->mutable_row();
    CHECK_OK(row->SetInt32(0, key));
    if (val & 1) {
      CHECK_OK(row->SetNull(1));
    } else {
      CHECK_OK(row->SetInt32(1, val));
    }
    string ret = row->ToString();
    CHECK_OK(session_->Apply(ins.release()));
    return ret;
  }

  // Adds an update of the given key/value pair to 'ops', returning the new stringified
  // value of the row.
  string MutateRow(int key, uint32_t new_val) {
    unique_ptr<KuduUpdate> update(table_->NewUpdate());
    KuduPartialRow* row = update->mutable_row();
    CHECK_OK(row->SetInt32(0, key));
    if (new_val & 1) {
      CHECK_OK(row->SetNull(1));
    } else {
      CHECK_OK(row->SetInt32(1, new_val));
    }
    string ret = row->ToString();
    CHECK_OK(session_->Apply(update.release()));
    return ret;
  }

  // Adds a delete of the given row to 'ops', returning an empty string (indicating that
  // the row no longer exists).
  string DeleteRow(int key) {
    unique_ptr<KuduDelete> del(table_->NewDelete());
    KuduPartialRow* row = del->mutable_row();
    CHECK_OK(row->SetInt32(0, key));
    CHECK_OK(session_->Apply(del.release()));
    return "";
  }

  // Random-read the given row, returning its current value.
  // If the row doesn't exist, returns "()".
  string GetRow(int key) {
    KuduScanner s(table_.get());
    CHECK_OK(s.AddConjunctPredicate(table_->NewComparisonPredicate(
        "key", KuduPredicate::EQUAL, KuduValue::FromInt(key))));
    CHECK_OK(s.Open());
    while (s.HasMoreRows()) {
      KuduScanBatch batch;
      CHECK_OK(s.NextBatch(&batch));
      for (KuduScanBatch::RowPtr row : batch) {
        return row.ToString();
      }
    }
    return "()";
  }

 protected:
  void RunFuzzCase(const vector<TestOp>& ops,
                   int update_multiplier);

  KuduSchema schema_;
  gscoped_ptr<MiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  shared_ptr<KuduSession> session_;
  shared_ptr<KuduTable> table_;

  scoped_refptr<TabletPeer> tablet_peer_;
};


void GenerateTestCase(vector<TestOp>* ops, int len) {
  vector<bool> exists(FLAGS_keyspace_size);
  bool ops_pending = false;
  bool data_in_mrs = false;
  bool worth_compacting = false;
  bool data_in_dms = false;
  ops->clear();
  while (ops->size() < len) {
    TestOpType r = tight_enum_cast<TestOpType>(rand() % enum_limits<TestOpType>::max_enumerator);
    int row_arg = rand() % FLAGS_keyspace_size;
    switch (r) {
      case TEST_INSERT:
        if (exists[row_arg]) continue;
        ops->push_back({TEST_INSERT, row_arg});
        exists[row_arg] = true;
        ops_pending = true;
        data_in_mrs = true;
        break;
      case TEST_UPDATE:
        if (!exists[row_arg]) continue;
        ops->push_back({TEST_UPDATE, row_arg});
        ops_pending = true;
        if (!data_in_mrs) {
          data_in_dms = true;
        }
        break;
      case TEST_DELETE:
        if (!exists[row_arg]) continue;
        ops->push_back({TEST_DELETE, row_arg});
        ops_pending = true;
        exists[row_arg] = false;
        if (!data_in_mrs) {
          data_in_dms = true;
        }
        break;
      case TEST_FLUSH_OPS:
        if (ops_pending) {
          ops->push_back({TEST_FLUSH_OPS, 0});
          ops_pending = false;
        }
        break;
      case TEST_FLUSH_TABLET:
        if (data_in_mrs) {
          if (ops_pending) {
            ops->push_back({TEST_FLUSH_OPS, 0});
            ops_pending = false;
          }
          ops->push_back({TEST_FLUSH_TABLET, 0});
          data_in_mrs = false;
          worth_compacting = true;
        }
        break;
      case TEST_COMPACT_TABLET:
        if (worth_compacting) {
          if (ops_pending) {
            ops->push_back({TEST_FLUSH_OPS, 0});
            ops_pending = false;
          }
          ops->push_back({TEST_COMPACT_TABLET, 0});
          worth_compacting = false;
        }
        break;
      case TEST_FLUSH_DELTAS:
        if (data_in_dms) {
          if (ops_pending) {
            ops->push_back({TEST_FLUSH_OPS, 0});
            ops_pending = false;
          }
          ops->push_back({TEST_FLUSH_DELTAS, 0});
          data_in_dms = false;
        }
        break;
      case TEST_MAJOR_COMPACT_DELTAS:
        ops->push_back({TEST_MAJOR_COMPACT_DELTAS, 0});
        break;
      case TEST_MINOR_COMPACT_DELTAS:
        ops->push_back({TEST_MINOR_COMPACT_DELTAS, 0});
        break;
      case TEST_RESTART_TS:
        ops->push_back({TEST_RESTART_TS, 0});
        break;
      default:
        LOG(FATAL);
    }
  }
}

string DumpTestCase(const vector<TestOp>& ops) {
  vector<string> strs;
  for (TestOp test_op : ops) {
    strs.push_back(test_op.ToString());
  }
  return JoinStrings(strs, ",\n");
}

void FuzzTest::RunFuzzCase(const vector<TestOp>& test_ops,
                                   int update_multiplier = 1) {
  LOG(INFO) << "test case: " << DumpTestCase(test_ops);

  vector<string> cur_val(FLAGS_keyspace_size);
  vector<string> pending_val(FLAGS_keyspace_size);

  int i = 0;
  for (const TestOp& test_op : test_ops) {
    string val_in_table = GetRow(test_op.arg);
    EXPECT_EQ("(" + cur_val[test_op.arg] + ")", val_in_table);

    LOG(INFO) << test_op.ToString();
    switch (test_op.type) {
      case TEST_INSERT:
        pending_val[test_op.arg] = InsertRow(test_op.arg, i++);
        break;
      case TEST_UPDATE:
        for (int j = 0; j < update_multiplier; j++) {
          pending_val[test_op.arg] = MutateRow(test_op.arg, i++);
        }
        break;
      case TEST_DELETE:
        pending_val[test_op.arg] = DeleteRow(test_op.arg);
        break;
      case TEST_FLUSH_OPS:
        FlushSessionOrDie(session_);
        cur_val = pending_val;
        break;
      case TEST_FLUSH_TABLET:
        ASSERT_OK(tablet()->Flush());
        break;
      case TEST_FLUSH_DELTAS:
        ASSERT_OK(tablet()->FlushBiggestDMS());
        break;
      case TEST_MAJOR_COMPACT_DELTAS:
        ASSERT_OK(tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
        break;
      case TEST_MINOR_COMPACT_DELTAS:
        ASSERT_OK(tablet()->CompactWorstDeltas(RowSet::MINOR_DELTA_COMPACTION));
        break;
      case TEST_COMPACT_TABLET:
        ASSERT_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
        break;
      case TEST_RESTART_TS:
        NO_FATALS(RestartTabletServer());
        break;
      default:
        LOG(FATAL) << test_op.type;
    }
  }
}


// Generates a random test sequence and runs it.
// The logs of this test are designed to easily be copy-pasted and create
// more specific test cases like TestFuzz<N> below.
TEST_F(FuzzTest, TestFuzz) {
  SeedRandom();
  vector<TestOp> test_ops;
  GenerateTestCase(&test_ops, AllowSlowTests() ? 1000 : 50);
  RunFuzzCase(test_ops);
}

// Generates a random test case, but the UPDATEs are all repeated 1000 times.
// This results in very large batches which are likely to span multiple delta blocks
// when flushed.
TEST_F(FuzzTest, TestFuzzHugeBatches) {
  SeedRandom();
  vector<TestOp> test_ops;
  GenerateTestCase(&test_ops, AllowSlowTests() ? 1000 : 50);
  RunFuzzCase(test_ops, 1000);
}

// A particular test case which previously failed TestFuzz.
TEST_F(FuzzTest, TestFuzz1) {
  vector<TestOp> test_ops = {
    // Get an inserted row in a DRS.
    {TEST_INSERT, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},

    // DELETE in DMS, INSERT in MRS and flush again.
    {TEST_DELETE, 0},
    {TEST_INSERT, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},

    // State:
    // RowSet RowSet(0):
    //   (int32 key=1, int32 val=NULL) Undos: [@1(DELETE)] Redos (in DMS): [@2 DELETE]
    // RowSet RowSet(1):
    //   (int32 key=1, int32 val=NULL) Undos: [@2(DELETE)] Redos: []

    {TEST_COMPACT_TABLET, 0},
  };
  RunFuzzCase(test_ops);
}

// A particular test case which previously failed TestFuzz.
TEST_F(FuzzTest, TestFuzz2) {
  vector<TestOp> test_ops = {
    {TEST_INSERT, 0},
    {TEST_DELETE, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},
    // (int32 key=1, int32 val=NULL)
    // Undo Mutations: [@1(DELETE)]
    // Redo Mutations: [@1(DELETE)]

    {TEST_INSERT, 0},
    {TEST_DELETE, 0},
    {TEST_INSERT, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},
    // (int32 key=1, int32 val=NULL)
    // Undo Mutations: [@2(DELETE)]
    // Redo Mutations: []

    {TEST_COMPACT_TABLET, 0},
    // Output Row: (int32 key=1, int32 val=NULL)
    // Undo Mutations: [@1(DELETE)]
    // Redo Mutations: [@1(DELETE)]

    {TEST_DELETE, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_COMPACT_TABLET, 0},
  };
  RunFuzzCase(test_ops);
}

// A particular test case which previously failed TestFuzz.
TEST_F(FuzzTest, TestFuzz3) {
  vector<TestOp> test_ops = {
    {TEST_INSERT, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},
    // Output Row: (int32 key=1, int32 val=NULL)
    // Undo Mutations: [@1(DELETE)]
    // Redo Mutations: []

    {TEST_DELETE, 0},
    // Adds a @2 DELETE to DMS for above row.

    {TEST_INSERT, 0},
    {TEST_DELETE, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},
    // (int32 key=1, int32 val=NULL)
    // Undo Mutations: [@2(DELETE)]
    // Redo Mutations: [@2(DELETE)]

    // Compaction input:
    // Row 1: (int32 key=1, int32 val=NULL)
    //   Undo Mutations: [@2(DELETE)]
    //   Redo Mutations: [@2(DELETE)]
    // Row 2: (int32 key=1, int32 val=NULL)
    //  Undo Mutations: [@1(DELETE)]
    //  Redo Mutations: [@2(DELETE)]

    {TEST_COMPACT_TABLET, 0},
  };
  RunFuzzCase(test_ops);
}

// A particular test case which previously failed TestFuzz.
TEST_F(FuzzTest, TestFuzz4) {
  vector<TestOp> test_ops = {
    {TEST_INSERT, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_COMPACT_TABLET, 0},
    {TEST_DELETE, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_COMPACT_TABLET, 0},
    {TEST_INSERT, 0},
    {TEST_UPDATE, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},
    {TEST_DELETE, 0},
    {TEST_INSERT, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},
    {TEST_UPDATE, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},
    {TEST_UPDATE, 0},
    {TEST_DELETE, 0},
    {TEST_INSERT, 0},
    {TEST_DELETE, 0},
    {TEST_FLUSH_OPS, 0},
    {TEST_FLUSH_TABLET, 0},
    {TEST_COMPACT_TABLET, 0},
  };
  RunFuzzCase(test_ops);
}

// Previously caused incorrect data being read after restart.
// Failure:
//  Value of: val_in_table
//  Actual: "()"
//  Expected: "(" + cur_val + ")"
TEST_F(FuzzTest, TestFuzzWithRestarts1) {
  RunFuzzCase({
      {TEST_INSERT, 1},
      {TEST_FLUSH_OPS, 0},
      {TEST_FLUSH_TABLET, 0},
      {TEST_UPDATE, 1},
      {TEST_RESTART_TS, 0},
      {TEST_FLUSH_OPS, 0},
      {TEST_FLUSH_DELTAS, 0},
      {TEST_INSERT, 0},
      {TEST_DELETE, 1},
      {TEST_INSERT, 1},
      {TEST_FLUSH_OPS, 0},
      {TEST_FLUSH_TABLET, 0},
      {TEST_RESTART_TS, 0},
      {TEST_MINOR_COMPACT_DELTAS, 0},
      {TEST_COMPACT_TABLET, 0},
      {TEST_UPDATE, 1},
      {TEST_FLUSH_OPS, 0}
    });
}

// Previously caused KUDU-1341:
// deltafile.cc:134] Check failed: last_key_.CompareTo<UNDO>(key) <= 0 must
// insert undo deltas in sorted order (ascending key, then descending ts):
// got key (row 1@tx5965182714017464320) after (row 1@tx5965182713875046400)
TEST_F(FuzzTest, TestFuzzWithRestarts2) {
  RunFuzzCase({
      {TEST_INSERT, 0},
      {TEST_FLUSH_OPS, 0},
      {TEST_FLUSH_TABLET, 0},
      {TEST_DELETE, 0},
      {TEST_FLUSH_OPS, 0},
      {TEST_FLUSH_DELTAS, 0},
      {TEST_RESTART_TS, 0},
      {TEST_INSERT, 1},
      {TEST_INSERT, 0},
      {TEST_FLUSH_OPS, 0},
      {TEST_FLUSH_TABLET, 0},
      {TEST_DELETE, 0},
      {TEST_INSERT, 0},
      {TEST_UPDATE, 1},
      {TEST_FLUSH_OPS, 0},
      {TEST_FLUSH_TABLET, 0},
      {TEST_FLUSH_DELTAS, 0},
      {TEST_RESTART_TS, 0},
      {TEST_UPDATE, 1},
      {TEST_DELETE, 1},
      {TEST_FLUSH_OPS, 0},
      {TEST_RESTART_TS, 0},
      {TEST_INSERT, 1},
      {TEST_FLUSH_OPS, 0},
      {TEST_FLUSH_TABLET, 0},
      {TEST_RESTART_TS, 0},
      {TEST_COMPACT_TABLET, 0}
    });
}

} // namespace tablet
} // namespace kudu
