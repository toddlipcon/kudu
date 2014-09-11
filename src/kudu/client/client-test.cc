// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <tr1/memory>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/encoded_key.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/row_result.h"
#include "kudu/client/write_op.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master-test-util.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_data_block_fsync);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_bool(use_hybrid_clock);
DECLARE_int32(max_clock_sync_error_usec);

DEFINE_int32(test_scan_num_rows, 1000, "Number of rows to insert and scan");

using std::tr1::shared_ptr;

namespace kudu {
namespace client {


using base::subtle::Atomic32;
using base::subtle::NoBarrier_Load;
using base::subtle::NoBarrier_Store;
using base::subtle::NoBarrier_AtomicIncrement;
using master::CatalogManager;
using master::GetTableLocationsRequestPB;
using master::GetTableLocationsResponsePB;
using master::TabletLocationsPB;
using tablet::TabletPeer;
using tserver::MiniTabletServer;

const uint32_t kNonNullDefault = 12345;

class ClientTest : public KuduTest {
 public:
  ClientTest()
    : schema_(boost::assign::list_of
              (KuduColumnSchema("key", KuduColumnSchema::UINT32))
              (KuduColumnSchema("int_val", KuduColumnSchema::UINT32))
              (KuduColumnSchema("string_val", KuduColumnSchema::STRING, true))
              (KuduColumnSchema("non_null_with_default", KuduColumnSchema::UINT32, false,
                                &kNonNullDefault)),
              1) {
    FLAGS_enable_data_block_fsync = false; // Keep unit tests fast.
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // Reduce the TS<->Master heartbeat interval
    FLAGS_heartbeat_interval_ms = 10;

    // Use the hybrid clock for client tests
    FLAGS_use_hybrid_clock = true;

    // increase the max error tolerance, for tests, to 10 seconds.
    FLAGS_max_clock_sync_error_usec = 10000000;

    // Start minicluster and wait for tablet servers to connect to master.
    cluster_.reset(new MiniCluster(env_.get(), MiniClusterOptions()));
    ASSERT_STATUS_OK(cluster_->Start());

    // Connect to the cluster.
    ASSERT_STATUS_OK(KuduClientBuilder()
                     .master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
                     .Build(&client_));

    // Set up two test tables inside the server. One has a single split, just so
    // that code is exercised a little more.
    ASSERT_STATUS_OK(client_->NewTableCreator()
                     ->table_name(kTableName)
                     .schema(&schema_)
                     .split_keys(GenerateSplitKeys())
                     .Create());
    ASSERT_STATUS_OK(client_->NewTableCreator()
                     ->table_name(kTable2Name)
                     .schema(&schema_)
                     .Create());

    ASSERT_STATUS_OK(client_->OpenTable(kTableName, &client_table_));
    ASSERT_STATUS_OK(client_->OpenTable(kTable2Name, &client_table2_));
  }

  // Generate a set of split keys for tablets used in this test.
  vector<string> GenerateSplitKeys() {
    vector<string> keys;
    KuduEncodedKeyBuilder builder(schema_);
    int val = 9;
    builder.AddColumnKey(&val);
    gscoped_ptr<KuduEncodedKey> key(builder.BuildEncodedKey());
    keys.push_back(key->ToString());
    return keys;
  }

  virtual void TearDown() OVERRIDE {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    KuduTest::TearDown();
  }

 protected:

  static const char *kTableName;
  static const char *kTable2Name;
  static const uint32_t kNoBound;

  string GetFirstTabletId(KuduTable* table) {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_name(table->name());
    CHECK_OK(cluster_->mini_master()->master()->catalog_manager()->GetTableLocations(
        &req, &resp));
    CHECK(resp.tablet_locations_size() > 0);
    return resp.tablet_locations(0).tablet_id();
  }

  void CheckNoRpcOverflow() {
    ASSERT_EQ(0, cluster_->mini_tablet_server(0)->server()->rpc_server()->
        service_pool("kudu.tserver.TabletServerService")->RpcsQueueOverflowMetric()->value());
  }

  // Inserts 'num_rows' test rows via RPC.
  void InsertTestRows(KuduTable* table, int num_rows, int first_row = 0) {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(5000);
    for (int i = first_row; i < num_rows + first_row; i++) {
      gscoped_ptr<KuduInsert> insert(BuildTestRow(table, i));
      ASSERT_STATUS_OK(session->Apply(insert.Pass()));
    }
    FlushSessionOrDie(session);
    ASSERT_NO_FATAL_FAILURE(CheckNoRpcOverflow());
  }

  void UpdateTestRows(KuduTable* table, int lo, int hi) {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(5000);
    for (int i = lo; i < hi; i++) {
      gscoped_ptr<KuduUpdate> update(UpdateTestRow(table, i));
      ASSERT_STATUS_OK(session->Apply(update.Pass()));
    }
    FlushSessionOrDie(session);
    ASSERT_NO_FATAL_FAILURE(CheckNoRpcOverflow());
  }

  void DeleteTestRows(KuduTable* table, int lo, int hi) {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(5000);
    for (int i = lo; i < hi; i++) {
      gscoped_ptr<KuduDelete> del(DeleteTestRow(table, i));
      ASSERT_STATUS_OK(session->Apply(del.Pass()))
    }
    FlushSessionOrDie(session);
    ASSERT_NO_FATAL_FAILURE(CheckNoRpcOverflow());
  }

  gscoped_ptr<KuduInsert> BuildTestRow(KuduTable* table, int index) {
    gscoped_ptr<KuduInsert> insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    CHECK_OK(row->SetUInt32(0, index));
    CHECK_OK(row->SetUInt32(1, index * 2));
    CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello %d", index))));
    CHECK_OK(row->SetUInt32(3, index * 3));
    return insert.Pass();
  }

  gscoped_ptr<KuduUpdate> UpdateTestRow(KuduTable* table, int index) {
    gscoped_ptr<KuduUpdate> update = table->NewUpdate();
    KuduPartialRow* row = update->mutable_row();
    CHECK_OK(row->SetUInt32(0, index));
    CHECK_OK(row->SetUInt32(1, index * 2 + 1));
    CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello again %d", index))));
    return update.Pass();
  }

  gscoped_ptr<KuduDelete> DeleteTestRow(KuduTable* table, int index) {
    gscoped_ptr<KuduDelete> del = table->NewDelete();
    KuduPartialRow* row = del->mutable_row();
    CHECK_OK(row->SetUInt32(0, index));
    return del.Pass();
  }

  void DoTestScanWithoutPredicates() {
    KuduSchema projection = schema_.CreateKeyProjection();
    KuduScanner scanner(client_table_.get());
    ASSERT_STATUS_OK(scanner.SetProjection(&projection));
    LOG_TIMING(INFO, "Scanning with no predicates") {
      ASSERT_STATUS_OK(scanner.Open());

      ASSERT_TRUE(scanner.HasMoreRows());
      vector<KuduRowResult> rows;
      uint64_t sum = 0;
      while (scanner.HasMoreRows()) {
        ASSERT_STATUS_OK(scanner.NextBatch(&rows));

        BOOST_FOREACH(const KuduRowResult& row, rows) {
          uint32_t value;
          ASSERT_STATUS_OK(row.GetUInt32(0, &value));
          sum += value;
        }
        rows.clear();
      }
      // The sum should be the sum of the arithmetic series from
      // 0..FLAGS_test_scan_num_rows-1
      uint64_t expected = implicit_cast<uint64_t>(FLAGS_test_scan_num_rows) *
                            (0 + (FLAGS_test_scan_num_rows - 1)) / 2;
      ASSERT_EQ(expected, sum);
    }
  }

  void DoTestScanWithStringPredicate() {
    KuduScanner scanner(client_table_.get());
    Slice lower("hello 2");
    Slice upper("hello 3");
    KuduColumnRangePredicate pred(schema_.Column(2), &lower, &upper);
    ASSERT_STATUS_OK(scanner.AddConjunctPredicate(pred));

    LOG_TIMING(INFO, "Scanning with string predicate") {
      ASSERT_STATUS_OK(scanner.Open());

      ASSERT_TRUE(scanner.HasMoreRows());
      vector<KuduRowResult> rows;
      while (scanner.HasMoreRows()) {
        ASSERT_STATUS_OK(scanner.NextBatch(&rows));

        BOOST_FOREACH(const KuduRowResult& row, rows) {
          Slice s;
          ASSERT_STATUS_OK(row.GetString(2, &s));
          if (!s.starts_with("hello 2") && !s.starts_with("hello 3")) {
            FAIL() << row.ToString();
          }
        }
        rows.clear();
      }
    }
  }

  void DoTestScanWithKeyPredicate() {
    KuduScanner scanner(client_table_.get());
    uint32_t lower = 5;
    uint32_t upper = 10;
    KuduColumnRangePredicate pred(schema_.Column(0), &lower, &upper);
    ASSERT_STATUS_OK(scanner.AddConjunctPredicate(pred));

    LOG_TIMING(INFO, "Scanning with key predicate") {
      ASSERT_STATUS_OK(scanner.Open());

      ASSERT_TRUE(scanner.HasMoreRows());
      vector<KuduRowResult> rows;
      while (scanner.HasMoreRows()) {
        ASSERT_STATUS_OK(scanner.NextBatch(&rows));

        BOOST_FOREACH(const KuduRowResult& row, rows) {
          uint32_t k;
          ASSERT_STATUS_OK(row.GetUInt32(0, &k));
          if (k < 5 || k > 10) {
            FAIL() << row.ToString();
          }
        }
        rows.clear();
      }
    }
  }

  int CountRowsFromClient(KuduTable* table) {
    return CountRowsFromClient(table, kNoBound, kNoBound);
  }

  int CountRowsFromClient(KuduTable* table, uint32_t lower_bound, uint32_t upper_bound) {
    return CountRowsFromClient(table, KuduClient::LEADER_ONLY, lower_bound, upper_bound);
  }

  int CountRowsFromClient(KuduTable* table, KuduClient::ReplicaSelection selection,
                          uint32_t lower_bound, uint32_t upper_bound) {
    KuduScanner scanner(table);
    CHECK_OK(scanner.SetSelection(selection));
    KuduSchema empty_projection(vector<KuduColumnSchema>(), 0);
    CHECK_OK(scanner.SetProjection(&empty_projection));
    if (lower_bound != kNoBound && upper_bound != kNoBound) {
      KuduColumnRangePredicate pred(table->schema().Column(0), &lower_bound, &upper_bound);
      CHECK_OK(scanner.AddConjunctPredicate(pred));
    } else if (lower_bound != kNoBound) {
      KuduColumnRangePredicate pred(table->schema().Column(0), &lower_bound, NULL);
      CHECK_OK(scanner.AddConjunctPredicate(pred));
    } else if (upper_bound != kNoBound) {
      KuduColumnRangePredicate pred(table->schema().Column(0), NULL, &upper_bound);
      CHECK_OK(scanner.AddConjunctPredicate(pred));
    }
    CHECK_OK(scanner.Open());
    int count = 0;
    vector<KuduRowResult> rows;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&rows));
      count += rows.size();
      rows.clear();
    }
    return count;
  }

  void ScanRowsToStrings(KuduTable* table, vector<string>* row_strings) {
    row_strings->clear();
    KuduScanner scanner(table);
    ASSERT_STATUS_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
    ASSERT_STATUS_OK(scanner.Open());
    vector<KuduRowResult> rows;
    while (scanner.HasMoreRows()) {
      ASSERT_STATUS_OK(scanner.NextBatch(&rows));
      BOOST_FOREACH(const KuduRowResult& row, rows) {
        row_strings->push_back(row.ToString());
      }
      rows.clear();
    }
  }

  void CreateReplicatedTable(const string& table_name, int num_replicas,
                             scoped_refptr<KuduTable>* table) {
    // Add more tablet servers to satisfy all replicas.
    // We assume there's one TS to begin with.
    for (int i = 0; i < num_replicas - 1; i++) {
      ASSERT_STATUS_OK(cluster_->AddTabletServer());
    }
    ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(num_replicas));
    ASSERT_STATUS_OK(client_->NewTableCreator()
                     ->table_name(table_name)
                     .schema(&schema_)
                     .num_replicas(num_replicas)
                     .split_keys(GenerateSplitKeys())
                     .Create());
    ASSERT_STATUS_OK(client_->OpenTable(table_name, table));
  }

  void DoApplyWithoutFlushTest(int sleep_micros);

  enum WhichServerToKill {
    DEAD_MASTER,
    DEAD_TSERVER
  };
  void DoTestWriteWithDeadServer(WhichServerToKill which);

  KuduSchema schema_;

  gscoped_ptr<MiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  scoped_refptr<KuduTable> client_table_;
  scoped_refptr<KuduTable> client_table2_;
};

const char *ClientTest::kTableName = "client-testtb";
const char *ClientTest::kTable2Name = "client-testtb2";
const uint32_t ClientTest::kNoBound = kuint32max;

TEST_F(ClientTest, TestBadTable) {
  scoped_refptr<KuduTable> t;
  Status s = client_->OpenTable("xxx-does-not-exist", &t);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "Not found: The table does not exist");
}

// Test that, if the master is down, we get an appropriate error
// message.
TEST_F(ClientTest, TestMasterDown) {
  cluster_->mini_master()->Shutdown();
  scoped_refptr<KuduTable> t;
  Status s = client_->OpenTable("other-tablet", &t);
  ASSERT_TRUE(s.IsNetworkError());
  const string& msg = s.ToString();
  ASSERT_TRUE(msg.find("Connection refused") != string::npos ||
              msg.find("EOF") != string::npos ||
              msg.find("Connection reset") != string::npos)
      << "Unexpected client error message: " << msg;
}

TEST_F(ClientTest, TestScan) {
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(
      client_table_.get(), FLAGS_test_scan_num_rows));

  // Scan after insert
  DoTestScanWithoutPredicates();
  DoTestScanWithStringPredicate();
  DoTestScanWithKeyPredicate();

  // Scan after update
  UpdateTestRows(client_table_.get(), 0, FLAGS_test_scan_num_rows);
  DoTestScanWithKeyPredicate();

  // Scan after delete half
  DeleteTestRows(client_table_.get(), 0, FLAGS_test_scan_num_rows / 2);
  DoTestScanWithKeyPredicate();

  // Scan after delete all
  DeleteTestRows(client_table_.get(), FLAGS_test_scan_num_rows / 2 + 1,
                 FLAGS_test_scan_num_rows);
  DoTestScanWithKeyPredicate();

  // Scan after re-insert
  InsertTestRows(client_table_.get(), 1);
  DoTestScanWithKeyPredicate();
}

TEST_F(ClientTest, TestScanAtSnapshot) {
  int half_the_rows = FLAGS_test_scan_num_rows / 2;

  // Insert half the rows
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(client_table_.get(),
                                         half_the_rows));

  // get the time from the server and transform to micros disregarding any
  // logical values (we shouldn't have any with a single server anyway);
  int64_t ts = server::HybridClock::GetPhysicalValue(
      cluster_->mini_tablet_server(0)->server()->clock()->Now());

  // Insert the second half of the rows
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(client_table_.get(),
                                         half_the_rows, half_the_rows));

  KuduScanner scanner(client_table_.get());
  ASSERT_STATUS_OK(scanner.Open());
  vector<KuduRowResult> rows;
  uint64_t sum = 0;

  // Do a "normal", READ_LATEST scan
  while (scanner.HasMoreRows()) {
    ASSERT_STATUS_OK(scanner.NextBatch(&rows));
    sum += rows.size();
    rows.clear();
  }

  ASSERT_EQ(FLAGS_test_scan_num_rows, sum);

  // Now close the scanner and perform a scan at 'ts'
  scanner.Close();
  ASSERT_STATUS_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  ASSERT_STATUS_OK(scanner.SetSnapshot(ts));
  ASSERT_STATUS_OK(scanner.Open());

  sum = 0;

  while (scanner.HasMoreRows()) {
    ASSERT_STATUS_OK(scanner.NextBatch(&rows));
    sum += rows.size();
    rows.clear();
  }

  ASSERT_EQ(half_the_rows, sum);

}

TEST_F(ClientTest, TestScanMultiTablet) {
  // 5 tablets, each with 10 rows worth of space.
  KuduEncodedKeyBuilder key_builder(schema_);
  gscoped_ptr<KuduEncodedKey> key;
  vector<string> keys;
  for (int i = 1; i < 5; i++) {
    int val = i * 10;
    key_builder.Reset();
    key_builder.AddColumnKey(&val);
    key.reset(key_builder.BuildEncodedKey());
    keys.push_back(key->ToString());
  }
  ASSERT_STATUS_OK(client_->NewTableCreator()
                   ->table_name("TestScanMultiTablet")
                   .schema(&schema_)
                   .split_keys(keys)
                   .Create());

  scoped_refptr<KuduTable> table;
  ASSERT_STATUS_OK(client_->OpenTable("TestScanMultiTablet", &table));

  // Insert rows with keys 12, 13, 15, 17, 22, 23, 25, 27...47 into each
  // tablet, except the first which is empty.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);
  for (int i = 1; i < 5; i++) {
    gscoped_ptr<KuduInsert> insert;
    insert = BuildTestRow(table.get(), 2 + (i * 10));
    ASSERT_STATUS_OK(session->Apply(insert.Pass()));
    insert = BuildTestRow(table.get(), 3 + (i * 10));
    ASSERT_STATUS_OK(session->Apply(insert.Pass()));
    insert = BuildTestRow(table.get(), 5 + (i * 10));
    ASSERT_STATUS_OK(session->Apply(insert.Pass()));
    insert = BuildTestRow(table.get(), 7 + (i * 10));
    ASSERT_STATUS_OK(session->Apply(insert.Pass()));
  }
  FlushSessionOrDie(session);

  // Run through various scans.
  ASSERT_EQ(16, CountRowsFromClient(table.get(), kNoBound, kNoBound));
  ASSERT_EQ(3, CountRowsFromClient(table.get(), kNoBound, 15));
  ASSERT_EQ(9, CountRowsFromClient(table.get(), 27, kNoBound));
  ASSERT_EQ(3, CountRowsFromClient(table.get(), 0, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 0, 10));
  ASSERT_EQ(4, CountRowsFromClient(table.get(), 0, 20));
  ASSERT_EQ(8, CountRowsFromClient(table.get(), 0, 30));
  ASSERT_EQ(6, CountRowsFromClient(table.get(), 14, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 30, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 50, kNoBound));

  // Update every other row
  for (int i = 1; i < 5; ++i) {
    gscoped_ptr<KuduUpdate> update;
    update = UpdateTestRow(table.get(), 2 + i * 10);
    ASSERT_STATUS_OK(session->Apply(update.Pass()));
    update = UpdateTestRow(table.get(), 5 + i * 10);
    ASSERT_STATUS_OK(session->Apply(update.Pass()));
  }
  FlushSessionOrDie(session);

  // Check all counts the same (make sure updates don't change # of rows)
  ASSERT_EQ(16, CountRowsFromClient(table.get(), kNoBound, kNoBound));
  ASSERT_EQ(3, CountRowsFromClient(table.get(), kNoBound, 15));
  ASSERT_EQ(9, CountRowsFromClient(table.get(), 27, kNoBound));
  ASSERT_EQ(3, CountRowsFromClient(table.get(), 0, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 0, 10));
  ASSERT_EQ(4, CountRowsFromClient(table.get(), 0, 20));
  ASSERT_EQ(8, CountRowsFromClient(table.get(), 0, 30));
  ASSERT_EQ(6, CountRowsFromClient(table.get(), 14, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 30, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 50, kNoBound));

  // Delete half the rows
  for (int i = 1; i < 5; ++i) {
    gscoped_ptr<KuduDelete> del;
    del = DeleteTestRow(table.get(), 5 + i*10);
    ASSERT_STATUS_OK(session->Apply(del.Pass()));
    del = DeleteTestRow(table.get(), 7 + i*10);
    ASSERT_STATUS_OK(session->Apply(del.Pass()));
  }
  FlushSessionOrDie(session);

  // Check counts changed accordingly
  ASSERT_EQ(8, CountRowsFromClient(table.get(), kNoBound, kNoBound));
  ASSERT_EQ(2, CountRowsFromClient(table.get(), kNoBound, 15));
  ASSERT_EQ(4, CountRowsFromClient(table.get(), 27, kNoBound));
  ASSERT_EQ(2, CountRowsFromClient(table.get(), 0, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 0, 10));
  ASSERT_EQ(2, CountRowsFromClient(table.get(), 0, 20));
  ASSERT_EQ(4, CountRowsFromClient(table.get(), 0, 30));
  ASSERT_EQ(2, CountRowsFromClient(table.get(), 14, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 30, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 50, kNoBound));

  // Delete rest of rows
  for (int i = 1; i < 5; ++i) {
    gscoped_ptr<KuduDelete> del;
    del = DeleteTestRow(table.get(), 2 + i*10);
    ASSERT_STATUS_OK(session->Apply(del.Pass()));
    del = DeleteTestRow(table.get(), 3 + i*10);
    ASSERT_STATUS_OK(session->Apply(del.Pass()));
  }
  FlushSessionOrDie(session);

  // Check counts changed accordingly
  ASSERT_EQ(0, CountRowsFromClient(table.get(), kNoBound, kNoBound));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), kNoBound, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 27, kNoBound));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 0, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 0, 10));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 0, 20));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 0, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 14, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 30, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), 50, kNoBound));
}

TEST_F(ClientTest, TestScanEmptyTable) {
  KuduScanner scanner(client_table_.get());
  KuduSchema empty_projection(vector<KuduColumnSchema>(), 0);
  ASSERT_STATUS_OK(scanner.SetProjection(&empty_projection));
  ASSERT_STATUS_OK(scanner.Open());

  // There are two tablets in the table, both empty. Until we scan to
  // the last tablet, HasMoreRows will return true (because it doesn't
  // know whether there's data in subsequent tablets).
  ASSERT_TRUE(scanner.HasMoreRows());
  vector<KuduRowResult> rows;
  ASSERT_STATUS_OK(scanner.NextBatch(&rows));
  ASSERT_TRUE(rows.empty());
  ASSERT_FALSE(scanner.HasMoreRows());
}

// Test scanning with an empty projection. This should yield an empty
// row block with the proper number of rows filled in. Impala issues
// scans like this in order to implement COUNT(*).
TEST_F(ClientTest, TestScanEmptyProjection) {
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(client_table_.get(),
                                         FLAGS_test_scan_num_rows));
  KuduSchema empty_projection(vector<KuduColumnSchema>(), 0);
  KuduScanner scanner(client_table_.get());
  ASSERT_STATUS_OK(scanner.SetProjection(&empty_projection));
  LOG_TIMING(INFO, "Scanning with no projected columns") {
    ASSERT_STATUS_OK(scanner.Open());

    ASSERT_TRUE(scanner.HasMoreRows());
    vector<KuduRowResult> rows;
    uint64_t count = 0;
    while (scanner.HasMoreRows()) {
      ASSERT_STATUS_OK(scanner.NextBatch(&rows));
      count += rows.size();
      rows.clear();
    }
    ASSERT_EQ(FLAGS_test_scan_num_rows, count);
  }
}


// Test a scan where we have a predicate on a key column that is not
// in the projection.
TEST_F(ClientTest, TestScanPredicateKeyColNotProjected) {
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(client_table_.get(),
                                         FLAGS_test_scan_num_rows));
  KuduScanner scanner(client_table_.get());
  KuduSchema no_key_projection(boost::assign::list_of
                               (schema_.Column(1)), 0);
  ASSERT_STATUS_OK(scanner.SetProjection(&no_key_projection));
  uint32_t lower = 5;
  uint32_t upper = 10;
  KuduColumnRangePredicate pred(schema_.Column(0), &lower, &upper);
  ASSERT_STATUS_OK(scanner.AddConjunctPredicate(pred));

  size_t nrows = 0;
  uint32_t curr_key = lower;
  LOG_TIMING(INFO, "Scanning with predicate columns not projected") {
    ASSERT_STATUS_OK(scanner.Open());

    ASSERT_TRUE(scanner.HasMoreRows());
    vector<KuduRowResult> rows;
    while (scanner.HasMoreRows()) {
      ASSERT_STATUS_OK(scanner.NextBatch(&rows));

      BOOST_FOREACH(const KuduRowResult& row, rows) {
        uint32_t val;
        ASSERT_STATUS_OK(row.GetUInt32(0, &val));
        ASSERT_EQ(curr_key * 2, val);
        nrows++;
        curr_key++;
      }
      rows.clear();
    }
  }
  ASSERT_EQ(nrows, 6);
}

// Test a scan where we have a predicate on a non-key column that is
// not in the projection.
TEST_F(ClientTest, TestScanPredicateNonKeyColNotProjected) {
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(client_table_.get(),
                                         FLAGS_test_scan_num_rows));
  KuduScanner scanner(client_table_.get());
  KuduSchema key_projection = schema_.CreateKeyProjection();

  uint32_t lower = 10;
  uint32_t upper = 20;
  KuduColumnRangePredicate pred(schema_.Column(1), &lower, &upper);
  ASSERT_STATUS_OK(scanner.AddConjunctPredicate(pred));

  size_t nrows = 0;
  uint32_t curr_key = lower;

  ASSERT_STATUS_OK(scanner.SetProjection(&key_projection));

  LOG_TIMING(INFO, "Scanning with predicate columns not projected") {
    ASSERT_STATUS_OK(scanner.Open());

    ASSERT_TRUE(scanner.HasMoreRows());
    vector<KuduRowResult> rows;
    while (scanner.HasMoreRows()) {
      ASSERT_STATUS_OK(scanner.NextBatch(&rows));

      BOOST_FOREACH(const KuduRowResult& row, rows) {
        uint32_t val;
        ASSERT_STATUS_OK(row.GetUInt32(0, &val));
        ASSERT_EQ(curr_key / 2, val);
        nrows++;
        curr_key += 2;
      }
      rows.clear();
    }
  }
  ASSERT_EQ(nrows, 6);
}

static void AssertScannersDisappear(const tserver::ScannerManager* manager) {
  // The Close call is async, so we may have to loop a bit until we see it disappear.
  // This loops for ~10sec. Typically it succeeds in only a few milliseconds.
  int i = 0;
  for (i = 0; i < 500; i++) {
    if (manager->CountActiveScanners() == 0) {
      LOG(INFO) << "Successfully saw scanner close on iteration " << i;
      return;
    }
    // Sleep 2ms on first few times through, then longer on later iterations.
    usleep(i < 10 ? 2000 : 20000);
  }
  FAIL() << "Waited too long for the scanner to close";
}

// Test cleanup of scanners on the server side when closed.
TEST_F(ClientTest, TestCloseScanner) {
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(client_table_.get(), 10));

  const tserver::ScannerManager* manager =
    cluster_->mini_tablet_server(0)->server()->scanner_manager();
  // Open the scanner, make sure it gets closed right away
  {
    SCOPED_TRACE("Implicit close");
    KuduScanner scanner(client_table_.get());
    ASSERT_STATUS_OK(scanner.Open());
    ASSERT_EQ(0, manager->CountActiveScanners());
    scanner.Close();
    AssertScannersDisappear(manager);
  }

  // Open the scanner, make sure we see 1 registered scanner.
  {
    SCOPED_TRACE("Explicit close");
    KuduScanner scanner(client_table_.get());
    ASSERT_STATUS_OK(scanner.SetBatchSizeBytes(0)); // won't return data on open
    ASSERT_STATUS_OK(scanner.Open());
    ASSERT_EQ(1, manager->CountActiveScanners());
    scanner.Close();
    AssertScannersDisappear(manager);
  }

  {
    SCOPED_TRACE("Close when out of scope");
    {
      KuduScanner scanner(client_table_.get());
      ASSERT_STATUS_OK(scanner.SetBatchSizeBytes(0));
      ASSERT_STATUS_OK(scanner.Open());
      ASSERT_EQ(1, manager->CountActiveScanners());
    }
    // Above scanner went out of scope, so the destructor should close asynchronously.
    AssertScannersDisappear(manager);
  }
}

// Simplest case of inserting through the client API: a single row
// with manual batching.
TEST_F(ClientTest, TestInsertSingleRowManualBatch) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_FALSE(session->HasPendingOperations());

  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  gscoped_ptr<KuduInsert> insert = client_table_->NewInsert();
  // Try inserting without specifying a key: should fail.
  ASSERT_STATUS_OK(insert->mutable_row()->SetUInt32("int_val", 54321));
  ASSERT_STATUS_OK(insert->mutable_row()->SetStringCopy("string_val", "hello world"));

  KuduInsert* ptr = insert.get();
  Status s = session->Apply(insert.Pass());
  ASSERT_EQ("Illegal state: Key not specified: "
            "INSERT uint32 int_val=54321, string string_val=hello world",
            s.ToString());

  // Get error
  ASSERT_EQ(session->CountPendingErrors(), 1) << "Should report bad key to error container";
  vector<KuduError*> errs;
  ElementDeleter del_errs(&errs);
  bool overflow;
  session->GetPendingErrors(&errs, &overflow);
  ASSERT_EQ(errs.size(), 1);
  KuduWriteOperation* failed_op = errs.front()->release_failed_op().release();
  ASSERT_EQ(failed_op, ptr) << "Should be able to retrieve failed operation";
  insert.reset(ptr);

  // Retry
  ASSERT_STATUS_OK(insert->mutable_row()->SetUInt32("key", 12345));
  ASSERT_STATUS_OK(session->Apply(insert.Pass()));
  ASSERT_TRUE(insert == NULL) << "Successful insert should take ownership";
  ASSERT_TRUE(session->HasPendingOperations()) << "Should be pending until we Flush";

  FlushSessionOrDie(session);
}

static Status ApplyInsertToSession(KuduSession* session,
                                   const scoped_refptr<KuduTable>& table,
                                   int row_key,
                                   int int_val,
                                   const char* string_val) {
  gscoped_ptr<KuduInsert> insert = table->NewInsert();
  RETURN_NOT_OK(insert->mutable_row()->SetUInt32("key", row_key));
  RETURN_NOT_OK(insert->mutable_row()->SetUInt32("int_val", int_val));
  RETURN_NOT_OK(insert->mutable_row()->SetStringCopy("string_val", string_val));
  return session->Apply(insert.Pass());
}

static Status ApplyUpdateToSession(KuduSession* session,
                                   const scoped_refptr<KuduTable>& table,
                                   int row_key,
                                   int int_val) {
  gscoped_ptr<KuduUpdate> update = table->NewUpdate();
  RETURN_NOT_OK(update->mutable_row()->SetUInt32("key", row_key));
  RETURN_NOT_OK(update->mutable_row()->SetUInt32("int_val", int_val));
  return session->Apply(update.Pass());
}

static Status ApplyDeleteToSession(KuduSession* session,
                                   const scoped_refptr<KuduTable>& table,
                                   int row_key) {
  gscoped_ptr<KuduDelete> del = table->NewDelete();
  RETURN_NOT_OK(del->mutable_row()->SetUInt32("key", row_key));
  return session->Apply(del.Pass());
}

// Test which does an async flush and then drops the reference
// to the Session. This should still call the callback.
TEST_F(ClientTest, TestAsyncFlushResponseAfterSessionDropped) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "row"));
  Synchronizer s;
  session->FlushAsync(s.AsStatusCallback());
  session.reset();
  ASSERT_STATUS_OK(s.Wait());

  // Try again, this time with an error response (trying to re-insert the same row).
  s.Reset();
  session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "row"));
  ASSERT_EQ(1, session->CountBufferedOperations());
  session->FlushAsync(s.AsStatusCallback());
  ASSERT_EQ(0, session->CountBufferedOperations());
  session.reset();
  ASSERT_FALSE(s.Wait().ok());
}

// Test which sends multiple batches through the same session, each of which
// contains multiple rows spread across multiple tablets.
TEST_F(ClientTest, TestMultipleMultiRowManualBatches) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  const int kNumBatches = 5;
  const int kRowsPerBatch = 10;

  int row_key = 0;

  for (int batch_num = 0; batch_num < kNumBatches; batch_num++) {
    for (int i = 0; i < kRowsPerBatch; i++) {
      ASSERT_STATUS_OK(ApplyInsertToSession(
                         session.get(),
                         (row_key % 2 == 0) ? client_table_ : client_table2_,
                         row_key, row_key * 10, "hello world"));
      row_key++;
    }
    ASSERT_TRUE(session->HasPendingOperations()) << "Should be pending until we Flush";
    FlushSessionOrDie(session);
    ASSERT_FALSE(session->HasPendingOperations()) << "Should have no more pending ops after flush";
  }

  const int kNumRowsPerTablet = kNumBatches * kRowsPerBatch / 2;
  ASSERT_EQ(kNumRowsPerTablet, CountRowsFromClient(client_table_.get()));
  ASSERT_EQ(kNumRowsPerTablet, CountRowsFromClient(client_table2_.get()));

  // Verify the data looks right.
  vector<string> rows;
  ScanRowsToStrings(client_table_.get(), &rows);
  std::sort(rows.begin(), rows.end());
  ASSERT_EQ(kNumRowsPerTablet, rows.size());
  ASSERT_EQ("(uint32 key=0, uint32 int_val=0, string string_val=hello world, "
            "uint32 non_null_with_default=12345)"
            , rows[0]);
}

// Test a batch where one of the inserted rows succeeds while another
// fails.
TEST_F(ClientTest, TestBatchWithPartialError) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Insert a row with key "1"
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "original row"));
  FlushSessionOrDie(session);

  // Now make a batch that has key "1" (which will fail) along with
  // key "2" which will succeed. Flushing should return an error.
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "Attempted dup"));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 2, 1, "Should succeed"));
  Status s = session->Flush();
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "Some errors occurred");

  // Fetch and verify the reported error.
  ASSERT_EQ(1, session->CountPendingErrors());
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors.size());
  ASSERT_TRUE(errors[0]->status().IsAlreadyPresent());
  ASSERT_EQ(errors[0]->failed_op().ToString(),
            "INSERT uint32 key=1, uint32 int_val=1, string string_val=Attempted dup");

  // Verify that the other row was successfully inserted
  vector<string> rows;
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(2, rows.size());
  std::sort(rows.begin(), rows.end());
  ASSERT_EQ("(uint32 key=1, uint32 int_val=1, string string_val=original row, "
            "uint32 non_null_with_default=12345)", rows[0]);
  ASSERT_EQ("(uint32 key=2, uint32 int_val=1, string string_val=Should succeed, "
            "uint32 non_null_with_default=12345)", rows[1]);
}

// Test flushing an empty batch (should be a no-op).
TEST_F(ClientTest, TestEmptyBatch) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  FlushSessionOrDie(session);
}

void ClientTest::DoTestWriteWithDeadServer(WhichServerToKill which) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Shut down the server.
  switch (which) {
    case DEAD_MASTER:
      cluster_->mini_master()->Shutdown();
      break;
    case DEAD_TSERVER:
      cluster_->mini_tablet_server(0)->Shutdown();
      break;
  }

  // Try a write.
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "x"));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_EQ(1, session->CountPendingErrors());

  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors.size());
  ASSERT_TRUE(errors[0]->status().IsNetworkError());
  ASSERT_EQ(errors[0]->failed_op().ToString(),
            "INSERT uint32 key=1, uint32 int_val=1, string string_val=x");
}

// Test error handling cases where the master is down (tablet resolution fails)
TEST_F(ClientTest, TestWriteWithDeadMaster) {
  DoTestWriteWithDeadServer(DEAD_MASTER);
}

// Test error handling when the TS is down (actual write fails its RPC)
TEST_F(ClientTest, TestWriteWithDeadTabletServer) {
  DoTestWriteWithDeadServer(DEAD_TSERVER);
}

void ClientTest::DoApplyWithoutFlushTest(int sleep_micros) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "x"));
  usleep(sleep_micros);
  session.reset(); // should not crash!

  // Should have no rows.
  vector<string> rows;
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(0, rows.size());
}


// Applies some updates to the session, and then drops the reference to the
// Session before flushing. Makes sure that the tablet resolution callbacks
// properly deal with the session disappearing underneath.
//
// This test doesn't sleep between applying the operations and dropping the
// reference, in hopes that the reference will be dropped while DNS is still
// in-flight, etc.
TEST_F(ClientTest, TestApplyToSessionWithoutFlushing_OpsInFlight) {
  DoApplyWithoutFlushTest(0);
}

// Same as the above, but sleeps a little bit after applying the operations,
// so that the operations are already in the per-TS-buffer.
TEST_F(ClientTest, TestApplyToSessionWithoutFlushing_OpsBuffered) {
  DoApplyWithoutFlushTest(10000);
}

// Test that update updates and delete deletes with expected use
TEST_F(ClientTest, TestMutationsWork) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "original row"));
  FlushSessionOrDie(session);

  ASSERT_STATUS_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 2));
  FlushSessionOrDie(session);
  vector<string> rows;
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("(uint32 key=1, uint32 int_val=2, string string_val=original row, "
            "uint32 non_null_with_default=12345)", rows[0]);
  rows.clear();

  ASSERT_STATUS_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  FlushSessionOrDie(session);
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(0, rows.size());
}

TEST_F(ClientTest, TestMutateDeletedRow) {
  vector<string> rows;
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "original row"));
  FlushSessionOrDie(session);
  ASSERT_STATUS_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  FlushSessionOrDie(session);
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(0, rows.size());

  // Attempt update deleted row
  ASSERT_STATUS_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 2));
  Status s = session->Flush();
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "Some errors occurred");
  // verify error
  ASSERT_EQ(1, session->CountPendingErrors());
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors.size());
  ASSERT_EQ(errors[0]->failed_op().ToString(),
            "UPDATE uint32 key=1, uint32 int_val=2");
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(0, rows.size());

  // Attempt delete deleted row
  ASSERT_STATUS_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  s = session->Flush();
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "Some errors occurred");
  // verify error
  ASSERT_EQ(1, session->CountPendingErrors());
  vector<KuduError*> errors2;
  ElementDeleter d2(&errors2);
  session->GetPendingErrors(&errors2, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors2.size());
  ASSERT_EQ(errors2[0]->failed_op().ToString(),
            "DELETE uint32 key=1");
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(0, rows.size());
}

TEST_F(ClientTest, TestMutateNonexistentRow) {
  vector<string> rows;
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Attempt update nonexistent row
  ASSERT_STATUS_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 2));
  Status s = session->Flush();
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "Some errors occurred");
  // verify error
  ASSERT_EQ(1, session->CountPendingErrors());
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors.size());
  ASSERT_EQ(errors[0]->failed_op().ToString(),
            "UPDATE uint32 key=1, uint32 int_val=2");
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(0, rows.size());

  // Attempt delete nonexistent row
  ASSERT_STATUS_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  s = session->Flush();
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "Some errors occurred");
  // verify error
  ASSERT_EQ(1, session->CountPendingErrors());
  vector<KuduError*> errors2;
  ElementDeleter d2(&errors2);
  session->GetPendingErrors(&errors2, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors2.size());
  ASSERT_EQ(errors2[0]->failed_op().ToString(),
            "DELETE uint32 key=1");
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(0, rows.size());
}

TEST_F(ClientTest, TestWriteWithBadColumn) {
  scoped_refptr<KuduTable> table;
  ASSERT_STATUS_OK(client_->OpenTable(kTableName, &table));

  // Try to do a write with the bad schema.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  gscoped_ptr<KuduInsert> insert = table->NewInsert();
  ASSERT_STATUS_OK(insert->mutable_row()->SetUInt32("key", 12345));
  Status s = insert->mutable_row()->SetUInt32("bad_col", 12345);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "No such column: bad_col");
}

// Do a write with a bad schema on the client side. This should make the Prepare
// phase of the write fail, which will result in an error on the RPC response.
TEST_F(ClientTest, TestWriteWithBadSchema) {
  scoped_refptr<KuduTable> table;
  ASSERT_STATUS_OK(client_->OpenTable(kTableName, &table));

  // Remove the 'int_val' column.
  // Now the schema on the client is "old"
  ASSERT_STATUS_OK(client_->NewTableAlterer()
                   ->table_name(kTableName)
                   .drop_column("int_val")
                   .Alter());

  // Try to do a write with the bad schema.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_,
                                        12345, 12345, "x"));
  Status s = session->Flush();
  ASSERT_FALSE(s.ok());

  // Verify the specific error.
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors.size());
  ASSERT_TRUE(errors[0]->status().IsInvalidArgument());
  ASSERT_EQ(errors[0]->status().ToString(),
            "Invalid argument: Client provided column int_val[uint32 NOT NULL] "
            "not present in tablet");
  ASSERT_EQ(errors[0]->failed_op().ToString(),
            "INSERT uint32 key=12345, uint32 int_val=12345, string string_val=x");
}

TEST_F(ClientTest, TestBasicAlterOperations) {
  // test that missing the table's name throws an error
  {
    Status s = client_->NewTableAlterer()
        ->drop_column("key")
        .Alter();
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "Missing table name");
  }

  // test that having no steps throws an error
  {
    Status s = client_->NewTableAlterer()
        ->table_name(kTableName)
        .Alter();
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "No alter steps provided");
  }

  // test that adding a non-nullable column with no default value throws an error
  {
    Status s = client_->NewTableAlterer()
        ->table_name(kTableName)
        .add_column("key", KuduColumnSchema::UINT32, NULL)
        .Alter();
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "A new column must have a default value");
  }

  // test that remove key should throws an error
  {
    Status s = client_->NewTableAlterer()
        ->table_name(kTableName)
        .drop_column("key")
        .Alter();
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "cannot remove a key column");
  }

  // test that renaming a key should throws an error
  {
    Status s = client_->NewTableAlterer()
        ->table_name(kTableName)
        .rename_column("key", "key2")
        .Alter();
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "cannot rename a key column");
  }

  // test that renaming to an already-existing name throws an error
  {
    Status s = client_->NewTableAlterer()
        ->table_name(kTableName)
        .rename_column("int_val", "string_val")
        .Alter();
    ASSERT_TRUE(s.IsAlreadyPresent());
    ASSERT_STR_CONTAINS(s.ToString(), "The column already exists: string_val");
  }

  // Need a tablet peer for the next set of tests.
  string tablet_id = GetFirstTabletId(client_table_.get());
  scoped_refptr<TabletPeer> tablet_peer;
  ASSERT_TRUE(cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
      tablet_id, &tablet_peer));

  {
    ASSERT_STATUS_OK(client_->NewTableAlterer()
                     ->table_name(kTableName)
                     .drop_column("int_val")
                     .add_nullable_column("new_col", KuduColumnSchema::UINT32)
                     .Alter());
    ASSERT_EQ(1, tablet_peer->tablet()->metadata()->schema_version());
  }

  // test that specifying an encoding incompatible with the column's
  // type throws an error
  {
    Status s = client_->NewTableAlterer()
        ->table_name(kTableName)
        .add_nullable_column("new_string_val", KuduColumnSchema::STRING,
                             KuduColumnStorageAttributes(
                                 KuduColumnStorageAttributes::GROUP_VARINT))
        .Alter();
    ASSERT_TRUE(s.IsNotSupported());
    ASSERT_STR_CONTAINS(s.ToString(), "Unsupported type/encoding pair");
    ASSERT_EQ(1, tablet_peer->tablet()->metadata()->schema_version());
  }

  {
    ASSERT_STATUS_OK(client_->NewTableAlterer()
                     ->table_name(kTableName)
                     .add_nullable_column("new_string_val", KuduColumnSchema::STRING,
                                          KuduColumnStorageAttributes(
                                              KuduColumnStorageAttributes::PREFIX_ENCODING))
                     .Alter());
    ASSERT_EQ(2, tablet_peer->tablet()->metadata()->schema_version());
  }

  {
    const char *kRenamedTableName = "RenamedTable";
    ASSERT_STATUS_OK(client_->NewTableAlterer()
                     ->table_name(kTableName)
                     .rename_table(kRenamedTableName)
                     .Alter());
    ASSERT_EQ(3, tablet_peer->tablet()->metadata()->schema_version());
    ASSERT_EQ(kRenamedTableName, tablet_peer->tablet()->metadata()->table_name());

    CatalogManager *catalog_manager = cluster_->mini_master()->master()->catalog_manager();
    ASSERT_TRUE(catalog_manager->TableNameExists(kRenamedTableName));
    ASSERT_FALSE(catalog_manager->TableNameExists(kTableName));
  }
}

TEST_F(ClientTest, TestDeleteTable) {
  string tablet_id = GetFirstTabletId(client_table_.get());

  // Remove the table
  // NOTE that it returns when the operation is completed on the master side
  ASSERT_STATUS_OK(client_->DeleteTable(kTableName));
  CatalogManager *catalog_manager = cluster_->mini_master()->master()->catalog_manager();
  ASSERT_FALSE(catalog_manager->TableNameExists(kTableName));

  // Wait until the table is removed from the TS
  int wait_time = 1000;
  bool tablet_found = true;
  for (int i = 0; i < 80 && tablet_found; ++i) {
    scoped_refptr<TabletPeer> tablet_peer;
    tablet_found = cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
                      tablet_id, &tablet_peer);
    usleep(wait_time);
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
  ASSERT_FALSE(tablet_found);

  // Try to open the deleted table
  Status s = client_->OpenTable(kTableName, &client_table_);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "The table does not exist");
}

TEST_F(ClientTest, TestGetTableSchema) {
  KuduSchema schema;

  // Verify the schema for the current table
  ASSERT_STATUS_OK(client_->GetTableSchema(kTableName, &schema));
  ASSERT_TRUE(schema_.Equals(schema));

  // Verify that a get schema request for a missing table throws not found
  Status s = client_->GetTableSchema("MissingTableName", &schema);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "The table does not exist");
}

TEST_F(ClientTest, TestStaleLocations) {
  string tablet_id = GetFirstTabletId(client_table2_.get());

  // The Tablet is up and running the location should not be stale
  master::TabletLocationsPB locs_pb;
  ASSERT_TRUE(cluster_->mini_master()->master()->catalog_manager()->GetTabletLocations(
                  tablet_id, &locs_pb));
  ASSERT_FALSE(locs_pb.stale());

  // On Master restart and no tablet report we expect the locations to be stale
  cluster_->mini_tablet_server(0)->Shutdown();
  cluster_->mini_master()->Restart();
  ASSERT_TRUE(cluster_->mini_master()->master()->catalog_manager()->GetTabletLocations(
                  tablet_id, &locs_pb));
  ASSERT_TRUE(locs_pb.stale());

  // Restart the TS and Wait for the tablets to be reported to the master.
  cluster_->mini_tablet_server(0)->Start();
  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));
  ASSERT_TRUE(cluster_->mini_master()->master()->catalog_manager()->GetTabletLocations(
                  tablet_id, &locs_pb));

  // It may take a while to bootstrap the tablet and send the location report
  // so spin until we get a non-stale location.
  int wait_time = 1000;
  for (int i = 0; i < 80; ++i) {
    ASSERT_TRUE(cluster_->mini_master()->master()->catalog_manager()->GetTabletLocations(
                    tablet_id, &locs_pb));
    if (!locs_pb.stale()) {
      break;
    }
    usleep(wait_time);
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
  ASSERT_FALSE(locs_pb.stale());
}

// Test creating and accessing a table which has multiple tablets,
// each of which is replicated.
//
// TODO: this should probably be the default for _all_ of the tests
// in this file. However, some things like alter table are not yet
// working on replicated tables - see KUDU-304
TEST_F(ClientTest, TestReplicatedMultiTabletTable) {
  const string kReplicatedTable = "replicated";
  const int kNumRowsToWrite = 100;

  scoped_refptr<KuduTable> table;
  ASSERT_NO_FATAL_FAILURE(CreateReplicatedTable(kReplicatedTable, 3, &table));

  // Should have no rows to begin with.
  ASSERT_EQ(0, CountRowsFromClient(table.get()));

  // Insert some data.
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(table.get(), kNumRowsToWrite));

  // Should now see the data.
  ASSERT_EQ(kNumRowsToWrite, CountRowsFromClient(table.get()));

  // TODO: once leader re-election is in, should somehow force a re-election
  // and ensure that the client handles refreshing the leader.
}

TEST_F(ClientTest, TestReplicatedMultiTabletTableFailover) {
  const string kReplicatedTable = "replicated_with_failover";
  const int kNumRowsToWrite = 100;
  const int kNumReplicas = 3;
  const int kNumTries = 10;

  scoped_refptr<KuduTable> table;
  ASSERT_NO_FATAL_FAILURE(CreateReplicatedTable(
      kReplicatedTable, kNumReplicas, &table));

  // Insert some data.
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(table.get(), kNumRowsToWrite));

  // Find the first replica that will be scanned.
  Synchronizer sync;
  scoped_refptr<internal::RemoteTablet> rt;
  client_->data_->meta_cache_->LookupTabletByKey(table.get(), Slice(),
                                                 &rt, sync.AsStatusCallback());
  ASSERT_STATUS_OK(sync.Wait());
  internal::RemoteTabletServer *rts;
  ASSERT_STATUS_OK(client_->data_->GetTabletServer(client_.get(),
                                                   rt->tablet_id(),
                                                   KuduClient::FIRST_REPLICA, &rts));

  // Kill that replica's tablet server.
  bool ts_killed = false;
  for (int i = 0; i < kNumReplicas; i++) {
    MiniTabletServer* ts = cluster_->mini_tablet_server(i);
    if (ts->server()->instance_pb().permanent_uuid() == rts->ToString()) {
      LOG(INFO) << "Killing TS running on " << ts->bound_rpc_addr().ToString();
      ts->Shutdown();
      ts_killed = true;
      break;
    }
  }
  CHECK(ts_killed);

  // The first tablet to be scanned will fail, but we should fail over to
  // another. Of course, there's no guarantee that the non-leader replica
  // is up-to-date, so let's loop until we get an answer we like.
  ASSERT_EQ(0, rt->GetNumFailedReplicas());
  int tries = 0;
  for (;;) {
    tries++;
    int num_rows = CountRowsFromClient(table.get(),
                                       KuduClient::FIRST_REPLICA,
                                       kNoBound, kNoBound);
    if (num_rows == kNumRowsToWrite) {
      break;
    } else {
      LOG(INFO) << "Only found " << num_rows << " rows on try "
                << tries << ", retrying";
      ASSERT_LE(tries, kNumTries);
      usleep(10000 * tries); // sleep a bit more with each attempt.
    }
  }
  ASSERT_EQ(1, rt->GetNumFailedReplicas());
}

namespace {

void CheckCorrectness(KuduScanner* scanner, int expected[], int nrows) {
  scanner->Open();
  int readrows = 0;
  vector<KuduRowResult> rows;
  if (nrows) {
    ASSERT_TRUE(scanner->HasMoreRows());
  } else {
    ASSERT_FALSE(scanner->HasMoreRows());
  }

  while (scanner->HasMoreRows()) {
    ASSERT_STATUS_OK(scanner->NextBatch(&rows));
    BOOST_FOREACH(const KuduRowResult& r, rows) {
      uint32_t key;
      uint32_t val;
      Slice strval;
      ASSERT_STATUS_OK(r.GetUInt32(0, &key));
      ASSERT_STATUS_OK(r.GetUInt32(1, &val));
      ASSERT_STATUS_OK(r.GetString(2, &strval));
      ASSERT_NE(expected[key], -1) << "Deleted key found in table in table " << key;
      ASSERT_EQ(expected[key], val) << "Incorrect int value for key " <<  key;
      ASSERT_EQ(strval.size(), 0) << "Incorrect string value for key " << key;
      ++readrows;
    }
    rows.clear();
  }
  ASSERT_EQ(readrows, nrows);
  scanner->Close();
}

} // anonymous namespace

// Randomized mutations accuracy testing
TEST_F(ClientTest, TestRandomWriteOperation) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(5000);
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  int row[FLAGS_test_scan_num_rows]; // -1 indicates empty
  int nrows;
  KuduScanner scanner(client_table_.get());

  // First half-fill
  for (int i = 0; i < FLAGS_test_scan_num_rows/2; ++i) {
    ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, i, i, ""));
    row[i] = i;
  }
  for (int i = FLAGS_test_scan_num_rows/2; i < FLAGS_test_scan_num_rows; ++i) {
    row[i] = -1;
  }
  nrows = FLAGS_test_scan_num_rows/2;

  // Randomized testing
  // TODO: Currently, multiple mutations of the same row are not supported
  // in the same batch. Once KUDU-352 is resolved, then the changed set
  // should be removed (if per-row per-batch ordering is preserved).
  // If batches remain with no ordering guarantee, then only this comment
  // should be removed.
  unordered_set<int> changed;
  LOG(INFO) << "Randomized mutations testing.";
  SeedRandom();
  for (int i = 0; i <= 1000; ++i) {
    // Test correctness every so often
    if (i % 50 == 0) {
      LOG(INFO) << "Correctness test " << i;
      FlushSessionOrDie(session);
      ASSERT_NO_FATAL_FAILURE(CheckCorrectness(&scanner, row, nrows));
      LOG(INFO) << "...complete";
      changed.clear();
    }

    int change = rand() % FLAGS_test_scan_num_rows;
    // Insert if empty
    while (changed.find(change) != changed.end())
      change = (change + 1) % FLAGS_test_scan_num_rows;
    changed.insert(change);
    if (row[change] == -1) {
      ASSERT_STATUS_OK(ApplyInsertToSession(session.get(),
                                            client_table_,
                                            change,
                                            change,
                                            ""));
      row[change] = change;
      ++nrows;
      VLOG(1) << "Insert " << change;
    } else {
      // Update or delete otherwise
      int update = rand() & 1;
      if (update) {
        ASSERT_STATUS_OK(ApplyUpdateToSession(session.get(),
                                              client_table_,
                                              change,
                                              ++row[change]));
        VLOG(1) << "Update " << change;
      } else {
        ASSERT_STATUS_OK(ApplyDeleteToSession(session.get(),
                                              client_table_,
                                              change));
        row[change] = -1;
        --nrows;
        VLOG(1) << "Delete " << change;
      }
    }
  }

  // And one more time for the last batch.
  FlushSessionOrDie(session);
  ASSERT_NO_FATAL_FAILURE(CheckCorrectness(&scanner, row, nrows));
}

// Test whether a batch can handle several mutations in a batch
// Disabled. See KUDU-352
TEST_F(ClientTest, DISABLED_TestSeveralRowMutatesPerBatch) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(5000);
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Test insert/update
  LOG(INFO) << "Testing insert/update in same batch, key " << 1 << ".";
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, ""));
  ASSERT_STATUS_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 2));
  FlushSessionOrDie(session);
  vector<string> rows;
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("(uint32 key=1, uint32 int_val=2, string string_val=, "
            "uint32 non_null_with_default=12345)", rows[0]);
  rows.clear();


  LOG(INFO) << "Testing insert/delete in same batch, key " << 2 << ".";
  // Test insert/delete
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 2, 1, ""));
  FlushSessionOrDie(session);
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("(uint32 key=1, uint32 int_val=2, string string_val=, "
            "uint32 non_null_with_default=12345)", rows[0]);
  rows.clear();

  // Test update/delete
  LOG(INFO) << "Testing update/delete in same batch, key " << 1 << ".";
  ASSERT_STATUS_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 1));
  ASSERT_STATUS_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  FlushSessionOrDie(session);
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(0, rows.size());

  // Test delete/insert (insert a row first)
  LOG(INFO) << "Inserting row for delete/insert test, key " << 1 << ".";
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, ""));
  FlushSessionOrDie(session);
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("(uint32 key=1, uint32 int_val=1, string string_val=, "
            "uint32 non_null_with_default=12345)", rows[0]);
  rows.clear();
  LOG(INFO) << "Testing delete/insert in same batch, key " << 1 << ".";
  ASSERT_STATUS_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 2, ""));
  FlushSessionOrDie(session);
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("(uint32 key=1, uint32 int_val=2, string string_val=, "
            "uint32 non_null_with_default=12345)", rows[0]);
            rows.clear();
}

// Tests that master permits are properly released after a whole bunch of
// rows are inserted.
TEST_F(ClientTest, TestMasterLookupPermits) {
  int initial_value = client_->data_->meta_cache_->master_lookup_sem_.GetValue();
  ASSERT_NO_FATAL_FAILURE(InsertTestRows(client_table_.get(),
                                         FLAGS_test_scan_num_rows));
  ASSERT_EQ(initial_value,
            client_->data_->meta_cache_->master_lookup_sem_.GetValue());
}

// Define callback for deadlock simulation, as well as various helper methods.
namespace {
  class DLSCallback : public RefCountedThreadSafe<DLSCallback> {
  public:
    explicit DLSCallback(Atomic32* i)
      : i(i) {}
    void StatusCB(const Status& s) {
      CHECK_OK(s);
      NoBarrier_AtomicIncrement(i, 1);
    }
    StatusCallback AsStatusCallback() {
      return Bind(&DLSCallback::StatusCB, this);
    }
  private:
    Atomic32* const i;
  };

  // Returns col1 value of first row.
  uint32_t ReadFirstRowKeyFirstCol(scoped_refptr<KuduTable> tbl) {
    KuduScanner scanner(tbl.get());

    scanner.Open();
    vector<KuduRowResult> rows;
    CHECK(scanner.HasMoreRows());
    CHECK_OK(scanner.NextBatch(&rows));
    KuduRowResult& row = rows.front();
    uint32_t val;
    CHECK_OK(row.GetUInt32(1, &val));
    return val;
  }

  // Checks that all rows have value equal to expected, return number of rows.
  int CheckRowsEqual(scoped_refptr<KuduTable> tbl, uint32_t expected) {
    KuduScanner scanner(tbl.get());
    scanner.Open();
    vector<KuduRowResult> rows;
    int cnt = 0;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&rows));
      BOOST_FOREACH(const KuduRowResult& row, rows) {
        // Check that for every key:
        // 1. Column 1 uint32_t value == expected
        // 2. Column 2 string value is empty
        // 3. Column 3 uint32_t value is default, 12345
        uint32_t key;
        uint32_t val;
        Slice strval;
        uint32_t val2;
        CHECK_OK(row.GetUInt32(0, &key));
        CHECK_OK(row.GetUInt32(1, &val));
        CHECK_OK(row.GetString(2, &strval));
        CHECK_OK(row.GetUInt32(3, &val2));
        CHECK_EQ(expected, val) << "Incorrect int value for key " << key;
        CHECK_EQ(strval.size(), 0) << "Incorrect string value for key " << key;
        CHECK_EQ(12345, val2);
        ++cnt;
      }
      rows.clear();
    }
    return cnt;
  }

  // Return a session "loaded" with updates. Sets the session timeout
  // to the parameter value. Larger timeouts decrease false positives.
  shared_ptr<KuduSession> LoadedSession(shared_ptr<KuduClient> client,
                                        scoped_refptr<KuduTable> tbl,
                                        bool fwd, int max, int timeout) {
    shared_ptr<KuduSession> session = client->NewSession();
    session->SetTimeoutMillis(timeout);
    CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int i = 0; i < max; ++i) {
      int key = fwd ? i : max - i;
      CHECK_OK(ApplyUpdateToSession(session.get(), tbl, key, fwd));
    }
    return session;
  }
} // anonymous namespace

// Currently, deadlock simulation is disabled. See KUDU-47.
TEST_F(ClientTest, DISABLED_TestDeadlockSimulation) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "TestDeadlockSimulation disabled since slow.";
    return;
  }

  // Make reverse client who will make batches that update rows
  // in reverse order. Separate client used so rpc calls come in at same time.
  shared_ptr<KuduClient> rev_client;
  ASSERT_STATUS_OK(KuduClientBuilder()
                   .master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
                   .Build(&rev_client));
  scoped_refptr<KuduTable> rev_table;
  ASSERT_STATUS_OK(client_->OpenTable(kTableName, &rev_table));

  // Load up some rows
  const int kNumRows = 3000; // Increase to reduce deadlock false-negative.
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(5000);
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  for (int i = 0; i < kNumRows; ++i)
    ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, i, i,  ""));
  FlushSessionOrDie(session);

  // Check both clients see rows
  int fwd = CountRowsFromClient(client_table_.get());
  ASSERT_EQ(kNumRows, fwd);
  int rev = CountRowsFromClient(rev_table.get());
  ASSERT_EQ(kNumRows, rev);

  // Generate sessions
  const int kNumSessions = 100; // Increase to reduce deadlock false-negative.
  const int kTimoutMilis = 5000; // Increase to reduce false-positives.
  shared_ptr<KuduSession> fwd_sessions[kNumSessions];
  shared_ptr<KuduSession> rev_sessions[kNumSessions];
  for (int i = 0; i < kNumSessions; ++i) {
    fwd_sessions[i] = LoadedSession(client_, client_table_, true, kNumRows, kTimoutMilis);
    rev_sessions[i] = LoadedSession(rev_client, rev_table, true, kNumRows, kTimoutMilis);
  }

  // Run async calls - one thread updates sequentially, another in reverse.
  Atomic32 ctr1, ctr2;
  NoBarrier_Store(&ctr1, 0);
  NoBarrier_Store(&ctr2, 0);
  for (int i = 0; i < kNumSessions; ++i) {
    DLSCallback* cb1 = new DLSCallback(&ctr1);
    DLSCallback* cb2 = new DLSCallback(&ctr2);

    // 'cb1' and 'cb2' will be freed after the callbacks are invoked.
    fwd_sessions[i]->FlushAsync(cb1->AsStatusCallback());
    rev_sessions[i]->FlushAsync(cb2->AsStatusCallback());
  }

  // Spin while waiting for ops to complete.
  int lctr1, lctr2, prev1 = 0, prev2 = 0;
  do {
    lctr1 = NoBarrier_Load(&ctr1);
    lctr2 = NoBarrier_Load(&ctr2);
    // Display progress in 10% increments.
    if (prev1 == 0 || lctr1 + lctr2 - prev1 - prev2 > kNumSessions / 10) {
      LOG(INFO) << "# updates: " << lctr1 << " fwd, " << lctr2 << " rev";
      prev1 = lctr1;
      prev2 = lctr2;
    }
  } while (lctr1 != kNumSessions|| lctr2 != kNumSessions);
  uint32_t expected = ReadFirstRowKeyFirstCol(client_table_);

  // Check transaction from forward client.
  fwd = CheckRowsEqual(client_table_, expected);
  ASSERT_EQ(fwd, kNumRows);

  // Check from reverse client side.
  rev = CheckRowsEqual(rev_table, expected);
  ASSERT_EQ(rev, kNumRows);
}

} // namespace client
} // namespace kudu
