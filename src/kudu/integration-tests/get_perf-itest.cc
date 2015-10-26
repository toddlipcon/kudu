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
#include <atomic>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <string>

#include "kudu/client/client.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

DEFINE_int32(get_test_time, 5,
             "Number of seconds to run get test");

DEFINE_int32(get_test_num_tablet, 1,
             "Number of tablets of the table");

DEFINE_int32(get_test_num_thread, 4,
             "Number of threads for write and get workload");

DEFINE_int32(get_test_rpc_worker_thread, 0,
             "Number of threads for tablet server rpc service, 0 for default");

DEFINE_int32(get_test_rows_per_thread, 500000,
             "Number of rows to insert per thread");

DEFINE_int32(get_test_cfile_block_size, 256*1024,
             "Cfile block size for the projected column");

DEFINE_string(get_test_insert_pattern, "seq",
              "Insert pattern for the test table, seq/rand/seqrand");

DEFINE_bool(get_test_use_scan, false,
            "Use scan for get operation");

DEFINE_bool(get_test_use_multi_client, false,
            "Use one client per get thread");

DEFINE_bool(get_test_verify, false,
            "Verify get operation results");

namespace kudu {
namespace tablet {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduGetter;
using client::KuduInsert;
using client::KuduPredicate;
using client::KuduScanBatch;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableCreator;
using client::KuduValue;

class GetPerfTest : public ExternalMiniClusterITestBase {
 protected:
  GetPerfTest() : get_workload_seconds_(5) {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
    b.AddColumn("int32_val")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("int64_val")->Type(KuduColumnSchema::INT64)->Nullable();
    b.AddColumn("float_val")->Type(KuduColumnSchema::FLOAT)->NotNull();
    b.AddColumn("string_val0")->Type(KuduColumnSchema::STRING)->BlockSize(
        FLAGS_get_test_cfile_block_size)->NotNull();
    b.AddColumn("string_val1")->Type(KuduColumnSchema::STRING)->Nullable();
    CHECK_OK(b.Build(&schema_));
  }

  void CreateTable() {
    gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    if (FLAGS_get_test_num_tablet > 1) {
      table_creator->add_hash_partitions({"key"}, FLAGS_get_test_num_tablet);
    }
    ASSERT_OK(table_creator->table_name(kTableName)
             .schema(&schema_)
             .num_replicas(1)
             .Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table_));
  }

  void InsertWorkload() {
    total_insert_ = 0;
    std::vector<scoped_refptr<Thread> > threads;
    for (int i = 0; i < FLAGS_get_test_num_thread; i++) {
      scoped_refptr<kudu::Thread> new_thread;
      CHECK_OK(kudu::Thread::Create("test", strings::Substitute("insert-thread-$0", i),
                                    &GetPerfTest::InsertThread, this, i,
                                    &new_thread));
      threads.push_back(new_thread);
    }
    MonoTime last = MonoTime::Now(MonoTime::FINE);
    int last_total = 0;
    while (true) {
      SleepFor(MonoDelta::FromMilliseconds(3000));
      int total = total_insert_;
      MonoTime now = MonoTime::Now(MonoTime::FINE);
      LOG(INFO) << "Insert total: " << total << " QPS: "
                << ((total - last_total) / now.GetDeltaSince(last).ToSeconds());
      last = now;
      last_total = total;
      if (total >= FLAGS_get_test_num_thread * FLAGS_get_test_rows_per_thread) {
        break;
      }
    }
    for (int i = 0; i < FLAGS_get_test_num_thread; i++) {
      threads[i]->Join();
    }
    threads.clear();
  }

  void GetWorkload(int thread_num, bool use_scan = false) {
    total_get_ = 0;
    std::vector<scoped_refptr<Thread> > threads;
    for (int i = 0; i < thread_num; i++) {
      scoped_refptr<kudu::Thread> new_thread;
      if (use_scan) {
        CHECK_OK(kudu::Thread::Create("test", strings::Substitute("scan-thread-$0", i),
                                      &GetPerfTest::ScanThread, this, i,
                                      &new_thread));
      } else {
        CHECK_OK(kudu::Thread::Create("test", strings::Substitute("get-thread-$0", i),
                                      &GetPerfTest::GetThread, this, i,
                                      &new_thread));
      }
      threads.push_back(new_thread);
    }
    MonoTime last = MonoTime::Now(MonoTime::FINE);
    MonoTime end_time = last;
    end_time.AddDelta(MonoDelta::FromSeconds(get_workload_seconds_));
    int last_total = 0;
    while (true) {
      SleepFor(MonoDelta::FromMilliseconds(2000));
      int total = total_get_;
      MonoTime now = MonoTime::Now(MonoTime::FINE);
      LOG(INFO) << (use_scan ? "Scan" : "Get")  << " total: " << total << " QPS: "
                << ((total - last_total) / now.GetDeltaSince(last).ToSeconds());
      last = now;
      last_total = total;
      if (end_time.ComesBefore(now)) {
        break;
      }
    }
    for (int i = 0; i < thread_num; i++) {
      threads[i]->Join();
    }
    threads.clear();
  }

 public:
  void InsertThread(int idx) {
    client::KuduClientBuilder builder;

    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(5000);
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

    // Generate row keys
    int nthreads = FLAGS_get_test_num_thread;
    int nrows = FLAGS_get_test_rows_per_thread;
    vector<int> rowkeys;
    rowkeys.reserve(nrows);
    for (int i = 0; i < nrows; i++) {
      rowkeys.push_back(i * nthreads + idx);
    }
    if (FLAGS_get_test_insert_pattern == "rand") {
      std::random_shuffle(rowkeys.begin(), rowkeys.end());
    } else if (FLAGS_get_test_insert_pattern == "seqrand") {
      for (int i = 0; i < nrows - 2000; i += 2000) {
        int end = std::min(nrows, i + 2000);
        std::random_shuffle(rowkeys.begin() + i, rowkeys.begin() + end);
      }
    }
    // Insert rows
    for (int i = 0; i < rowkeys.size(); i++) {
      int key = rowkeys[i];
      gscoped_ptr<KuduInsert> insert(table_->NewInsert());
      KuduPartialRow* row = insert->mutable_row();
      CHECK_OK(row->SetInt64("key", key));
      CHECK_OK(row->SetInt32("int32_val", key * 2));
      if (key % 2 == 0) {
        CHECK_OK(row->SetInt64("int64_val", key));
      } else {
        row->SetNull("int64_val");
      }
      CHECK_OK(row->SetFloat("float_val", key * 2));
      CHECK_OK(row->SetStringCopy("string_val0", StringPrintf("string_val0 %d", key)));
      CHECK_OK(row->SetStringCopy("string_val1", StringPrintf("string_val1 %d", key)));
      CHECK_OK(session->Apply(insert.release()));
      total_insert_++;
      if (i % 100 == 0) {
        CHECK_OK(session->Flush());
      }
    }
    CHECK_OK(session->Flush());
  }

  void GetThread(int idx) {
    client::sp::shared_ptr<client::KuduClient> client;
    client::sp::shared_ptr<KuduTable> table;
    if (FLAGS_get_test_use_multi_client) {
      client::KuduClientBuilder builder;
      ASSERT_OK(cluster_->CreateClient(builder, &client));
      ASSERT_OK(client->OpenTable(kTableName, &table));
    } else {
      client = client_;
      table = table_;
    }

    MonoTime end = MonoTime::Now(MonoTime::FINE);
    end.AddDelta(MonoDelta::FromSeconds(get_workload_seconds_));
    Random rand(idx);
    uint32_t total = FLAGS_get_test_num_thread * FLAGS_get_test_rows_per_thread;
    kudu::client::KuduRowResult result;
    KuduGetter getter(table.get());
    getter.SetProjectedColumnIndexes({4});
    gscoped_ptr<KuduPartialRow> rowkey(table->schema().NewRow());
    while (MonoTime::Now(MonoTime::FINE).ComesBefore(end)) {
      for (int i = 0; i < 1000; i++) {
        int key = rand.Uniform(total);
        rowkey->SetInt64(0, key);
        CHECK_OK(getter.Get(*rowkey, &result));
        if (FLAGS_get_test_verify) {
          Slice v;
          result.GetString(0, &v);
          CHECK(v.ToString() == StringPrintf("string_val0 %d", key));
        }
        total_get_++;
      }
    }
  }

  void ScanThread(int idx) {
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(5000);
    MonoTime end = MonoTime::Now(MonoTime::FINE);
    end.AddDelta(MonoDelta::FromSeconds(get_workload_seconds_));
    Random rand(idx);
    uint32_t total = FLAGS_get_test_num_thread * FLAGS_get_test_rows_per_thread;
    gscoped_ptr<KuduPartialRow> rowkey(table_->schema().NewRow());
    while (MonoTime::Now(MonoTime::FINE).ComesBefore(end)) {
      for (int i = 0; i < 1000; i++) {
        int key = rand.Uniform(total);
        KuduScanner scanner(table_.get());
        CHECK_OK(scanner.SetProjectedColumnIndexes({ 4 }));
        CHECK_OK(scanner.AddConjunctPredicate(table_->NewComparisonPredicate(
            "key", KuduPredicate::EQUAL, KuduValue::FromInt(key))));
        CHECK_OK(scanner.Open());
        CHECK(scanner.HasMoreRows());
        KuduScanBatch batch;
        CHECK_OK(scanner.NextBatch(&batch));
        CHECK(1 == batch.NumRows());
        total_get_++;
      }
    }
  }

 protected:
  static const char* const kTableName;

  KuduSchema schema_;
  client::sp::shared_ptr<KuduTable> table_;
  int get_workload_seconds_;
  std::atomic<int> total_insert_;
  std::atomic<int> total_get_;
};

const char* const GetPerfTest::kTableName = "get-perf-test-tbl";

TEST_F(GetPerfTest, GetPerf) {
  if (FLAGS_get_test_rpc_worker_thread == 0) {
    NO_FATALS(StartCluster({}, {}, 1));
  } else {
    string option = StringPrintf("--rpc_num_service_threads=%d", FLAGS_get_test_rpc_worker_thread);
    NO_FATALS(StartCluster({ option }, {}, 1));
  }
  NO_FATALS(CreateTable());

  NO_FATALS(InsertWorkload());

  bool use_scan = FLAGS_get_test_use_scan;
  int num_thread = FLAGS_get_test_num_thread;
  while (true) {
    if (FLAGS_get_test_time <= 0) {
      // manual test
      std::cout << std::endl;
      std::cout << "Input <scan|get> <seconds to run, 0(exit)> <num thread>: ";
      string method;
      std::cin >> method >> get_workload_seconds_ >> num_thread;
      if (get_workload_seconds_ == 0) {
        break;
      }
      use_scan = method == "scan";
    } else {
      get_workload_seconds_ = FLAGS_get_test_time;
    }
    NO_FATALS(GetWorkload(num_thread, use_scan));
    if (FLAGS_get_test_time > 0) {
      break;
    }
  }
}

} // namespace tablet
} // namespace kudu
