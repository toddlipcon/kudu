// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// These tests find the "breaking points" of the end-to-end system
// in terms of number of columns, size of data, size of insert batch, etc.

#include <boost/assign/list_of.hpp>
#include <string>
#include <tr1/memory>

#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/tools/data_gen_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"

using boost::assign::list_of;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduRowResult;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using std::tr1::shared_ptr;
using strings::Substitute;

namespace kudu {

class SizeLimitsITest : public ExternalMiniClusterITestBase {
 protected:
  Status InsertRandomRows(const shared_ptr<KuduTable>& table,
                          int64_t num_rows,
                          int64_t rows_per_batch);

};

Status SizeLimitsITest::InsertRandomRows(const shared_ptr<KuduTable>& table,
                                         int64_t num_rows,
                                         int64_t rows_per_batch) {

  Random rng(GetRandomSeed32());
  shared_ptr<KuduSession> session(client_->NewSession());
  session->SetTimeoutMillis(60000);
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  for (int64_t i = 0; i < num_rows; i++) {
    gscoped_ptr<KuduInsert> insert(table->NewInsert());
    tools::GenerateDataForRow(table->schema(), i, &rng, insert->mutable_row());
    RETURN_NOT_OK(session->Apply(insert.release()));

    if (i > 0 && i % rows_per_batch == 0) {
      RETURN_NOT_OK(session->Flush());
    }
  }
  RETURN_NOT_OK(session->Flush());
  return Status::OK();
}

// Parameterized test which creates extremely wide columns with 2^N
// int64 columns, writes a few hundred MB of data, then scans them.
class ManyIntColumnsTest : public SizeLimitsITest,
                           public ::testing::WithParamInterface<int> {
};

#ifdef NDEBUG
TEST_P(ManyIntColumnsTest, Test) {
  const int kExponent = GetParam();
  const int kNumCols = 1L << kExponent;
  const int kBytesPerRow = 8 * kNumCols;
  const string kTableName = "int_cols";

  // Inserting 300MB should be enough to cause flushes, and is also much larger than the
  // max RPC size of 8MB. So we expect this to trigger most potential issues.
  const int64_t kMbToInsert = 300;
  const int64_t kRowCount = kMbToInsert * 1024 * 1024 / kBytesPerRow;

  // If we try to insert too much in a single batch, the client side
  // limiting will stop us. So aim for 4MB per batch.
  const int64_t kRowsPerInsertBatch = 4 * 1024 * 1024 / kBytesPerRow;

  NO_FATALS(StartCluster(list_of("--log_container_preallocate_bytes=0")));

  LOG(INFO) << "Will create a table with " << kNumCols << " columns and "
            << kRowCount << " rows...";

  // Create a schema with lots of integer columns
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
  for (int i = 0; i < kNumCols; i++) {
    b.AddColumn(Substitute("c$0", i))->Type(KuduColumnSchema::INT64)->NotNull();
  }

  KuduSchema schema;
  ASSERT_OK(b.Build(&schema));
  shared_ptr<KuduTable> table;

  gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&schema)
            .num_replicas(3)
            .Create());
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  ASSERT_OK(InsertRandomRows(table, kRowCount, kRowsPerInsertBatch));

  // Verify that the table correctly converges through consensus.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  v.CheckRowCount(kTableName, ClusterVerifier::EXACTLY, kRowCount);

  // Verify that we can read back the table (even if we're scanning all the rows).
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.Open());
  vector<KuduRowResult> rows;
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&rows));
    rows.clear();
  }
}
// Test 2^8, 2^10, 2^12, 2^14 columns.
INSTANTIATE_TEST_CASE_P(AcrossExponents, ManyIntColumnsTest, ::testing::Range(8, 14, 2));

#endif // NDEBUG

} // namespace kudu
