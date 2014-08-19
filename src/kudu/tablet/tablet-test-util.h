// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_TEST_UTIL_H
#define KUDU_TABLET_TABLET_TEST_UTIL_H

#include <gflags/gflags.h>
#include <string>
#include <vector>

#include "kudu/common/iterator.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/casts.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_util.h"

DEFINE_bool(tablet_test_enable_metrics, false,
            "Enable metrics collection in tablet-tests");

namespace kudu {
namespace tablet {

using metadata::QuorumPB;
using std::string;
using std::vector;

class KuduTabletTest : public KuduTest {
 public:
  explicit KuduTabletTest(const Schema& schema)
  : schema_(schema.CopyWithColumnIds()),
    client_schema_(schema) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    SetUpTestTablet();
  }

  void SetUpTestTablet(const string& root_dir = "") {
    string dir = root_dir.empty() ? test_dir_ : root_dir;
    TabletHarness::Options opts(dir);
    opts.enable_metrics = FLAGS_tablet_test_enable_metrics;
    harness_.reset(new TabletHarness(schema_, opts));
    ASSERT_OK(harness_->Open());
  }

  void TabletReOpen(const string& root_dir = "") {
    SetUpTestTablet(root_dir);
  }

  const Schema &schema() const {
    return schema_;
  }

  const Schema &client_schema() const {
    return client_schema_;
  }

  server::Clock* clock() {
    return harness_->clock();
  }

  FsManager* fs_manager() {
    return harness_->fs_manager();
  }

  void AlterSchema(const Schema& schema) {
    tserver::AlterSchemaRequestPB req;
    req.set_schema_version(tablet()->metadata()->schema_version() + 1);

    AlterSchemaTransactionState tx_state(&req);
    ASSERT_STATUS_OK(tablet()->CreatePreparedAlterSchema(&tx_state, &schema));
    ASSERT_STATUS_OK(tablet()->AlterSchema(&tx_state));
    tx_state.commit();
  }

  const std::tr1::shared_ptr<Tablet>& tablet() const {
    return harness_->tablet();
  }

  TabletHarness* harness() {
    return harness_.get();
  }

 protected:
  const Schema schema_;
  const Schema client_schema_;

  gscoped_ptr<TabletHarness> harness_;
};

class KuduRowSetTest : public KuduTabletTest {
 public:
  explicit KuduRowSetTest(const Schema& schema)
    : KuduTabletTest(schema) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();
    ASSERT_STATUS_OK(tablet()->metadata()->CreateRowSet(&rowset_meta_,
                                                       SchemaBuilder(schema_).Build()));
  }

  Status FlushMetadata() {
    return tablet()->metadata()->Flush();
  }

 protected:
  shared_ptr<metadata::RowSetMetadata> rowset_meta_;
};

static inline Status IterateToStringList(RowwiseIterator *iter,
                                         vector<string> *out,
                                         int limit = INT_MAX) {
  out->clear();
  Schema schema = iter->schema();
  Arena arena(1024, 1024);
  RowBlock block(schema, 100, &arena);
  int fetched = 0;
  while (iter->HasNext() && fetched < limit) {
    RETURN_NOT_OK(iter->NextBlock(&block));
    for (size_t i = 0; i < block.nrows() && fetched < limit; i++) {
      if (block.selection_vector()->IsRowSelected(i)) {
        out->push_back(schema.DebugRow(block.row(i)));
        fetched++;
      }
    }
  }
  return Status::OK();
}

// Performs snapshot reads, under each of the snapshots in 'snaps', and stores
// the results in 'collected_rows'.
static inline void CollectRowsForSnapshots(Tablet* tablet,
                                           const Schema& schema,
                                           const vector<MvccSnapshot>& snaps,
                                           vector<vector<string>* >* collected_rows) {
  BOOST_FOREACH(const MvccSnapshot& snapshot, snaps) {
    DVLOG(1) << "Snapshot: " <<  snapshot.ToString();
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet->NewRowIterator(schema,
                                            snapshot,
                                            &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));
    vector<string>* collector = new vector<string>();
    ASSERT_STATUS_OK(IterateToStringList(iter.get(), collector));
    for (int i = 0; i < collector->size(); i++) {
      DVLOG(1) << "Got from MRS: " << (*collector)[i];
    }
    collected_rows->push_back(collector);
  }
}

// Performs snapshot reads, under each of the snapshots in 'snaps', and verifies that
// the results match the ones in 'expected_rows'.
static inline void VerifySnapshotsHaveSameResult(Tablet* tablet,
                                                 const Schema& schema,
                                                 const vector<MvccSnapshot>& snaps,
                                                 const vector<vector<string>* >& expected_rows) {
  int idx = 0;
  // Now iterate again and make sure we get the same thing.
  BOOST_FOREACH(const MvccSnapshot& snapshot, snaps) {
    DVLOG(1) << "Snapshot: " <<  snapshot.ToString();
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet->NewRowIterator(schema,
                                            snapshot,
                                            &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));
    vector<string> collector;
    ASSERT_STATUS_OK(IterateToStringList(iter.get(), &collector));
    ASSERT_EQ(collector.size(), expected_rows[idx]->size());

    for (int i = 0; i < expected_rows[idx]->size(); i++) {
      DVLOG(1) << "Got from DRS: " << collector[i];
      DVLOG(1) << "Expected: " << (*expected_rows[idx])[i];
      ASSERT_EQ((*expected_rows[idx])[i], collector[i]);
    }
    idx++;
  }
}

// Construct a new iterator from the given rowset, and dump
// all of its results into 'out'. The previous contents
// of 'out' are cleared.
static inline Status DumpRowSet(const RowSet &rs,
                                const Schema &projection,
                                const MvccSnapshot &snap,
                                vector<string> *out,
                                int limit = INT_MAX) {
  gscoped_ptr<RowwiseIterator> iter(rs.NewRowIterator(&projection, snap));
  RETURN_NOT_OK(iter->Init(NULL));
  RETURN_NOT_OK(IterateToStringList(iter.get(), out, limit));
  return Status::OK();
}

// Take an un-initialized iterator, Init() it, and iterate through all of its rows.
// The resulting string contains a line per entry.
static inline string InitAndDumpIterator(gscoped_ptr<RowwiseIterator> iter) {
  CHECK_OK(iter->Init(NULL));

  vector<string> out;
  CHECK_OK(IterateToStringList(iter.get(), &out));
  return JoinStrings(out, "\n");
}

// Write a single row to the given RowSetWriter (which may be of the rolling
// or non-rolling variety).
template<class RowSetWriterClass>
static Status WriteRow(const Slice &row_slice, RowSetWriterClass *writer) {
  const Schema &schema = writer->schema();
  DCHECK_EQ(row_slice.size(), schema.byte_size());

  RowBlock block(schema, 1, NULL);
  ConstContiguousRow row(&schema, row_slice.data());
  RowBlockRow dst_row = block.row(0);
  RETURN_NOT_OK(CopyRow(row, &dst_row, reinterpret_cast<Arena*>(NULL)));

  return writer->AppendBlock(block);
}

} // namespace tablet
} // namespace kudu
#endif
