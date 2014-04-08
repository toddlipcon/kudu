// Copyright (c) 2012, Cloudera, inc.
#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <iterator>
#include <limits>
#include <tr1/memory>
#include <vector>

#include "cfile/cfile.h"
#include "common/iterator.h"
#include "common/row_changelist.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "consensus/consensus.h"
#include "consensus/consensus.pb.h"
#include "consensus/opid_anchor_registry.h"
#include "gutil/atomicops.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "tablet/compaction.h"
#include "tablet/compaction_policy.h"
#include "tablet/delta_compaction.h"
#include "tablet/diskrowset.h"
#include "tablet/tablet.h"
#include "tablet/tablet_metrics.h"
#include "tablet/rowset_tree.h"
#include "tablet/transactions/alter_schema_transaction.h"
#include "tablet/transactions/write_transaction.h"
#include "tablet/transactions/write_util.h"
#include "util/bloom_filter.h"
#include "util/env.h"
#include "util/metrics.h"

DEFINE_bool(tablet_do_dup_key_checks, true,
            "Whether to check primary keys for duplicate on insertion. "
            "Use at your own risk!");

DEFINE_string(tablet_compaction_policy, "budget",
              "Which compaction policy to use. Valid options are currently "
              "'size' or 'budget'");

DEFINE_int32(tablet_compaction_budget_mb, 128,
             "Budget for a single compaction, if the 'budget' compaction "
             "algorithm is selected");

namespace kudu {
namespace tablet {

using consensus::Consensus;
using consensus::OperationPB;
using consensus::CommitMsg;
using consensus::ConsensusContext;
using consensus::OpId;
using log::MaximumOpId;
using log::OpIdAnchorRegistry;
using metadata::RowSetMetadata;
using metadata::RowSetMetadataIds;
using metadata::RowSetMetadataVector;
using metadata::TabletMetadata;
using metadata::ColumnIndexes;
using std::string;
using std::set;
using std::vector;
using std::tr1::shared_ptr;
using strings::Substitute;
using base::subtle::Barrier_AtomicIncrement;

static const int64_t kNoMrsFlushed = -1;

static CompactionPolicy *CreateCompactionPolicy() {
  if (FLAGS_tablet_compaction_policy == "size") {
    return new SizeRatioCompactionPolicy();
  } else if (FLAGS_tablet_compaction_policy == "budget") {
    return new BudgetedCompactionPolicy(FLAGS_tablet_compaction_budget_mb);
  } else {
    LOG(FATAL) << "Unknown compaction policy: " << FLAGS_tablet_compaction_policy;
  }
  return NULL;
}

Tablet::Tablet(gscoped_ptr<TabletMetadata> metadata,
               const scoped_refptr<server::Clock>& clock,
               const MetricContext* parent_metric_context,
               OpIdAnchorRegistry* opid_anchor_registry)
  : schema_(metadata->schema()),
    key_schema_(schema_.CreateKeyProjection()),
    metadata_(metadata.Pass()),
    rowsets_(new RowSetTree()),
    consensus_(NULL),
    opid_anchor_registry_(opid_anchor_registry),
    next_mrs_id_(0),
    mvcc_(clock),
    open_(false) {
  CHECK(schema_.has_column_ids());
  compaction_policy_.reset(CreateCompactionPolicy());

  if (parent_metric_context) {
    metric_context_.reset(new MetricContext(*parent_metric_context,
                                            Substitute("tablet.tablet-$0", tablet_id())));
    metrics_.reset(new TabletMetrics(*metric_context_));
  }
}

Tablet::~Tablet() {
}

Status Tablet::Open() {
  CHECK(!open_) << "already open";
  CHECK(schema_.has_column_ids());
  // TODO: track a state_ variable, ensure tablet is open, etc.

  next_mrs_id_ = metadata_->last_durable_mrs_id() + 1;

  RowSetVector rowsets_opened;

  // open the tablet row-sets
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& rowset_meta, metadata_->rowsets()) {
    shared_ptr<DiskRowSet> rowset;
    Status s = DiskRowSet::Open(rowset_meta, opid_anchor_registry_, &rowset);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to open rowset " << rowset_meta->ToString() << ": "
                 << s.ToString();
      return s;
    }

    rowsets_opened.push_back(rowset);
  }

  CHECK_OK(rowsets_->Reset(rowsets_opened));

  // now that the current state is loaded create the new MemRowSet with the next id
  memrowset_.reset(new MemRowSet(next_mrs_id_, schema_, opid_anchor_registry_));
  next_mrs_id_++;

  open_ = true;
  return Status::OK();
}

Status Tablet::GetMappedReadProjection(const Schema& projection,
                                       Schema *mapped_projection) const {
  return schema_.GetMappedReadProjection(projection, mapped_projection);
}

void Tablet::SetConsensus(Consensus* consensus) {
  consensus_ = consensus;
}

BloomFilterSizing Tablet::bloom_sizing() const {
  // TODO: make this configurable
  return BloomFilterSizing::BySizeAndFPRate(64*1024, 0.01f);
}

Status Tablet::NewRowIterator(const Schema &projection,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  // Yield current rows.
  MvccSnapshot snap(mvcc_);
  return NewRowIterator(projection, snap, iter);
}


Status Tablet::NewRowIterator(const Schema &projection,
                              const MvccSnapshot &snap,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  if (metrics_) {
    metrics_->scans_started->Increment();
  }
  VLOG(2) << "Created new Iterator under snap: " << snap.ToString();
  iter->reset(new Iterator(this, projection, snap));
  return Status::OK();
}

Status Tablet::CreatePreparedInsert(const WriteTransactionContext* tx_ctx,
                                    const ConstContiguousRow* row,
                                    gscoped_ptr<PreparedRowWrite>* row_write) {
  gscoped_ptr<tablet::RowSetKeyProbe> probe(new tablet::RowSetKeyProbe(*row));
  gscoped_ptr<ScopedRowLock> row_lock(new ScopedRowLock(&lock_manager_,
                                                        tx_ctx,
                                                        probe->encoded_key_slice(),
                                                        LockManager::LOCK_EXCLUSIVE));
  row_write->reset(new PreparedRowWrite(row, probe.Pass(), row_lock.Pass()));

  // when we have a more advanced lock manager, acquiring the row lock might fail
  // but for now always return OK.
  return Status::OK();
}

Status Tablet::InsertForTesting(WriteTransactionContext *tx_ctx,
                                const ConstContiguousRow& row) {
  CHECK(open_) << "must Open() first!";
  DCHECK(tx_ctx) << "you must have a transaction context";

  DCHECK_KEY_PROJECTION_SCHEMA_EQ(key_schema_, row.schema());

  // The order of the various locks is critical!
  // See comment block in MutateRow(...) below for details.

  gscoped_ptr<boost::shared_lock<rw_semaphore> > lock(
      new boost::shared_lock<rw_semaphore>(component_lock_));
  tx_ctx->set_component_lock(lock.Pass());

  // Convert the client row to a server row (with IDs)
  // TODO: We have now three places where we do the projection (RPC, Tablet, Bootstrap)
  //       One is the RPC side, the other is this method that should be renamed InsertForTesting()
  DCHECK(!row.schema().has_column_ids());
  RowProjector row_projector(&row.schema(), &schema_);
  if (!row_projector.is_identity()) {
    RETURN_NOT_OK(schema_.VerifyProjectionCompatibility(row.schema()));
    RETURN_NOT_OK(row_projector.Init());
  }
  const ConstContiguousRow* proj_row = ProjectRowForInsert(tx_ctx, &schema_,
                                                           row_projector, row.row_data());

  gscoped_ptr<PreparedRowWrite> row_write;
  RETURN_NOT_OK(CreatePreparedInsert(tx_ctx, proj_row, &row_write));
  tx_ctx->add_prepared_row(row_write.Pass());

  gscoped_ptr<ScopedTransaction> mvcc_tx(new ScopedTransaction(&mvcc_));
  tx_ctx->set_current_mvcc_tx(mvcc_tx.Pass());

  // Create a "fake" OpId and set it in the TransactionContext for anchoring.
  tx_ctx->mutable_op_id()->CopyFrom(MaximumOpId());

  Status s = InsertUnlocked(tx_ctx, tx_ctx->rows()[0]);
  tx_ctx->commit();
  return s;
}

Status Tablet::InsertUnlocked(WriteTransactionContext *tx_ctx,
                              const PreparedRowWrite* insert) {
  CHECK(open_) << "must Open() first!";
  // make sure that the WriteTransactionContext has the component lock and that
  // there the PreparedRowWrite has the row lock.
  DCHECK(tx_ctx->component_lock()) << "WriteTransactionContext must hold the component lock.";
  DCHECK(insert->row_lock()) << "PreparedRowWrite must hold the row lock.";
  DCHECK_KEY_PROJECTION_SCHEMA_EQ(key_schema_, insert->row()->schema());
  DCHECK(tx_ctx->op_id().IsInitialized()) << "TransactionContext OpId needed for anchoring";

  ProbeStats stats;

  // Submit the stats before returning from this function
  ProbeStatsSubmitter submitter(stats, metrics_.get());

  // First, ensure that it is a unique key by checking all the open RowSets.
  if (FLAGS_tablet_do_dup_key_checks) {
    vector<RowSet *> to_check;
    rowsets_->FindRowSetsWithKeyInRange(insert->probe()->encoded_key_slice(), &to_check);

    BOOST_FOREACH(const RowSet *rowset, to_check) {
      bool present = false;
      RETURN_NOT_OK(rowset->CheckRowPresent(*insert->probe(), &present, &stats));
      if (PREDICT_FALSE(present)) {
        Status s = Status::AlreadyPresent("key already present");
        if (metrics_) {
          metrics_->insertions_failed_dup_key->Increment();
        }
        tx_ctx->AddFailedInsert(s);
        return s;
      }
    }
  }

  Timestamp ts = tx_ctx->timestamp();

  // TODO: the Insert() call below will re-encode the key, which is a
  // waste. Should pass through the KeyProbe structure perhaps.

  // Now try to insert into memrowset. The memrowset itself will return
  // AlreadyPresent if it has already been inserted there.
  Status s = memrowset_->Insert(ts, *insert->row(), tx_ctx->op_id());
  if (PREDICT_TRUE(s.ok())) {
    RETURN_NOT_OK(tx_ctx->AddInsert(ts, memrowset_->mrs_id()));
  } else {
    if (s.IsAlreadyPresent() && metrics_) {
      metrics_->insertions_failed_dup_key->Increment();
    }
    tx_ctx->AddFailedInsert(s);
  }
  return s;
}

Status Tablet::CreatePreparedMutate(const WriteTransactionContext* tx_ctx,
                                    const ConstContiguousRow* row_key,
                                    const RowChangeList* changelist,
                                    gscoped_ptr<PreparedRowWrite>* row_write) {
  gscoped_ptr<tablet::RowSetKeyProbe> probe(new tablet::RowSetKeyProbe(*row_key));
  gscoped_ptr<ScopedRowLock> row_lock(new ScopedRowLock(&lock_manager_,
                                                        tx_ctx,
                                                        probe->encoded_key_slice(),
                                                        LockManager::LOCK_EXCLUSIVE));
  row_write->reset(new PreparedRowWrite(row_key, changelist, probe.Pass(), row_lock.Pass()));

  // when we have a more advanced lock manager, acquiring the row lock might fail
  // but for now always return OK.
  return Status::OK();
}

Status Tablet::MutateRowForTesting(WriteTransactionContext *tx_ctx,
                                   const ConstContiguousRow& row_key,
                                   const Schema& update_schema,
                                   const RowChangeList& update) {
  // TODO: use 'probe' when calling UpdateRow on each rowset.
  DCHECK_SCHEMA_EQ(key_schema_, row_key.schema());
  DCHECK_KEY_PROJECTION_SCHEMA_EQ(key_schema_, update_schema);
  DCHECK(tx_ctx) << "you must have a transaction context";
  CHECK(tx_ctx->rows().empty()) << "WriteTransactionContext must have no PreparedRowWrites.";

  // The order of the next three steps is critical!
  //
  // Row-lock before ScopedTransaction:
  // -------------------------------------
  // We must take the row-lock before we assign a transaction ID in order to ensure
  // that within each row, transaction IDs only move forward. If we took a timestamp before
  // getting the row lock, we could have the following situation:
  //
  //   Thread 1         |  Thread 2
  //   ----------------------
  //   Start tx 1       |
  //                    |  Start tx 2
  //                    |  Obtain row lock
  //                    |  Update row
  //                    |  Commit tx 2
  //   Obtain row lock  |
  //   Delete row       |
  //   Commit tx 1
  //
  // This would cause the mutation list to look like: @t1: DELETE, @t2: UPDATE
  // which is invalid, since we expect to be able to be able to replay mutations
  // in increasing timestamp order on a given row.
  //
  // This requirement is basically two-phase-locking: the order in which row locks
  // are acquired for transactions determines their serialization order. If/when
  // we support multi-row serializable transactions, we'll have to acquire _all_
  // row locks before obtaining a timestamp.
  //
  // component_lock_ before ScopedTransaction:
  // -------------------------------------
  // Obtaining the timestamp inside of component_lock_ ensures that, in AtomicSwapRowSets,
  // we can cleanly differentiate a set of transactions that saw the "old" rowsets
  // vs the "new" rowsets. If we created the timestamp before taking the lock, then
  // the in-flight transaction could either have mutated the old rowsets or the new.
  //
  // There may be a more "fuzzy" way of doing this barrier which would cause less of
  // a locking hiccup during the swap, but let's keep things simple for now.
  //
  // RowLock before component_lock
  // ------------------------------
  // It currently doesn't matter which order these happen, but it makes more sense
  // to logically lock the rows before doing anything on the "physical" layer.
  // It is critical, however, that we're consistent with this choice between here
  // and Insert() or else there's a possibility of deadlock.


  gscoped_ptr<boost::shared_lock<rw_semaphore> > lock(
      new boost::shared_lock<rw_semaphore>(component_lock_));
  tx_ctx->set_component_lock(lock.Pass());

  // Convert the client RowChangeList to a server RowChangeList (with IDs)
  // TODO: We have now three places where we do the projection (RPC, Tablet, Bootstrap)
  //       One is the RPC side, the other is this method that should be renamed MutateForTesting()
  DCHECK(!update_schema.has_column_ids());
  DeltaProjector delta_projector(&update_schema, &schema_);
  if (!delta_projector.is_identity()) {
    RETURN_NOT_OK(schema_.VerifyProjectionCompatibility(update_schema));
    RETURN_NOT_OK(update_schema.GetProjectionMapping(schema_, &delta_projector));
  }

  const RowChangeList *changelist = ProjectMutation(tx_ctx, delta_projector, &update);

  gscoped_ptr<PreparedRowWrite> row_write;
  RETURN_NOT_OK(CreatePreparedMutate(tx_ctx, &row_key, changelist, &row_write));
  tx_ctx->add_prepared_row(row_write.Pass());

  gscoped_ptr<ScopedTransaction> mvcc_tx(new ScopedTransaction(&mvcc_));
  tx_ctx->set_current_mvcc_tx(mvcc_tx.Pass());

  // Create a "fake" OpId and set it in the TransactionContext for anchoring.
  tx_ctx->mutable_op_id()->CopyFrom(MaximumOpId());

  Status s = MutateRowUnlocked(tx_ctx, tx_ctx->rows()[0]);
  tx_ctx->commit();
  return s;
}

Status Tablet::MutateRowUnlocked(WriteTransactionContext *tx_ctx,
                                 const PreparedRowWrite* mutate) {
  DCHECK(tx_ctx != NULL) << "you must have a WriteTransactionContext";
  DCHECK(tx_ctx->op_id().IsInitialized()) << "TransactionContext OpId needed for anchoring";

  gscoped_ptr<OperationResultPB> result(new OperationResultPB());

  // Validate the update.
  RowChangeListDecoder rcl_decoder(schema_, *mutate->changelist());
  Status s = rcl_decoder.Init();
  if (rcl_decoder.is_reinsert()) {
    // REINSERT mutations are the byproduct of an INSERT on top of a ghost
    // row, not something the user is allowed to specify on their own.
    s = Status::InvalidArgument("User may not specify REINSERT mutations");
  }
  if (!s.ok()) {
    tx_ctx->AddFailedMutation(s);
    return s;
  }


  Timestamp ts = tx_ctx->timestamp();

  ProbeStats stats;
  // Submit the stats before returning from this function
  ProbeStatsSubmitter submitter(stats, metrics_.get());

  // First try to update in memrowset.
  s = memrowset_->MutateRow(ts,
                            *mutate->probe(),
                            *mutate->changelist(),
                            tx_ctx->op_id(),
                            &stats,
                            result.get());
  if (s.ok()) {
    RETURN_NOT_OK(tx_ctx->AddMutation(ts, result.Pass()));
    return s;
  }
  if (!s.IsNotFound()) {
    tx_ctx->AddFailedMutation(s);
    return s;
  }

  // Next, check the disk rowsets.

  // TODO: could iterate the rowsets in a smart order
  // based on recent statistics - eg if a rowset is getting
  // updated frequently, pick that one first.
  vector<RowSet *> to_check;
  rowsets_->FindRowSetsWithKeyInRange(mutate->probe()->encoded_key_slice(), &to_check);
  BOOST_FOREACH(RowSet *rs, to_check) {
    s = rs->MutateRow(ts, *mutate->probe(), *mutate->changelist(), tx_ctx->op_id(),
                      &stats, result.get());
    if (s.ok()) {
      RETURN_NOT_OK(tx_ctx->AddMutation(ts, result.Pass()));
      return s;
    }
    if (!s.IsNotFound()) {
      tx_ctx->AddFailedMutation(s);
      return s;
    }
  }

  s = Status::NotFound("key not found");
  tx_ctx->AddFailedMutation(s);
  return s;
}

void Tablet::AtomicSwapRowSets(const RowSetVector &old_rowsets,
                               const RowSetVector &new_rowsets,
                               MvccSnapshot *snap_under_lock = NULL) {
  boost::lock_guard<rw_semaphore> lock(component_lock_);
  AtomicSwapRowSetsUnlocked(old_rowsets, new_rowsets, snap_under_lock);
}

void Tablet::AtomicSwapRowSetsUnlocked(const RowSetVector &old_rowsets,
                                       const RowSetVector &new_rowsets,
                                       MvccSnapshot *snap_under_lock = NULL) {

  RowSetVector post_swap;

  // O(n^2) diff algorithm to collect the set of rowsets excluding
  // the rowsets that were included in the compaction
  int num_replaced = 0;

  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_->all_rowsets()) {
    // Determine if it should be removed
    bool should_remove = false;
    BOOST_FOREACH(const shared_ptr<RowSet> &l_input, old_rowsets) {
      if (l_input == rs) {
        should_remove = true;
        num_replaced++;
        break;
      }
    }
    if (!should_remove) {
      post_swap.push_back(rs);
    }
  }

  CHECK_EQ(num_replaced, old_rowsets.size());

  // Then push the new rowsets on the end.
  std::copy(new_rowsets.begin(), new_rowsets.end(), std::back_inserter(post_swap));
  shared_ptr<RowSetTree> new_tree(new RowSetTree());
  CHECK_OK(new_tree->Reset(post_swap));
  rowsets_.swap(new_tree);

  if (snap_under_lock != NULL) {
    *snap_under_lock = MvccSnapshot(mvcc_);

    // We expect that there are no transactions in flight, since we hold component_lock_
    // in exclusive mode. For our compaction logic to be correct, we need to ensure that
    // no mutations in the 'old_rowsets' are associated with transactions that are
    // uncommitted in 'snap_under_lock'. If there were an in-flight transaction in
    // 'snap_under_lock', it would be possible that it wrote some mutations into
    // 'old_rowsets'.
    //
    // This property is ensured by the ordering between shared-locking 'component_lock_'
    // and creating the ScopedTransaction during mutations.  The transaction should be
    // started only after the 'component_lock' is taken, and committed before it is
    // released.
    CHECK_EQ(snap_under_lock->num_transactions_in_flight(), 0);
  }
}

Status Tablet::DoMajorDeltaCompaction(const ColumnIndexes& column_indexes,
                                      shared_ptr<RowSet> input_rs) {
  vector<shared_ptr<RowSet> > new_rowsets;
  vector<shared_ptr<RowSet> > input_rowsets;

  gscoped_ptr<RowSetColumnUpdater> updater;
  DiskRowSet* input_drs = NULL;
  int64_t delta_store_id;
  gscoped_ptr<MajorDeltaCompaction> compaction;

  {
    // Avoid holding component_lock_ for too long
    boost::shared_lock<rw_semaphore> lock(component_lock_);
    updater.reset(new RowSetColumnUpdater(metadata(), input_rs->metadata(), column_indexes));
    input_drs = down_cast<DiskRowSet*>(input_rs.get());
    compaction.reset(input_drs->NewMajorDeltaCompaction(updater.get(), &delta_store_id));
  }

  shared_ptr<DiskRowSet> new_rowset;
  shared_ptr<RowSetMetadata> meta;

  // TODO: isn't there a race here? If someone delta-flushed this DRS right here,
  // we'd end up not having included the newly flushed delta file in our compaction,
  // and then the "SetDMSFrom" down below would carry over an empty DRS.
  // I also wonder whether the rowset IDs are right.

  {
    // TODO: make this more fine-grained if possible. Will make sense
    // to re-touch this area once integrated with maintenance ops
    // scheduling.
    shared_ptr<boost::mutex::scoped_try_lock> input_rs_lock(
        new boost::mutex::scoped_try_lock(*input_drs->compact_flush_lock()));
    CHECK(input_rs_lock->owns_lock());

    shared_ptr<boost::mutex::scoped_try_lock> input_dt_lock(
        new boost::mutex::scoped_try_lock(*input_drs->delta_tracker()->compact_flush_lock()));
    CHECK(input_dt_lock->owns_lock());

    boost::shared_lock<rw_spinlock> lock(schema_lock_.get_lock());

    BlockId delta_block;
    size_t ndeltas = 0;

    RETURN_NOT_OK(compaction->Compact(&meta, &delta_block, &ndeltas));
    if (ndeltas > 0) {
      RETURN_NOT_OK(meta->CommitRedoDeltaDataBlock(delta_store_id, delta_block));
    }
    RETURN_NOT_OK_PREPEND(meta->Flush(),
                          "Unable to commit rowset metadata " + meta->ToString());
    RETURN_NOT_OK_PREPEND(DiskRowSet::Open(meta, opid_anchor_registry_, &new_rowset),
                          "Unable to open compaction results " + meta->ToString());

    new_rowset->SetDMSFrom(input_drs);

    new_rowsets.push_back(new_rowset);
    input_rowsets.push_back(input_rs);

    // Ensure that the latest schema is set to the new RowSets
    RETURN_NOT_OK(new_rowset->AlterSchema(schema_));

    AtomicSwapRowSets(input_rowsets, new_rowsets);
    return FlushMetadata(input_rowsets, boost::assign::list_of(meta), kNoMrsFlushed);
  }
}

Status Tablet::DeleteCompactionInputs(const RowSetsInCompaction &input) {
  //BOOST_FOREACH(const shared_ptr<RowSet> &l_input, input.rowsets()) {
  //  LOG(INFO) << "Removing compaction input rowset " << l_input->ToString();
  //}
  return Status::OK();
}


Status Tablet::Flush() {
  RowSetsInCompaction input;
  shared_ptr<MemRowSet> old_ms;
  {
    // Lock the component_lock_ in exclusive mode.
    // This shuts out any concurrent readers or writers for as long
    // as the swap takes.
    boost::lock_guard<rw_semaphore> lock(component_lock_);
    RETURN_NOT_OK(ReplaceMemRowSetUnlocked(schema(), &input, &old_ms));
  }
  return Flush(input, old_ms, schema());
}

Status Tablet::ReplaceMemRowSetUnlocked(const Schema& schema,
                                        RowSetsInCompaction *compaction,
                                        shared_ptr<MemRowSet> *old_ms) {
  // swap in a new memrowset
  *old_ms = memrowset_;
  memrowset_.reset(new MemRowSet(next_mrs_id_, schema, opid_anchor_registry_));
  // increment the next mrs_id
  next_mrs_id_++;

  if ((*old_ms)->empty()) {
    return Status::OK();
  }

  // Mark the memrowset rowset as locked, so compactions won't consider it
  // for inclusion in any concurrent compactions.
  shared_ptr<boost::mutex::scoped_try_lock> ms_lock(
    new boost::mutex::scoped_try_lock(*((*old_ms)->compact_flush_lock())));
  CHECK(ms_lock->owns_lock());
  compaction->AddRowSet(*old_ms, ms_lock);

  AtomicSwapRowSetsUnlocked(RowSetVector(), boost::assign::list_of(*old_ms), NULL);

  return Status::OK();
}

Status Tablet::Flush(const RowSetsInCompaction& input,
                     const shared_ptr<MemRowSet>& old_ms,
                     const Schema& schema) {
  CHECK(open_);

  if (input.num_rowsets() == 1 && old_ms->empty()) {
    // flushing empty memrowset is a no-op
    LOG(INFO) << "Flush requested on empty memrowset";
    return Status::OK();
  }

  // Step 1. Freeze the old memrowset by blocking readers and swapping
  // it in as a new rowset, replacing it with an empty one.

  // TODO(perf): there's a memrowset.Freeze() call which we might be able to
  // use to improve iteration performance during the flush. The old design
  // used this, but not certain whether it's still doable with the new design.

  uint64_t start_insert_count = old_ms->debug_insert_count();
  int64_t mrs_being_flushed = old_ms->mrs_id();

  LOG(INFO) << "Flush: entering stage 1 (freezing old memrowset from inserts)";

  if (flush_hooks_) {
    RETURN_NOT_OK_PREPEND(flush_hooks_->PostSwapNewMemRowSet(),
                          "PostSwapNewMemRowSet hook failed");
  }

  input.DumpToLog();
  LOG(INFO) << "Memstore in-memory size: " << old_ms->memory_footprint() << " bytes";

  RETURN_NOT_OK(DoCompactionOrFlush(schema, input, mrs_being_flushed));

  // Sanity check that no insertions happened during our flush.
  CHECK_EQ(start_insert_count, old_ms->debug_insert_count())
    << "Sanity check failed: insertions continued in memrowset "
    << "after flush was triggered! Aborting to prevent dataloss.";

  return Status::OK();
}

Status Tablet::CreatePreparedAlterSchema(AlterSchemaTransactionContext *tx_ctx,
                                         const Schema* schema) {
  if (!key_schema_.KeyEquals(*schema)) {
    return Status::InvalidArgument("Schema keys cannot be altered",
                                   schema->CreateKeyProjection().ToString());
  }

  if (!schema->has_column_ids()) {
    // this probably means that the request is not from the Master
    return Status::InvalidArgument("Missing Column IDs");
  }

  // TODO: Take a "global tablet lock", We should probably add the RW sched.
  //
  // User Operation Sched:
  //  SHARED_LOCK: read and writes can happen concurrently
  //  EXCLUSIVE_LOCK: alter schema must run when no reads/writes are in progress
  //                  but we can keep compaction running, since is not a big deal
  //                  if we compact something with the old schema, we have already
  //                  plenty of files with the old schema.
  tx_ctx->acquire_tablet_lock(component_lock_);

  tx_ctx->set_schema(schema);
  return Status::OK();
}

Status Tablet::AlterSchema(AlterSchemaTransactionContext *tx_ctx) {
  DCHECK(key_schema_.KeyEquals(*DCHECK_NOTNULL(tx_ctx->schema()))) <<
    "Schema keys cannot be altered";

  RowSetsInCompaction input;
  shared_ptr<MemRowSet> old_ms;
  {
    boost::unique_lock<percpu_rwlock> schema_lock(schema_lock_);

    // If the current version >= new version, there is nothing to do.
    bool same_schema = schema_.Equals(*tx_ctx->schema());
    if (metadata_->schema_version() >= tx_ctx->schema_version()) {
      LOG(INFO) << "Already running schema version " << metadata_->schema_version()
                << " got alter request for version " << tx_ctx->schema_version();
      return Status::OK();
    }

    LOG(INFO) << "Alter schema from " << schema_.ToString()
              << " version " << metadata_->schema_version()
              << " to " << tx_ctx->schema()->ToString()
              << " version " << tx_ctx->schema_version();
    schema_ = *tx_ctx->schema();
    metadata_->SetSchema(schema_, tx_ctx->schema_version());
    if (tx_ctx->has_new_table_name()) {
      metadata_->SetTableName(tx_ctx->new_table_name());
    }

    // If the current schema and the new one are equal, there is nothing to do.
    if (same_schema) {
      schema_lock.unlock();
      return metadata_->Flush();
    }

    // Update the DiskRowSet/DeltaTracker
    // TODO: This triggers a flush of the DeltaMemStores...
    //       The flush should be just a message (async)...
    //       with the current code the only way we can do a flush ouside this big lock
    //       is to get the list of DeltaMemStores out from the AlterSchema method...
    BOOST_FOREACH(const shared_ptr<RowSet>& rs, rowsets_->all_rowsets()) {
      RETURN_NOT_OK(rs->AlterSchema(schema_));
    }
  }

  // Replace the MemRowSet
  RETURN_NOT_OK(ReplaceMemRowSetUnlocked(schema_, &input, &old_ms));

  // The "global tablet lock" is acquired in CreatePreparedAlterSchema()
  tx_ctx->release_tablet_lock();

  // Flush the old MemRowSet
  return Flush(input, old_ms, *tx_ctx->schema());
}

void Tablet::SetCompactionHooksForTests(
  const shared_ptr<Tablet::CompactionFaultHooks> &hooks) {
  compaction_hooks_ = hooks;
}

void Tablet::SetFlushHooksForTests(
  const shared_ptr<Tablet::FlushFaultHooks> &hooks) {
  flush_hooks_ = hooks;
}

void Tablet::SetFlushCompactCommonHooksForTests(
  const shared_ptr<Tablet::FlushCompactCommonHooks> &hooks) {
  common_hooks_ = hooks;
}

int32_t Tablet::CurrentMrsIdForTests() const {
  return memrowset_->mrs_id();
}

Status Tablet::PickRowSetsToCompact(RowSetsInCompaction *picked,
                                    CompactFlags flags) const {
  // Grab a local reference to the current RowSetTree. This is to avoid
  // holding the component_lock_ for too long. See the comment on component_lock_
  // in tablet.h for details on why that would be bad.
  shared_ptr<RowSetTree> rowsets_copy;
  {
    boost::shared_lock<rw_semaphore> lock(component_lock_);
    rowsets_copy = rowsets_;
  }

  boost::lock_guard<boost::mutex> compact_lock(compact_select_lock_);
  CHECK_EQ(picked->num_rowsets(), 0);

  unordered_set<RowSet*> picked_set;

  if (flags & FORCE_COMPACT_ALL) {
    // Compact all rowsets, regardless of policy.
    BOOST_FOREACH(const shared_ptr<RowSet>& rs, rowsets_copy->all_rowsets()) {
      if (rs->IsAvailableForCompaction()) {
        picked_set.insert(rs.get());
      }
    }
  } else {
    // Let the policy decide which rowsets to compact.
    RETURN_NOT_OK(compaction_policy_->PickRowSets(*rowsets_copy, &picked_set));
  }

  boost::shared_lock<rw_semaphore> lock(component_lock_);
  BOOST_FOREACH(const shared_ptr<RowSet>& rs, rowsets_->all_rowsets()) {
    if (picked_set.erase(rs.get()) == 0) {
      // Not picked.
      continue;
    }

    // Grab the compact_flush_lock: this prevents any other concurrent
    // compaction from selecting this same rowset, and also ensures that
    // we don't select a rowset which is currently in the middle of being
    // flushed.
    shared_ptr<boost::mutex::scoped_try_lock> lock(
      new boost::mutex::scoped_try_lock(*rs->compact_flush_lock()));
    CHECK(lock->owns_lock()) << rs->ToString() << " appeared available for "
      "compaction when inputs were selected, but was unable to lock its "
      "compact_flush_lock to prepare for compaction.";

    // Push the lock on our scoped list, so we unlock when done.
    picked->AddRowSet(rs, lock);
  }

  // When we iterated through the current rowsets, we should have found all of the
  // rowsets that we picked. If we didn't, that implies that some other thread swapped
  // them out while we were making our selection decision -- that's not possible
  // since we only picked rowsets that were marked as available for compaction.
  if (!picked_set.empty()) {
    BOOST_FOREACH(const RowSet* not_found, picked_set) {
      LOG(ERROR) << "Rowset selected for compaction but not available anymore: "
                 << not_found->ToString();
    }
    LOG(FATAL) << "Was unable to find all rowsets selected for compaction";
  }
  return Status::OK();
}

void Tablet::GetRowSetsForTests(RowSetVector* out) {
  shared_ptr<RowSetTree> rowsets_copy;
  {
    boost::shared_lock<rw_semaphore> lock(component_lock_);
    rowsets_copy = rowsets_;
  }
  BOOST_FOREACH(const shared_ptr<RowSet>& rs, rowsets_copy->all_rowsets()) {
    out->push_back(rs);
  }
}

Status Tablet::FlushMetadata(const RowSetVector& to_remove,
                             const RowSetMetadataVector& to_add,
                             int64_t mrs_being_flushed) {
  RowSetMetadataIds to_remove_meta;
  BOOST_FOREACH(const shared_ptr<RowSet>& rowset, to_remove) {
    // Skip MemRowSet & DuplicatingRowSets which don't have metadata
    if (rowset->metadata().get() == NULL) continue;
    to_remove_meta.insert(rowset->metadata()->id());
  }

  // If we're flushing an mrs update the latest durable one in the metadata
  if (mrs_being_flushed != kNoMrsFlushed) {
    return metadata_->UpdateAndFlush(to_remove_meta, to_add, mrs_being_flushed, NULL);
  }
  return metadata_->UpdateAndFlush(to_remove_meta, to_add, NULL);
}

Status Tablet::DoCompactionOrFlush(const Schema& schema,
                                   const RowSetsInCompaction &input,
                                   int64_t mrs_being_flushed) {
  LOG(INFO) << "Compaction: entering phase 1 (flushing snapshot)";

  MvccSnapshot flush_snap(mvcc_);

  VLOG(1) << "Flushing with MVCC snapshot: " << flush_snap.ToString();

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostTakeMvccSnapshot(),
                          "PostTakeMvccSnapshot hook failed");
  }

  shared_ptr<CompactionInput> merge;
  RETURN_NOT_OK(input.CreateCompactionInput(flush_snap, &schema, &merge));

  RollingDiskRowSetWriter drsw(metadata_.get(), merge->schema(), bloom_sizing(),
                               compaction_policy_->target_rowset_size());
  RETURN_NOT_OK_PREPEND(drsw.Open(), "Failed to open DiskRowSet for flush");
  RETURN_NOT_OK_PREPEND(FlushCompactionInput(merge.get(), flush_snap, &drsw),
                        "Flush to disk failed");
  RETURN_NOT_OK_PREPEND(drsw.Finish(), "Failed to finish DRS writer");

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostWriteSnapshot(),
                          "PostWriteSnapshot hook failed");
  }

  // Though unlikely, it's possible that all of the input rows were actually
  // GCed in this compaction. In that case, we don't actually want to reopen.
  bool gced_all_input = drsw.written_count() == 0;
  if (gced_all_input) {
    LOG(INFO) << "Compaction resulted in no output rows (all input rows were GCed!)";
    LOG(INFO) << "Removing all input rowsets.";
    AtomicSwapRowSets(input.rowsets(), RowSetVector());

    // Remove old rowsets.
    // TODO: Consensus catch-up may want to reserve the compaction inputs.
    WARN_NOT_OK(DeleteCompactionInputs(input),
                "Unable to remove compaction inputs. Will GC later.");

    // Write out the new Tablet Metadata
    return FlushMetadata(input.rowsets(), RowSetMetadataVector(), mrs_being_flushed);
  }

  // The RollingDiskRowSet writer wrote out one or more RowSets as the compaction
  // output. Open these into 'new_rowsets'.
  vector<shared_ptr<RowSet> > new_disk_rowsets;
  RowSetMetadataVector new_drs_metas;
  drsw.GetWrittenRowSetMetadata(&new_drs_metas);

  if (metrics_.get()) metrics_->bytes_flushed->IncrementBy(drsw.written_size());
  CHECK(!new_drs_metas.empty());
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, new_drs_metas) {
    shared_ptr<DiskRowSet> new_rowset;
    Status s = DiskRowSet::Open(meta, opid_anchor_registry_, &new_rowset);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to open snapshot compaction results " << meta->ToString() << ": "
                   << s.ToString();
      return s;
    }
    new_disk_rowsets.push_back(new_rowset);
  }

  // Finished Phase 1. Start duplicating any new updates into the new on-disk rowsets.
  //
  // During Phase 1, we may have missed some updates which came into the input rowsets
  // while we were writing. So, we can't immediately start reading from the on-disk
  // rowsets alone. Starting here, we continue to read from the original rowset(s), but
  // mirror updates to both the input and the output data.
  //
  LOG(INFO) << "Compaction: entering phase 2 (starting to duplicate updates in new rowsets)";
  shared_ptr<DuplicatingRowSet> inprogress_rowset(
    new DuplicatingRowSet(input.rowsets(), new_disk_rowsets));
  MvccSnapshot snap2;
  AtomicSwapRowSets(input.rowsets(), boost::assign::list_of(inprogress_rowset), &snap2);

  // Ensure that the latest schema is set to the new RowSets
  {
    boost::shared_lock<rw_spinlock> lock(schema_lock_.get_lock());
    BOOST_FOREACH(const shared_ptr<RowSet>& rs, new_disk_rowsets) {
      RETURN_NOT_OK_PREPEND(rs->AlterSchema(schema_),
                            "Failed to set current schema on latest RS");
    }
  }

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostSwapInDuplicatingRowSet(),
                          "PostSwapInDuplicatingRowSet hook failed");
  }

  // Phase 2. Some updates may have come in during Phase 1 which are only reflected in the
  // input rowsets, but not in the output rowsets. Here we re-scan the compaction input, copying
  // those missed updates into the new rowset's DeltaTracker.
  //
  // TODO: is there some bug here? Here's a potentially bad scenario:
  // - during flush, timestamp 1 updates a flushed row
  // - At the beginning of step 4, timestamp 2 updates the same flushed row, followed by ~1000
  //   more updates against the new rowset. This causes the new rowset to flush its deltas
  //   before timestamp 1 is transferred to it.
  // - Now the redos_0 deltafile in the new rowset includes timestamp 2-1000, and the DMS is empty.
  // - This code proceeds, and pushes timestamp1 into the DMS.
  // - DMS eventually flushes again, and redos_1 includes an _earlier_ update than redos_0.
  // At read time, since we apply updates from the redo logs in order, we might end up reading
  // the earlier data instead of the later data.
  //
  // Potential solutions:
  // 1) don't apply the changes in step 4 directly into the new rowset's DMS. Instead, reserve
  //    redos_0 for these edits, and write them directly to that file, even though it will likely
  //    be very small.
  // 2) at read time, as deltas are applied, keep track of the max timestamp for each of the columns
  //    and don't let an earlier update overwrite a later one.
  // 3) don't allow DMS to flush in an in-progress rowset.
  LOG(INFO) << "Compaction Phase 2: carrying over any updates which arrived during Phase 1";
  LOG(INFO) << "Phase 2 snapshot: " << snap2.ToString();
  {
    boost::shared_lock<rw_spinlock> lock(schema_lock_.get_lock());
    RETURN_NOT_OK_PREPEND(input.CreateCompactionInput(snap2, &schema_, &merge),
                          "Failed to create compaction inputs");
  }

  // Update the output rowsets with the deltas that came in in phase 1, before we swapped
  // in the DuplicatingRowSets. This will perform a flush of the updated DeltaTrackers
  // in the end so that the data that is reported in the log as belonging to the input
  // rowsets is flushed.
  RETURN_NOT_OK_PREPEND(ReupdateMissedDeltas(metadata_->oid(),
                                             merge.get(),
                                             flush_snap,
                                             snap2,
                                             new_disk_rowsets),
                        "Failed to re-update deltas missed during compaction phase 1");

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostReupdateMissedDeltas(),
                          "PostReupdateMissedDeltas hook failed");
  }

  // ------------------------------
  // Flush was successful.

  // TODO: Commit the swap. We used to write to a 'tmp' location and then
  // rename into place, but now that compaction uses a rolling writer, we have
  // multiple outputs, and we can't do the atomic multi-file rename. This will
  // be made atomic by Matteo's metadata branch.

  // Replace the compacted rowsets with the new on-disk rowsets.
  AtomicSwapRowSets(boost::assign::list_of(inprogress_rowset), new_disk_rowsets);

  // Write out the new Tablet Metadata
  RETURN_NOT_OK_PREPEND(FlushMetadata(input.rowsets(), new_drs_metas, mrs_being_flushed),
                        "Failed to flush new tablet metadata");

  // Remove old rowsets
  WARN_NOT_OK(DeleteCompactionInputs(input),
              "Unable to remove compaction inputs. Will GC later.");

  LOG(INFO) << "Successfully flush/compacted " << drsw.written_count()
            << " rows" << "(" << drsw.written_size() << " bytes)";

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostSwapNewRowSet(),
                          "PostSwapNewRowSet hook failed");
  }

  return Status::OK();
}

Status Tablet::Compact(CompactFlags flags) {
  CHECK(open_);

  RowSetsInCompaction input;
  // Step 1. Capture the rowsets to be merged
  RETURN_NOT_OK_PREPEND(PickRowSetsToCompact(&input, flags),
                        "Failed to pick rowsets to compact");
  if (input.num_rowsets() < 2) {
    VLOG(1) << "Not enough rowsets to run compaction! Aborting...";
    return Status::OK();
  }
  LOG(INFO) << "Compaction: stage 1 complete, picked "
            << input.num_rowsets() << " rowsets to compact";
  if (compaction_hooks_) {
    RETURN_NOT_OK_PREPEND(compaction_hooks_->PostSelectIterators(),
                          "PostSelectIterators hook failed");
  }

  input.DumpToLog();

  return DoCompactionOrFlush(schema(), input, kNoMrsFlushed);
}

Status Tablet::DebugDump(vector<string> *lines) {
  boost::shared_lock<rw_semaphore> lock(component_lock_);

  LOG_STRING(INFO, lines) << "Dumping tablet:";
  LOG_STRING(INFO, lines) << "---------------------------";

  LOG_STRING(INFO, lines) << "MRS " << memrowset_->ToString() << ":";
  RETURN_NOT_OK(memrowset_->DebugDump(lines));

  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_->all_rowsets()) {
    LOG_STRING(INFO, lines) << "RowSet " << rs->ToString() << ":";
    RETURN_NOT_OK(rs->DebugDump(lines));
  }

  return Status::OK();
}

Status Tablet::CaptureConsistentIterators(
  const Schema *projection,
  const MvccSnapshot &snap,
  const ScanSpec *spec,
  vector<shared_ptr<RowwiseIterator> > *iters) const {
  boost::shared_lock<rw_semaphore> lock(component_lock_);

  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  vector<shared_ptr<RowwiseIterator> > ret;

  // Grab the memrowset iterator.
  shared_ptr<RowwiseIterator> ms_iter(memrowset_->NewRowIterator(projection, snap));
  ret.push_back(ms_iter);

  // We can only use this optimization if there is a single encoded predicate
  // TODO : should we even support multiple predicates on the key, given they're
  // currently ANDed together? This should be the job for a separate query
  // optimizer.
  if (spec != NULL && spec->encoded_ranges().size() == 1) {
    const EncodedKeyRange &range = *(spec->encoded_ranges()[0]);
    // TODO : support open-ended intervals
    if (range.has_lower_bound() && range.has_upper_bound()) {
      vector<RowSet *> interval_sets;
      rowsets_->FindRowSetsIntersectingInterval(range.lower_bound().encoded_key(),
                                                range.upper_bound().encoded_key(),
                                                &interval_sets);
      BOOST_FOREACH(const RowSet *rs, interval_sets) {
        shared_ptr<RowwiseIterator> row_it(rs->NewRowIterator(projection, snap));
        ret.push_back(row_it);
      }
      ret.swap(*iters);
      return Status::OK();
    }
  }

  // If there are no encoded predicates or they represent an open-ended range, then
  // fall back to grabbing all rowset iterators
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, rowsets_->all_rowsets()) {
    shared_ptr<RowwiseIterator> row_it(rs->NewRowIterator(projection, snap));
    ret.push_back(row_it);
  }

  // Swap results into the parameters.
  ret.swap(*iters);
  return Status::OK();
}

Status Tablet::CountRows(uint64_t *count) const {
  // First grab a consistent view of the components of the tablet.
  shared_ptr<MemRowSet> memrowset;
  shared_ptr<RowSetTree> rowsets_copy;

  {
    boost::shared_lock<rw_semaphore> lock(component_lock_);
    memrowset = memrowset_;
    rowsets_copy = rowsets_;
  }

  // Now sum up the counts.
  *count = memrowset->entry_count();
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, rowsets_copy->all_rowsets()) {
    rowid_t l_count;
    RETURN_NOT_OK(rowset->CountRows(&l_count));
    *count += l_count;
  }

  return Status::OK();
}

size_t Tablet::MemRowSetSize() const {
  return memrowset_->memory_footprint();
}

size_t Tablet::EstimateOnDiskSize() const {
  shared_ptr<RowSetTree> rowsets_copy;

  {
    boost::shared_lock<rw_semaphore> lock(component_lock_);
    rowsets_copy = rowsets_;
  }

  size_t ret = 0;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, rowsets_copy->all_rowsets()) {
    ret += rowset->EstimateOnDiskSize();
  }

  return ret;
}

size_t Tablet::DeltaMemStoresSize() const {
  shared_ptr<RowSetTree> rowsets_copy;

  {
    boost::shared_lock<rw_semaphore> lock(component_lock_);
    rowsets_copy = rowsets_;
  }

  size_t ret = 0;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, rowsets_copy->all_rowsets()) {
    ret += rowset->DeltaMemStoreSize();
  }

  return ret;
}

Status Tablet::FlushBiggestDMS() {
  shared_ptr<RowSetTree> rowsets_copy;

  {
    boost::shared_lock<rw_semaphore> lock(component_lock_);
    rowsets_copy = rowsets_;
  }

  int64_t max_size = -1;
  shared_ptr<RowSet> biggest_dms;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, rowsets_copy->all_rowsets()) {
    int64_t current = rowset->DeltaMemStoreSize();
    if (current > max_size) {
      max_size = current;
      biggest_dms = rowset;
    }
  }
  return max_size > 0 ? biggest_dms->FlushDeltas() : Status::OK();
}

Status Tablet::MinorCompactWorstDeltas() {
  shared_ptr<RowSetTree> rowsets_copy;

  {
    boost::shared_lock<rw_semaphore> lock(component_lock_);
    rowsets_copy = rowsets_;
  }

  int worst_delta_count = -1;
  shared_ptr<RowSet> worst_rs;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, rowsets_copy->all_rowsets()) {
    int count = rowset->CountDeltaStores();
    if (count > worst_delta_count) {
      worst_rs = rowset;
      worst_delta_count = count;
    }
  }

  if (worst_delta_count > 1) {
    RETURN_NOT_OK_PREPEND(worst_rs->MinorCompactDeltaStores(),
                          "Failed minor delta compaction on " + worst_rs->ToString());
  }
  return Status::OK();
}

size_t Tablet::num_rowsets() const {
  boost::shared_lock<rw_semaphore> lock(component_lock_);
  return rowsets_->all_rowsets().size();
}

Tablet::Iterator::Iterator(const Tablet *tablet,
                           const Schema &projection,
                           const MvccSnapshot &snap)
    : tablet_(tablet),
      projection_(projection),
      snap_(snap),
      encoder_(tablet_->key_schema()) {
}

Tablet::Iterator::~Iterator() {}

Status Tablet::Iterator::Init(ScanSpec *spec) {
  DCHECK(iter_.get() == NULL);

  RETURN_NOT_OK(tablet_->GetMappedReadProjection(projection_, &projection_));

  vector<shared_ptr<RowwiseIterator> > iters;
  if (spec != NULL) {
    encoder_.EncodeRangePredicates(spec);
  }
  RETURN_NOT_OK(tablet_->CaptureConsistentIterators(
      &projection_, snap_, spec, &iters));
  iter_.reset(new UnionIterator(iters));
  RETURN_NOT_OK(iter_->Init(spec));
  return Status::OK();
}

Status Tablet::Iterator::PrepareBatch(size_t *nrows) {
  DCHECK(iter_.get() != NULL) << "Not initialized!";
  return iter_->PrepareBatch(nrows);
}

bool Tablet::Iterator::HasNext() const {
  DCHECK(iter_.get() != NULL) << "Not initialized!";
  return iter_->HasNext();
}

Status Tablet::Iterator::MaterializeBlock(RowBlock *dst) {
  DCHECK(iter_.get() != NULL) << "Not initialized!";
  return iter_->MaterializeBlock(dst);
}

Status Tablet::Iterator::FinishBatch() {
  DCHECK(iter_.get() != NULL) << "Not initialized!";
  return iter_->FinishBatch();
}

string Tablet::Iterator::ToString() const {
  string s;
  s.append("tablet iterator: ");
  if (iter_.get() == NULL) {
    s.append("NULL");
  } else {
    s.append(iter_->ToString());
  }
  return s;
}


} // namespace tablet
} // namespace kudu
