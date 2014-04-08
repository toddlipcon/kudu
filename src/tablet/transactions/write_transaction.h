// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_WRITE_TRANSACTION_H_
#define KUDU_TABLET_WRITE_TRANSACTION_H_

#include <vector>

#include "common/schema.h"
#include "common/row_changelist.h"
#include "gutil/macros.h"
#include "tablet/mvcc.h"
#include "tablet/tablet.pb.h"
#include "tablet/transactions/transaction.h"
#include "tablet/transactions/write_util.h"
#include "util/task_executor.h"

namespace kudu {
class ConstContiguousRow;
class RowwiseRowBlockPB;

namespace consensus {
class Consensus;
}

namespace tserver {
class WriteRequestPB;
class WriteResponsePB;
}

namespace tablet {
class PreparedRowWrite;
class RowSetKeyProbe;
class ScopedRowLock;

// A transaction context for a batch of inserts/mutates. This class holds and
// owns most everything related to a transaction, including the acquired locks
// (row and component), the PreparedRowWrites, the Replicate and Commit messages.
// With the exception of the rows (ConstContiguousRow), and the mutations
// (RowChangeList), all the transaction related pointers are owned by this class
// and destroyed on Reset() or by the destructor.
//
// IMPORTANT: All the acquired locks will not be released unless the transaction
// context is either destroyed or Reset() or release_locks() is called, beware of
// this when using the transaction context or there will be lock leaks.
//
// Used when logging to WAL in that we keep track of where inserts/updates
// were applied and add that information to the commit message that is stored
// on the WAL.
//
// NOTE: this class isn't thread safe.
class WriteTransactionContext : public TransactionContext {

 public:
  WriteTransactionContext()
      : TransactionContext(NULL),
        failed_operations_(0),
        request_(NULL),
        response_(NULL),
        component_lock_(NULL),
        mvcc_tx_(NULL) {
  }

  // ctor used by the LEADER replica
  WriteTransactionContext(TabletPeer* tablet_peer,
                          const tserver::WriteRequestPB *request,
                          tserver::WriteResponsePB *response)
      : TransactionContext(tablet_peer),
        failed_operations_(0),
        request_(request),
        response_(response),
        component_lock_(NULL),
        mvcc_tx_(NULL) {
  }

  // ctor used by FOLLOWER/LEARNER replicas
  WriteTransactionContext(TabletPeer* tablet_peer,
                          const tserver::WriteRequestPB *request)
      : TransactionContext(tablet_peer),
        failed_operations_(0),
        request_(request),
        response_(NULL),
        component_lock_(NULL),
        mvcc_tx_(NULL) {
  }

  // Adds an applied insert to this TransactionContext, including the
  // id of the MemRowSet to which it was applied.
  Status AddInsert(const Timestamp &tx_id,
                   int64_t mrs_id);

  // Adds a failed insert to this TransactionContext, including the status
  // explaining why the insert failed.
  void AddFailedInsert(const Status &status);

  // Adds an applied mutation to this TransactionContext, including the
  // tablet id, the mvcc transaction id, the mutation that was applied
  // and the delta stores that were mutated.
  Status AddMutation(const Timestamp &tx_id,
                     gscoped_ptr<OperationResultPB> result);

  // Adds a missed mutation to this TransactionContext.
  // Missed mutations are the ones that are applied on Phase 2 of compaction
  // and reflect updates to the old DeltaMemStore that were not yet present
  // in the new DeltaMemStore.
  // The passed 'changelist' is copied into a protobuf and does not need to
  // be alive after this method returns.
  Status AddMissedMutation(const Timestamp& timestamp,
                           gscoped_ptr<RowwiseRowBlockPB> row_key,
                           const RowChangeList& changelist,
                           gscoped_ptr<OperationResultPB> result);

  // Adds a failed mutation to this TransactionContext, including the status
  // explaining why it failed.
  void AddFailedMutation(const Status &status);

  bool is_all_success() const {
    return failed_operations_ == 0;
  }

  // Returns the result of this transaction in its protocol buffers form.
  // The transaction result holds information on exactly which memory stores
  // were mutated in the context of this transaction and can be used to
  // perform recovery.
  const TxResultPB& Result() const {
    return result_pb_;
  }

  // Returns the original client request for this transaction, if there was
  // one.
  const tserver::WriteRequestPB *request() const {
    return request_;
  }

  // Returns the prepared response to the client that will be sent when this
  // transaction is completed, if this transaction was started by a client.
  tserver::WriteResponsePB *response() {
    return response_;
  }

  // Starts an Mvcc transaction, the ScopedTransaction will not commit until
  // commit_mvcc_tx is called. To be able to start an Mvcc transaction this
  // TransactionContext must have a hold on the MvccManager.
  Timestamp start_mvcc_tx();

  // Allows to set the current Mvcc transaction externally when
  // this TransactionContext doesn't have a handle to MvccManager.
  void set_current_mvcc_tx(gscoped_ptr<ScopedTransaction> mvcc_tx);

  // Commits the Mvcc transaction and releases the component lock. After
  // this method is called all the inserts and mutations will become
  // visible to other transactions.
  void commit();

  // Adds a PreparedRowWrite to be managed by this transaction context, as
  // created in the prepare phase.
  void add_prepared_row(gscoped_ptr<PreparedRowWrite> row) {
    rows_.push_back(row.release());
  }

  // Returns all the prepared row writes for this transaction. Usually called
  // on the apply phase to actually make changes to the tablet.
  vector<PreparedRowWrite *> &rows() {
    return rows_;
  }

  // Sets the component lock for this transaction. The lock will not be
  // unlocked unless either release_locks() or Reset() is called or this
  // TransactionContext is destroyed.
  void set_component_lock(gscoped_ptr<boost::shared_lock<rw_semaphore> > lock) {
    component_lock_.reset(lock.release());
  }

  boost::shared_lock<rw_semaphore>* component_lock() {
    return component_lock_.get();
  }

  // Releases all the row locks acquired by this transaction.
  void release_row_locks();

  // Resets this TransactionContext, releasing all locks, destroying all prepared
  // writes, clearing the transaction result _and_ committing the current Mvcc
  // transaction.
  void Reset();

  ~WriteTransactionContext() {
    Reset();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(WriteTransactionContext);

  TxResultPB result_pb_;
  int32_t failed_operations_;

  // pointers to the rpc context, request and response, lifecyle
  // is managed by the rpc subsystem. These pointers maybe NULL if the
  // transaction was not initiated by an RPC call.
  const tserver::WriteRequestPB* request_;
  tserver::WriteResponsePB* response_;

  // the rows and locks as transformed/acquired by the prepare task
  vector<PreparedRowWrite*> rows_;
  // the component lock, acquired by all inserters/updaters
  gscoped_ptr<boost::shared_lock<rw_semaphore> > component_lock_;
  gscoped_ptr<ScopedTransaction> mvcc_tx_;
};

// Executes a write transaction, leader side.
//
// Transaction execution is illustrated in the next diagram. (Further
// illustration of the inner workings of the consensus system can be found
// in consensus/consensus.h).
//
//                                 + 1) Execute()
//                                 |
//                                 |
//                                 |
//                         +-------+-------+
//                         |       |       |
// 3) Consensus::Append()  |       v       | 2) Prepare()
//                         |   (returns)   |
//                         v               v
//                  +------------------------------+
//               4) |      PrepareSucceeded()      |
//                  |------------------------------|
//                  | - continues once both        |
//                  |   phases have finished       |
//                  +--------------+---------------+
//                                 |
//                                 | Submits Apply()
//                                 v
//                     +-------------------------+
//                  5) |         Apply           |
//                     |-------------------------|
//                     | - applies transaction   |
//                     +-------------------------+
//                                 |
//                                 | 6) Calls Consensus::Commit()
//                                 v
//                                 + ApplySucceeded().
//
// 1) TabletPeer calls LeaderWriteTransaction::Execute() which creates the ReplicateMsg,
//    calls Consensus::Append() and LeaderWriteTransaction::Prepare(). Both calls execute
//    asynchronously.
//
// 2) The Prepare() call executes the sequence of steps required for a write
//    transaction, leader side. This means, decoding the client's request, and acquiring
//    all relevant row locks, as well as the component lock in shared mode.
//
// 3) Consensus::Append() completes when having replicated and persisted the client's
//    request. Depending on the consensus algorithm this might mean just writing to
//    local disk, or replicating the request across quorum replicas.
//
// 4) PrepareSucceeded() is called as a callback when both Consensus::Append() and
//    Prepare() complete. When PrepareSucceeded() is is called twice (independently
//    of who finishes first) Apply() is called, asynchronously.
//
// 5) When Apply() starts execution the TransactionContext must have been
//    passed all the PreparedRowWrites (each containing a row lock) and the
//    'component_lock'. Apply() starts the mvcc transaction and calls
//    Tablet::InsertUnlocked/Tablet::MutateUnlocked with each of the
//    PreparedRowWrites. Apply() keeps track of any single row errors
//    that might have occurred while inserting/mutating and sets those in
//    WriteResponse. TransactionContext is passed with each insert/mutate
//    to keep track of which in-memory stores were mutated.
//    After all the inserts/mutates are performed Apply() releases row
//    locks (see 'Implementation Techniques for Main Memory Database Systems',
//    DeWitt et. al.). It then readies the CommitMsg with the TXResultPB in
//    transaction context and calls ConsensusContext::Commit() which will
//    in turn trigger a commit of the consensus system.
//
// 6) After the consensus system deems the CommitMsg committed (which might
//    have different requirements depending on the consensus algorithm) the
//    ApplySucceeded callback is called and the transaction is considered
//    completed, the mvcc transaction committed (making the updates visible
//    to other transactions), the metrics updated and the transaction's
//    resources released.
//
class LeaderWriteTransaction : public LeaderTransaction {
 public:
  LeaderWriteTransaction(TransactionTracker *txn_tracker,
                         WriteTransactionContext* tx_ctx,
                         consensus::Consensus* consensus,
                         TaskExecutor* prepare_executor,
                         TaskExecutor* apply_executor,
                         simple_spinlock& prepare_replicate_lock);
 protected:

  void NewReplicateMsg(gscoped_ptr<consensus::ReplicateMsg>* replicate_msg);

  // Executes a Prepare for a write transaction, leader side.
  //
  // Acquires all the relevant row locks for the transaction, the tablet
  // component_lock in shared mode and starts an Mvcc transaction. When the task
  // is finished, the next one in the pipeline must take ownership of these.
  virtual Status Prepare();

  // Releases the write transaction's row locks and sets up the error in the WriteResponse
  virtual void PrepareFailedPreCommitHooks(gscoped_ptr<consensus::CommitMsg>* commit_msg);

  // Executes an Apply for a write transaction, leader side.
  //
  // Actually applies inserts/mutates into the tablet. After these start being
  // applied, the transaction must run to completion as there is currently no
  // means of undoing an update.
  //
  // After completing the inserts/mutates, the row locks and the mvcc transaction
  // can be released, allowing other transactions to update the same rows.
  // However the component lock must not be released until the commit msg, which
  // indicates where each of the inserts/mutates were applied, is persisted to
  // stable storage. Because of this ApplyTask must enqueue a CommitTask before
  // releasing both the row locks and deleting the MvccTransaction as we need to
  // make sure that Commits that touch the same set of rows are persisted in
  // order, for recovery.
  // This, of course, assumes that commits are executed in the same order they
  // are placed in the queue (but not necessarily in the same order of the
  // original requests) which is already a requirement of the consensus
  // algorithm.
  virtual Status Apply();

  // Actually commits the mvcc transaction.
  virtual void ApplySucceeded();

  virtual void UpdateMetrics();

  virtual WriteTransactionContext* tx_ctx() { return tx_ctx_.get(); }

 private:
  gscoped_ptr<WriteTransactionContext> tx_ctx_;
  DISALLOW_COPY_AND_ASSIGN(LeaderWriteTransaction);
};

// A context for a single row in a transaction. Contains the row, the probe
// and the row lock for an insert, or the row_key, the probe, the mutation
// and the row lock, for a mutation.
//
// This class owns the 'probe' and the 'row_lock' but does not own the 'row', 'row_key'
// or 'mutations'. The non-owned data structures are expected to last for the lifetime
// of this class.
class PreparedRowWrite {
 public:
  const ConstContiguousRow* row() const {
    return row_;
  }

  const ConstContiguousRow* row_key() const {
    return row_key_;
  }

  const RowSetKeyProbe* probe() const {
    return probe_.get();
  }

  const RowChangeList* changelist() const {
    return changelist_;
  }

  const ScopedRowLock* row_lock() const {
    return row_lock_.get();
  }

  const OperationResultPB::TxOperationTypePB write_type() const {
    return op_type_;
  }

 private:

  friend class Tablet;

  // ctor for inserts
  PreparedRowWrite(const ConstContiguousRow* row,
                   const gscoped_ptr<RowSetKeyProbe> probe,
                   const gscoped_ptr<ScopedRowLock> lock);

  // ctor for mutations
  PreparedRowWrite(const ConstContiguousRow* row_key,
                   const RowChangeList* mutations,
                   const gscoped_ptr<RowSetKeyProbe> probe,
                   const gscoped_ptr<tablet::ScopedRowLock> lock);

  const ConstContiguousRow *row_;
  const ConstContiguousRow *row_key_;
  const RowChangeList *changelist_;

  const gscoped_ptr<RowSetKeyProbe> probe_;
  const gscoped_ptr<ScopedRowLock> row_lock_;
  const OperationResultPB::TxOperationTypePB op_type_;

};

}  // namespace tablet
}  // namespace kudu


#endif /* KUDU_TABLET_WRITE_TRANSACTION_H_ */
