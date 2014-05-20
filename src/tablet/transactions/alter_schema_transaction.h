// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_ALTER_SCHEMA_TRANSACTION_H_
#define KUDU_TABLET_ALTER_SCHEMA_TRANSACTION_H_

#include <string>

#include "gutil/macros.h"
#include "tablet/transactions/transaction.h"
#include "util/task_executor.h"

namespace kudu {

class Schema;

namespace consensus {
class Consensus;
}

namespace tablet {

// Transaction Context for the AlterSchema operation.
// Keeps track of the Transaction states (request, result, ...)
class AlterSchemaTransactionState : public TransactionState {
 public:
  AlterSchemaTransactionState(TabletPeer* tablet_peer,
                              const tserver::AlterSchemaRequestPB* request,
                              tserver::AlterSchemaResponsePB* response,
                              gscoped_ptr<TransactionCompletionCallback> callback)
      : TransactionState(tablet_peer, callback),
        schema_(NULL),
        request_(request),
        response_(response) {
  }

  ~AlterSchemaTransactionState() {
    release_component_lock();
  }

  const tserver::AlterSchemaRequestPB* request() const { return request_; }
  tserver::AlterSchemaResponsePB* response() { return response_; }

  void set_schema(const Schema* schema) { schema_ = schema; }
  const Schema* schema() const { return schema_; }

  std::string new_table_name() const {
    return request_->new_table_name();
  }

  bool has_new_table_name() const {
    return request_->has_new_table_name();
  }

  uint32_t schema_version() const {
    return request_->schema_version();
  }

  void acquire_component_lock(rw_semaphore& component_lock) {
    component_lock_ = boost::unique_lock<rw_semaphore>(component_lock);
    DCHECK(component_lock_.owns_lock());
  }

  void release_component_lock() {
    if (component_lock_.owns_lock()) {
      component_lock_.unlock();
    }
  }

  void commit() {
    release_component_lock();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(AlterSchemaTransactionState);

  const Schema* schema_;
  const tserver::AlterSchemaRequestPB *request_;
  tserver::AlterSchemaResponsePB *response_;
  boost::unique_lock<rw_semaphore> component_lock_;
};

// Executes the alter schema transaction,.
class AlterSchemaTransaction : public Transaction {
 public:
  AlterSchemaTransaction(AlterSchemaTransactionState* tx_state, DriverType type);

  virtual AlterSchemaTransactionState* state() OVERRIDE { return tx_state_.get(); }
  virtual const AlterSchemaTransactionState* state() const OVERRIDE { return tx_state_.get(); }

  void NewReplicateMsg(gscoped_ptr<consensus::ReplicateMsg>* replicate_msg) OVERRIDE;

  // Executes a Prepare for the alter schema transaction.
  //
  // Acquires the tablet component lock for the transaction.
  virtual Status Prepare() OVERRIDE;

  virtual void NewCommitAbortMessage(gscoped_ptr<consensus::CommitMsg>* commit_msg) OVERRIDE;

  // Executes an Apply for the alter schema transaction
  virtual Status Apply(gscoped_ptr<consensus::CommitMsg>* commit_msg) OVERRIDE;

  // Actually commits the transaction.
  virtual void Finish() OVERRIDE;

 private:
  gscoped_ptr<AlterSchemaTransactionState> tx_state_;
  DISALLOW_COPY_AND_ASSIGN(AlterSchemaTransaction);
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_ALTER_SCHEMA_TRANSACTION_H_ */
