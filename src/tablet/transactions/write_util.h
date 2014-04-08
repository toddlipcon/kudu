// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TRANSACTIONS_WRITE_UTIL_H_
#define KUDU_TABLET_TRANSACTIONS_WRITE_UTIL_H_

#include <vector>

#include "util/status.h"
#include "tablet/tablet.pb.h"
#include "tserver/tserver.pb.h"

namespace kudu {
class ConstContiguousRow;
class DeltaProjector;
class RowChangeList;
class RowProjector;
class Schema;

namespace rpc {
class RpcContext;
}

namespace tserver {
class WriteResponsePB;
}

namespace tablet {
class Tablet;

class WriteTransactionContext;

// Decodes the row block and sets up a client error if something fails.
Status DecodeRowBlock(WriteTransactionContext* tx_ctx,
                      RowwiseRowBlockPB* block_pb,
                      const Schema& tablet_key_projection,
                      bool is_inserts_block,
                      Schema* client_schema,
                      std::vector<const uint8_t*>* row_block);

Status CreatePreparedInsertsAndMutates(Tablet* tablet,
                                       WriteTransactionContext* tx_ctx,
                                       gscoped_ptr<Schema> client_schema,
                                       const RowOperationsPB& to_insert_rows,
                                       gscoped_ptr<Schema> mutates_client_schema,
                                       const vector<const uint8_t *>& to_mutate,
                                       const vector<const RowChangeList *>& mutations);

// Return a row that is the Projection of the 'user_row_ptr' on the 'tablet_schema'.
// The 'tablet_schema' pointer will be referenced by the returned row, so must
// remain valid as long as the row.
// The returned ConstContiguousRow is added to the AutoReleasePool of the 'tx_ctx'.
// No projection is performed if the two schemas are the same.
//
// TODO: this is now only used by the testing code path
const ConstContiguousRow* ProjectRowForInsert(WriteTransactionContext* tx_ctx,
                                              const Schema* tablet_schema,
                                              const RowProjector& row_projector,
                                              const uint8_t *user_row_ptr);



// Return a mutation that is the Projection of the 'user_mutation' on the 'tablet_schema'.
// The returned RowChangeList is added to the AutoReleasePool of the 'tx_ctx'.
// No projection is performed if the two schemas are the same.
const RowChangeList* ProjectMutation(WriteTransactionContext *tx_ctx,
                                     const DeltaProjector& delta_projector,
                                     const RowChangeList *user_mutation);

}  // namespace tablet
}  // namespace kudu


#endif /* KUDU_TABLET_TRANSACTIONS_WRITE_UTIL_H_ */
