// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tablet_service.h"

#include <algorithm>
#include <string>
#include <tr1/memory>
#include <vector>

#include "common/iterator.h"
#include "common/wire_protocol.h"
#include "consensus/consensus.h"
#include "gutil/casts.h"
#include "gutil/stl_util.h"
#include "gutil/strings/escaping.h"
#include "rpc/rpc_context.h"
#include "tablet/tablet_peer.h"
#include "tablet/tablet_metrics.h"
#include "tablet/transactions/alter_schema_transaction.h"
#include "tablet/transactions/change_config_transaction.h"
#include "tablet/transactions/write_transaction.h"
#include "server/hybrid_clock.h"
#include "tserver/scanners.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "tserver/tserver.pb.h"
#include "util/monotime.h"
#include "util/status.h"
#include "util/trace.h"

using kudu::consensus::ConsensusRequestPB;
using kudu::consensus::ConsensusResponsePB;
using kudu::metadata::QuorumPB;
using kudu::metadata::QuorumPeerPB;
using kudu::tablet::TabletStatusPB;
using kudu::rpc::RpcContext;
using kudu::tablet::TabletPeer;
using kudu::tablet::AlterSchemaTransactionState;
using kudu::tablet::ChangeConfigTransactionState;
using kudu::tablet::WriteTransactionState;
using kudu::tablet::TransactionCompletionCallback;
using std::tr1::shared_ptr;
using std::vector;
using google::protobuf::RepeatedPtrField;

DEFINE_int32(tablet_server_default_scan_batch_size_bytes, 1024 * 1024,
             "The default size for batches of scan results");
DEFINE_int32(tablet_server_max_scan_batch_size_bytes, 8 * 1024 * 1024,
             "The maximum batch size that a client may request for "
             "scan results.");
DEFINE_int32(tablet_server_scan_batch_size_rows, 100,
             "The number of rows to batch for servicing scan requests.");

namespace kudu {
namespace tserver {

static void SetupErrorAndRespond(TabletServerErrorPB* error,
                                 const Status& s,
                                 TabletServerErrorPB::Code code,
                                 rpc::RpcContext* context) {
  StatusToPB(s, error->mutable_status());
  error->set_code(code);
  // TODO: rename RespondSuccess() to just "Respond" or
  // "SendResponse" since we use it for application-level error
  // responses, and this just looks confusing!
  context->RespondSuccess();
}

static void RespondGenericError(const string& doing_what,
                                TabletServerErrorPB* error,
                                const Status& s,
                                rpc::RpcContext* context) {
  LOG(WARNING) << "Generic error " << doing_what << " for request "
               << context->request_pb()->ShortDebugString()
               << ": " << s.ToString();
  SetupErrorAndRespond(error, s, TabletServerErrorPB::UNKNOWN_ERROR, context);
}

// A transaction completion callback that responds to the client when transactions
// complete and sets the client error if there is one to set.
// TODO find a way to avoid passing specific responses (templating is worse as
// is pb reflection)
class RpcTransactionCompletionCallback : public TransactionCompletionCallback {
 public:
  RpcTransactionCompletionCallback(rpc::RpcContext* context,
                                   WriteResponsePB* w_resp)
 : context_(context),
   w_resp_(w_resp),
   as_resp_(NULL),
   cc_resp_(NULL) {}

  RpcTransactionCompletionCallback(rpc::RpcContext* context,
                                   AlterSchemaResponsePB* as_resp)
  : context_(context),
    w_resp_(NULL),
    as_resp_(as_resp),
    cc_resp_(NULL) {}

  RpcTransactionCompletionCallback(rpc::RpcContext* context,
                                   ChangeConfigResponsePB* cc_resp)
  : context_(context),
    w_resp_(NULL),
    as_resp_(NULL),
    cc_resp_(cc_resp) {}

  virtual void TransactionCompleted() {
    if (!status_.ok()) {
      SetupErrorAndRespond(get_error(), status_, code_, context_);
    } else {
      context_->RespondSuccess();
    }
  };

 private:

  TabletServerErrorPB* get_error() {
    if (w_resp_)
      return w_resp_->mutable_error();
    if (as_resp_)
      return as_resp_->mutable_error();
    return cc_resp_->mutable_error();
  }

  rpc::RpcContext* context_;
  WriteResponsePB* w_resp_;
  AlterSchemaResponsePB* as_resp_;
  ChangeConfigResponsePB* cc_resp_;
};

TabletServiceImpl::TabletServiceImpl(TabletServer* server)
  : TabletServerServiceIf(server->metric_context()),
    server_(server) {
}

template<class RespClass>
bool TabletServiceImpl::LookupTabletOrRespond(const string& tablet_id,
                                              shared_ptr<TabletPeer>* peer,
                                              RespClass* resp,
                                              rpc::RpcContext* context) {
  // Check that it exists.
  if (PREDICT_FALSE(!server_->tablet_manager()->LookupTablet(tablet_id, peer))) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::NotFound("Tablet not found"),
                         TabletServerErrorPB::TABLET_NOT_FOUND, context);
    return false;
  }

  // Check RUNNING state.
  metadata::TabletStatePB state = (*peer)->state();
  if (PREDICT_FALSE(state != metadata::RUNNING)) {
    Status s = Status::ServiceUnavailable("Tablet not RUNNING",
                                          metadata::TabletStatePB_Name(state));
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::TABLET_NOT_RUNNING, context);
    return false;
  }
  return true;
}

void TabletServiceImpl::Ping(const PingRequestPB* req,
                             PingResponsePB* resp,
                             rpc::RpcContext* context) {
  context->RespondSuccess();
}

void TabletServiceImpl::AlterSchema(const AlterSchemaRequestPB* req,
                                    AlterSchemaResponsePB* resp,
                                    rpc::RpcContext* context) {
  DVLOG(3) << "Received Alter Schema RPC: " << req->DebugString();

  shared_ptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(req->tablet_id(), &tablet_peer, resp, context)) return;

  uint32_t schema_version = tablet_peer->tablet()->metadata()->schema_version();

  // If the schema was already applied, respond as succeded
  if (schema_version == req->schema_version()) {
    // Sanity check, to verify that the tablet should have the same schema
    // specified in the request.
    Schema req_schema;
    Status s = SchemaFromPB(req->schema(), &req_schema);
    if (!s.ok()) {
      SetupErrorAndRespond(resp->mutable_error(), s,
                           TabletServerErrorPB::INVALID_SCHEMA, context);
      return;
    }

    Schema tablet_schema = tablet_peer->tablet()->metadata()->schema();
    if (req_schema.Equals(tablet_schema)) {
      context->RespondSuccess();
      return;
    }

    schema_version = tablet_peer->tablet()->metadata()->schema_version();
    if (schema_version == req->schema_version()) {
      LOG(ERROR) << "The current schema does not match the request schema."
                 << " version=" << schema_version
                 << " current-schema=" << tablet_schema.ToString()
                 << " request-schema=" << req_schema.ToString()
                 << " (corruption)";
      SetupErrorAndRespond(resp->mutable_error(),
                           Status::Corruption("got a different schema for the same version number"),
                           TabletServerErrorPB::MISMATCHED_SCHEMA, context);
      return;
    }
  }

  // If the current schema is newer than the one in the request reject the request.
  if (schema_version > req->schema_version()) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::InvalidArgument("Tablet has a newer schema"),
                         TabletServerErrorPB::TABLET_HAS_A_NEWER_SCHEMA, context);
    return;
  }

  AlterSchemaTransactionState *tx_state =
    new AlterSchemaTransactionState(tablet_peer.get(), req, resp,
                                    make_gscoped_ptr(new RpcTransactionCompletionCallback(context, resp)));

  // Submit the alter schema op. The RPC will be responded to asynchronously.
  Status s = tablet_peer->SubmitAlterSchema(tx_state);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
  }
}

void TabletServiceImpl::CreateTablet(const CreateTabletRequestPB* req,
                                     CreateTabletResponsePB* resp,
                                     rpc::RpcContext* context) {
  LOG(INFO) << "Processing CreateTablet for tablet " << req->tablet_id()
            << " (table=" << req->table_name()
            << " [id=" << req->table_id() << "]), range=[\""
            << strings::CHexEscape(req->start_key()) << "\", \""
            << strings::CHexEscape(req->end_key()) << "\"]";
  VLOG(1) << "Full request: " << req->DebugString();

  Schema schema;
  Status s = SchemaFromPB(req->schema(), &schema);
  DCHECK(schema.has_column_ids());
  if (!s.ok()) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::IllegalState("Invalid Schema."),
                         TabletServerErrorPB::INVALID_SCHEMA, context);
  }

  s = server_->tablet_manager()->CreateNewTablet(req->table_id(),
                                                 req->tablet_id(),
                                                 req->start_key(),
                                                 req->end_key(),
                                                 req->table_name(),
                                                 schema,
                                                 req->quorum(),
                                                 NULL);
  if (PREDICT_FALSE(!s.ok())) {
    TabletServerErrorPB::Code code;
    if (s.IsAlreadyPresent()) {
      code = TabletServerErrorPB::TABLET_ALREADY_EXISTS;
    } else {
      code = TabletServerErrorPB::UNKNOWN_ERROR;
    }
    SetupErrorAndRespond(resp->mutable_error(), s, code, context);
    return;
  }
  context->RespondSuccess();
}

void TabletServiceImpl::DeleteTablet(const DeleteTabletRequestPB* req,
                                     DeleteTabletResponsePB* resp,
                                     rpc::RpcContext* context) {
  DVLOG(3) << "Received Delete Tablet RPC: " << req->DebugString();

  shared_ptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(req->tablet_id(), &tablet_peer, resp, context)) return;

  Status s = server_->tablet_manager()->DeleteTablet(tablet_peer);
  if (PREDICT_FALSE(!s.ok())) {
    TabletServerErrorPB::Code code;
    if (s.IsNotFound()) {
      code = TabletServerErrorPB::TABLET_NOT_FOUND;
    } else {
      code = TabletServerErrorPB::UNKNOWN_ERROR;
    }
    SetupErrorAndRespond(resp->mutable_error(), s, code, context);
    return;
  }
  context->RespondSuccess();
}

void TabletServiceImpl::ChangeConfig(const ChangeConfigRequestPB* req,
                                     ChangeConfigResponsePB* resp,
                                     rpc::RpcContext* context) {
  DVLOG(3) << "Received Change Config RPC: " << req->DebugString();

  shared_ptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(req->tablet_id(), &tablet_peer, resp, context)) return;

  ChangeConfigTransactionState *tx_state =
    new ChangeConfigTransactionState(tablet_peer.get(), req, resp,
                                     make_gscoped_ptr(new RpcTransactionCompletionCallback(context, resp)));

  // Submit the change config op. The RPC will be responded to asynchronously.
  Status s = tablet_peer->SubmitChangeConfig(tx_state);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
  }
}

void TabletServiceImpl::Write(const WriteRequestPB* req,
                              WriteResponsePB* resp,
                              rpc::RpcContext* context) {
  DVLOG(3) << "Received Write RPC: " << req->DebugString();

  shared_ptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(req->tablet_id(), &tablet_peer, resp, context)) return;

  if (req->external_consistency_mode() != NO_CONSISTENCY) {
    if (!server_->clock()->SupportsExternalConsistencyMode(req->external_consistency_mode())) {
      Status s = Status::ServiceUnavailable("The configured clock does not support the"
          " required consistency mode.");
      SetupErrorAndRespond(resp->mutable_error(), s,
                                 TabletServerErrorPB::UNKNOWN_ERROR,
                                 context);
      return;
    }
  }

  WriteTransactionState *state =
    new WriteTransactionState(tablet_peer.get(), req, resp,
                              make_gscoped_ptr(new RpcTransactionCompletionCallback(context, resp)));

  // If the consistency mode is set to CLIENT_PROPAGATED and the client
  // sent us a timestamp, decode it and set it in the transaction context.
  // Also update the clock so that all future timestamps are greater than
  // the passed timestamp.
  if (req->external_consistency_mode() == CLIENT_PROPAGATED) {
    Status s;
    if (req->has_propagated_timestamp()) {
      Timestamp ts(req->propagated_timestamp());
      if (PREDICT_TRUE(s.ok())) {
        state->set_client_propagated_timestamp(ts);
        // update the clock with the client's timestamp
        s = server_->clock()->Update(ts);
      }
    }
    // TODO(dralves): doesn't this leak 'state'?
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s,
                           TabletServerErrorPB::UNKNOWN_ERROR,
                           context);
      return;
    }
  }

  // Submit the write. The RPC will be responded to asynchronously.
  WARN_NOT_OK(tablet_peer->SubmitWrite(state), "Could not execute write transaction.");
  return;
}

void TabletServiceImpl::UpdateConsensus(const ConsensusRequestPB* req,
                                        ConsensusResponsePB* resp,
                                        rpc::RpcContext* context) {
  DVLOG(3) << "Received Consensus Execute RPC: " << req->DebugString();

  shared_ptr<TabletPeer> tablet_peer;

  if (!LookupTabletOrRespond(req->tablet_id(), &tablet_peer, resp, context)) return;

  DCHECK(tablet_peer) << "Null tablet peer";

  // Can't answer update requests if peer is not RUNNING
  if (tablet_peer->state() != metadata::RUNNING) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::NotFound("Tablet Peer not in RUNNING state"),
                         TabletServerErrorPB::TABLET_NOT_FOUND, context);
    return;
  }
  // Submit the update directly to the TabletPeer's Consensus instance.
  Status s = tablet_peer->consensus()->Update(req, resp);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
  }
  context->RespondSuccess();
}

void TabletServiceImpl::Scan(const ScanRequestPB* req,
                             ScanResponsePB* resp,
                             rpc::RpcContext* context) {
  // Validate the request: user must pass a new_scan_request or
  // a scanner ID, but not both.
  if (PREDICT_FALSE(req->has_scanner_id() &&
                    req->has_new_scan_request())) {
    context->RespondFailure(Status::InvalidArgument(
                              "Must not pass both a scanner_id and new_scan_request"));
    return;
  }

  if (req->has_new_scan_request()) {
    HandleNewScanRequest(req, resp, context);
  } else if (req->has_scanner_id()) {
    HandleContinueScanRequest(req, resp, context);
  } else {
    context->RespondFailure(Status::InvalidArgument(
                              "Must pass either a scanner_id or new_scan_request"));
  }
}

void TabletServiceImpl::ListTablets(const ListTabletsRequestPB* req,
                                    ListTabletsResponsePB* resp,
                                    rpc::RpcContext* context) {
  vector<shared_ptr<TabletPeer> > peers;
  server_->tablet_manager()->GetTabletPeers(&peers);
  RepeatedPtrField<TabletStatusPB>* peer_status = resp->mutable_tablet_status();
  BOOST_FOREACH(const shared_ptr<TabletPeer>& peer, peers) {
    TabletStatusPB* status = peer_status->Add();
    peer->GetTabletStatusPB(status);
  }
  context->RespondSuccess();
}

// Extract a void* pointer suitable for use in a ColumnRangePredicate from the
// user-specified protobuf field.
// This validates that the pb_value has the correct length, copies the data into
// 'pool', and sets *result to point to it.
// Returns bad status if the user-specified value is the wrong length.
static Status ExtractPredicateValue(const ColumnSchema& schema,
                                    const string& pb_value,
                                    AutoReleasePool* pool,
                                    const void** result) {
  // Copy the data from the protobuf into the pool.
  uint8_t* data_copy = pool->AddArray(new uint8_t[pb_value.size()]);
  memcpy(data_copy, &pb_value[0], pb_value.size());

  // If the type is a STRING, then we need to return a pointer to a Slice
  // element pointing to the string. Otherwise, just verify that the provided
  // value was the right size.
  if (schema.type_info()->type() == STRING) {
    Slice* s = pool->Add(new Slice(data_copy, pb_value.size()));
    *result = s;
  } else {
    // TODO: add test case for this invalid request
    size_t expected_size = schema.type_info()->size();
    if (pb_value.size() != expected_size) {
      return Status::InvalidArgument(
        StringPrintf("Bad predicate on %s. Expected value size %zd, got %zd",
                     schema.ToString().c_str(), expected_size, pb_value.size()));
    }
    *result = data_copy;
  }

  return Status::OK();
}

static Status SetupScanSpec(const NewScanRequestPB& scan_pb,
                            gscoped_ptr<ScanSpec>* spec,
                            AutoReleasePool* pool) {
  gscoped_ptr<ScanSpec> ret(new ScanSpec);
  BOOST_FOREACH(const ColumnRangePredicatePB& pred_pb, scan_pb.range_predicates()) {
    if (!pred_pb.has_lower_bound() && !pred_pb.has_upper_bound()) {
      return Status::InvalidArgument(
        string("Invalid predicate ") + pred_pb.ShortDebugString() +
        ": has no lower or upper bound.");
    }
    ColumnSchema col(ColumnSchemaFromPB(pred_pb.column()));

    boost::optional<const void*> lower_bound, upper_bound;
    if (pred_pb.has_lower_bound()) {
      const void* val;
      RETURN_NOT_OK(ExtractPredicateValue(col, pred_pb.lower_bound(), pool,
                                          &val));
      lower_bound = val;
    }
    if (pred_pb.has_upper_bound()) {
      const void* val;
      RETURN_NOT_OK(ExtractPredicateValue(col, pred_pb.upper_bound(), pool,
                                          &val));
      upper_bound = val;
    }

    ColumnRangePredicate pred(col, lower_bound, upper_bound);
    if (VLOG_IS_ON(3)) {
      VLOG(3) << "Parsed predicate " << pred.ToString() << " from " << scan_pb.ShortDebugString();
    }
    ret->AddPredicate(pred);
  }
  spec->swap(ret);
  return Status::OK();
}

// Return the batch size to use for a given request, after clamping
// the user-requested request within the server-side allowable range.
static size_t GetBatchSizeBytes(const ScanRequestPB* req) {
  if (!req->has_batch_size_bytes()) {
    return FLAGS_tablet_server_default_scan_batch_size_bytes;
  }

  return std::min(req->batch_size_bytes(),
                  implicit_cast<uint32_t>(FLAGS_tablet_server_max_scan_batch_size_bytes));
}

// Start a new scan.
void TabletServiceImpl::HandleNewScanRequest(const ScanRequestPB* req,
                                             ScanResponsePB* resp,
                                             rpc::RpcContext* context) {
  DCHECK(req->has_new_scan_request());

  const NewScanRequestPB& scan_pb = req->new_scan_request();
  shared_ptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(scan_pb.tablet_id(), &tablet_peer, resp, context)) return;

  // Create the user's requested projection.
  // TODO: add test cases for bad projections including 0 columns
  Schema projection;
  Status s = ColumnPBsToSchema(scan_pb.projected_columns(), &projection);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::INVALID_SCHEMA,
                         context);
    return;
  }

  if (projection.has_column_ids()) {
    s = Status::InvalidArgument("User requests should not have Column IDs");
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::INVALID_SCHEMA,
                         context);
    return;
  }

  AutoReleasePool pool;
  gscoped_ptr<ScanSpec> spec(new ScanSpec);
  s = SetupScanSpec(scan_pb, &spec, &pool);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::INVALID_SCAN_SPEC,
                         context);
    return;
  }

  TRACE("Creating iterator");
  // preset the error code for when creating the iterator on the tablet fails
  TabletServerErrorPB::Code error_code = TabletServerErrorPB::MISMATCHED_SCHEMA;

  gscoped_ptr<RowwiseIterator> iter;
  switch (scan_pb.read_mode()) {
    case READ_LATEST: {
      s = tablet_peer->tablet()->NewRowIterator(projection, &iter);
      break;
    }
    case READ_AT_SNAPSHOT: {
      s = HandleScanAtSnapshot(&iter, resp, scan_pb, projection, tablet_peer);
      if (!s.ok()) {
        error_code = TabletServerErrorPB::INVALID_SNAPSHOT;
      }
      break;
    }
    default: {
      s = Status::IllegalState("Unsupported read mode");
    }
  }
  TRACE("Iterator initialized");

  if (PREDICT_TRUE(s.ok())) {
    s = iter->Init(spec.get());
  }

  if (PREDICT_FALSE(s.IsInvalidArgument())) {
    // An invalid projection returns InvalidArgument above.
    // TODO: would be nice if we threaded these more specific
    // error codes throughout Kudu.
    SetupErrorAndRespond(resp->mutable_error(), s,
                         error_code,
                         context);
    return;
  } else if (PREDICT_FALSE(!s.ok())) {
    RespondGenericError("Error setting up scanner", resp->mutable_error(), s, context);
    return;
  }

  bool has_more = iter->HasNext();
  resp->set_has_more_results(has_more);
  if (!has_more) {
    // If there are no more rows, there is no need to assign a scanner ID.
    // Just respond immediately instead.
    context->RespondSuccess();
    return;
  }

  SharedScanner scanner;
  server_->scanner_manager()->NewScanner(&scanner);
  scanner->Init(iter.Pass());

  // The ScanSpec has to remain valid as long as the scanner, so move its
  // ownership into the scanner itself.
  scanner->autorelease_pool()->Add(spec.release());
  pool.DonateAllTo(scanner->autorelease_pool());

  // TODO: could start the scan here unless batch_size_bytes is 0
  resp->set_scanner_id(scanner->id());

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Started scanner " << scanner->id() << ": " << scanner->iter()->ToString();
  }

  size_t batch_size_bytes = GetBatchSizeBytes(req);

  if (batch_size_bytes > 0) {
    // TODO: instead of copying the pb, instead split HandleContinueScanRequest
    // and call the second half directly
    ScanRequestPB continue_req(*req);
    continue_req.set_scanner_id(scanner->id());
    HandleContinueScanRequest(&continue_req, resp, context);
  } else {
    context->RespondSuccess();
  }
}

// Continue an existing scan request.
void TabletServiceImpl::HandleContinueScanRequest(const ScanRequestPB* req,
                                                  ScanResponsePB* resp,
                                                  rpc::RpcContext* context) {
  DCHECK(req->has_scanner_id());

  // TODO: need some kind of concurrency control on these scanner objects
  // in case multiple RPCs hit the same scanner at the same time. Probably
  // just a trylock and fail the RPC if it contends.
  SharedScanner scanner;
  if (!server_->scanner_manager()->LookupScanner(req->scanner_id(), &scanner)) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::NotFound("Scanner not found"),
                         TabletServerErrorPB::SCANNER_EXPIRED, context);
    return;
  }
  VLOG(2) << "Found existing scanner " << scanner->id() << " for request: "
          << req->ShortDebugString();
  TRACE("Found scanner $0", scanner->id());

  size_t batch_size_bytes = GetBatchSizeBytes(req);

  if (batch_size_bytes == 0 && req->close_scanner()) {
    resp->set_has_more_results(false);
    bool success = server_->scanner_manager()->UnregisterScanner(req->scanner_id());
    LOG_IF(WARNING, !success) << "Scanner " << scanner->id() <<
      " not removed successfully from scanner manager. May be a bug.";
    context->RespondSuccess();
    return;
  }

  // TODO: check the call_seq_id!

  scanner->UpdateAccessTime();

  RowwiseIterator* iter = scanner->iter();

  // TODO: could size the RowBlock based on the user's requested batch size?
  // If people had really large indirect objects, we would currently overshoot
  // their requested batch size by a lot.
  Arena arena(32 * 1024, 1 * 1024 * 1024);
  RowBlock block(scanner->iter()->schema(),
                 FLAGS_tablet_server_scan_batch_size_rows, &arena);

  resp->mutable_data()->mutable_rows()->reserve(batch_size_bytes * 11 / 10);

  // TODO: in the future, use the client timeout to set a budget. For now,
  // just use a half second, which should be plenty to amortize call overhead.
  int budget_ms = 500;
  MonoTime deadline = MonoTime::Now(MonoTime::COARSE);
  deadline.AddDelta(MonoDelta::FromMilliseconds(budget_ms));

  while (iter->HasNext()) {
    Status s = RowwiseIterator::CopyBlock(iter, &block);
    if (PREDICT_FALSE(!s.ok())) {
      RespondGenericError("copying rows from internal iterator",
                          resp->mutable_error(), s, context);
      return;
    }

    ConvertRowBlockToPB(block, resp->mutable_data());

    // TODO: could break if it's been looping too long - eg with restrictive predicates,
    // we don't want to loop here for too long monopolizing a thread and risking a
    // client timeout.
    //
    // TODO: should check if RPC got cancelled, once we implement RPC cancellation.
    size_t response_size = resp->data().rows().size() + resp->data().indirect_data().size();
    TRACE("Copied block, new size=$0", response_size);

    MonoTime now = MonoTime::Now(MonoTime::COARSE);
    if (!now.ComesBefore(deadline)) {
      TRACE("Deadline expired - responding early");
      break;
    }

    if (response_size >= batch_size_bytes) {
      break;
    }
  }

  scanner->UpdateAccessTime();
  bool has_more = !req->close_scanner() && iter->HasNext();
  resp->set_has_more_results(has_more);
  if (!has_more) {
    VLOG(2) << "Scanner " << scanner->id() << " complete: removing...";
    bool success = server_->scanner_manager()->UnregisterScanner(req->scanner_id());
    LOG_IF(WARNING, !success) << "Scanner " << scanner->id() <<
      " not removed successfully from scanner manager. May be a bug.";
  }

  context->RespondSuccess();
}

Status TabletServiceImpl::HandleScanAtSnapshot(gscoped_ptr<RowwiseIterator>* iter,
                                               ScanResponsePB* resp,
                                               const NewScanRequestPB& scan_pb,
                                               const Schema& projection,
                                               shared_ptr<TabletPeer> tablet_peer) {

  // TODO check against the earliest boundary (i.e. how early can we go) right
  // now we're keeping all undos/redos forever!

  // If the client sent a timestamp update our clock with it.
  if (scan_pb.has_propagated_timestamp()) {
    Timestamp propagated_timestamp(scan_pb.propagated_timestamp());

    // Update the clock so that we never generate snapshots lower that
    // 'propagated_timestamp'. If 'propagated_timestamp' is lower than
    // 'now' this call has no effect. If 'propagated_timestamp' is too much
    // into the future this will fail and we abort.
    RETURN_NOT_OK(server_->clock()->Update(propagated_timestamp));
  }

  Timestamp now = server_->clock()->Now();
  Timestamp snap_timestamp;

  // If the client provided no snapshot timestamp we take the current clock
  // time as the snapshot timestamp.
  if (!scan_pb.has_snap_timestamp()) {
    snap_timestamp = now;
  // ... else we use the client provided one, but make sure it is less than
  // or equal to the current clock read.
  } else {
    snap_timestamp.FromUint64(scan_pb.snap_timestamp());
    if (snap_timestamp.CompareTo(now) > 0) {
      return Status::InvalidArgument("Snapshot time in the future");
    }
  }

  tablet::MvccSnapshot snap;
  tablet_peer->tablet()->mvcc_manager()->TakeSnapshotAtTimestamp(&snap, snap_timestamp);

  // Wait for the in-flights in the snapshot to be finished
  TRACE("Waiting for operations in snapshot to commit");
  MonoTime before = MonoTime::Now(MonoTime::FINE);
  tablet_peer->tablet()->mvcc_manager()->WaitUntilAllCommitted(snap);
  uint64_t duration_usec = MonoTime::Now(MonoTime::FINE).GetDeltaSince(before).ToMicroseconds();
  tablet_peer->tablet()->metrics()->snapshot_scan_inflight_wait_duration->Increment(duration_usec);
  TRACE("All operations in snapshot committed. Waited for $0 microseconds", duration_usec);

  RETURN_NOT_OK(tablet_peer->tablet()->NewRowIterator(projection, snap, iter));
  resp->set_snap_timestamp(snap_timestamp.ToUint64());
  return Status::OK();
}

} // namespace tserver
} // namespace kudu
