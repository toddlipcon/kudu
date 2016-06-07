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
#pragma once

#include <memory>
#include <string>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/monotime.h"

namespace kudu {
namespace rpc {

namespace internal {
typedef rpc::RequestTracker::SequenceNumber SequenceNumber;
}

// Merges the sequence of errors encountered in some complex RPC path
// to determine the most appropriate error to expose in the final resulting
// Status.
//
// Example scenarios:
//
// 1) TS Error followed by a timed out lookup
//
//   - client talks to a tablet server, and immediately gets some error like TABLET_NOT_FOUND
//   - it falls back to the master to try to resolve a new location
//   - the master request itself times out
//
//   Result: we should pin the error on the master
//
// 2) Slow 'locate' process leaves short time for real operation
//
//   - Client wants to write to a tablet, and has to look up the
//     location from the master, then do a DNS lookup for the
//     tserver, etc.
//   - One of these "locate" workflows is slow (e.g. due to a slow
//     master or slow DNS)
//   - The lookup eventually succeeds, but leaves only a few milliseconds
//     left before the deadline for the real operation
//   - The real operation receives a Status::TimedOut
//
//   Result: we should pin the error on whatever phase of the lookup was
//           longest
//
// 3) 
//

class ErrorMerger {
 public:
  enum Phase {
    LOCATE,
    PERFORM
  };

  void Record(Phase p, const Status &s) {
    if (s.ok()) return;

    

    if (s_.ok()) {
      s_ = s;
      prev_phase_ = p;
    }
  }

  const Status& get() { return s_; }

 private:
  Phase prev_phase_;
  Status s_;
};

// A base class for retriable RPCs that handles replica picking and retry logic.
//
// The 'Server' template parameter refers to the the type of the server that will be looked up
// and passed to the derived classes on Try(). For instance in the case of WriteRpc it's
// RemoteTabletServer.
//
// TODO merge RpcRetrier into this class? Can't be done right now as the retrier is used
// independently elsewhere, but likely possible when all replicated RPCs have a ReplicaPicker.
//
// TODO allow to target replicas other than the leader, if needed.
//
// TOOD once we have retry handling on all the RPCs merge this with rpc::Rpc.
template <class Server, class RequestPB, class ResponsePB>
class RetriableRpc : public Rpc {
 public:
  RetriableRpc(const scoped_refptr<ServerPicker<Server>>& server_picker,
               const scoped_refptr<RequestTracker>& request_tracker,
               const MonoTime& deadline,
               const std::shared_ptr<Messenger>& messenger)
   : Rpc(deadline, messenger),
     server_picker_(server_picker),
     request_tracker_(request_tracker),
     sequence_number_(RequestTracker::NO_SEQ_NO),
     num_attempts_(0) {}

  virtual ~RetriableRpc() {
    DCHECK_EQ(sequence_number_, RequestTracker::NO_SEQ_NO);
  }

  // Performs server lookup/initialization.
  // If/when the server is looked up and initialized successfully RetriableRpc will call
  // Try() to actually send the request.
  void SendRpc() override;

 protected:
  // Subclasses implement this method to actually try the RPC.
  // The server been looked up and is ready to be used.
  virtual void Try(Server* replica, const ResponseCallback& callback) = 0;

  // Subclasses implement this method to analyze 'status', the controller status or
  // the response and return a RetriableRpcStatus which will then be used
  // to decide how to proceed (retry or give up).
  virtual RetriableRpcStatus AnalyzeResponse(const Status& status) = 0;

  // Subclasses implement this method to perform cleanup and/or final steps.
  // After this is called the RPC will be no longer retried.
  virtual void Finish(const Status& status) = 0;

  // Request body.
  RequestPB req_;

  // Response body.
  ResponsePB resp_;

 private:
  friend class CalculatorServiceRpc;
  // Decides whether to retry the RPC, based on the result of AnalyzeResponse() and retries
  // if that is the case.
  // Returns true if the RPC was retried or false otherwise.
  bool RetryIfNeeded(const RetriableRpcStatus& result, Server* server);

  // Called when the replica has been looked up.
  void ReplicaFoundCb(const Status& status, Server* server);

  // Called when after the RPC was performed.
  void SendRpcCb(const Status& status) override;

  // Performs final cleanup, after the RPC is done (independently of success).
  void FinishInternal();

  scoped_refptr<ServerPicker<Server>> server_picker_;
  scoped_refptr<RequestTracker> request_tracker_;
  const MonoTime deadline_;
  std::shared_ptr<Messenger> messenger_;

  // The sequence number for this RPC.
  internal::SequenceNumber sequence_number_;

  // The number of times this RPC has been attempted
  int32 num_attempts_;

  // Keeps track of the replica the RPCs were sent to.
  // TODO Remove this and pass the used replica around. For now we need to keep this as
  // the retrier calls the SendRpcCb directly and doesn't know the replica that was
  // being written to.
  Server* current_;

  ErrorMerger error_merger_;
};

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::SendRpc()  {
  if (sequence_number_ == RequestTracker::NO_SEQ_NO) {
    CHECK_OK(request_tracker_->NewSeqNo(&sequence_number_));
  }
  server_picker_->PickLeader(Bind(&RetriableRpc::ReplicaFoundCb,
                                  Unretained(this)),
                             retrier().deadline());
}

template <class Server, class RequestPB, class ResponsePB>
bool RetriableRpc<Server, RequestPB, ResponsePB>::RetryIfNeeded(const RetriableRpcStatus& result,
                                                                Server* server) {
  // Handle the cases where we retry.
  switch (result.result) {
    // For writes, always retry a TOO_BUSY error on the same server.
    case RetriableRpcStatus::SERVER_BUSY: {
      break;
    }
    case RetriableRpcStatus::SERVER_NOT_ACCESSIBLE: {
      VLOG(1) << "Failing " << ToString() << " to a new target: " << result.status.ToString();
      if (server) {
        server_picker_->MarkServerFailed(server, result.status);
      }
      break;
    }
      // The TabletServer was not part of the config serving the tablet.
      // We mark our tablet cache as stale, forcing a master lookup on the next attempt.
      // TODO: Don't backoff the first time we hit this error (see KUDU-1314).
    case RetriableRpcStatus::RESOURCE_NOT_FOUND: {
      if (server) {
        server_picker_->MarkResourceNotFound(server);
      }

      break;
    }
      // The TabletServer was not the leader of the quorum.
    case RetriableRpcStatus::REPLICA_NOT_LEADER: {
      if (server) {
        server_picker_->MarkReplicaNotLeader(server);
      }
      break;
    }
      // For the OK and NON_RETRIABLE_ERROR cases we can't/won't retry.
    default:
      return false;
  }
  resp_.Clear();
  current_ = nullptr;
  mutable_retrier()->DelayedRetry(this, result.status);
  return true;
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::FinishInternal() {
  // Mark the RPC as completed and set the sequence number to NO_SEQ_NO to make
  // sure we're in the appropriate state before destruction.
  request_tracker_->RpcCompleted(sequence_number_);
  sequence_number_ = RequestTracker::NO_SEQ_NO;
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::ReplicaFoundCb(const Status& status,
                                                                 Server* server) {
  error_merger_.Record(ErrorMerger::LOCATE, status);
  RetriableRpcStatus result = AnalyzeResponse(status);
  if (RetryIfNeeded(result, server)) return;

  if (result.result == RetriableRpcStatus::NON_RETRIABLE_ERROR) {
    FinishInternal();
    Finish(error_merger_.get());
    return;
  }

  // We successfully found a replica, so prepare the RequestIdPB before we send out the call.
  std::unique_ptr<RequestIdPB> request_id(new RequestIdPB());
  request_id->set_client_id(request_tracker_->client_id());
  request_id->set_seq_no(sequence_number_);
  request_id->set_first_incomplete_seq_no(request_tracker_->FirstIncomplete());
  request_id->set_attempt_no(num_attempts_++);

  mutable_retrier()->mutable_controller()->SetRequestIdPB(std::move(request_id));

  DCHECK_EQ(result.result, RetriableRpcStatus::OK);
  current_ = server;
  Try(server, boost::bind(&RetriableRpc::SendRpcCb, this, Status::OK()));
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::SendRpcCb(const Status& status) {
  RetriableRpcStatus result = AnalyzeResponse(status);

  Status nice_status = result.status;
  if (!nice_status.ok()) {
    string error_string;
    if (current_) {
      error_string = strings::Substitute("Failed to write to server: $0", current_->ToString());
    } else {
      error_string = "Failed to write to server: (no server available)";
    }
    nice_status = nice_status.CloneAndPrepend(error_string);
  }

  error_merger_.Record(ErrorMerger::PERFORM, nice_status);
  if (RetryIfNeeded(result, current_)) return;

  // From here on out the rpc has either succeeded of suffered a non-retriable
  // failure.
  FinishInternal();
  Finish(error_merger_.get());
}

} // namespace rpc
} // namespace kudu
