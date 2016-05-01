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

#ifndef KUDU_RPC_TRANSFER_H
#define KUDU_RPC_TRANSFER_H

#include <boost/function.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/utility.hpp>
#include <gflags/gflags.h>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

DECLARE_int32(rpc_max_message_size);

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace kudu {

class Socket;

namespace rpc {

class Messenger;
struct TransferCallbacks;

// This class is used internally by the RPC layer to represent an inbound
// transfer in progress.
//
// Inbound Transfer objects are created by a Connection receiving data. When the
// message is fully received, it is either parsed as a call, or a call response,
// and the InboundTransfer object itself is handed off.
class InboundTransfer {
 public:

  InboundTransfer();

  // Create a transfer with known body_length, so buffer can be pre-allocated,
  // also an extra space of kMsgLengthPrefixLength is allocated to hold
  // next prefix.
  explicit InboundTransfer(int32_t body_length);

  // Read from the socket into our buffer.
  // If there is extra data, create another transfer and put into it.
  Status ReceiveBuffer(Socket &socket, gscoped_ptr<InboundTransfer>& next);

  // Return true if any bytes have yet been sent.
  bool TransferStarted() const;

  // Return true if the entire transfer has been sent.
  bool TransferFinished() const;

  Slice data() const {
    return Slice(buf_);
  }

  // Return a string indicating the status of this transfer (number of bytes received, etc)
  // suitable for logging.
  std::string StatusAsString() const;

 private:

  Status ProcessInboundHeader();

  faststring buf_;

  int32_t total_length_;
  int32_t cur_offset_;

  DISALLOW_COPY_AND_ASSIGN(InboundTransfer);
};

// When the connection wants to send data, it creates an OutboundTransfer object
// to encompass it. This sits on a queue within the Connection, so that each time
// the Connection wakes up with a writable socket, it consumes more bytes off
// the next pending transfer in the queue.
//
// Upon completion of the transfer, a callback is triggered.
class OutboundTransfer : public boost::intrusive::list_base_hook<> {
 public:
  enum { kMaxPayloadSlices = 10 };

  // Create a new transfer. The 'payload' slices will be concatenated and
  // written to the socket. When the transfer completes or errors, the
  // appropriate method of 'callbacks' is invoked.
  //
  // Does not take ownership of the callbacks object or the underlying
  // memory of the slices. The slices must remain valid until the callback
  // is triggered.
  //
  // When the transfer starts, the required features are checked against the set
  // of features which the server supports. The check is delayed until just
  // before the transfer starts because it depends on the negotiation with the
  // server being complete. The call_id allows the call to be canceled if
  // required features are not supported.
  //
  // NOTE: 'payload' is currently restricted to a maximum of kMaxPayloadSlices
  // slices.
  OutboundTransfer(int32_t call_id,
                   const std::vector<Slice> &payload,
                   std::set<RpcFeatureFlag> required_features,
                   TransferCallbacks *callbacks);

  // Destruct the transfer. A transfer object should never be deallocated
  // before it has either (a) finished transferring, or (b) been Abort()ed.
  ~OutboundTransfer();

  // Abort the current transfer, with the given status.
  // This triggers TransferCallbacks::NotifyTransferAborted.
  void Abort(const Status &status);

  // send from our buffers into the sock
  Status SendBuffer(Socket &socket);

  // Return true if any bytes have yet been sent.
  bool TransferStarted() const;

  // Return true if the entire transfer has been sent.
  bool TransferFinished() const;

  const std::set<RpcFeatureFlag>& required_features() const {
    return required_features_;
  }

  // Return the total number of bytes to be sent (including those already sent)
  int32_t TotalLength() const;

  std::string HexDump() const;

  int32_t call_id() const {
    return call_id_;
  }

 private:
  // Slices to send. Uses an array here instead of a vector to avoid an expensive
  // vector construction (improved performance a couple percent).
  Slice payload_slices_[kMaxPayloadSlices];
  size_t n_payload_slices_;

  // The current slice that is being sent.
  int32_t cur_slice_idx_;
  // The number of bytes in the above slice which has already been sent.
  int32_t cur_offset_in_slice_;

  std::set<RpcFeatureFlag> required_features_;

  TransferCallbacks *callbacks_;

  int32_t call_id_;

  bool aborted_;

  DISALLOW_COPY_AND_ASSIGN(OutboundTransfer);
};

// Callbacks made after a transfer completes.
struct TransferCallbacks {
 public:
  virtual ~TransferCallbacks();

  // The transfer finished successfully.
  virtual void NotifyTransferFinished() = 0;

  // The transfer was aborted (e.g because the connection died or an error occurred).
  virtual void NotifyTransferAborted(const Status &status) = 0;
};

} // namespace rpc
} // namespace kudu
#endif
