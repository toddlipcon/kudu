// Copyright (c) 2013, Cloudera, inc.

#include "rpc/connection.h"

#include <boost/foreach.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/lexical_cast.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <stdint.h>

#include "gutil/map-util.h"
#include "rpc/messenger.h"
#include "rpc/reactor.h"
#include "rpc/rpc_controller.h"
#include "rpc/sockaddr.h"
#include "rpc/transfer.h"
#include "util/status.h"

using std::tr1::shared_ptr;
using std::vector;

namespace kudu {
namespace rpc {

///
/// Connection
///
Connection::Connection(ReactorThread *reactor_thread, const Sockaddr &remote,
                       int socket, bool connect_in_progress, Direction direction)
  : reactor_thread_(reactor_thread),
    socket_(socket),
    remote_(remote),
    connect_in_progress_(connect_in_progress),
    direction_(direction),
    last_activity_time_(reactor_thread_->cur_time()),
    next_call_id_(1)
{
}

void Connection::EpollRegister(ev::loop_ref& loop) {
  write_io_.set(loop);
  write_io_.set(socket_.GetFd(), ev::WRITE);
  write_io_.set<Connection, &Connection::WriteHandler>(this);
  if (connect_in_progress_) {
    write_io_.start();
  }
  read_io_.set(loop);
  read_io_.set(socket_.GetFd(), ev::READ);
  read_io_.set<Connection, &Connection::ReadHandler>(this);
  read_io_.start();
}

Connection::~Connection() {
  // Must clear the outbound_transfers_ list before deleting.
  CHECK(outbound_transfers_.begin() == outbound_transfers_.end());

  // It's crucial that the connection is Shutdown first -- otherwise
  // our destructor will end up calling read_io_.stop() and write_io_.stop()
  // from a possibly non-reactor thread context. This can then make all
  // hell break loose with libev.
  CHECK(!shutdown_status_.ok()) << "destructing a connection which was not shutdown";
}

bool Connection::Idle() const {
  // check if we're in the middle of receiving something
  InboundTransfer *transfer = inbound_.get();
  if (transfer && (transfer->TransferStarted())) {
    return false;
  }
  // check if we still need to send something
  if (!outbound_transfers_.empty()) {
    return false;
  }
  // can't kill a connection if calls are waiting response
  if (!awaiting_response_.empty()) {
    return false;
  }

  if (!calls_being_handled_.empty()) {
    return false;
  }

  return true;
}

void Connection::Shutdown(const Status &status) {
  DCHECK(reactor_thread_->IsCurrentThread());
  shutdown_status_ = status;

  // Clear any calls which have been sent and were awaiting a response.
  BOOST_FOREACH(const car_map_t::value_type &v, awaiting_response_) {
    CallAwaitingResponse *c = v.second;
    if (c->call.get()) {
      c->call->SetFailed(status);
    }
    // And we must return the CallAwaitingResponse to the pool
    car_pool_.Destroy(c);
  }
  awaiting_response_.clear();

  // Clear any outbound transfers.
  while (!outbound_transfers_.empty()) {
    OutboundTransfer *t = &outbound_transfers_.front();
    outbound_transfers_.pop_front();
    delete t;
  }

  read_io_.stop();
  write_io_.stop();
}

void Connection::QueueOutbound(gscoped_ptr<OutboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());

  if (!shutdown_status_.ok()) {
    // If we've already shut down, then we just need to abort the
    // transfer rather than bothering to queue it.
    transfer->Abort(shutdown_status_);
    return;
  }

  DVLOG(3) << "Queued transfer: " << transfer->HexDump();
  if (outbound_transfers_.empty()) {
    // If we weren't currently in the middle of sending anything,
    // then our write_io_ interest is stopped. Need to re-start it.
    write_io_.start();
  }
  outbound_transfers_.push_back(*transfer.release());
}

Connection::CallAwaitingResponse::~CallAwaitingResponse() {
  DCHECK(conn->reactor_thread_->IsCurrentThread());
}

void Connection::CallAwaitingResponse::HandleTimeout(ev::timer &watcher, int revents) {
  conn->HandleOutboundCallTimeout(this);
}

void Connection::HandleOutboundCallTimeout(CallAwaitingResponse *car) {
  // Detach the call object from the CallAwaitingResponse, mark it as failed.
  shared_ptr<OutboundCall> &call = car->call;
  call->TimedOut();

  // Drop the reference to the call. If the original caller has moved on after
  // seeing the timeout, we no longer need to hold onto the allocated memory
  // from the request.
  call.reset();

  // We still leave the CallAwaitingResponse in the map -- this is because we may still
  // receive a response from the server, and we don't want a spurious log message
  // when we do finally receive the response. The fact that CallAwaitingResponse::call
  // is a NULL pointer indicates to the response processing code that the call
  // already timed out.
}


// Callbacks after sending a call on the wire.
// This notifies the OutboundCall object to change its state to SENT once it
// has been fully transmitted.
struct CallTransferCallbacks : public TransferCallbacks {
 public:
  CallTransferCallbacks(const shared_ptr<OutboundCall> &call) :
    call_(call)
  {}

  virtual void NotifyTransferFinished() {
    call_->CallSent();
    delete this;
  }

  virtual void NotifyTransferAborted(const Status &status) {
    LOG(WARNING) << "Connection torn down before " <<
      call_->ToString() << " could send its call";
    // TODO: need to SetFailed? Probably not because it's also in the CallAwaitingResponse map.
    // Check that we have code coverage for this.
    delete this;
  }

 private:
  shared_ptr<OutboundCall> call_;
};

void Connection::QueueOutboundCall(const shared_ptr<OutboundCall> &call) {
  DCHECK_EQ(direction_, CLIENT);
  DCHECK(reactor_thread_->IsCurrentThread());

  if (PREDICT_FALSE(!shutdown_status_.ok())) {
    // Already shutdown
    call->SetFailed(shutdown_status_);
    return;
  }

  // At this point the call has a serialized request, but no call header, since we haven't
  // yet assigned a call ID.
  DCHECK(!call->call_id_assigned());

  // Assign the call ID.
  uint32_t call_id = next_call_id_++;
  call->set_call_id(call_id);

  // Serialize the actual bytes to be put on the wire.
  slices_tmp_.clear();
  Status s = call->SerializeTo(&slices_tmp_);
  if (PREDICT_FALSE(!s.ok())) {
    call->SetFailed(s);
    return;
  }

  call->CallQueued();

  scoped_car car(car_pool_.make_scoped_ptr(car_pool_.Construct()));
  car->conn = this;
  car->call = call;

  // Set up the timeout timer.
  float timeout_secs = call->controller()->timeout().ToSeconds();
  if (timeout_secs > 0) {
    reactor_thread_->RegisterTimeout(&car->timeout_timer);
    car->timeout_timer.set<CallAwaitingResponse, &CallAwaitingResponse::HandleTimeout>(car.get());
    car->timeout_timer.start(timeout_secs, 0);
  }

  TransferCallbacks *cb = new CallTransferCallbacks(call);
  awaiting_response_[call_id] = car.release();
  QueueOutbound(gscoped_ptr<OutboundTransfer>(
                  new OutboundTransfer(slices_tmp_, cb)));
}

// Callbacks for sending an RPC call response from the server.
// This takes ownership of the InboundCall object so that, once it has
// been responded to, we can free up all of the associated memory.
struct ResponseTransferCallbacks : public TransferCallbacks {
 public:
  ResponseTransferCallbacks(gscoped_ptr<InboundCall> call,
                            Connection *conn) :
    call_(call.Pass()),
    conn_(conn)
  {}

  ~ResponseTransferCallbacks() {
    // Remove the call from the map.
    InboundCall *call_from_map = EraseKeyReturnValuePtr(
      &conn_->calls_being_handled_, call_->call_id());
    DCHECK_EQ(call_from_map, call_.get());
  }

  virtual void NotifyTransferFinished() {
    delete this;
  }

  virtual void NotifyTransferAborted(const Status &status) {
    LOG(WARNING) << "Connection torn down before " <<
      call_->ToString() << " could send its response";
    delete this;
  }

 private:
  gscoped_ptr<InboundCall> call_;
  Connection *conn_;
};

// Reactor task which puts a transfer on the outbound transfer queue.
class QueueTransferTask : public ReactorTask {
 public:
  QueueTransferTask(gscoped_ptr<OutboundTransfer> transfer,
                    Connection *conn)
    : transfer_(transfer.Pass()),
      conn_(conn)
  {}

  virtual void Run(ReactorThread *thr) {
    conn_->QueueOutbound(transfer_.Pass());
    delete this;
  }

  virtual void Abort(const Status &status) {
    transfer_->Abort(status);
    delete this;
  }

 private:
  gscoped_ptr<OutboundTransfer> transfer_;
  Connection *conn_;
};

void Connection::QueueResponseForCall(gscoped_ptr<InboundCall> call) {
  // This is called by the IPC worker thread when the response is set.
  DCHECK(!reactor_thread_->IsCurrentThread());
  DCHECK_EQ(direction_, SERVER);

  // If the connection is torn down, then the QueueOutbound() call that
  // eventually runs in the reactor thread will take care of calling
  // ResponseTransferCallbacks::NotifyTransferAborted.

  std::vector<Slice> slices;
  call->SerializeResponseTo(&slices);

  TransferCallbacks *cb = new ResponseTransferCallbacks(call.Pass(), this);
  // After the response is sent, can delete the InboundCall object.
  gscoped_ptr<OutboundTransfer> t(new OutboundTransfer(slices, cb));

  QueueTransferTask *task = new QueueTransferTask(t.Pass(), this);
  reactor_thread_->reactor()->ScheduleReactorTask(task);
}

const Sockaddr& Connection::remote() const {
  return remote_;
}

void Connection::ReadHandler(ev::io &watcher, int revents) {
  DCHECK(reactor_thread_->IsCurrentThread());

  DVLOG(3) << ToString() << " readHandler(revents=" << revents << ")";
  if (revents & EV_ERROR) {
    reactor_thread_->DestroyConnection(this, Status::NetworkError(ToString() +
                                     ": readHandler encountered an error"));
    return;
  }
  last_activity_time_ = reactor_thread_->cur_time();

  while (true) {
    if (!inbound_) {
      inbound_.reset(new InboundTransfer());
    }
    Status status = inbound_->ReceiveBuffer(socket_);
    if (PREDICT_FALSE(!status.ok())) {
      if (status.posix_code() == ESHUTDOWN) {
        VLOG(1) << ToString() << " shut down by remote end.";
      } else {
        LOG(WARNING) << ToString() << " recv error: " << status.ToString();
      }
      reactor_thread_->DestroyConnection(this, status);
      return;
    }
    if (!inbound_->TransferFinished()) {
      DVLOG(3) << ToString() << ": read is not yet finished yet.";
      return;
    }
    DVLOG(3) << ToString() << ": finished reading " << inbound_->data().size() << " bytes";

    if (direction_ == CLIENT) {
      HandleCallResponse(inbound_.Pass());
    } else if (direction_ == SERVER) {
      HandleIncomingCall(inbound_.Pass());
    } else {
      LOG(FATAL) << "Invalid direction: " << direction_;
    }

    // TODO: it would seem that it would be good to loop around and see if
    // there is more data on the socket by trying another recv(), but it turns
    // out that it really hurts throughput to do so. A better approach
    // might be for each InboundTransfer to actually try to read an extra byte,
    // and if it succeeds, then we'd copy that byte into a new InboundTransfer
    // and loop around, since it's likely the next call also arrived at the
    // same time.
    break;
  }
}

void Connection::HandleIncomingCall(gscoped_ptr<InboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());

  gscoped_ptr<InboundCall> call(new InboundCall(shared_from_this()));
  Status s = call->ParseFrom(transfer.Pass());
  if (!s.ok()) {
    LOG(WARNING) << ToString() << ": received bad data: " << s.ToString();
    // TODO: shutdown? probably, since any future stuff on this socket will be
    // "unsynchronized"
    return;
  }

  if (!InsertIfNotPresent(&calls_being_handled_, call->call_id(), call.get())) {
    LOG(WARNING) << ToString() << ": received call ID " << call->call_id() <<
      " but was already processing this ID! Ignoring";
    reactor_thread_->DestroyConnection(
      this, Status::RuntimeError("Deceived duplicate call id",
                                 boost::lexical_cast<string>(call->call_id())));
    return;
  }

  reactor_thread_->reactor()->messenger()->QueueInboundCall(call.Pass());
}

void Connection::HandleCallResponse(gscoped_ptr<InboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());
  gscoped_ptr<CallResponse> resp(new CallResponse);
  CHECK_OK(resp->ParseFrom(transfer.Pass()));

  CallAwaitingResponse *car_ptr =
    EraseKeyReturnValuePtr(&awaiting_response_, resp->call_id());
  if (PREDICT_FALSE(car_ptr == NULL)) {
    LOG(WARNING) << ToString() << ": Got a response for call id " << resp->call_id() << " which "
                 << "was not pending! Ignoring.";
    return;
  }
  scoped_car car(car_pool_.make_scoped_ptr(car_ptr));

  if (PREDICT_FALSE(car->call.get() == NULL)) {
    // The call already failed due to a timeout.
    VLOG(1) << "Got response to call id " << resp->call_id() << " after client already timed out";
    return;
  }

  car->call->SetResponse(resp.Pass());
}

void Connection::WriteHandler(ev::io &watcher, int revents) {
  DCHECK(reactor_thread_->IsCurrentThread());

  if (revents & EV_ERROR) {
    reactor_thread_->DestroyConnection(this, Status::NetworkError(ToString() +
          ": writeHandler encountered an error"));
    return;
  }
  DVLOG(3) << ToString() << ": writeHandler: revents = " << revents;
  if (connect_in_progress_) {
    // If the connect operation was in progress last time we checked on this
    // socket, we need to determine whether it finished.
    Status status = socket_.GetSockError();
    if (!status.ok()) {
      reactor_thread_->DestroyConnection(this, status);
      return;
    }
    VLOG(3) << ToString() << ": writeHandler: finished connect.";
    connect_in_progress_ = false;
  }

  OutboundTransfer *transfer;
  if (outbound_transfers_.empty()) {
    LOG(WARNING) << ToString() << " got a ready-to-write callback, but there is "
      "nothing to write.";
    write_io_.stop();
    return;
  }

  while (!outbound_transfers_.empty()) {
    transfer = &(outbound_transfers_.front());

    last_activity_time_ = reactor_thread_->cur_time();
    Status status = transfer->SendBuffer(socket_);
    if (PREDICT_FALSE(!status.ok())) {
      LOG(WARNING) << ToString() << " send error: " << status.ToString();
      reactor_thread_->DestroyConnection(this, status);
      return;
    }

    if (!transfer->TransferFinished()) {
      DVLOG(3) << ToString() << ": writeHandler: xfer not finished.";
      return;
    }

    outbound_transfers_.pop_front();
    delete transfer;
  }


  // If we were able to write all of our outbound transfers,
  // we don't have any more to write.
  write_io_.stop();
}

std::string Connection::ToString() const {
  return StringPrintf("Connection{socket_=%d, remote_=%s, "
        "reactor_=%s, direction_=%s, "
        "outbound_transfers_.empty()=%s, "
        "connect_in_progress_=%s, last_activity_time=%s}",
        socket_.GetFd(), remote_.ToString().c_str(),
        reactor_thread_->reactor()->name().c_str(),
        direction_ == SERVER ? "server" : "client",
        (outbound_transfers_.empty() ? "true" : "false"),
        (connect_in_progress_ ? "true" : "false"),
        last_activity_time_.ToString().c_str());
}

} // namespace rpc
} // namespace kudu
