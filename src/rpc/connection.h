// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_RPC_CONNECTION_H
#define KUDU_RPC_CONNECTION_H

#include <boost/intrusive/list.hpp>
#include <boost/utility.hpp>
#include <ev++.h>
#include <gutil/gscoped_ptr.h>
#include <stdint.h>
#include <tr1/memory>
#include <tr1/unordered_map>

#include "rpc/client_call.h"
#include "rpc/server_call.h"
#include "rpc/sockaddr.h"
#include "rpc/socket.h"
#include "rpc/transfer.h"
#include "util/monotime.h"
#include "util/object_pool.h"
#include "util/status.h"

namespace kudu {
namespace rpc {

class ReactorThread;

//
// A connection between an endpoint and us.
//
// Inbound connections are created by AcceptorPools, which eventually schedule
// RegisterConnection() to be called from the reactor thread.
//
// Outbound connections are created by the Reactor thread in order to service
// outbound calls.
//
// Once a Connection is created, it can be used both for sending messages and
// receiving them, but any given connection is explicitly a client or server.
// If a pair of servers are making bidirectional RPCs, they will use two separate
// TCP connections (and Connection objects).
//
// This class is not fully thread-safe.  It is accessed only from the context of a
// single ReactorThread except where otherwise specified.
//
class Connection : public std::tr1::enable_shared_from_this<Connection> {
 public:
  enum Direction {
    // This host is sending calls via this connection.
    CLIENT,
    // This host is receiving calls via this connection.
    SERVER
  };

  // Create a new Connection.
  // reactor_thread: the reactor that owns us.
  // remote: the address of the remote end
  // socket: the socket to take ownership of.
  // connect_in_progress: true only if we are in the middle of a connect()
  //                      operation.
  // direction: whether we are the client or server side
  Connection(ReactorThread *reactor_thread, const Sockaddr &remote,
             int socket, bool connect_in_progress, Direction direction);

  // Register our socket with an epoll loop.  We will only ever be registered in
  // one epoll loop at a time.
  void EpollRegister(ev::loop_ref& loop);

  ~Connection();

  bool connect_in_progress() const {
    return connect_in_progress_;
  }

  MonoTime last_activity_time() const {
    return last_activity_time_;
  }

  // Returns true if we are not in the process of receiving or sending a
  // message, and we have no outstanding calls.
  bool Idle() const;

  // Fail any calls which are currently queued or awaiting response.
  // Prohibits any future calls (they will be failed immediately with this
  // same Status).
  void Shutdown(const Status &status);

  // Queue a new call to be made. If the queueing fails, the call will be
  // marked failed.
  // Takes ownership of the 'call' object regardless of whether it succeeds or fails.
  // This may be called from a non-reactor thread.
  void QueueOutboundCall(const std::tr1::shared_ptr<OutboundCall> &call);

  // Queue a call response back to the client on the server side.
  //
  // This may be called from a non-reactor thread.
  void QueueResponseForCall(gscoped_ptr<InboundCall> call);

  const Sockaddr& remote() const;

  // libev callback when data is available to read.
  void ReadHandler(ev::io &watcher, int revents);

  // libev callback when we may write to the socket.
  void WriteHandler(ev::io &watcher, int revents);

  std::string ToString() const;

  Direction direction() const { return direction_; }

private:
  friend struct CallAwaitingResponse;
  friend class QueueTransferTask;
  friend struct ResponseTransferCallbacks;

  // A call which has been fully sent to the server, which we're waiting for
  // the server to process. This is used on the client side only.
  struct CallAwaitingResponse {
    ~CallAwaitingResponse();

    // Notification from libev that the call has timed out.
    void HandleTimeout(ev::timer &watcher, int revents);

    Connection *conn;
    std::tr1::shared_ptr<OutboundCall> call;
    ev::timer timeout_timer;
  };

  typedef std::tr1::unordered_map<uint64_t, CallAwaitingResponse *> car_map_t;
  typedef std::tr1::unordered_map<uint64_t, InboundCall *> inbound_call_map_t;

  // An incoming packet has completed transferring on the server side.
  // This parses the call and delivers it into the call queue.
  void HandleIncomingCall(gscoped_ptr<InboundTransfer> transfer);

  // An incoming packet has completed on the client side. This parses the
  // call response, looks up the CallAwaitingResponse, and calls the
  // client callback.
  void HandleCallResponse(gscoped_ptr<InboundTransfer> transfer);

  // The given CallAwaitingResponse has elapsed its user-defined timeout.
  // Set it to Failed.
  void HandleOutboundCallTimeout(CallAwaitingResponse *car);

  // Queue a transfer for sending on this connection.
  // We will take ownership of the transfer.
  // This must be called from the reactor thread.
  void QueueOutbound(gscoped_ptr<OutboundTransfer> transfer);

  // The reactor thread that created this connection.
  ReactorThread * const reactor_thread_; 

  // The socket we're communicating on.
  Socket socket_;

  // The remote address we're talking to.
  Sockaddr remote_; 

  // with non-blocking I/O, connect may not return immediately.
  // This boolean tracks whether we are in the middle of a connect()
  // operation, connecting to a remote host.
  bool connect_in_progress_;

  // whether we are client or server
  Direction direction_;

  // The last time we read or wrote from the socket.
  MonoTime last_activity_time_;

  // the inbound transfer, if any
  gscoped_ptr<InboundTransfer> inbound_;

  // notifies us when our socket is writable.
  ev::io write_io_; 

  // notifies us when our socket is readable.
  ev::io read_io_;

  // waiting to be sent
  boost::intrusive::list<OutboundTransfer> outbound_transfers_;

  // Calls which have been sent and are now waiting for a response.
  car_map_t awaiting_response_;

  // Calls which have been received on the server and are currently
  // being handled.
  inbound_call_map_t calls_being_handled_;

  // the next call ID to use
  uint64_t next_call_id_;

  // Starts as Status::OK, gets set to a shutdown status upon Shutdown().
  Status shutdown_status_;

  // Temporary vector used when serializing - avoids an allocation
  // when serializing calls.
  std::vector<Slice> slices_tmp_;

  // Pool from which CallAwaitingResponse objects are allocated.
  // Also a funny name.
  ObjectPool<CallAwaitingResponse> car_pool_;
  typedef ObjectPool<CallAwaitingResponse>::scoped_ptr scoped_car;
  
};

}
}

#endif
