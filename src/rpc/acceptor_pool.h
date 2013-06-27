// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_RPC_ACCEPTOR_POOL_H
#define KUDU_RPC_ACCEPTOR_POOL_H

#include <boost/thread.hpp>
#include <tr1/memory>
#include <vector>

#include "gutil/atomicops.h"
#include "rpc/sockaddr.h"
#include "rpc/socket.h"
#include "util/status.h"

namespace kudu {
namespace rpc {

class Messenger;
class Socket;

// A pool of threads calling accept() to create new connections.
// Acceptor pool threads terminate when they notice that the messenger has been
// shut down.
class AcceptorPool {
public:
  // Create a new acceptor pool.  Calls socket::Release to take ownership of the
  // socket.
  AcceptorPool(Messenger *messenger,
               Socket *socket, const Sockaddr &bind_address);
  ~AcceptorPool();
  Status Init(int num_threads);
  void Shutdown();
  Sockaddr bind_address() const;

private:
  void RunThread();

  Messenger *messenger_;
  Socket socket_;
  Sockaddr bind_address_;
  std::vector<std::tr1::shared_ptr<boost::thread> > threads_;
  Atomic32 closing_;

  DISALLOW_COPY_AND_ASSIGN(AcceptorPool);
};

} // namespace rpc
} // namespace kudu
#endif
