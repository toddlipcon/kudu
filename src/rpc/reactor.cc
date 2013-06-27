// Copyright (c) 2013, Cloudera, inc.

#include "rpc/reactor.h"

#include <arpa/inet.h>
#include <boost/intrusive/list.hpp>
#include <boost/foreach.hpp>
#include <boost/thread.hpp>
#include <ev++.h>
#include <glog/logging.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "gutil/gscoped_ptr.h"
#include "rpc/connection.h"
#include "rpc/messenger.h"
#include "rpc/socket.h"
#include "rpc/transfer.h"
#include "util/countdown_latch.h"
#include "util/errno.h"
#include "util/monotime.h"
#include "util/status.h"

using std::string;
using std::tr1::shared_ptr;

namespace kudu {
namespace rpc {

const Status Reactor::SHUTDOWN_ERROR(
      Status::NetworkError("reactor is shutting down", "", ESHUTDOWN));

ReactorThread::ReactorThread(Reactor *reactor, const MessengerBuilder &bld)
  : loop_(0),
    cur_time_(MonoTime::Now(MonoTime::COARSE)),
    last_unused_tcp_scan_(cur_time_),
    reactor_(reactor),
    connection_keepalive_time_(bld.connection_keepalive_time_),
    coarse_timer_granularity_(bld.coarse_timer_granularity_)
{
}

Status ReactorThread::Init() {
  // Register to get async notifications in our epoll loop.
  async_.set(loop_);
  async_.set<ReactorThread, &ReactorThread::AsyncHandler>(this);
  async_.start();

  // Register the timer watcher.
  // The timer is used for closing old TCP connections and applying
  // backpressure.
  timer_.set(loop_);
  timer_.set<ReactorThread, &ReactorThread::TimerHandler>(this);
  timer_.start(coarse_timer_granularity_.ToSeconds(),
               coarse_timer_granularity_.ToSeconds());

  // Create Reactor thread.
  try {
    thread_.reset(new boost::thread(
          boost::bind(&ReactorThread::RunThread, this)));
  } catch (boost::thread_resource_error &e) {
    // boost::thread uses exceptions to signal failure to create a thread
    return Status::RuntimeError(e.what());
  }
  return Status::OK();
}

void ReactorThread::Shutdown() {
  CHECK(reactor_->closing()) << "Should be called after setting closing_ flag";
  DCHECK(!IsCurrentThread());

  VLOG(1) << name() << ": joining Reactor thread.";
  WakeThread();
  thread_->join();

  // TODO: what about pending calls? need to abort them?
}

void ReactorThread::ShutdownInternal() {
  DCHECK(IsCurrentThread());

  // Tear down any outbound TCP connections.
  VLOG(1) << name() << ": tearing down outbound TCP connections...";
  for (conn_map_t::iterator c = client_conns_.begin();
       c != client_conns_.end(); c = client_conns_.begin()) {
    const shared_ptr<Connection> &conn = (*c).second;
    conn->Shutdown(Reactor::SHUTDOWN_ERROR);
    client_conns_.erase(c);
  }

  // Tear down any inbound TCP connections.
  BOOST_FOREACH(const shared_ptr<Connection> &conn, server_conns_) {
    conn->Shutdown(Reactor::SHUTDOWN_ERROR);
  }
  server_conns_.clear();
}

ReactorTask::ReactorTask() {
}
ReactorTask::~ReactorTask() {
}

void ReactorThread::GetMetricsInternal(ReactorMetrics *metrics) {
  DCHECK(IsCurrentThread());
  metrics->num_client_connections_ = client_conns_.size();
  metrics->num_server_connections_ = server_conns_.size();
}

void ReactorThread::WakeThread() {
  DCHECK(!IsCurrentThread());
  async_.send();
}

// Handle async events.  These events are sent to the reactor by other
// threads that want to bring something to our attention, like the fact that
// we're shutting down, or the fact that there is a new outbound Transfer
// ready to send.
void ReactorThread::AsyncHandler(ev::async &watcher, int revents) {
  DCHECK(IsCurrentThread());

  if (PREDICT_FALSE(reactor_->closing())) {
    ShutdownInternal();
    loop_.break_loop(); // break the epoll loop and terminate the thread
    return;
  }

  boost::intrusive::list<ReactorTask> tasks;
  reactor_->DrainTaskQueue(&tasks);

  while (!tasks.empty()) {
    ReactorTask &task = tasks.front();
    tasks.pop_front();
    task.Run(this);
  }
}

void ReactorThread::RegisterConnection(const shared_ptr<Connection> &conn) {
  DCHECK(IsCurrentThread());
  conn->EpollRegister(loop_);
  server_conns_.push_back(conn);
}

void ReactorThread::AssignOutboundCall(const shared_ptr<OutboundCall> &call) {
  DCHECK(IsCurrentThread());
  shared_ptr<Connection> conn;
  Status s = FindOrStartConnection(call->remote(), &conn);
  if (PREDICT_FALSE(!s.ok())) {
    call->SetFailed(s);
    return;
  }

  conn->QueueOutboundCall(call);
}

//
// Handles timer events.  The periodic timer:
//
// 1. updates Reactor::cur_time_
// 2. every tcp_conn_timeo_ seconds, close down connections older than
//    tcp_conn_timeo_ seconds.
//
void ReactorThread::TimerHandler(ev::timer &watcher, int revents) {
  DCHECK(IsCurrentThread());
  if (EV_ERROR & revents) {
    LOG(WARNING) << "Reactor " << name() << " got an error in "
      "the timer handler.";
    return;
  }
  MonoTime now(MonoTime::Now(MonoTime::COARSE));
  VLOG(2) << name() << ": timer tick at " << now.ToString();
  cur_time_ = now;

  ScanIdleConnections();
}

void ReactorThread::RegisterTimeout(ev::timer *watcher) {
  watcher->set(loop_);
}

void ReactorThread::ScanIdleConnections() {
  DCHECK(IsCurrentThread());
  // enforce TCP connection timeouts
  conn_list_t::iterator c = server_conns_.begin();
  conn_list_t::iterator c_end = server_conns_.end();
  uint64_t timed_out = 0;
  for (; c != c_end; ) {
    const shared_ptr<Connection> &conn = *c;
    if (!conn->Idle()) {
      VLOG(3) << "Connection " << conn->ToString() << " not idle";
      ++c; // TODO: clean up this loop
      continue;
    }

    MonoDelta connection_delta(cur_time_.GetDeltaSince(conn->last_activity_time()));
    if (connection_delta.MoreThan(connection_keepalive_time_)) {
      conn->Shutdown(Status::NetworkError(
                       StringPrintf("connection timed out after %s seconds",
                                    connection_keepalive_time_.ToString().c_str())));
      server_conns_.erase(c++);
      ++timed_out;
    } else {
      ++c;
    }
  }

  // TODO: above only times out on the server side.
  // Clients may want to set their keepalive timeout as well.

  VLOG_IF(1, timed_out > 0) << name() << ": timed out " << timed_out << " TCP connections."; 
}

const std::string &ReactorThread::name() const {
  return reactor_->name();
}

MonoTime ReactorThread::cur_time() const {
  return cur_time_;
}

Reactor *ReactorThread::reactor() {
  return reactor_;
}

bool ReactorThread::IsCurrentThread() const {
  return thread_ && thread_->get_id() == boost::this_thread::get_id();
}

void ReactorThread::RunThread() {
  loop_.run(0);
  VLOG(1) << name() << " thread exiting.";
}

Status ReactorThread::FindOrStartConnection(const Sockaddr &remote, shared_ptr<Connection> *conn) {
  DCHECK(IsCurrentThread());
  conn_map_t::const_iterator c = client_conns_.find(remote);
  if (c != client_conns_.end()) {
    *conn = (*c).second;
    return Status::OK();
  }

  // No connection to this remote. Need to create one.
  VLOG(2) << name() << " FindOrStartConnection: creating "
          << "new connection for " << remote.ToString();

  // Create a new socket and start connecting to the remote.
  Socket sock;
  RETURN_NOT_OK(CreateClientSocket(&sock));
  bool connect_in_progress;
  RETURN_NOT_OK(StartConnect(&sock, remote, &connect_in_progress));

  // Register the new connection in our map.
  (*conn).reset(new Connection(this, remote, sock.Release(), connect_in_progress,
                             Connection::CLIENT));
  (*conn)->EpollRegister(loop_);
  client_conns_.insert(conn_map_t::value_type(remote, *conn));
  return Status::OK();
}

Status ReactorThread::CreateClientSocket(Socket *sock) {
  Status ret = sock->Init(Socket::FLAG_NONBLOCKING);
  if (ret.ok()) {
    ret = sock->SetNoDelay(true);
  }
  LOG_IF(WARNING, !ret.ok()) << "failed to create an "
    "outbound connection because a new socket could not "
    "be created: " << ret.ToString();
  return ret;
}

Status ReactorThread::StartConnect(Socket *sock, const Sockaddr &remote, bool *in_progress) {
  Status ret = sock->Connect(remote);
  if (ret.ok()) {
    VLOG(3) << "StartConnect: connect finished immediately for " << remote.ToString();
    *in_progress = false; // connect() finished immediately.
    return ret;
  }

  int posix_code = ret.posix_code();
  if (Socket::IsTemporarySocketError(posix_code) || (posix_code == EINPROGRESS)) {
    // The connect operation is in progress.
    *in_progress = true;
    VLOG(3) << "StartConnect: connect in progress for " << remote.ToString();
    return Status::OK();
  } else {
    LOG(WARNING) << "failed to create an outbound connection to " << remote.ToString()
                 << " because connect failed: " << ret.ToString();
    return ret;
  }
}

void ReactorThread::DestroyConnection(Connection *conn,
                                      const Status &conn_status) {
  DCHECK(IsCurrentThread());

  conn->Shutdown(conn_status);

  // Unlink connection from lists.
  if (conn->direction() == Connection::CLIENT) {
    conn_map_t::iterator it = client_conns_.find(conn->remote());
    CHECK(it != client_conns_.end()) << "Couldn't find connection " << conn->ToString();
    client_conns_.erase(it);
  } else if (conn->direction() == Connection::SERVER) {
    conn_list_t::iterator it = server_conns_.begin();
    while (it != server_conns_.end()) {
      if ((*it).get() == conn) {
        server_conns_.erase(it);
        break;
      }
      ++it;
    }
  }
}

Reactor::Reactor(Messenger *messenger,
                 int index, const MessengerBuilder &bld)
  : messenger_(messenger),
    name_(StringPrintf("%s_R%03d", messenger->name().c_str(), index)),
    closing_(false),
    thread_(this, bld)
{
}

Status Reactor::Init() {
  return thread_.Init();
}

void Reactor::Shutdown() {
  {
    boost::lock_guard<LockType> lock_guard(lock_);
    if (closing_) {
      return;
    }
    closing_ = true;
  }

  thread_.Shutdown();

  // Abort all pending tasks. No new tasks can get scheduled after this
  // because ScheduleReactorTask() tests the closing_ flag set above.
  BOOST_FOREACH(ReactorTask &task, pending_tasks_) {
    task.Abort(SHUTDOWN_ERROR);
  }
}

Reactor::~Reactor() {
  Shutdown();
}

const std::string &Reactor::name() const {
  return name_;
}

// Task to call GetMetricsInternal within the thread.
class GetMetricsTask : public ReactorTask {
 public:
  GetMetricsTask(ReactorMetrics *metrics) :
    metrics_(metrics),
    latch_(1)
  {}

  virtual void Run(ReactorThread *reactor) {
    reactor->GetMetricsInternal(metrics_);
    latch_.CountDown();
  }
  virtual void Abort(const Status &status) {
    status_ = status;
    latch_.CountDown();
  }
  void Wait() {
    latch_.Wait();
  }
  const Status &status() const { return status_; }

 private:
  ReactorMetrics *metrics_;
  Status status_;
  CountDownLatch latch_;
};

Status Reactor::GetMetrics(ReactorMetrics *metrics) {
  GetMetricsTask task(metrics);
  ScheduleReactorTask(&task);
  task.Wait();
  return task.status();
}

class RegisterConnectionTask : public ReactorTask {
 public:
  RegisterConnectionTask(const shared_ptr<Connection> &conn) :
    conn_(conn)
  {}

  virtual void Run(ReactorThread *thread) {
    thread->RegisterConnection(conn_);
    delete this;
  }

  virtual void Abort(const Status &status) {
    conn_->Shutdown(status);
    delete this;
  }

 private:
  shared_ptr<Connection> conn_;
};

void Reactor::RegisterInboundSocket(Socket *socket, const Sockaddr &remote) {
  VLOG(3) << name_ << ": new inbound connection to " << remote.ToString();
  shared_ptr<Connection> conn(
    new Connection(&thread_, remote, socket->Release(), false, Connection::SERVER));
  RegisterConnectionTask *task = new RegisterConnectionTask(conn);
  ScheduleReactorTask(task);
}

// Task which runs in the reactor thread to assign an outbound call
// to a connection.
class AssignOutboundCallTask : public ReactorTask {
 public:
  AssignOutboundCallTask(const shared_ptr<OutboundCall> &call) :
    call_(call)
  {}

  virtual void Run(ReactorThread *reactor) {
    reactor->AssignOutboundCall(call_);
    delete this;
  }

  virtual void Abort(const Status &status) {
    call_->SetFailed(status);
    delete this;
  }

 private:
  shared_ptr<OutboundCall> call_;
};

void Reactor::QueueOutboundCall(const shared_ptr<OutboundCall> &call) {
  DVLOG(3) << name_ << ": queueing outbound call "
           << call->ToString() << " to remote " << call->remote().ToString();
  AssignOutboundCallTask *task = new AssignOutboundCallTask(call);
  ScheduleReactorTask(task);
}

void Reactor::ScheduleReactorTask(ReactorTask *task) {
  {
    boost::lock_guard<LockType> lock_guard(lock_);
    if (closing_) {
      task->Abort(SHUTDOWN_ERROR);
      return;
    }
    pending_tasks_.push_back(*task);
  }
  thread_.WakeThread();
}

bool Reactor::DrainTaskQueue(boost::intrusive::list<ReactorTask> *tasks) {
  boost::lock_guard<LockType> lock_guard(lock_);
  if (closing_) {
    return false;
  }
  tasks->swap(pending_tasks_);
  return true;
}

} // namespace rpc
} // namespace kudu
