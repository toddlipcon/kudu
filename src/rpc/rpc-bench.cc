// Copyright (c) 2013, Cloudera, inc

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <gtest/gtest.h>
#include <string>

#include "rpc/rpc-test-base.h"
#include "rpc/rtest.proxy.h"
#include "util/countdown_latch.h"
#include "util/test_util.h"

using std::string;

namespace kudu {
namespace rpc {

class RpcBench : public RpcTestBase {
 public:
  RpcBench()
    : should_run_(true)
  {}

 protected:
  friend class ClientThread;

  Sockaddr server_addr_;
  shared_ptr<Messenger> client_messenger_;
  volatile bool should_run_;
};

class ClientThread {
 public:
  ClientThread(RpcBench *bench) :
    bench_(bench),
    request_count_(0)
  {}

  void Start() {
    thread_.reset(new boost::thread(&ClientThread::Run, this));
  }

  void Join() {
    thread_->join();
  }

  void Run() {
    shared_ptr<Messenger> client_messenger = bench_->CreateMessenger("Client", 1);

    CalculatorServiceProxy p(client_messenger, bench_->server_addr_);

    AddRequestPB req;
    AddResponsePB resp;
    while (bench_->should_run_) {
      req.set_x(request_count_);
      req.set_y(request_count_);
      RpcController controller;
      CHECK_OK(p.Add(req, &resp, &controller));
      CHECK_EQ(req.x() + req.y(), resp.result());
      request_count_++;
    }
  }

  gscoped_ptr<boost::thread> thread_;
  RpcBench *bench_;
  int request_count_;
};


// Test making successful RPC calls.
TEST_F(RpcBench, BenchmarkCalls) {
  n_worker_threads_ = 4;

  // Set up server.
  StartTestServerWithGeneratedCode(&server_addr_);

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr_.ToString();
  client_messenger_ = CreateMessenger("Client", 2);

  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();

  boost::ptr_vector<ClientThread> threads;
  for (int i = 0; i < 4; i++) {
    ClientThread *thr = new ClientThread(this);
    thr->Start();
    threads.push_back(thr);
  }

  sleep(AllowSlowTests() ? 10 : 1);
  should_run_ = false;

  int total_reqs = 0;

  BOOST_FOREACH(ClientThread &thr, threads) {
    thr.Join();
    total_reqs += thr.request_count_;
  }
  sw.stop();

  float reqs_per_second = (float)total_reqs / sw.elapsed().wall_seconds();
  float user_cpu_micros_per_req = (float)sw.elapsed().user / 1000.0 / total_reqs;
  float sys_cpu_micros_per_req = (float)sw.elapsed().system / 1000.0 / total_reqs;

  LOG(INFO) << "Reqs/sec:         " << reqs_per_second;
  LOG(INFO) << "User CPU per req: " << user_cpu_micros_per_req << "us";
  LOG(INFO) << "Sys CPU per req:  " << sys_cpu_micros_per_req << "us";
}

} // namespace rpc
} // namespace kudu

