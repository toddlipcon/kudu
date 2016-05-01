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

#include <boost/bind.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "kudu/gutil/atomicops.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/test_util.h"

using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;

DEFINE_int32(client_thread, 16,
             "Number of client thread");

DEFINE_int32(client_concurrency, 60,
             "Number of concurrent requests");

DEFINE_int32(worker_thread, 1,
             "Number of server worker thread");

DEFINE_int32(server_reactor, 4,
             "Number of server reactor");

DEFINE_int32(run_seconds, 1,
             "Seconds to run the test");

namespace kudu {
namespace rpc {

class RpcBench : public RpcTestBase {
 public:
  RpcBench()
    : should_run_(true),
      stop_(0)
  {}

 protected:
  friend class ClientThread;
  friend class ClientAsyncWorkload;

  Sockaddr server_addr_;
  Atomic32 should_run_;
  CountDownLatch stop_;
};

class ClientThread {
 public:
  explicit ClientThread(RpcBench *bench)
    : bench_(bench),
      request_count_(0) {
  }

  void Start() {
    thread_.reset(new boost::thread(&ClientThread::Run, this));
  }

  void Join() {
    thread_->join();
  }

  void Run() {
    shared_ptr<Messenger> client_messenger = bench_->CreateMessenger("Client");

    CalculatorServiceProxy p(client_messenger, bench_->server_addr_);

    AddRequestPB req;
    AddResponsePB resp;
    while (Acquire_Load(&bench_->should_run_)) {
      req.set_x(request_count_);
      req.set_y(request_count_);
      RpcController controller;
      controller.set_timeout(MonoDelta::FromSeconds(10));
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
  n_worker_threads_ = FLAGS_worker_thread;
  n_server_reactor_threads_ = FLAGS_server_reactor;

  // Set up server.
  StartTestServerWithGeneratedCode(&server_addr_);

  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();

  boost::ptr_vector<ClientThread> threads;
  for (int i = 0; i < FLAGS_client_thread; i++) {
    auto thr = new ClientThread(this);
    thr->Start();
    threads.push_back(thr);
  }

  SleepFor(MonoDelta::FromSeconds(AllowSlowTests() ? 10 : FLAGS_run_seconds));
  Release_Store(&should_run_, false);

  int total_reqs = 0;

  for (ClientThread &thr : threads) {
    thr.Join();
    total_reqs += thr.request_count_;
  }
  sw.stop();

  float reqs_per_second = static_cast<float>(total_reqs / sw.elapsed().wall_seconds());
  float user_cpu_micros_per_req = static_cast<float>(sw.elapsed().user / 1000.0 / total_reqs);
  float sys_cpu_micros_per_req = static_cast<float>(sw.elapsed().system / 1000.0 / total_reqs);

  LOG(INFO) << "Reqs/sec:         " << reqs_per_second;
  LOG(INFO) << "User CPU per req: " << user_cpu_micros_per_req << "us";
  LOG(INFO) << "Sys CPU per req:  " << sys_cpu_micros_per_req << "us";
}

class ClientAsyncWorkload {
 public:
  ClientAsyncWorkload(RpcBench *bench, shared_ptr<Messenger> messenger)
    : bench_(bench),
      messenger_(messenger),
      request_count_(0) {
    controller_.set_timeout(MonoDelta::FromSeconds(10));
    proxy_.reset(new CalculatorServiceProxy(messenger_, bench_->server_addr_));
  }

  void CallOneRpc() {
    if (request_count_ > 0) {
      CHECK_OK(controller_.status());
      CHECK_EQ(req_.x() + req_.y(), resp_.result());
    }
    if (!Acquire_Load(&bench_->should_run_)) {
      bench_->stop_.CountDown();
      return;
    }
    controller_.Reset();
    req_.set_x(request_count_);
    req_.set_y(request_count_);
    request_count_++;
    proxy_->AddAsync(req_,
                     &resp_,
                     &controller_,
                     boost::bind(&ClientAsyncWorkload::CallOneRpc, this));
  }

  void Start() {
    CallOneRpc();
  }

  RpcBench *bench_;
  shared_ptr<Messenger> messenger_;
  unique_ptr<CalculatorServiceProxy> proxy_;
  uint32_t request_count_;
  RpcController controller_;
  AddRequestPB req_;
  AddResponsePB resp_;
};

TEST_F(RpcBench, BenchmarkCallAsync) {
  n_worker_threads_ = FLAGS_worker_thread;
  n_server_reactor_threads_ = FLAGS_server_reactor;

  // Set up server.
  StartTestServerWithGeneratedCode(&server_addr_);

  int threads = FLAGS_client_thread;
  int concurrency = FLAGS_client_concurrency;

  vector<shared_ptr<Messenger>> messengers;
  for (int i = 0; i < threads; i++) {
    messengers.push_back(CreateMessenger("Client"));
  }

  vector<shared_ptr<ClientAsyncWorkload>> workloads;
  for (int i = 0; i < concurrency; i++) {
    workloads.push_back(shared_ptr<ClientAsyncWorkload>(
        new ClientAsyncWorkload(this, messengers[i%threads])));
  }

  stop_.Reset(concurrency);

  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();

  for (int i = 0; i < concurrency; i++) {
    workloads[i]->Start();
  }

  SleepFor(MonoDelta::FromSeconds(AllowSlowTests() ? 10 : FLAGS_run_seconds));
  Release_Store(&should_run_, false);

  sw.stop();

  stop_.Wait();
  int total_reqs = 0;
  for (int i = 0; i < concurrency; i++) {
    total_reqs += workloads[i]->request_count_;
  }

  float reqs_per_second = static_cast<float>(total_reqs / sw.elapsed().wall_seconds());
  float user_cpu_micros_per_req = static_cast<float>(sw.elapsed().user / 1000.0 / total_reqs);
  float sys_cpu_micros_per_req = static_cast<float>(sw.elapsed().system / 1000.0 / total_reqs);

  LOG(INFO) << "Reqs/sec:         " << reqs_per_second;
  LOG(INFO) << "User CPU per req: " << user_cpu_micros_per_req << "us";
  LOG(INFO) << "Sys CPU per req:  " << sys_cpu_micros_per_req << "us";
}

} // namespace rpc
} // namespace kudu

