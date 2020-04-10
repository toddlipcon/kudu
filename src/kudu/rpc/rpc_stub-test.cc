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

#include <algorithm>
#include <array>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <linux/memfd.h>
#include <sched.h>
#include <stdint.h>
#include <sys/mman.h>
#include <syscall.h>
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpcz_store.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fd.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/user.h"

DEFINE_bool(is_panic_test_child, false, "Used by TestRpcPanic");
DECLARE_bool(socket_inject_short_recvs);

DEFINE_string(benchmark_setup_pattern, "*", "Which tests to run for the data transfer benchmark");
DEFINE_string(benchmark_method_pattern, "*", "Which methods to run for the data transfer benchmark");
DEFINE_int64(benchmark_total_mb, 1024, "how many MB to benchmark");

using base::subtle::NoBarrier_Load;
using kudu::pb_util::SecureDebugString;
using std::array;
using std::cout;
using std::endl;
using std::map;
using std::pair;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

class ScopedSetCpuAffinity {
 public:
  ScopedSetCpuAffinity(int tid, int target_cpu)
      : tid_(tid) {
    CHECK_ERR(sched_getaffinity(tid_, sizeof(orig_cpus_), &orig_cpus_));
    cpu_set_t new_cpus;
    CPU_ZERO(&new_cpus);
    CPU_SET(target_cpu, &new_cpus);
    CHECK_ERR(sched_setaffinity(tid_, sizeof(new_cpus), &new_cpus));
  }
  ~ScopedSetCpuAffinity() {
    CHECK_ERR(sched_setaffinity(tid_, sizeof(orig_cpus_), &orig_cpus_));
  }
private:
  const int tid_;
  cpu_set_t orig_cpus_;
  DISALLOW_COPY_AND_ASSIGN(ScopedSetCpuAffinity);
};

class RpcStubTest : public RpcTestBase {
 public:
  void SetUp() override {
    RpcTestBase::SetUp();
    // Use a shorter queue length since some tests below need to start enough
    // threads to saturate the queue.
    service_queue_length_ = 10;
    ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr_));
    ASSERT_OK(CreateMessenger("Client", &client_messenger_));
  }
 protected:
  void SendSimpleCall() {
    CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

    RpcController controller;
    AddRequestPB req;
    req.set_x(10);
    req.set_y(20);
    AddResponsePB resp;
    ASSERT_OK(p.Add(req, &resp, &controller));
    ASSERT_EQ(30, resp.result());
  }

  Sockaddr server_addr_;
  shared_ptr<Messenger> client_messenger_;
};

TEST_F(RpcStubTest, TestSimpleCall) {
  SendSimpleCall();
}

// Regression test for a bug in which we would not properly parse a call
// response when recv() returned a 'short read'. This injects such short
// reads and then makes a number of calls.
TEST_F(RpcStubTest, TestShortRecvs) {
  FLAGS_socket_inject_short_recvs = true;
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  for (int i = 0; i < 100; i++) {
    NO_FATALS(SendSimpleCall());
  }
}

// Test calls which are rather large.
// This test sends many of them at once using the async API and then
// waits for them all to return. This is meant to ensure that the
// IO threads can deal with read/write calls that don't succeed
// in sending the entire data in one go.
TEST_F(RpcStubTest, TestBigCallData) {
  const int kNumSentAtOnce = 20;
  const size_t kMessageSize = 5 * 1024 * 1024;
  string data;
  data.resize(kMessageSize);

  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  EchoRequestPB req;
  req.set_data(data);

  vector<unique_ptr<EchoResponsePB>> resps;
  vector<unique_ptr<RpcController>> controllers;

  CountDownLatch latch(kNumSentAtOnce);
  for (int i = 0; i < kNumSentAtOnce; i++) {
    resps.emplace_back(new EchoResponsePB);
    controllers.emplace_back(new RpcController);

    p.EchoAsync(req, resps.back().get(), controllers.back().get(),
                [&latch]() { latch.CountDown(); });
  }

  latch.Wait();

  for (const auto& c : controllers) {
    ASSERT_OK(c->status());
  }
}

TEST_F(RpcStubTest, TestRespondDeferred) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  RpcController controller;
  SleepRequestPB req;
  req.set_sleep_micros(1000);
  req.set_deferred(true);
  SleepResponsePB resp;
  ASSERT_OK(p.Sleep(req, &resp, &controller));
}

// Test that the default user credentials are propagated to the server.
TEST_F(RpcStubTest, TestDefaultCredentialsPropagated) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  string expected;
  ASSERT_OK(GetLoggedInUser(&expected));

  RpcController controller;
  WhoAmIRequestPB req;
  WhoAmIResponsePB resp;
  ASSERT_OK(p.WhoAmI(req, &resp, &controller));
  ASSERT_EQ(expected, resp.credentials().real_user());
  ASSERT_FALSE(resp.credentials().has_effective_user());
}

// Test that the user can specify other credentials.
TEST_F(RpcStubTest, TestCustomCredentialsPropagated) {
  const char* const kFakeUserName = "some fake user";
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  UserCredentials creds;
  creds.set_real_user(kFakeUserName);
  p.set_user_credentials(creds);

  RpcController controller;
  WhoAmIRequestPB req;
  WhoAmIResponsePB resp;
  ASSERT_OK(p.WhoAmI(req, &resp, &controller));
  ASSERT_EQ(kFakeUserName, resp.credentials().real_user());
  ASSERT_FALSE(resp.credentials().has_effective_user());
}

TEST_F(RpcStubTest, TestAuthorization) {
  // First test calling WhoAmI() as user "alice", who is disallowed.
  {
    CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());
    UserCredentials creds;
    creds.set_real_user("alice");
    p.set_user_credentials(creds);

    // Alice is disallowed by all RPCs.
    {
      RpcController controller;
      WhoAmIRequestPB req;
      WhoAmIResponsePB resp;
      Status s = p.WhoAmI(req, &resp, &controller);
      ASSERT_FALSE(s.ok());
      ASSERT_EQ(s.ToString(),
                "Remote error: Not authorized: alice is not allowed to call this method");
    }

    // KUDU-2540: Authorization failures on exactly-once RPCs cause FATAL
    {
      RpcController controller;

      unique_ptr<RequestIdPB> request_id(new RequestIdPB);
      request_id->set_client_id("client-id");
      request_id->set_attempt_no(0);
      request_id->set_seq_no(0);
      request_id->set_first_incomplete_seq_no(-1);
      controller.SetRequestIdPB(std::move(request_id));

      ExactlyOnceRequestPB req;
      req.set_value_to_add(1);
      ExactlyOnceResponsePB resp;
      Status s = p.AddExactlyOnce(req, &resp, &controller);
      ASSERT_FALSE(s.ok());
      ASSERT_EQ(s.ToString(),
                "Remote error: Not authorized: alice is not allowed to call this method");
    }
  }

  // Try some calls as "bob".
  {
    CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());
    UserCredentials creds;
    creds.set_real_user("bob");
    p.set_user_credentials(creds);

    // "bob" is allowed to call WhoAmI().
    {
      RpcController controller;
      WhoAmIRequestPB req;
      WhoAmIResponsePB resp;
      ASSERT_OK(p.WhoAmI(req, &resp, &controller));
    }

    // "bob" is not allowed to call "Sleep".
    {
      RpcController controller;
      SleepRequestPB req;
      req.set_sleep_micros(10);
      SleepResponsePB resp;
      Status s = p.Sleep(req, &resp, &controller);
      ASSERT_EQ(s.ToString(),
                "Remote error: Not authorized: bob is not allowed to call this method");
    }
  }
}

// Test that the user's remote address is accessible to the server.
TEST_F(RpcStubTest, TestRemoteAddress) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  RpcController controller;
  WhoAmIRequestPB req;
  WhoAmIResponsePB resp;
  ASSERT_OK(p.WhoAmI(req, &resp, &controller));
  ASSERT_STR_CONTAINS(resp.address(), "127.0.0.1:");
}

////////////////////////////////////////////////////////////
// Tests for error cases
////////////////////////////////////////////////////////////

// Test sending a PB parameter with a missing field, where the client
// thinks it has sent a full PB. (eg due to version mismatch)
TEST_F(RpcStubTest, TestCallWithInvalidParam) {
  Proxy p(client_messenger_, server_addr_, server_addr_.host(),
          CalculatorService::static_service_name());

  rpc_test::AddRequestPartialPB req;
  req.set_x(rand());
  // AddRequestPartialPB is missing the 'y' field.
  AddResponsePB resp;
  RpcController controller;
  Status s = p.SyncRequest("Add", req, &resp, &controller);
  ASSERT_TRUE(s.IsRemoteError()) << "Bad status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Invalid argument: invalid parameter for call "
                      "kudu.rpc_test.CalculatorService.Add: "
                      "missing fields: y");
}

// Wrapper around AtomicIncrement, since AtomicIncrement returns the 'old'
// value, and our callback needs to be a void function.
static void DoIncrement(Atomic32* count) {
  base::subtle::Barrier_AtomicIncrement(count, 1);
}

// Test sending a PB parameter with a missing field on the client side.
// This also ensures that the async callback is only called once
// (regression test for a previously-encountered bug).
TEST_F(RpcStubTest, TestCallWithMissingPBFieldClientSide) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  RpcController controller;
  AddRequestPB req;
  req.set_x(10);
  // Request is missing the 'y' field.
  AddResponsePB resp;
  Atomic32 callback_count = 0;
  p.AddAsync(req, &resp, &controller, [&callback_count]() { DoIncrement(&callback_count); });
  while (NoBarrier_Load(&callback_count) == 0) {
    SleepFor(MonoDelta::FromMicroseconds(10));
  }
  SleepFor(MonoDelta::FromMicroseconds(100));
  ASSERT_EQ(1, NoBarrier_Load(&callback_count));
  ASSERT_STR_CONTAINS(controller.status().ToString(),
                      "Invalid argument: invalid parameter for call "
                      "kudu.rpc_test.CalculatorService.Add: missing fields: y");
}

TEST_F(RpcStubTest, TestResponseWithMissingField) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  RpcController rpc;
  TestInvalidResponseRequestPB req;
  TestInvalidResponseResponsePB resp;
  req.set_error_type(rpc_test::TestInvalidResponseRequestPB_ErrorType_MISSING_REQUIRED_FIELD);
  Status s = p.TestInvalidResponse(req, &resp, &rpc);
  ASSERT_STR_CONTAINS(s.ToString(),
                      "invalid RPC response, missing fields: response");
}

// Test case where the server responds with a message which is larger than the maximum
// configured RPC message size. The server should send the response, but the client
// will reject it.
TEST_F(RpcStubTest, TestResponseLargerThanFrameSize) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  RpcController rpc;
  TestInvalidResponseRequestPB req;
  TestInvalidResponseResponsePB resp;
  req.set_error_type(rpc_test::TestInvalidResponseRequestPB_ErrorType_RESPONSE_TOO_LARGE);
  Status s = p.TestInvalidResponse(req, &resp, &rpc);
  ASSERT_STR_CONTAINS(s.ToString(), "Network error: RPC frame had a length of");
}

// Test sending a call which isn't implemented by the server.
TEST_F(RpcStubTest, TestCallMissingMethod) {
  Proxy p(client_messenger_, server_addr_, server_addr_.host(),
          CalculatorService::static_service_name());

  Status s = DoTestSyncCall(p, "DoesNotExist");
  ASSERT_TRUE(s.IsRemoteError()) << "Bad status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "with an invalid method name: DoesNotExist");
}

TEST_F(RpcStubTest, TestApplicationError) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  RpcController controller;
  SleepRequestPB req;
  SleepResponsePB resp;
  req.set_sleep_micros(1);
  req.set_return_app_error(true);
  Status s = p.Sleep(req, &resp, &controller);
  ASSERT_TRUE(s.IsRemoteError());
  EXPECT_EQ("Remote error: Got some error", s.ToString());
  EXPECT_EQ("message: \"Got some error\"\n"
            "[kudu.rpc_test.CalculatorError.app_error_ext] {\n"
            "  extra_error_data: \"some application-specific error data\"\n"
            "}\n",
            SecureDebugString(*controller.error_response()));
}

TEST_F(RpcStubTest, TestRpcPanic) {
  if (!FLAGS_is_panic_test_child) {
    // This is a poor man's death test. We call this same
    // test case, but set the above flag, and verify that
    // it aborted. gtest death tests don't work here because
    // there are already threads started up.
    vector<string> argv;
    string executable_path;
    CHECK_OK(env_->GetExecutablePath(&executable_path));
    argv.push_back(executable_path);
    argv.emplace_back("--is_panic_test_child");
    argv.emplace_back("--gtest_filter=RpcStubTest.TestRpcPanic");
    Subprocess subp(argv);
    subp.ShareParentStderr(false);
    CHECK_OK(subp.Start());
    FILE* in = fdopen(subp.from_child_stderr_fd(), "r");
    PCHECK(in);

    // Search for string "Test method panicking!" somewhere in stderr
    char buf[1024];
    bool found_string = false;
    while (fgets(buf, sizeof(buf), in)) {
      if (strstr(buf, "Test method panicking!")) {
        found_string = true;
        break;
      }
    }
    CHECK(found_string);

    // Check return status
    int wait_status = 0;
    CHECK_OK(subp.Wait(&wait_status));
    CHECK(!WIFEXITED(wait_status)); // should not have been successful
    if (WIFSIGNALED(wait_status)) {
      CHECK_EQ(WTERMSIG(wait_status), SIGABRT);
    } else {
      // On some systems, we get exit status 134 from SIGABRT rather than
      // WIFSIGNALED getting flagged.
      CHECK_EQ(WEXITSTATUS(wait_status), 134);
    }
    return;
  } else {
    // Before forcing the panic, explicitly remove the test directory. This
    // should be safe; this test doesn't generate any data.
    CHECK_OK(env_->DeleteRecursively(test_dir_));

    // Make an RPC which causes the server to abort.
    CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());
    RpcController controller;
    PanicRequestPB req;
    PanicResponsePB resp;
    p.Panic(req, &resp, &controller);
  }
}

struct AsyncSleep {
  AsyncSleep() : latch(1) {}

  RpcController rpc;
  SleepRequestPB req;
  SleepResponsePB resp;
  CountDownLatch latch;
};

TEST_F(RpcStubTest, TestDontHandleTimedOutCalls) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());
  vector<unique_ptr<AsyncSleep>> sleeps;
  sleeps.reserve(n_worker_threads_);

  // Send enough sleep calls to occupy the worker threads.
  for (int i = 0; i < n_worker_threads_; i++) {
    unique_ptr<AsyncSleep> sleep(new AsyncSleep);
    sleep->rpc.set_timeout(MonoDelta::FromSeconds(1));
    sleep->req.set_sleep_micros(1000*1000); // 1sec
    auto& l = sleep->latch;
    p.SleepAsync(sleep->req, &sleep->resp, &sleep->rpc,
                 [&l]() { l.CountDown(); });
    sleeps.emplace_back(std::move(sleep));
  }

  // We asynchronously sent the RPCs above, but the RPCs might still
  // be in the queue. Because the RPC we send next has a lower timeout,
  // it would take priority over the long-timeout RPCs. So, we have to
  // wait until the above RPCs are being processed before we continue
  // the test.
  const Histogram* queue_time_metric = service_pool_->IncomingQueueTimeMetricForTests();
  while (queue_time_metric->TotalCount() < n_worker_threads_) {
    SleepFor(MonoDelta::FromMilliseconds(1));
  }

  // Send another call with a short timeout. This shouldn't get processed, because
  // it'll get stuck in the queue for longer than its timeout.
  ASSERT_EVENTUALLY([&]() {
    RpcController rpc;
    SleepRequestPB req;
    SleepResponsePB resp;
    req.set_sleep_micros(1); // unused but required.
    rpc.set_timeout(MonoDelta::FromMilliseconds(5));
    Status s = p.Sleep(req, &resp, &rpc);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    // Since our timeout was short, it's possible in rare circumstances
    // that we time out the RPC on the outbound queue, in which case
    // we won't trigger the desired behavior here. In that case, the
    // timeout error status would have the string 'ON_OUTBOUND_QUEUE'
    // instead of 'SENT', so this assertion would fail and cause the
    // ASSERT_EVENTUALLY to loop.
    ASSERT_STR_CONTAINS(s.ToString(), "SENT");
  });

  for (const auto& s : sleeps) {
    s->latch.Wait();
  }

  // Verify that the timedout call got short circuited before being processed.
  // We may need to loop a short amount of time as we are racing with the reactor
  // thread to process the remaining elements of the queue.
  const Counter* timed_out_in_queue = service_pool_->RpcsTimedOutInQueueMetricForTests();
  ASSERT_EVENTUALLY([&]{
    ASSERT_EQ(1, timed_out_in_queue->value());
  });
}

// Test which ensures that the RPC queue accepts requests with the earliest
// deadline first (EDF), and upon overflow rejects requests with the latest deadlines.
//
// In particular, this simulates a workload experienced with Impala where the local
// impalad would spawn more scanner threads than the total number of handlers plus queue
// slots, guaranteeing that some of those clients would see SERVER_TOO_BUSY rejections on
// scan requests and be forced to back off and retry.  Without EDF scheduling, we saw that
// the "unlucky" threads that got rejected would likely continue to get rejected upon
// retries, and some would be starved continually until they missed their overall deadline
// and failed the query.
//
// With EDF scheduling, the retries take priority over the original requests (because
// they retain their original deadlines). This prevents starvation of unlucky threads.
TEST_F(RpcStubTest, TestEarliestDeadlineFirstQueue) {
  const int num_client_threads = service_queue_length_ + n_worker_threads_ + 5;
  vector<std::thread> threads;
  vector<int> successes(num_client_threads);
  std::atomic<bool> done(false);
  for (int thread_id = 0; thread_id < num_client_threads; thread_id++) {
    threads.emplace_back([&, thread_id] {
        Random rng(thread_id);
        CalculatorServiceProxy p(
            client_messenger_, server_addr_, server_addr_.host());
        while (!done.load()) {
          // Set a deadline in the future. We'll keep using this same deadline
          // on each of our retries.
          MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(8);

          for (int attempt = 1; !done.load(); attempt++) {
            RpcController controller;
            SleepRequestPB req;
            SleepResponsePB resp;
            controller.set_deadline(deadline);
            req.set_sleep_micros(100000);
            Status s = p.Sleep(req, &resp, &controller);
            if (s.ok()) {
              successes[thread_id]++;
              break;
            }
            // We expect to get SERVER_TOO_BUSY errors because we have more clients than the
            // server has handlers and queue slots. No other errors are expected.
            CHECK(s.IsRemoteError() &&
                  controller.error_response()->code() == rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY)
                << "Unexpected RPC failure: " << s.ToString();
            // Randomized exponential backoff (similar to that done by the scanners in the Kudu
            // client.).
            int backoff = (0.5 + rng.NextDoubleFraction() * 0.5) * (std::min(1 << attempt, 1000));
            VLOG(1) << "backoff " << backoff << "ms";
            SleepFor(MonoDelta::FromMilliseconds(backoff));
          }
        }
      });
  }
  // Let the threads run for 5 seconds before stopping them.
  SleepFor(MonoDelta::FromSeconds(5));
  done.store(true);
  for (auto& t : threads) {
    t.join();
  }

  // Before switching to earliest-deadline-first scheduling, the results
  // here would typically look something like:
  //  1 1 0 1 10 17 6 1 12 12 17 10 8 7 12 9 16 15
  // With the fix, we see something like:
  //  9 9 9 8 9 9 9 9 9 9 9 9 9 9 9 9 9
  LOG(INFO) << "thread RPC success counts: " << successes;

  int sum = 0;
  int min = std::numeric_limits<int>::max();
  for (int x : successes) {
    sum += x;
    min = std::min(min, x);
  }
  int avg = sum / successes.size();
  ASSERT_GT(min, avg / 2)
      << "expected the least lucky thread to have at least half as many successes "
      << "as the average thread: min=" << min << " avg=" << avg;
}

TEST_F(RpcStubTest, TestDumpCallsInFlight) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());
  AsyncSleep sleep;
  sleep.req.set_sleep_micros(100 * 1000); // 100ms
  auto& l = sleep.latch;
  p.SleepAsync(sleep.req, &sleep.resp, &sleep.rpc,
               [&l]() { l.CountDown(); });

  // Check the running RPC status on the client messenger.
  DumpConnectionsRequestPB dump_req;
  DumpConnectionsResponsePB dump_resp;
  dump_req.set_include_traces(true);

  ASSERT_OK(client_messenger_->DumpConnections(dump_req, &dump_resp));
  LOG(INFO) << "client messenger: " << SecureDebugString(dump_resp);
  ASSERT_EQ(1, dump_resp.outbound_connections_size());
  ASSERT_EQ(1, dump_resp.outbound_connections(0).calls_in_flight_size());
  ASSERT_EQ("Sleep", dump_resp.outbound_connections(0).calls_in_flight(0).
                        header().remote_method().method_name());
  ASSERT_GT(dump_resp.outbound_connections(0).calls_in_flight(0).micros_elapsed(), 0);

  // And the server messenger.
  // We have to loop this until we find a result since the actual call is sent
  // asynchronously off of the main thread (ie the server may not be handling it yet)
  for (int i = 0; i < 100; i++) {
    dump_resp.Clear();
    ASSERT_OK(server_messenger_->DumpConnections(dump_req, &dump_resp));
    if (dump_resp.inbound_connections_size() > 0 &&
        dump_resp.inbound_connections(0).calls_in_flight_size() > 0) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }

  LOG(INFO) << "server messenger: " << SecureDebugString(dump_resp);
  ASSERT_EQ(1, dump_resp.inbound_connections_size());
  ASSERT_EQ(1, dump_resp.inbound_connections(0).calls_in_flight_size());
  ASSERT_EQ("Sleep", dump_resp.inbound_connections(0).calls_in_flight(0).
                        header().remote_method().method_name());
  ASSERT_GT(dump_resp.inbound_connections(0).calls_in_flight(0).micros_elapsed(), 0);
  ASSERT_STR_CONTAINS(dump_resp.inbound_connections(0).calls_in_flight(0).trace_buffer(),
                      "Inserting onto call queue");
  sleep.latch.Wait();
}

TEST_F(RpcStubTest, TestDumpSampledCalls) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  // Issue two calls that fall into different latency buckets.
  AsyncSleep sleeps[2];
  sleeps[0].req.set_sleep_micros(150 * 1000); // 150ms
  sleeps[1].req.set_sleep_micros(1500 * 1000); // 1500ms

  for (auto& sleep : sleeps) {
    auto& l = sleep.latch;
    p.SleepAsync(sleep.req, &sleep.resp, &sleep.rpc,
                 [&l]() { l.CountDown(); });
  }
  for (auto& sleep : sleeps) {
    sleep.latch.Wait();
  }

  // Dump the sampled RPCs and expect to see the calls
  // above.

  DumpRpczStoreResponsePB sampled_rpcs;
  server_messenger_->rpcz_store()->DumpPB(DumpRpczStoreRequestPB(), &sampled_rpcs);
  EXPECT_EQ(sampled_rpcs.methods_size(), 1);
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs),
                      "    metrics {\n"
                      "      key: \"test_sleep_us\"\n"
                      "      value: 150000\n"
                      "    }\n");
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs),
                      "    metrics {\n"
                      "      key: \"test_sleep_us\"\n"
                      "      value: 1500000\n"
                      "    }\n");
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs),
                      "    metrics {\n"
                      "      child_path: \"test_child\"\n"
                      "      key: \"related_trace_metric\"\n"
                      "      value: 1\n"
                      "    }");
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs), "SleepRequestPB");
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs), "duration_ms");
}

namespace {
struct RefCountedTest : public RefCountedThreadSafe<RefCountedTest> {
};

// Test callback which takes a refcounted pointer.
// We don't use this parameter, but it's used to validate that the bound callback
// is cleared in TestCallbackClearedAfterRunning.
void MyTestCallback(CountDownLatch* latch, scoped_refptr<RefCountedTest> my_refptr) {
  latch->CountDown();
}
} // anonymous namespace

// Verify that, after a call has returned, no copy of the call's callback
// is held. This is important when the callback holds a refcounted ptr,
// since we expect to be able to release that pointer when the call is done.
TEST_F(RpcStubTest, TestCallbackClearedAfterRunning) {
  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  CountDownLatch latch(1);
  scoped_refptr<RefCountedTest> my_refptr(new RefCountedTest);
  RpcController controller;
  AddRequestPB req;
  req.set_x(10);
  req.set_y(20);
  AddResponsePB resp;
  p.AddAsync(req, &resp, &controller,
             [&latch, my_refptr]() { MyTestCallback(&latch, my_refptr); });
  latch.Wait();

  // The ref count should go back down to 1. However, we need to loop a little
  // bit, since the deref is happening on another thread. If the other thread gets
  // descheduled directly after calling our callback, we'd fail without these sleeps.
  for (int i = 0; i < 100 && !my_refptr->HasOneRef(); i++) {
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
  ASSERT_TRUE(my_refptr->HasOneRef());
}

// Regression test for KUDU-1409: if the client reactor thread is blocked (e.g due to a
// process-wide pause or a slow callback) then we should not cause RPC calls to time out.
TEST_F(RpcStubTest, DontTimeOutWhenReactorIsBlocked) {
  CHECK_EQ(client_messenger_->num_reactors(), 1)
      << "This test requires only a single reactor. Otherwise the injected sleep might "
      << "be scheduled on a different reactor than the RPC call.";

  CalculatorServiceProxy p(client_messenger_, server_addr_, server_addr_.host());

  // Schedule a 1-second sleep on the reactor thread.
  //
  // This will cause us the reactor to be blocked while the call response is received, and
  // still be blocked when the timeout would normally occur. Despite this, the call should
  // not time out.
  //
  //  0s         0.5s          1.2s     1.5s
  //  RPC call running
  //  |---------------------|
  //              Reactor blocked in sleep
  //             |----------------------|
  //                            \_ RPC would normally time out

  client_messenger_->ScheduleOnReactor([](const Status& s) {
      ThreadRestrictions::ScopedAllowWait allow_wait;
      SleepFor(MonoDelta::FromSeconds(1));
    }, MonoDelta::FromSeconds(0.5));

  RpcController controller;
  SleepRequestPB req;
  SleepResponsePB resp;
  req.set_sleep_micros(800 * 1000);
  controller.set_timeout(MonoDelta::FromMilliseconds(1200));
  ASSERT_OK(p.Sleep(req, &resp, &controller));
}

class RpcStubTestUnixSocket : public RpcStubTest {
 public:
  void SetUp() override {
    n_server_reactor_threads_ = 1;
    CHECK_OK(server_addr_.ParseUnixDomainPath(strings::Substitute("@test-$0", getpid())));
    RpcStubTest::SetUp();

    CHECK(server_addr_.is_unix());
  }
};

class DataTransferMethod {
 public:
  virtual ~DataTransferMethod() {}
  virtual void SetupRequest(RpcController* rpc,
                            SendBackDataRequestPB* req) = 0;
  virtual const uint8_t* ResponseData(RpcController* rpc) = 0;
  virtual string ToString() const = 0;
};

class FdBasedMethod : public DataTransferMethod {
 public:
  FdBasedMethod(int num_bytes,
                SendBackDataRequestPB::Method method,
                bool client_reuse,
                bool server_reuse,
                bool non_temporal)
      : num_bytes_(num_bytes),
        method_(method),
        client_reuse_(client_reuse),
        server_reuse_(server_reuse),
        non_temporal_(non_temporal) {
  }

  void SetupRequest(RpcController* rpc,
                    SendBackDataRequestPB* req) override {
    if (!client_reuse_) {
      fd_.Close();
      Unmap();
    }
    if (fd_.get() == -1) {
      int fd_num = syscall(__NR_memfd_create, "test", MFD_CLOEXEC);
      PCHECK(fd_num > 0);

      fd_ = FileDescriptor(fd_num);
    }
    int idx;
    ASSERT_OK(rpc->SendFileDescriptor(fd_, &idx));
    req->set_method(method_);
    req->set_reuse_mmaps(server_reuse_);
    req->set_non_temporal(non_temporal_);
  }

  const uint8_t* ResponseData(RpcController*) override {
    if (!map_) {
      map_ = reinterpret_cast<uint8_t*>(mmap(
          nullptr, num_bytes_, PROT_READ | PROT_WRITE,
          MAP_SHARED | MAP_POPULATE, fd_.get(), 0));
      PCHECK(map_ != MAP_FAILED);
    }
    return map_;
  }
  string ToString() const override {
    const char* method_str;
    switch (method_) {
      case SendBackDataRequestPB::WRITE_TO_FILE_DESCRIPTOR:
        method_str = "s-write";
        break;
      case SendBackDataRequestPB::MMAP_FILE_DESCRIPTOR:
        method_str = "s-mmap";
        break;
      default:
        LOG(FATAL);
    }
    vector<string> flags { method_str };
    if (client_reuse_) flags.emplace_back("c-reuse");
    if (server_reuse_) flags.emplace_back("s-reuse");
    if (non_temporal_) flags.emplace_back("nt");

    return strings::Substitute("fd($0)", JoinStrings(flags, ","));
  }

  ~FdBasedMethod() {
    Unmap();
  }
 private:
  void Unmap() {
    if (map_) {
      CHECK_ERR(munmap(map_, num_bytes_));
      map_ = nullptr;
    }
  }
  const int num_bytes_;
  const SendBackDataRequestPB::Method method_;
  const bool client_reuse_;
  const bool server_reuse_;
  const bool non_temporal_;
  FileDescriptor fd_;
  uint8_t* map_ = nullptr;
};

class SidecarBasedMethod : public DataTransferMethod {
 public:
  explicit SidecarBasedMethod(bool non_temporal)
      : non_temporal_(non_temporal) {
  }
  void SetupRequest(RpcController* rpc,
                    SendBackDataRequestPB* req) override {
    req->set_method(SendBackDataRequestPB::SEND_BACK_SIDECAR);
    req->set_non_temporal(non_temporal_);
  }

  const uint8_t* ResponseData(RpcController* rpc) override {
    Slice sidecar;
    CHECK_OK(rpc->GetInboundSidecar(0, &sidecar));
    return sidecar.data();
  }
  string ToString() const override {
    string ret = "sidecar";
    if (non_temporal_) {
      ret += ",nt";
    }
    return ret;
  }
 private:
  const bool non_temporal_;
};

// Parse the contents of /proc/cpuinfo to determine the NUMA topology
// of the current system. Returns a map of CPU names to CPU identifiers.
// The names here are "S<socket>C<core>T<thread>" with the socket, core, and
// thread numbers being zero-indexed.
map<string, int> ParseCpuList() {
  map<string, int> ret;

  faststring buf;
  CHECK_OK(ReadFileToString(Env::Default(), "/proc/cpuinfo", &buf));
  StringPiece buf_sp(reinterpret_cast<const char*>(buf.data()), buf.size());
  vector<StringPiece> stanzas = strings::Split(buf_sp, "\n\n",
                                               strings::SkipWhitespace());
  map<string, int> thread_counts;
  for (const auto& stanza : stanzas) {
    int phys_id = -1;
    int core_id = -1;
    int processor = -1;
    for (const auto& line : strings::Split(stanza, "\n")) {
      pair<StringPiece, string> kv = strings::Split(line, ": ");
      StripWhiteSpace(&kv.first);
      StripWhiteSpace(&kv.second);
      if (kv.first == "physical id") {
        CHECK(SimpleAtoi(kv.second, &phys_id));
      } else if (kv.first == "core id") {
        CHECK(SimpleAtoi(kv.second, &core_id));
      } else if (kv.first == "processor") {
        CHECK(SimpleAtoi(kv.second, &processor));
      }
    }
    CHECK_NE(phys_id, -1);
    CHECK_NE(core_id, -1);
    CHECK_NE(processor, -1);
    string core = strings::Substitute("S$0C$1", phys_id, core_id);
    string thread = strings::Substitute("$0T$1", core, thread_counts[core]++);
    ret[thread] = processor;
  }
  return ret;
}

// Return true if the given data consists fully of the character 'x'.
bool IsAllXs(const uint8_t* data, size_t len) {
  bool ok = true;
  for (int i = 0; i < len; i++) {
    // NOTE: we don't early-return here -- doing so makes the loop harder
    // to vectorize since it doesn't have a constant trip count.
    ok &= data[i] == 'x';
  }
  return ok;
}

// Compute a baseline for how long it takes to perform the same work as our
// benchmark, but in a local thread that just memsets and verifies it.
void ComputeSpeedLimit(int bytes_per_call, int total_bytes) {
  faststring buf;
  buf.resize(bytes_per_call);

  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  for (int i = 0; i < total_bytes / bytes_per_call; i++) {
    memset(buf.data(), 'x', bytes_per_call);
    base::subtle::MemoryBarrier();
    CHECK(IsAllXs(buf.data(), bytes_per_call));
    base::subtle::MemoryBarrier();
  }
  sw.stop();
  double cpu_secs = sw.elapsed().user_cpu_seconds() + sw.elapsed().system_cpu_seconds();
  cout << StringPrintf("Speed-limit (optimal) performance: %.1f MB/sec",
                       total_bytes / 1024 / 1024 / cpu_secs) << endl;
}

TEST_F(RpcStubTestUnixSocket, BenchmarkDataTransfer) {
  auto cpu_by_name = ParseCpuList();
/*
have four threads: client, client reactor, server reactor, server worker
four levels of possible collocation:
 - same hyperthread (T)
 - same core different thread (C)
 - same socket different core (S)
 - remote socket (R)
*/

  map<string, array<string, 3>> setups {
    { "all-same-core",              { "S0C0T0", "S0C0T0", "S0C0T0" }},
    { "client-server-cross-core",   { "S0C0T0", "S0C1T0", "S0C1T0" }},
    { "client-server-cross-socket", { "S0C0T0", "S1C0T0", "S1C0T0" }},
    { "every-hop-crosses-socket",   { "S1C0T0", "S0C0T0", "S1C0T0" }},
    { "every-hop-crosses-core",     { "S0C1T0", "S0C2T0", "S0C3T0" }},
  };

  CalculatorServiceProxy p(client_messenger_, server_addr_, "localhost");
  constexpr int64_t kBytesPerCall = 2 * 1024 * 1024;
  const int64_t kTotalBytes = FLAGS_benchmark_total_mb * 1024 * 1024;

  cout << "Legend\n"
       << "===================\n"
       << "Methods:\n"
       << "  fd:      client sends a file descriptor for the server to fill\n"
       << "  s-write: server writes data the passed fd using write() syscall\n"
       << "  s-mmap:  server mmaps() the passed fd and writes using memset()\n"
       << "  s-reuse: server holds on to the mmapped region across calls\n"
       << "  c-reuse: client holds on to the mmapped region across calls\n"
       << "  sidecar: server sends back data over unix socket\n"
       << "  nt:      server uses nontemporal store instructions to write\n"
       << "\n"
       << "NOTE: s-reuse without c-reuse isn't very effective since the server\n"
       << "will detect that it cant actually reuse the mapped buffer, since the\n"
       << "client provided a new one."
       << "\n"
       << "Columns designate which core each thread is bound to:\n"
       << "  client: the client 'user' thread\n"
       << "  c-rx:   the client reactor thread\n"
       << "  s-rx:   the server reactor thread\n"
       << "  worker: the server RPC worker thread\n"
       << "\n"
       << "Core notation:\n"
       << "  SaCbTc: socket a core b thread c\n"
       << endl;

  vector<unique_ptr<DataTransferMethod>> methods;
  for (bool client_reuse : {false, true}) {
    methods.emplace_back(
        new FdBasedMethod(kBytesPerCall, SendBackDataRequestPB::WRITE_TO_FILE_DESCRIPTOR,
                          client_reuse, false, false));
    for (bool server_reuse : {false, true}) {
      for (bool non_temporal : { false, true }) {
        methods.emplace_back(
            new FdBasedMethod(kBytesPerCall, SendBackDataRequestPB::MMAP_FILE_DESCRIPTOR,
                              client_reuse, server_reuse, non_temporal));
      }
    }
  }
  methods.emplace_back(new SidecarBasedMethod(false));
  methods.emplace_back(new SidecarBasedMethod(true));

  methods.erase(std::remove_if(
      methods.begin(), methods.end(),
      [](const unique_ptr<DataTransferMethod>& m) {
        return !MatchPattern(m->ToString(), FLAGS_benchmark_method_pattern);
      }), methods.end());
  CHECK(!methods.empty());

  CHECK_EQ(1, client_messenger_->num_reactors());
  CHECK_EQ(1, server_messenger_->num_reactors());
  int client_reactor_pid = client_messenger_->reactors_[0]->pid_for_tests();
  int server_reactor_pid = server_messenger_->reactors_[0]->pid_for_tests();

  const char* client_cpu = "S0C0T0";
  ScopedSetCpuAffinity set_client_cpu(0, FindOrDie(cpu_by_name, client_cpu));
  ComputeSpeedLimit(kBytesPerCall, 1024 * 1024 * 1024);

  cout << StringPrintf(
      "%30s%7s%7s%7s%7s\t%s",
      "method", "client", "c-rx", "s-rx", "worker", "MB/sec") << endl;
  for (const auto& setup_pair : setups) {
    const auto& setup_name = setup_pair.first;
    if (!MatchPattern(setup_name, FLAGS_benchmark_setup_pattern)) continue;

    cout << "# " << setup_name << endl;
    const auto& setup = setup_pair.second;
    int client_reactor_cpu = FindWithDefault(cpu_by_name, setup[0], -1);
    int server_reactor_cpu = FindWithDefault(cpu_by_name, setup[1], -1);
    int server_worker_cpu = FindWithDefault(cpu_by_name, setup[2], -1);
    if (client_reactor_cpu == -1 ||
        server_reactor_cpu == -1 ||
        server_worker_cpu == -1) {
      LOG(WARNING) << "Local system doesn't have appropriate CPU topology "
                   << "to execute test scenario " << setup_name;
      continue;
    }


    vector<unique_ptr<ScopedSetCpuAffinity>> affinities;
    for (const auto& worker : service_pool_->GetWorkerThreadsForTests()) {
      affinities.emplace_back(new ScopedSetCpuAffinity(worker->tid(), server_worker_cpu));
    }
    ScopedSetCpuAffinity set_client_reactor_cpu(client_reactor_pid, client_reactor_cpu);
    ScopedSetCpuAffinity set_server_reactor_cpu(server_reactor_pid, server_reactor_cpu);

    for (const auto& method : methods) {
      Stopwatch sw(Stopwatch::ALL_THREADS);
      sw.start();
      for (int i = 0; i < kTotalBytes / kBytesPerCall; i++) {
        RpcController rpc;
        SendBackDataRequestPB req;
        SendBackDataResponsePB resp;
        req.set_num_bytes(kBytesPerCall);

        method->SetupRequest(&rpc, &req);

        ASSERT_OK(p.SendBackData(req, &resp, &rpc));

        const uint8_t* ret_data = method->ResponseData(&rpc);
        ASSERT_TRUE(IsAllXs(ret_data, req.num_bytes()));
      }
      sw.stop();
      double cpu_secs = sw.elapsed().user_cpu_seconds() + sw.elapsed().system_cpu_seconds();
      cout << StringPrintf(
          "%30s%7s%7s%7s%7s\t%.1f",
          method->ToString().c_str(),
          client_cpu, setup[0].c_str(), setup[1].c_str(), setup[2].c_str(),
          kTotalBytes / 1024 / 1024 / cpu_secs) << endl;
    }
  }
}

} // namespace rpc
} // namespace kudu
