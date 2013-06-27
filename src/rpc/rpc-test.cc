// Copyright (c) 2013, Cloudera, inc

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <gtest/gtest.h>
#include <string>

#include "rpc/rpc-test-base.h"
#include "util/countdown_latch.h"
#include "util/test_util.h"

using std::string;

namespace kudu {
namespace rpc {

class TestRpc : public RpcTestBase {
};

TEST_F(TestRpc, TestSockaddr) {
  Sockaddr addr1, addr2;
  addr1.set_port(1000);
  addr2.set_port(2000);
  // port is ignored when comparing Sockaddr objects
  ASSERT_FALSE(addr1 < addr2);
  ASSERT_FALSE(addr2 < addr1);
  ASSERT_EQ(1000, addr1.port());
  ASSERT_EQ(2000, addr2.port());
  ASSERT_EQ(string("0.0.0.0:1000"), addr1.ToString());
  ASSERT_EQ(string("0.0.0.0:2000"), addr2.ToString());
  Sockaddr addr3(addr1);
  ASSERT_EQ(string("0.0.0.0:1000"), addr3.ToString());
}

TEST_F(TestRpc, TestMessengerCreateDestroy) {
  shared_ptr<Messenger> messenger(CreateMessenger("TestCreateDestroy"));
  LOG(INFO) << "started messenger " << messenger->name();
  messenger->Shutdown();
  alarm(0);
}

// Test making successful RPC calls.
TEST_F(TestRpc, TestCall) {
  // Set up server.
  Sockaddr server_addr;
  StartTestServer(&server_addr);

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  Proxy p(client_messenger, server_addr);

  for (int i = 0; i < 10; i++) {
    ASSERT_STATUS_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
  }
}

// Test that connecting to an invalid server properly throws an error.
TEST_F(TestRpc, TestCallToBadServer) {
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  Sockaddr addr;
  addr.set_port(0);
  Proxy p(client_messenger, addr);

  // Loop a few calls to make sure that we properly set up and tear down
  // the connections.
  for (int i = 0; i < 5; i++) {
    Status s = DoTestSyncCall(p, GenericCalculatorService::kAddMethodName);
    LOG(INFO) << "Status: " << s.ToString();
    ASSERT_TRUE(s.IsNetworkError()) << "unexpected status: " << s.ToString();
  }
}

// Test that RPC calls can be failed with an error status on the server.
TEST_F(TestRpc, TestInvalidMethodCall) {
  // Set up server.
  Sockaddr server_addr;
  StartTestServer(&server_addr);

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  Proxy p(client_messenger, server_addr);

  // Call the method which fails.
  Status s = DoTestSyncCall(p, "ThisMethodDoesNotExist");
  ASSERT_TRUE(s.IsRuntimeError()) << "unexpected status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "bad method");
}


// Test that connections are kept alive between calls.
TEST_F(TestRpc, TestConnectionKeepalive) {
  // Only run one reactor per messenger, so we can grab the metrics from that
  // one without having to check all.
  n_server_reactor_threads_ = 1;

  // Set up server.
  Sockaddr server_addr;
  StartTestServer(&server_addr);

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  Proxy p(client_messenger, server_addr);

  ASSERT_STATUS_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));

  usleep(5000); // 5ms

  ReactorMetrics metrics;
  ASSERT_STATUS_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(1, metrics.num_server_connections_);
  ASSERT_EQ(0, metrics.num_client_connections_);

  ASSERT_STATUS_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_);
  ASSERT_EQ(1, metrics.num_client_connections_);

  // TODO: mock out time in the test!
  sleep(2);

  // After sleeping, the keepalive timer should have closed both sides of
  // the connection.
  ASSERT_STATUS_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_);
  ASSERT_EQ(0, metrics.num_client_connections_);

  ASSERT_STATUS_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_);
  ASSERT_EQ(0, metrics.num_client_connections_);
}

// Test that a call which takes longer than the keepalive time
// succeeds -- i.e that we don't consider a connection to be "idle" on the
// server if there is a call outstanding on it.
TEST_F(TestRpc, TestCallLongerThanKeepalive) {
  // set very short keepalive
  keepalive_time_ms_ = 1;

  // Set up server.
  Sockaddr server_addr;
  StartTestServer(&server_addr);

  // Set up client.
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  Proxy p(client_messenger, server_addr);

  // Make a call which sleeps longer than the keepalive.
  RpcController controller;
  SleepRequestPB req;
  req.set_sleep_micros(10000);
  req.set_deferred(true);
  SleepResponsePB resp;
  ASSERT_STATUS_OK(p.SyncRequest(GenericCalculatorService::kSleepMethodName,
                                 req, &resp, &controller));
}

// Test that timeouts are properly handled.
TEST_F(TestRpc, TestCallTimeout) {
  Sockaddr server_addr;
  StartTestServer(&server_addr);
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  Proxy p(client_messenger, server_addr);

  // Test a very short timeout - we expect this will time out while the
  // call is still in the send queue. This was triggering ASAN failures
  // before.
  ASSERT_NO_FATAL_FAILURE(DoTestExpectTimeout(p, MonoDelta::FromNanoseconds(1)));

  // Test a longer timeout - expect this will time out after we send the request.
  ASSERT_NO_FATAL_FAILURE(DoTestExpectTimeout(p, MonoDelta::FromMilliseconds(10)));
}

// Test that client calls get failed properly when the server they're connected to
// shuts down.
TEST_F(TestRpc, TestServerShutsDown) {
  // Set up a simple socket server which accepts a connection.
  Sockaddr server_addr;
  Socket listen_sock;
  ASSERT_STATUS_OK(StartFakeServer(&listen_sock, &server_addr));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  Proxy p(client_messenger, server_addr);

  // Send a call.
  AddRequestPB req;
  req.set_x(rand());
  req.set_y(rand());
  AddResponsePB resp;

  boost::ptr_vector<RpcController> controllers;

  // We'll send several calls async, and ensure that they all
  // get the error status when the connection drops.
  int n_calls = 5;

  CountDownLatch latch(n_calls);
  for (int i = 0; i < n_calls; i++) {
    RpcController *controller = new RpcController();
    controllers.push_back(controller);
    p.AsyncRequest("Add", req, &resp, controller,
                   boost::bind(&CountDownLatch::CountDown, boost::ref(latch)));
  }

  // Accept the TCP connection.
  Socket server_sock;
  Sockaddr remote;
  ASSERT_STATUS_OK(listen_sock.Accept(&server_sock, &remote, 0));

  // The call is still in progress at this point.
  BOOST_FOREACH(const RpcController &controller, controllers) {
    ASSERT_FALSE(controller.finished());
  }

  // Shut down the socket.
  ASSERT_STATUS_OK(listen_sock.Close());
  ASSERT_STATUS_OK(server_sock.Close());

  // Wait for the call to be marked finished.
  latch.Wait();

  // Should get the appropriate error on the client for all calls;
  BOOST_FOREACH(const RpcController &controller, controllers) {
    ASSERT_TRUE(controller.finished());
    Status s = controller.status();
    ASSERT_TRUE(s.IsNetworkError()) <<
      "Unexpected status: " << s.ToString();

    // Any of these errors could happen, depending on whether we were
    // in the middle of sending a call while the connection died, or
    // if we were already waiting for responses.
    //
    // ECONNREFUSED is possible because the sending of the calls is async.
    // For example, the following interleaving:
    // - Enqueue 3 calls
    // - Reactor wakes up, creates connection, starts writing calls
    // - Enqueue 2 more calls
    // - Shut down socket
    // - Reactor wakes up, tries to write more of the first 3 calls, gets error
    // - Reactor shuts down connection
    // - Reactor sees the 2 remaining calls, makes a new connection
    // - Because the socket is shut down, gets ECONNREFUSED.
    ASSERT_TRUE(s.posix_code() == EPIPE ||
                s.posix_code() == ECONNRESET ||
                s.posix_code() == ESHUTDOWN ||
                s.posix_code() == ECONNREFUSED);
  }
}

} // namespace rpc
} // namespace kudu

