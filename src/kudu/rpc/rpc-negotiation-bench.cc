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

#include <atomic>
#include <memory>
#include <ostream>
#include <string>
#include <thread>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/rpc/client_negotiation.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/security/crypto.h"
#include "kudu/security/init.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/tls_socket.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/test_util.h"

DEFINE_int32(client_threads, 1,
             "Number of client threads. Each thread has a single outstanding "
             "synchronous negotiation at a time.");

DEFINE_int32(run_seconds, 1, "Seconds to run the test");

DECLARE_bool(rpc_encrypt_loopback_connections);
DECLARE_string(keytab_file);
DECLARE_string(principal);

using std::atomic;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

using kudu::security::Cert;
using kudu::security::PkiConfig;
using kudu::security::PrivateKey;
using kudu::security::SignedTokenPB;
using kudu::security::TlsContext;
using kudu::security::TokenSigner;
using kudu::security::TokenSigningPrivateKey;

namespace kudu {
namespace rpc {

enum class Tls {
  NONE,
  ENABLED,
};

enum class Authentication {
  PLAIN,
  KERBEROS,
  CERTIFICATE,
  TOKEN,
};

struct NegotiationConfig {
  Tls tls;
  Authentication authentication;
};

std::ostream& operator<<(std::ostream& o, NegotiationConfig conf) {
  o << "{ tls: ";
  switch (conf.tls) {
    case Tls::NONE: o << "NONE"; break;
    case Tls::ENABLED: o << "ENABLED"; break;
  }
  o << ", authentication: ";
  switch (conf.authentication) {
    case Authentication::PLAIN: o << "PLAIN"; break;
    case Authentication::KERBEROS: o << "KERBEROS"; break;
    case Authentication::CERTIFICATE: o << "CERTIFICATE"; break;
    case Authentication::TOKEN: o << "TOKEN"; break;
  }
  o << " }";
  return o;
}

class RpcNegotiationBench : public RpcTestBase,
                            public ::testing::WithParamInterface<NegotiationConfig> {
 public:
  RpcNegotiationBench()
      : should_run_(true) {
  }

  // Setup the KDC once. InitKerberosForServer uses global state, so it's easier
  // not to create a new KDC for every test case.
  void SetUp() override {
    OverrideFlagForSlowTests("run_seconds", "10");
    FLAGS_principal = "kudu/127.0.0.1";

    // TODO(dan): this initializes global state that will break if more than one
    // test cases uses Kerberos authentication.
    if (GetParam().authentication == Authentication::KERBEROS) {
      string kt_path;
      CHECK_OK(kdc_.Start());
      CHECK_OK(kdc_.CreateServiceKeytab(FLAGS_principal, &kt_path));
      FLAGS_keytab_file = kt_path;

      // Create and kinit as a client user.
      ASSERT_OK(kdc_.CreateUserPrincipal("testuser"));
      ASSERT_OK(kdc_.Kinit("testuser"));

      ASSERT_OK(kdc_.SetKrb5Environment());
      CHECK_OK(security::InitKerberosForServer());
    }
    StartTestServerWithGeneratedCode(&server_addr_);
  }

  void SummarizePerf(CpuTimes elapsed, int negotiation_count) {
    float negos_per_seconds = static_cast<float>(negotiation_count / elapsed.wall_seconds());
    float user_cpu_micros_per_nego = static_cast<float>(elapsed.user / 1000.0 / negotiation_count);
    float sys_cpu_micros_per_nego = static_cast<float>(elapsed.system / 1000.0 / negotiation_count);
    float csw_per_nego = static_cast<float>(elapsed.context_switches) / negotiation_count;

    LOG(INFO) << "Config: " << GetParam() << ", client threads: " << FLAGS_client_threads;
    LOG(INFO) << "----------------------------------";
    LOG(INFO) << "Negotiations/sec:  " << negos_per_seconds;
    LOG(INFO) << "User CPU per nego: " << user_cpu_micros_per_nego << "us";
    LOG(INFO) << "Sys CPU per nego:  " << sys_cpu_micros_per_nego << "us";
    LOG(INFO) << "Ctx Sw. per nego:  " << csw_per_nego;
  }

 protected:
  friend class ClientThread;
  friend class ClientAsyncWorkload;

  Sockaddr server_addr_;
  Atomic32 should_run_;
  MiniKdc kdc_;
};

TEST_P(RpcNegotiationBench, BenchmarkNegotiation) {
  NegotiationConfig config = GetParam();

  // Generate a trusted root certificate.
  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  auto* server_tls_context = server_messenger_->mutable_tls_context();
  TlsContext client_tls_context;
  ASSERT_OK(client_tls_context.Init());

  if (config.tls == Tls::ENABLED) {
    ASSERT_OK(ConfigureTlsContext(PkiConfig::SIGNED, ca_cert, ca_key, server_tls_context));
  }

  boost::optional<SignedTokenPB> authn_token;

  // Configure authentication, if necessary.
  switch (config.authentication) {
    case Authentication::PLAIN: break;
    case Authentication::KERBEROS: break;
    case Authentication::CERTIFICATE:
      if (config.tls == Tls::ENABLED) {
        ASSERT_OK(ConfigureTlsContext(PkiConfig::SIGNED, ca_cert, ca_key, &client_tls_context));
      }
      break;
    case Authentication::TOKEN: {
      if (config.tls == Tls::ENABLED) {
        ASSERT_OK(ConfigureTlsContext(PkiConfig::TRUSTED, ca_cert, ca_key, &client_tls_context));
      }
      // Generate an optional client token and server token verifier.
      int validity = 30 * 24 * 60 * 60;
      TokenSigner token_signer(validity, validity / 2,
                              server_messenger_->shared_token_verifier());
      {
        unique_ptr<TokenSigningPrivateKey> key;
        ASSERT_OK(token_signer.CheckNeedKey(&key));
        // No keys are available yet, so should be able to add.
        ASSERT_NE(nullptr, key.get());
        ASSERT_OK(token_signer.AddKey(std::move(key)));
      }
      authn_token = SignedTokenPB();
      security::TokenPB token;
      token.set_expire_unix_epoch_seconds(WallTime_Now() + validity / 2);
      token.mutable_authn()->set_username("client-token");
      ASSERT_TRUE(token.SerializeToString(authn_token->mutable_token_data()));
      ASSERT_OK(token_signer.SignToken(&authn_token.get()));
      break;
    }
  }

  atomic<int> negotiation_count(0);

  vector<thread> threads;
  for (int i = 0; i < FLAGS_client_threads; i++) {
    threads.emplace_back([&] () {
        int local_count = 0;
        while (Acquire_Load(&should_run_)) {
          unique_ptr<Socket> socket(new Socket());
          CHECK_OK(socket->Init(0));
          CHECK_OK(socket->SetNoDelay(true));
          socket->Connect(server_addr_);
          ClientNegotiation negotiation(std::move(socket), &client_tls_context, authn_token);

          switch (config.authentication) {
            case Authentication::PLAIN:
              negotiation.EnablePlain("client", "");
              break;
            case Authentication::KERBEROS:
              negotiation.set_server_fqdn("127.0.0.1");
              negotiation.EnableGSSAPI();
              break;
            case Authentication::CERTIFICATE: break;
            case Authentication::TOKEN: break;
          }
          CHECK_OK(negotiation.Negotiate());
          local_count++;
        }
        negotiation_count += local_count;
    });
  }

  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  SleepFor(MonoDelta::FromSeconds(FLAGS_run_seconds));
  Release_Store(&should_run_, false);

  for (auto& thread : threads) {
    thread.join();
  }
  sw.stop();

  SummarizePerf(sw.elapsed(), negotiation_count);
}

INSTANTIATE_TEST_CASE_P(Negotiations,
                        RpcNegotiationBench,
                        ::testing::Values(
        NegotiationConfig { Tls::NONE, Authentication::KERBEROS },
        NegotiationConfig { Tls::NONE, Authentication::PLAIN },
        NegotiationConfig { Tls::ENABLED, Authentication::PLAIN },
        NegotiationConfig { Tls::ENABLED, Authentication::CERTIFICATE },
        NegotiationConfig { Tls::ENABLED, Authentication::TOKEN }
));

} // namespace rpc
} // namespace kudu
