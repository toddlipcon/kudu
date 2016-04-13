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

#include "kudu/rpc/service_if.h"

#include <string>

#include "kudu/gutil/strings/substitute.h"

#include "kudu/rpc/connection.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_header.pb.h"

using google::protobuf::Message;
using std::string;
using strings::Substitute;

namespace kudu {
namespace rpc {

ServiceIf::~ServiceIf() {
}

void ServiceIf::Shutdown() {
}

bool ServiceIf::SupportsFeature(uint32_t feature) const {
  return false;
}

bool ServiceIf::ParseParam(InboundCall *call, google::protobuf::Message *message) {
  Slice param(call->serialized_request());
  if (PREDICT_FALSE(!message->ParseFromArray(param.data(), param.size()))) {
    string err = Substitute("Invalid parameter for call $0: $1",
                            call->remote_method().ToString(),
                            message->InitializationErrorString().c_str());
    LOG(WARNING) << err;
    call->RespondFailure(ErrorStatusPB::ERROR_INVALID_REQUEST,
                         Status::InvalidArgument(err));
    return false;
  }
  return true;
}

void ServiceIf::RespondBadMethod(InboundCall *call) {
  Sockaddr local_addr, remote_addr;

  CHECK_OK(call->connection()->socket()->GetSocketAddress(&local_addr));
  CHECK_OK(call->connection()->socket()->GetPeerAddress(&remote_addr));
  string err = Substitute("Call on service $0 received at $1 from $2 with an "
                          "invalid method name: $3",
                          call->remote_method().service_name(),
                          local_addr.ToString(),
                          remote_addr.ToString(),
                          call->remote_method().method_name());
  LOG(WARNING) << err;
  call->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_METHOD,
                       Status::InvalidArgument(err));
}

GeneratedServiceIf::~GeneratedServiceIf() {
}


void GeneratedServiceIf::Handle(InboundCall *call) {
  const auto& method_name = call->remote_method().method_name();
  const auto& it = methods_by_name_.find(method_name);
  if (PREDICT_FALSE(it == methods_by_name_.end())) {
    RespondBadMethod(call);
    return;
  }
  const auto& method_info = it->second;
  Message* req = method_info->req_prototype->New();
  Message* resp = method_info->resp_prototype->New();
  if (PREDICT_FALSE(!ParseParam(call, req))) {
    return;
  }
  RpcContext* ctx = new RpcContext(call, req, resp, method_info->metrics);
  method_info->func(req, resp, ctx);
}

} // namespace rpc
} // namespace kudu
