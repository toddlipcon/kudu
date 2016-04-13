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

#include "kudu/rpc/rpcz_store.h"

#include <glog/stl_logging.h>
#include <mutex> // for unique_lock
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/walltime.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/util/atomic.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/trace.h"


DEFINE_bool(rpc_dump_all_traces, false,
            "If true, dump all RPC traces at INFO level");
TAG_FLAG(rpc_dump_all_traces, advanced);
TAG_FLAG(rpc_dump_all_traces, runtime);

using std::pair;
using std::vector;

namespace kudu {
namespace rpc {

class MethodSampler {
 public:
  MethodSampler() {
  }
  ~MethodSampler() {}

  void SampleCall(InboundCall* call);
  void GetSamplePBs(RpczMethodPB* pb);

 private:

  struct Sample {
    RequestHeader header;
    scoped_refptr<Trace> trace;
    int duration_ms;
  };
  struct SampleWithLock {
    SampleWithLock() : last_sample_time(0) {}

    AtomicInt<int64_t> last_sample_time;
    simple_spinlock sample_lock;
    Sample sample;
  };
  std::array<SampleWithLock, 4> samples_;

  DISALLOW_COPY_AND_ASSIGN(MethodSampler);
};

MethodSampler* RpczStore::SamplerForCall(InboundCall* call) {
  if (!call->method_info()) {
    return nullptr;
  }

  {
    shared_lock<rw_spinlock> lock(&samplers_lock_.get_lock());
    auto it = method_samplers_.find(call->method_info());
    if (it != method_samplers_.end()) {
      return it->second.get();
    }
  }

  // If missing, try again with wlock.
  lock_guard<percpu_rwlock> lock(&samplers_lock_);
  auto it = method_samplers_.find(call->method_info());
  if (it != method_samplers_.end()) {
    return it->second.get();
  }
  auto ms = new MethodSampler();
  method_samplers_[call->method_info()].reset(ms);
  return ms;
}

void MethodSampler::SampleCall(InboundCall* call) {
  // First determine which sample bucket to put this in.
  int duration_ms = call->timing().TotalDuration().ToMilliseconds();

  SampleWithLock* swl;
  if (duration_ms < 10) {
    swl = &samples_[0];
  } else if (duration_ms < 100) {
    swl = &samples_[1];
  } else if (duration_ms < 1000) {
    swl = &samples_[2];
  } else {
    swl = &samples_[3];
  }

  MicrosecondsInt64 now = GetMonoTimeMicros();
  int64_t us_since_trace = now - swl->last_sample_time.Load();
  if (us_since_trace > 1000000) {
    Sample new_sample = {call->header(), call->trace(), duration_ms};
    {
      std::unique_lock<simple_spinlock> lock(swl->sample_lock, std::try_to_lock);
      if (!lock.owns_lock()) {
        return;
      }
      std::swap(swl->sample, new_sample);
      swl->last_sample_time.Store(now);
    }
    LOG(INFO) << "Sampled call " << call->ToString();
  }
}

void MethodSampler::GetSamplePBs(RpczMethodPB* method_pb) {
  for (auto& swl : samples_) {
    if (swl.last_sample_time.Load() == 0) continue;

    std::unique_lock<simple_spinlock> lock(swl.sample_lock);
    auto* sample_pb = method_pb->add_samples();
    sample_pb->mutable_header()->CopyFrom(swl.sample.header);
    sample_pb->set_trace(swl.sample.trace->DumpToString(Trace::INCLUDE_TIME_DELTAS |
                                                     Trace::INCLUDE_METRICS));
    sample_pb->set_duration_ms(swl.sample.duration_ms);
  }
}

RpczStore::RpczStore() {
}

RpczStore::~RpczStore() {
}

void RpczStore::AddCall(InboundCall* call) {
  LogTrace(call);
  auto sampler = SamplerForCall(call);
  if (!sampler) return;
  sampler->SampleCall(call);
}

void RpczStore::DumpPB(const DumpRpczStoreRequestPB& req,
                       DumpRpczStoreResponsePB* resp) {
  vector<pair<RpcMethodInfo*, MethodSampler*>> samplers;
  {
    shared_lock<rw_spinlock> lock(&samplers_lock_.get_lock());
    for (const auto& p : method_samplers_) {
      samplers.emplace_back(p.first, p.second.get());
    }
  }

  for (const auto& p : samplers) {
    auto sampler = p.second;

    RpczMethodPB* method_pb = resp->add_methods();
    sampler->GetSamplePBs(method_pb);
  }
}

void RpczStore::LogTrace(InboundCall* call) {
  int duration_ms = call->timing().TotalDuration().ToMilliseconds();

  if (call->header_.has_timeout_millis() && call->header_.timeout_millis() > 0) {
    double log_threshold = call->header_.timeout_millis() * 0.75f;
    if (duration_ms > log_threshold) {
      // TODO: consider pushing this onto another thread since it may be slow.
      // The traces may also be too large to fit in a log message.
      LOG(WARNING) << call->ToString() << " took " << duration_ms << "ms (client timeout "
                   << call->header_.timeout_millis() << ").";
      std::string s = call->trace()->DumpToString();
      if (!s.empty()) {
        LOG(WARNING) << "Trace:\n" << s;
      }
      return;
    }
  }

  if (PREDICT_FALSE(FLAGS_rpc_dump_all_traces)) {
    LOG(INFO) << call->ToString() << " took " << duration_ms << "ms. Trace:";
    call->trace()->Dump(&LOG(INFO), true);
  } else if (duration_ms > 1000) {
    LOG(INFO) << call->ToString() << " took " << duration_ms << "ms. "
              << "Request Metrics: " << call->trace()->MetricsAsJSON();
  }
}



} // namespace rpc
} // namespace kudu
