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
#ifndef KUDU_UTIL_TRACE_METRICS_H
#define KUDU_UTIL_TRACE_METRICS_H

#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"

#include <map>
#include <string>

namespace kudu {

// A simple map of constant string names to integer counters.
//
// Typically, the TRACE_COUNTER_INCREMENT(...) macro defined in
// trace.h is used to increment a counter within this map.
//
// This currently is just a thin wrapper around a spinlocked map,
// but if it becomes noticeable in the CPU profile, various optimizations
// are plausible.
class TraceMetrics {
 public:
  TraceMetrics() {}
  ~TraceMetrics() {}

  // Internalize the given string by duplicating it into a process-wide
  // pool. If this string has already been interned, returns a pointer
  // to a previous instance. Otherwise, copies it into the pool.
  //
  // The resulting strings are purposefully leaked, so this should only
  // be used in cases where the number of unique strings that will be
  // passed is relatively low.
  static const char* InternName(const std::string& name);

  // Increment the given counter.
  void Increment(const char* name, int64_t amount);

  // Return a copy of the current counter map.
  std::map<const char*, int64_t> Get() const;

  struct CounterPair {
    Atomic64 name = 0;
    Atomic64 amount = 0;
  };
  std::array<CounterPair, 4> buffer_;
  Atomic32 last_match_pos_ = 0;

 private:
  void FlushUnlocked() const;

  mutable simple_spinlock lock_;
  std::map<const char*, int64_t> counters_;

  DISALLOW_COPY_AND_ASSIGN(TraceMetrics);
};

inline void TraceMetrics::Increment(const char* name, int64_t amount) {
  int start_pos = base::subtle::NoBarrier_Load(&last_match_pos_);
  for (int i = 0; i < buffer_.size(); i++) {
    int idx = (start_pos + i) % buffer_.size();
    auto& p = buffer_[idx];
    auto cur = base::subtle::NoBarrier_Load(&p.name);
    if (PREDICT_TRUE(cur == reinterpret_cast<uintptr_t>(name))) {
      p.amount += amount;
      base::subtle::NoBarrier_Store(&last_match_pos_, idx);
      return;
    }
    if (cur == 0 && PREDICT_TRUE(base::subtle::NoBarrier_CompareAndSwap(&p.name, 0, (uintptr_t)name) == 0)) {

      p.amount += amount;
      base::subtle::NoBarrier_Store(&last_match_pos_, idx);
      return;
    }
  }

  lock_guard<simple_spinlock> l(&lock_);
  counters_[name] += amount;
}

inline std::map<const char*, int64_t> TraceMetrics::Get() const {
  std::map<const char*, int64_t> ret;
  {
    lock_guard<simple_spinlock> l(&lock_);
    ret = counters_;
  }
  for (const auto& p : buffer_) {
    if (p.name) {
      ret[(const char*)p.name] += base::subtle::NoBarrier_Load(&p.amount);
    }
  }
  return ret;
}


} // namespace kudu
#endif /* KUDU_UTIL_TRACE_METRICS_H */
