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

 private:
  mutable simple_spinlock lock_;
  std::map<const char*, int64_t> counters_;

  DISALLOW_COPY_AND_ASSIGN(TraceMetrics);
};

inline void TraceMetrics::Increment(const char* name, int64_t amount) {
  lock_guard<simple_spinlock> l(&lock_);
  counters_[name] += amount;
}

inline std::map<const char*, int64_t> TraceMetrics::Get() const {
  lock_guard<simple_spinlock> l(&lock_);
  return counters_;
}

} // namespace kudu
#endif /* KUDU_UTIL_TRACE_METRICS_H */
