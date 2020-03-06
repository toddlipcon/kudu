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
#pragma once

#include "kudu/gutil/macros.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/linux_syscall_support.h"

namespace kudu {

// TODO: consolidate with CompletionFlag
class Notification {
 public:
  Notification() : state_(NOT_NOTIFIED_NO_WAITERS) {}
  ~Notification() = default;

  bool HasBeenNotified() const {
    return base::subtle::Acquire_Load(&state_) == NOTIFIED;
  }

  void WaitForNotification() const {
    while (true) {
      auto s = base::subtle::Acquire_Load(&state_);
      if (s == NOT_NOTIFIED_NO_WAITERS) {
        s = base::subtle::Acquire_CompareAndSwap(
            &state_, NOT_NOTIFIED_NO_WAITERS, NOT_NOTIFIED_HAS_WAITERS);
        s = NOT_NOTIFIED_HAS_WAITERS;
      }
      if (s == NOTIFIED) return;
      DCHECK_EQ(s, NOT_NOTIFIED_HAS_WAITERS);
      sys_futex(&state_, FUTEX_WAIT | FUTEX_PRIVATE_FLAG, NOT_NOTIFIED_HAS_WAITERS, /* timeout */ nullptr);
    }
  }

  void Notify() {
    auto s = base::subtle::Release_AtomicExchange(&state_, NOTIFIED);
    DCHECK_NE(s, NOTIFIED) << "may only notify once";
    if (s == NOT_NOTIFIED_HAS_WAITERS) {
      sys_futex(&state_, FUTEX_WAKE | FUTEX_PRIVATE_FLAG, INT_MAX,
                0 /* ignored */);
    }
  }

 private:
    enum {
      NOT_NOTIFIED_NO_WAITERS = 111,
      NOT_NOTIFIED_HAS_WAITERS = 222,
      NOTIFIED = 333
    };
  mutable Atomic32 state_;

  DISALLOW_COPY_AND_ASSIGN(Notification);
};
} // namespace kudu
