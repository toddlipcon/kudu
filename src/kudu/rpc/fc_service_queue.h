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

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <set>

#include <cds/algo/flat_combining.h>

#include "kudu/rpc/inbound_call.h"
#include "kudu/gutil/spinlock_internal.h"

namespace kudu {
namespace rpc {

class FCServiceQueue {
 public:
  // Return values for FCServiceQueue::Put()
  enum QueueStatus {
    QUEUE_SUCCESS = 0,
    QUEUE_SHUTDOWN = 1,
    QUEUE_FULL = 2
  };

  explicit FCServiceQueue(int max_size)
      : shutdown_(false),
        max_queue_size_(max_size) {
  }

  ~FCServiceQueue() {
    LOG(INFO) << "get immed count: " << get_immediate_count_;
    LOG(INFO) << "get blocked count: " << get_blocked_count_;
    DCHECK(queue_.empty())
        << "FCServiceQueue holds bare pointers at destruction time";
  }


  // Get an element from the queue.  Returns false if we were shut down prior to
  // getting the element.
  bool BlockingGet(std::unique_ptr<InboundCall> *out);

  // Add a new call to the queue.
  // Returns:
  // - QUEUE_SHUTDOWN if Shutdown() has already been called.
  // - QUEUE_FULL if the queue is full and 'call' has a later deadline than any
  //   RPC already in the queue.
  // - QUEUE_SUCCESS if 'call' was enqueued.
  //
  // In the case of a 'QUEUE_SUCCESS' response, the new element may have bumped
  // another call out of the queue. In that case, *evicted will be set to the
  // call that was bumped.
  QueueStatus Put(InboundCall* call, boost::optional<InboundCall*>* evicted);

  // Shut down the queue.
  // When a blocking queue is shut down, no more elements can be added to it,
  // and Put() will return QUEUE_SHUTDOWN.
  // Existing elements will drain out of it, and then BlockingGet will start
  // returning false.
  void Shutdown();

  bool empty() const;

  int max_size() const {
    return max_queue_size_;
  }

  std::string ToString() const {
    return "TODO";
  }

 private:
  enum FCOperationType {
    OP_PUT = cds::algo::flat_combining::req_Operation,
    OP_GET,
    OP_SHUTDOWN,
    OP_EMPTY
  };

  enum GetInternalResult {
    SHUTDOWN,
    GOT_VALUE,
    BLOCKED
  };

  enum BlockState {
    NOT_BLOCKED,
    SLEEPING,
    HAS_RESULT
  };

  struct FCRecord : public cds::algo::flat_combining::publication_record {
    // In-out (value to put, or result from get)
    InboundCall * val_;

    // Results
    InboundCall* evicted_;
    QueueStatus put_result_;
    GetInternalResult get_result_;

    Atomic32 block_state_;
    bool empty_result_;

    void Wait() {
      Atomic32 old = base::subtle::Acquire_CompareAndSwap(&block_state_, NOT_BLOCKED, SLEEPING);
      if (old == HAS_RESULT) {
        return;
      }
      DCHECK_EQ(old, NOT_BLOCKED);
      int loop = 0;
      while (base::subtle::Acquire_Load(&block_state_) != HAS_RESULT) {
        base::internal::SpinLockDelay(&block_state_, SLEEPING, loop++);
      }
    }

    void Wake() {
      if (base::subtle::Release_AtomicExchange(&block_state_, HAS_RESULT) == SLEEPING) {
        base::internal::SpinLockWake(&block_state_, false);
      }
    }
  };

  struct FCTraits : public cds::algo::flat_combining::traits {
    typedef cds::backoff::yield back_off;
  };

  friend class cds::algo::flat_combining::kernel<FCRecord, FCTraits>;
  typedef cds::algo::flat_combining::kernel<FCRecord, FCTraits> fc_kernel;
  mutable fc_kernel fc_;

  void CombineAndRelease(FCOperationType op, FCRecord* p_rec) const;

  void fc_apply(FCRecord* p_rec) {
    switch (p_rec->op()) {
      case OP_PUT:
        p_rec->put_result_ = PutInternal(p_rec);
        break;
      case OP_GET:
        p_rec->get_result_ = GetInternal(p_rec);
        break;
      case OP_SHUTDOWN:
        ShutdownInternal(p_rec);
        break;
      case OP_EMPTY:
        p_rec->empty_result_ = queue_.empty();
        break;
    }
  }

  void fc_process(typename fc_kernel::iterator it_begin, typename fc_kernel::iterator it_end) {
    // TODO
    //for (auto it = it_begin; it != it_end; ++it) {
    //}
  }

  QueueStatus PutInternal(FCRecord* p_rec) {
    p_rec->evicted_ = nullptr;

    if (shutdown_) {
      return QUEUE_SHUTDOWN;
    }

    if (queue_.empty()) {
      // If nothing is in the queue, possible there are blocked getters
      if (!blocked_getters_.empty()) {
        FCRecord* getter = blocked_getters_.back();
        blocked_getters_.pop_back();
        getter->val_ = p_rec->val_;
        getter->get_result_ = GOT_VALUE;

        getter->Wake();
        return QUEUE_SUCCESS;
      }
    }

    if (queue_.size() >= max_queue_size_) {
      DCHECK_EQ(queue_.size(), max_queue_size_);
      auto it = queue_.end();
      --it;
      if (DeadlineLess(*it, p_rec->val_)) {
        return QUEUE_FULL;
      }

      p_rec->evicted_ = *it;
      queue_.erase(it);
    }

    queue_.insert(p_rec->val_);
    return QUEUE_SUCCESS;
  }

  GetInternalResult GetInternal(FCRecord* p_rec) {
    if (shutdown_) {
      return SHUTDOWN;
    }

    if (!queue_.empty()) {
      get_immediate_count_++;

      auto it = queue_.begin();
      p_rec->val_ = *it;
      queue_.erase(it);
      return GOT_VALUE;
    }

    get_blocked_count_++;
    p_rec->block_state_ = NOT_BLOCKED;
    blocked_getters_.push_back(p_rec);
    return BLOCKED;
  }

  void ShutdownInternal(FCRecord* p_rec) {
    shutdown_ = true;
    for (FCRecord* getter : blocked_getters_) {
      getter->get_result_ = SHUTDOWN;
      getter->Wake();
    }
    blocked_getters_.clear();
  }


  // Comparison function which orders calls by their deadlines.
  static bool DeadlineLess(const InboundCall* a,
                           const InboundCall* b) {
    auto time_a = a->GetClientDeadline();
    auto time_b = b->GetClientDeadline();
    if (time_a.Equals(time_b)) {
      // If two calls have the same deadline (most likely because neither one specified
      // one) then we should order them by arrival order.
      time_a = a->GetTimeReceived();
      time_b = b->GetTimeReceived();
    }
    return time_a.ComesBefore(time_b);
  }

  // Struct functor wrapper for DeadlineLess.
  struct DeadlineLessStruct {
    bool operator()(const InboundCall* a, const InboundCall* b) const {
      return DeadlineLess(a, b);
    }
  };

  bool shutdown_;
  int max_queue_size_;
  std::multiset<InboundCall*, DeadlineLessStruct> queue_;
  std::vector<FCRecord*> blocked_getters_;

  int get_blocked_count_ = 0;
  int get_immediate_count_ = 0;
};

inline bool FCServiceQueue::BlockingGet(std::unique_ptr<InboundCall> *out) {
  FCRecord* p_rec = fc_.acquire_record();
  CombineAndRelease(OP_GET, p_rec);

  if (p_rec->get_result_ == BLOCKED) {
    p_rec->Wait();
  }

  switch (p_rec->get_result_) {
    case SHUTDOWN:
      return false;
    case GOT_VALUE:
      out->reset(p_rec->val_);
      return true;
    case BLOCKED:
      LOG(FATAL) << "should not reach here";
  }
}

inline FCServiceQueue::QueueStatus FCServiceQueue::Put(
    InboundCall* call, boost::optional<InboundCall*>* evicted) {
  FCRecord* p_rec = fc_.acquire_record();
  p_rec->val_ = call;
  CombineAndRelease(OP_PUT, p_rec);
  if (p_rec->evicted_) {
    *evicted = p_rec->evicted_;
  }
  return p_rec->put_result_;
}

inline void FCServiceQueue::Shutdown() {
  FCRecord* p_rec = fc_.acquire_record();
  CombineAndRelease(OP_SHUTDOWN, p_rec);
}

inline bool FCServiceQueue::empty() const {
  FCRecord* p_rec = fc_.acquire_record();
  CombineAndRelease(OP_EMPTY, p_rec);
  return p_rec->empty_result_;
}

inline void FCServiceQueue::CombineAndRelease(FCOperationType op, FCRecord* p_rec) const {
  fc_.combine(op, p_rec, *const_cast<FCServiceQueue*>(this));
  DCHECK(p_rec->is_done());
  fc_.release_record(p_rec);
}

} // namespace rpc
} // namespace kudu
