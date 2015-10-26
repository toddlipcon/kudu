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

#include "kudu/rpc/service_queue.h"
#include "kudu/util/logging.h"

namespace kudu {
namespace rpc {

__thread int LifoServiceQueue::consumer_idx_ = 0;

LifoServiceQueue::LifoServiceQueue(int max_size)
   : shutdown_(false),
     max_queue_size_(max_size),
     num_consumer_(0),
     max_consumer_(100), // TODO
     consumers_(max_consumer_ + 1) {
  waiting_consumers_.reserve(max_consumer_);
}

LifoServiceQueue::~LifoServiceQueue() {
  DCHECK(queue_.empty())
      << "ServiceQueue holds bare pointers at destruction time";
}

bool LifoServiceQueue::BlockingGet(std::unique_ptr<InboundCall> *out) {
  if (PREDICT_FALSE(consumer_idx_ == 0)) {
    lock_guard<simple_spinlock> l(&lock_);
    if (num_consumer_ >= max_consumer_) {
      LOG(WARNING) << "consumer thread exceed consumer limit: " << max_consumer_;
      return false;
    }
    num_consumer_++;
    consumer_idx_ = num_consumer_;
    consumers_[consumer_idx_].reset(new ConsumerState());
  }
  consumers_[consumer_idx_]->Reset();
  while (true) {
    {
      lock_guard<simple_spinlock> l(&lock_);
      if (!queue_.empty()) {
        auto it = queue_.begin();
        out->reset(*it);
        queue_.erase(it);
        return true;
      }
      if (shutdown_) {
        return false;
      }
      waiting_consumers_.push_back(consumer_idx_);
    }
    InboundCall* call = consumers_[consumer_idx_]->Wait();
    if (call != nullptr) {
      out->reset(call);
      return true;
    }
  }
}

QueueStatus LifoServiceQueue::Put(InboundCall* call,
                                  boost::optional<InboundCall*>* evicted) {
  lock_.lock();
  if (PREDICT_FALSE(shutdown_)) {
    lock_.unlock();
    return QUEUE_SHUTDOWN;
  }

  DCHECK(!(waiting_consumers_.size() > 0 && queue_.size() > 0));

  // fast path
  if (queue_.empty() && waiting_consumers_.size() > 0) {
    int idx = waiting_consumers_[waiting_consumers_.size() - 1];
    waiting_consumers_.pop_back();
    // Notify condition var(and wake up consumer thread) takes time,
    // so put it out of spinlock scope.
    lock_.unlock();
    consumers_[idx]->Post(call);
    return QUEUE_SUCCESS;
  }

  if (PREDICT_FALSE(queue_.size() >= max_queue_size_)) {
    // eviction
    DCHECK_EQ(queue_.size(), max_queue_size_);
    auto it = queue_.end();
    --it;
    if (DeadlineLess(*it, call)) {
      lock_.unlock();
      return QUEUE_FULL;
    }

    *evicted = *it;
    queue_.erase(it);
  }

  queue_.insert(call);
  lock_.unlock();
  return QUEUE_SUCCESS;
}

void LifoServiceQueue::Shutdown() {
  lock_guard<simple_spinlock> l(&lock_);
  shutdown_ = true;
  for (int idx : waiting_consumers_) {
    consumers_[idx]->Post(nullptr);
  }
  waiting_consumers_.clear();
}

bool LifoServiceQueue::empty() const {
  lock_guard<simple_spinlock> l(&lock_);
  return queue_.empty();
}

int LifoServiceQueue::max_size() const {
  return max_queue_size_;
}

std::string LifoServiceQueue::ToString() const {
  std::string ret;

  lock_guard<simple_spinlock> l(&lock_);
  for (const auto* t : queue_) {
    ret.append(t->ToString());
    ret.append("\n");
  }
  return ret;
}

} // namespace rpc
} // namespace kudu
