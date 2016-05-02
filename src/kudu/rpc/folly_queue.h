#pragma once


#include <folly/MPMCQueue.h>
#include <atomic>
#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <set>
#include <vector>

#include "kudu/rpc/service_queue.h"

namespace kudu {
namespace rpc {

class FollyServiceQueue {
 public:
  explicit FollyServiceQueue(int max_size)
      : queue_(max_size),
        shutdown_(false),
        max_queue_size_(max_size) {
  }

  ~FollyServiceQueue() {
    DCHECK(queue_.isEmpty())
        << "ServiceQueue holds bare pointers at destruction time";
  }


  // Get an element from the queue.  Returns false if we were shut down prior to
  // getting the element.
  bool BlockingGet(std::unique_ptr<InboundCall> *out) {
    if (shutdown_) {
      return QUEUE_SHUTDOWN;
    }

    InboundCall* b;
    queue_.blockingRead(b);
    if (b == nullptr) {
      return false;
    }
    out->reset(b);
    return true;
  }

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
  QueueStatus Put(InboundCall* call, boost::optional<InboundCall*>* evicted) {
    if (shutdown_) {
      return QUEUE_SHUTDOWN;
    }
    if (!queue_.writeIfNotFull(call)) {
      return QUEUE_FULL;
    }
    return QUEUE_SUCCESS;
  }

  // Shut down the queue.
  // When a blocking queue is shut down, no more elements can be added to it,
  // and Put() will return QUEUE_SHUTDOWN.
  // Existing elements will drain out of it, and then BlockingGet will start
  // returning false.
  void Shutdown() {
    shutdown_ = true;
    for (int i = 0; i < max_queue_size_; i++) {
      queue_.writeIfNotFull(nullptr);
    }
  }

  bool empty() const {
    return queue_.isEmpty();
  }

  int max_size() const {
    return max_queue_size_;
  }

  std::string ToString() const {
    return "TODO";
  }

 private:
  folly::MPMCQueue<InboundCall*> queue_;
  bool shutdown_;
  int max_queue_size_;
};

}
}
