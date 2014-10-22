// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_UTIL_IO_PRIORITY_H
#define KUDU_UTIL_IO_PRIORITY_H

#include <string>

#include "kudu/gutil/macros.h"

namespace kudu {

struct IOPriority {
  enum PriorityClass {
    CLASS_NONE,
    CLASS_REAL_TIME,
    CLASS_BEST_EFFORT,
    CLASS_IDLE
  };

  IOPriority()
    : priority_class(CLASS_NONE),
      priority(0) {
  }

  IOPriority(PriorityClass priority_class, int priority)
    : priority_class(priority_class),
      priority(priority) {
  }

  // Parse a string priority.
  // 's' must be either 'none', 'idle', or 'be/N' where N is between 0 and 7.
  // (RT priority is currently not supported, as it requires root).
  static IOPriority FromString(const std::string& s);

  // Set the IO priority of the current thread.
  // See ioprio_set(2) for descriptions of the priority classes and priorities.
  static void GetForCurrentThread(IOPriority* prio);
  static void SetForCurrentThread(const IOPriority& prio);

  PriorityClass priority_class;
  int priority;
};

// Sets the IO priority of the current thread which the object is alive.
// In the destructor, resets to the old priority.
class ScopedIOPriority {
 public:
  ScopedIOPriority(const IOPriority& prio) {
    IOPriority::GetForCurrentThread(&old_);
    IOPriority::SetForCurrentThread(prio);
  }
  ~ScopedIOPriority() {
    IOPriority::SetForCurrentThread(old_);
  }

 private:
  IOPriority old_;

  DISALLOW_COPY_AND_ASSIGN(ScopedIOPriority);
};

} // namespace kudu
#endif /* KUDU_UTIL_IO_PRIORITY_H */
