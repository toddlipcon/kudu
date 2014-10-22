// Copyright (c) 2014, Cloudera, inc.

#include <glog/logging.h>

#include "kudu/gutil/linux_syscall_support.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/util/io_priority.h"
#include "kudu/util/logging.h"

using std::string;

// glibc doesn't provide wrappers for ioprio syscalls, so we have to
// define a number of things here based on <kernel>/include/linux/ioprio.h

/*
 * Gives us 8 prio classes with 13-bits of data for each class
 */
#define IOPRIO_BITS             (16)
#define IOPRIO_CLASS_SHIFT      (13)
#define IOPRIO_PRIO_MASK        ((1UL << IOPRIO_CLASS_SHIFT) - 1)

#define IOPRIO_PRIO_CLASS(mask) ((mask) >> IOPRIO_CLASS_SHIFT)
#define IOPRIO_PRIO_DATA(mask)  ((mask) & IOPRIO_PRIO_MASK)
#define IOPRIO_PRIO_VALUE(class, data)  (((class) << IOPRIO_CLASS_SHIFT) | data)

enum {
        IOPRIO_CLASS_NONE,
        IOPRIO_CLASS_RT,
        IOPRIO_CLASS_BE,
        IOPRIO_CLASS_IDLE,
};

enum {
        IOPRIO_WHO_PROCESS = 1,
        IOPRIO_WHO_PGRP,
        IOPRIO_WHO_USER,
};

#define IOPRIO_NORM     (4)

namespace kudu {

IOPriority IOPriority::FromString(const string& prio_str) {
  if (prio_str == "none") {
    return IOPriority();
  }

  if (prio_str == "idle") {
    return IOPriority(CLASS_IDLE, 0);
  }

  string prio;
  if (TryStripPrefixString(prio_str, "be/", &prio)) {
    int32_t prio_int;
    if (safe_strto32(prio, &prio_int) && (prio_int >= 0 && prio_int < 8)) {
      return IOPriority(CLASS_BEST_EFFORT, prio_int);
    }
  }

  KLOG_FIRST_N(WARNING, 1) << "Invalid priority '" << prio_str << "': expected "
                           << "'none', 'idle' or 'be/N' with N between 0 and 7";
  return IOPriority();
}

void IOPriority::GetForCurrentThread(IOPriority* prio) {
  int result = sys_ioprio_get(IOPRIO_WHO_PROCESS, 0);
  if (result == -1) {
    PLOG(WARNING) << "ioprio_get failed";
    // Default ioprio is be/4, so we'll assume that.
    prio->priority_class = CLASS_BEST_EFFORT;
    prio->priority = 4;
    return;
  }

  LOG(INFO) << "res: " << result;

  switch (IOPRIO_PRIO_CLASS(result)) {
    case IOPRIO_CLASS_NONE:
      prio->priority_class = CLASS_NONE;
      break;
    case IOPRIO_CLASS_RT:
      prio->priority_class = CLASS_REAL_TIME;
      break;
    case IOPRIO_CLASS_BE:
      prio->priority_class = CLASS_BEST_EFFORT;
      break;
    case IOPRIO_CLASS_IDLE:
      prio->priority_class = CLASS_IDLE;
      break;
    default:
      prio->priority_class = CLASS_NONE;
      LOG(WARNING) << "Unknown priority class: " << IOPRIO_PRIO_CLASS(result);
  }
  prio->priority = IOPRIO_PRIO_DATA(result);
}

void IOPriority::SetForCurrentThread(const IOPriority& prio) {
  int linux_class;
  int priority = prio.priority;
  switch (prio.priority_class) {
    case CLASS_NONE:
      linux_class = IOPRIO_CLASS_NONE;
      // Workaround the fact that ioprio_get() on an unprioritized process
      // returns priority CLASS_NONE/IOPRIO_NORM, but then if you try to set
      // that same value back, you get EINVAL
      priority = 0;
      break;
    case CLASS_REAL_TIME:
      linux_class = IOPRIO_CLASS_RT;
      break;
    case CLASS_BEST_EFFORT:
      linux_class = IOPRIO_CLASS_BE;
      break;
    case CLASS_IDLE:
      linux_class = IOPRIO_CLASS_IDLE;
      break;
    default:
      LOG(DFATAL) << "bad priority class enum value: " << prio.priority_class;
      return;
  }

  int rc = sys_ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(linux_class, priority));
  if (rc != 0) {
    PLOG(WARNING) << "ioprio_set(" << linux_class << ", " << priority << ") failed";
  }
}


} // namespace kudu
