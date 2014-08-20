// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_UTIL_PERF_H
#define KUDU_UTIL_PERF_H

#include "kudu/gutil/gscoped_ptr.h"

namespace kudu {

class Subprocess;

class PerfRecord {
 public:
  PerfRecord();
  ~PerfRecord();

 private:

  gscoped_ptr<Subprocess> proc_;

  DISALLOW_COPY_AND_ASSIGN(PerfRecord);
};

} // namespace kudu
#endif /* KUDU_UTIL_PERF_H */
