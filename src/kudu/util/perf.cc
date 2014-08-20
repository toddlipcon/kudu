// Copyright (c) 2014, Cloudera, inc.

#include "kudu/util/perf.h"

#include <string>

#include <sys/types.h>
#include <unistd.h>
#include <signal.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/util/subprocess.h"

using std::string;

namespace kudu {

PerfRecord::PerfRecord() {
  string cmd = strings::Substitute("perf stat --pid=$0", getpid());
  LOG(INFO) << "Calling: \"" << cmd << "\"";
  proc_.reset(new Subprocess("perf", strings::Split(cmd, " ")));
  WARN_NOT_OK(proc_->Start(),
              "Unable to start perf-record");
}

PerfRecord::~PerfRecord() {
  if (proc_) {
    proc_->Kill(SIGINT);
    int rc;
    proc_->Wait(&rc);
  }
}

} // namespace kudu
