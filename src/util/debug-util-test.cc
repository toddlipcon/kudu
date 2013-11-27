// Copyright (c) 2013, Cloudera, inc.

#include "util/countdown_latch.h"
#include "util/debug-util.h"
#include "util/test_util.h"
#include "util/env.h"
#include "gutil/linux_syscall_support.h"
#include "gutil/stl_util.h"
#include <boost/thread/thread.hpp>
#include <gtest/gtest.h>
#include <sys/types.h>

using std::tr1::unordered_map;
using std::vector;

namespace kudu {
class DebugUtilTest : public KuduTest {};

TEST_F(DebugUtilTest, TestBacktrace) {
  //LOG(INFO) << GetStackTraceGNU();
  //backtrace(
}


static void OtherThread(pid_t* pid, CountDownLatch* started, CountDownLatch* finish) {
  *pid = syscall(__NR_gettid);
  started->CountDown();
  while (true) {}
  finish->Wait();
}

TEST_F(DebugUtilTest, TestRemoteBacktrace) {
  CountDownLatch started(1), finish(1);

  pid_t pid;
  boost::thread t(OtherThread, &pid, &started, &finish);
  started.Wait();
  LOG(INFO) << "pid is: " << pid;

  unordered_map<pid_t, vector<void*>*> stacks;
  ValueDeleter d(&stacks);
  CollectAllThreadStacks(&stacks);
  finish.CountDown();

  vector<void*>* frames = stacks[pid];

  LOG(INFO) << PrettifyBacktrace(&(*frames)[0], frames->size());
}

} // namespace kudu
