// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_DEBUG_UTIL_H
#define KUDU_UTIL_DEBUG_UTIL_H

#include <vector>
#include <string>
#include <tr1/unordered_map>
#include "util/status.h"

namespace kudu {

class Addr2Line;

struct FrameInfo {
  const void* address;
  std::string function;
  std::string file;
  int32_t line_number;
};

class StackFrameResolver {
 public:
  StackFrameResolver();
  ~StackFrameResolver();

  void GetInfo(const void* addr, FrameInfo* info);

 private:
  gscoped_ptr<Addr2Line> addr2line_;

  DISALLOW_COPY_AND_ASSIGN(StackFrameResolver);
};

// Return the current stack trace, stringified.
std::string GetStackTrace();
std::string GetStackTraceGNU();
std::string PrettifyBacktrace(void** frames, int num_frames);

Status AskForStackTrace(pid_t tid, std::vector<void*>* frames);

void CollectAllThreadStacks(std::tr1::unordered_map<pid_t, std::vector<void*>* >* stacks);

}
#endif
