// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_ADDR2LINE_H
#define KUDU_UTIL_ADDR2LINE_H

#include <sys/types.h>
#include <stdio.h>
#include "gutil/macros.h"

#include "util/status.h"

namespace kudu {

struct ChildArgs;

class Addr2Line {
 public:
  Addr2Line();
  ~Addr2Line();

  Status Init();

  Status Lookup(const void* addr, std::string* function,
                std::string* file, int32_t *line);

 private:
  enum {
    kChildStackSize = 8*1024
  };

  pid_t child_pid_;
  gscoped_ptr<ChildArgs> child_args_;
  gscoped_ptr<char[]> child_stack_;
  FILE* to_child_stdin_;
  FILE* from_child_stdout_;

  DISALLOW_COPY_AND_ASSIGN(Addr2Line);
};

} // namespace kudu
#endif /* KUDU_UTIL_ADDR2LINE_H */
