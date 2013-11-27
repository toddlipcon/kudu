// Copyright (c) 2013, Cloudera, inc.

#include "util/addr2line.h"

#include <gflags/gflags.h>
#include <dirent.h>
#include <errno.h>
#include <glog/logging.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "gutil/strings/numbers.h"
#include "gutil/strings/util.h"
#include "util/errno.h"

DEFINE_string(addr2line_path, "addr2line",
              "Name or path of the addr2line executable");

namespace kudu {

Addr2Line::Addr2Line()
  : child_pid_(0),
    to_child_stdin_(NULL),
    from_child_stdout_(NULL) {
}

Addr2Line::~Addr2Line() {
  if (to_child_stdin_) fclose(to_child_stdin_);
  if (from_child_stdout_) fclose(from_child_stdout_);
}

struct ChildArgs {
  int stdin_fd;
  int stdout_fd;
  int parent_pid;
  bool started;
};

// Close all open file descriptors other than stdin, stderr, stdout.
static void CloseNonStandardFDs() {
  // This is implemented by iterating over the open file descriptors
  // rather than using sysconf(SC_OPEN_MAX) -- the latter is error prone
  // since it may not represent the highest open fd if the fd soft limit
  // has changed since the process started. This should also be faster
  // since iterating over all possible fds is likely to cause 64k+ syscalls
  // in typical configurations.
  //
  // Note also that this doesn't use any of the Env utility functions, to
  // make it as lean and mean as possible -- this runs in the subprocess
  // while sharing the address space with the main process, and we don't
  // want to chance any weird interaction.
  errno = 0;
  DIR* d = opendir("/proc/self/fd");
  int dir_fd = dirfd(d);
  PCHECK(d != NULL);

  struct dirent64 *ent;
  while ((ent = readdir64(d)) != NULL) {
    uint32_t fd;
    if (!safe_strtou32(ent->d_name, &fd)) continue;
    if (fd >= 3 && fd != dir_fd) {
      close(fd);
    }
  }

  PCHECK(closedir(d) == 0);
}

ATTRIBUTE_NO_ADDRESS_SAFETY_ANALYSIS
static int ChildProcess(void* arg_void) {
  ChildArgs* args = reinterpret_cast<ChildArgs*>(arg_void);

  PCHECK(dup2(args->stdin_fd, STDIN_FILENO) == STDIN_FILENO);
  PCHECK(dup2(args->stdout_fd, STDOUT_FILENO) == STDOUT_FILENO);

  CloseNonStandardFDs();

  // Pass our own executable to addr2line
  char exe_path[1024];
  snprintf(exe_path, sizeof(exe_path), "/proc/%d/exe", args->parent_pid);

  args->started = true;
  execlp(FLAGS_addr2line_path.c_str(), "addr2line", "--functions", "--demangle",
         "--basenames", "--inlines", "-e", exe_path, NULL);
  // If we got here, there was an error starting addr2line.
  args->started = false;
  _exit(errno);
}

static void DisableSigPipe() {
  struct sigaction act;
 
  act.sa_handler=SIG_IGN;
  sigemptyset(&act.sa_mask);
  act.sa_flags=0;
  sigaction(SIGPIPE, &act, NULL); 
}

Status Addr2Line::Init() {
  CHECK_EQ(child_pid_, 0);

  DisableSigPipe();

  child_args_.reset(new ChildArgs);
  int fds[2];

  // Pipe from caller process to child's stdin
  PCHECK(pipe(fds) == 0);
  child_args_->stdin_fd = fds[0];
  PCHECK(to_child_stdin_ = fdopen(fds[1], "w"));

  // Pipe from child's stdout back to caller process
  PCHECK(pipe(fds) == 0);
  PCHECK(from_child_stdout_ = fdopen(fds[0], "r"));
  child_args_->stdout_fd = fds[1];

  child_args_->parent_pid = getpid();

  // Create a stack for our forked child.
  child_stack_.reset(new char[kChildStackSize]);

  int child = clone(ChildProcess, &child_stack_[kChildStackSize], CLONE_IO, child_args_.get());
  int clone_errno = errno;

  close(child_args_->stdin_fd);
  close(child_args_->stdout_fd);

  if (child == -1) {
    return Status::RuntimeError("Unable to clone addr2line child",
                                ErrnoToString(clone_errno));
  }
  child_pid_ = child;

  // Now make sure the addr2line process actually started by sending
  // a dummy address
  string junk1, junk2;
  int line;
  Status s = Lookup(0x0, &junk1, &junk2, &line);
  if (!s.ok()) {
    if (!child_args_->started) {
      int status;
      waitpid(child_pid_, &status, 0);
      int err = WEXITSTATUS(status);
      return Status::RuntimeError("Unable to exec addr2line",
                                  ErrnoToString(err));
    } else {
      // TODO: does this zombie the process?
      return Status::RuntimeError("addr2line failed to resolve an address");
    }
  }

  return s;
}


Status Addr2Line::Lookup(const void* addr, std::string* function, std::string* file,
                         int32_t* line) {
  if (!child_pid_) {
    return Status::IllegalState("Not initialized");
  }

  int written = fprintf(to_child_stdin_, "%p\n", addr);
  if (written <= 0) {
    return Status::IOError("Unable to write to addr2line process");
  }
  if (fflush(to_child_stdin_) != 0) {
    return Status::IOError("Unable to flush to addr2line",
                           ErrnoToString(errno), errno);
  }

  if (!GetlineFromStdioFile(from_child_stdout_, function, '\n')) {
    return Status::IOError("Expected newline-delimited response");
  }
  string file_line;
  if (!GetlineFromStdioFile(from_child_stdout_, &file_line, '\n')) {
    return Status::IOError("Expected newline-delimited response");
  }
  int colon_pos = ReverseFindNth(file_line, ':', 1);
  if (colon_pos == string::npos) {
    *file = file_line;
    *line = -1;
  } else {
    *file = file_line.substr(0, colon_pos);
    if (!safe_strto32(file_line.c_str() + colon_pos + 1, line)) {
      *line = -1;
    }
  }

  return Status::OK();
}

} // namespace kudu
