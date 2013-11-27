// Copyright (c) 2013, Cloudera,inc.

#include "gutil/gscoped_ptr.h"
#include "gutil/strings/substitute.h"
#include "gutil/stringprintf.h"
#include "util/addr2line.h"
#include "util/debug-util.h"
#include "util/errno.h"
#include <cxxabi.h>
#include <execinfo.h>
#include <glog/logging.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ptrace.h>
#include <sys/wait.h>
#include <sys/user.h> // for user_regs_struct
#include <sys/prctl.h>

#include <boost/scope_exit.hpp>
#include <boost/foreach.hpp>
#include <libunwind.h>
#include <libunwind-ptrace.h>
#include <sys/ptrace.h>
#include <sys/wait.h>
#include <tr1/unordered_map>

#include "gutil/atomicops.h"
#include "gutil/map-util.h"
#include "gutil/once.h"
#include "gutil/thread_lister.h"

// Evil hack to grab a function from glog
namespace google {
namespace glog_internal_namespace_ {
extern void DumpStackTraceToString(std::string *s);
}}

using std::tr1::unordered_map;
using std::vector;
using std::string;

namespace kudu {

std::string GetStackTrace() {
  std::string s;
  google::glog_internal_namespace_::DumpStackTraceToString(&s);
  return s;
}

static string TryDemangle(const string &mangled) {
  int rc;
  gscoped_ptr<char, base::FreeDeleter> ret;
  ret.reset(abi::__cxa_demangle(mangled.c_str(), NULL, NULL, &rc));
  if (rc == 0) {
    return string(ret.get());
  } else {
    return mangled;
  }
}

// Parse a GCC stack frame entry with the format:
//   ./build/latest/debug-util-test(_ZN4kudu16GetStackTraceGNUEv+0x26b) [0x4c058b]
// i.e
//   object(symbol+offset) [pointer]
// into its constituent parts.
// Returns true if successful.
static bool ParseGCCFrameEntry(const char* entry,
                               string* object,
                               string* symbol,
                               string* offset,
                               string* pointer) {
  gscoped_ptr<char, base::FreeDeleter> entry_copy(strdup(entry));

  char* saveptr;
  char* tok = strtok_r(entry_copy.get(), "(", &saveptr);
  if (tok == NULL) return false;
  object->assign(tok);
  tok = strtok_r(NULL, "+", &saveptr);
  if (tok == NULL) return false;
  symbol->assign(tok);
  tok = strtok_r(NULL, ")", &saveptr);
  if (tok == NULL) return false;
  offset->assign(tok);
  tok = strtok_r(NULL, "[", &saveptr);
  if (tok == NULL) return false;
  tok = strtok_r(NULL, "]", &saveptr);
  if (tok == NULL) return false;
  pointer->assign(tok);
  return true;
}

std::string GetStackTraceGNU() {
  const int kMaxFrames = 16;
  void* frames[16];
  int num_frames = backtrace(&frames[0], kMaxFrames);

  return PrettifyBacktrace(&frames[0], num_frames);
}


std::string GnuPrettifyBacktrace(void** frames, int num_frames) {
  gscoped_ptr<char*[], base::FreeDeleter> strs(backtrace_symbols(&frames[0], num_frames));
  std::string ret;

  std::string object, symbol, offset, pointer;
  for (int i = 0; i < num_frames; i++) {
    StringAppendF(&ret, "%p ", frames[i]);

    if (!ParseGCCFrameEntry(strs[i], &object, &symbol, &offset, &pointer)) {
      ret.append(strs[i]);
    } else {
      strings::SubstituteAndAppend(
        &ret, "$0 ($1) [$2]", TryDemangle(symbol), object, pointer);
    }
    ret.push_back('\n');
  }
  return ret;
}

StackFrameResolver::StackFrameResolver()
  : addr2line_(new Addr2Line()) {
  Status s = addr2line_->Init();
  if (1 || !s.ok()) {
    addr2line_.reset();
    static bool logged = false;
    if (!logged) {
      LOG(INFO) << "Unable to use addr2line to resolve symbols: " << s.ToString();
      logged = true;
    }
  }
}

StackFrameResolver::~StackFrameResolver() {
}

void StackFrameResolver::GetInfo(const void* addr, FrameInfo* info) {
  info->address = addr;
  if (addr2line_) {
    Status s = addr2line_->Lookup(addr, &info->function, &info->file, &info->line_number);
    if (s.ok() && info->function != "??") {
      return;
    }
    VLOG(1) << "addr2line(" << addr << ") failed: " << s.ToString()
            << ". Falling back to GNU symbolization";
  }

  info->function = "??";
  info->file = "??";
  info->line_number = 0;

  gscoped_ptr<char*[], base::FreeDeleter> strs(
    backtrace_symbols(const_cast<void* const*>(&addr), 1));
  if (strs) {
    string obj, offset, addr_str;
    if (!ParseGCCFrameEntry(strs[0], &obj, &info->function, &offset, &addr_str)) {
      VLOG(1) << "Unable to parse frame entry: " << strs[0];
    }
  }
}


std::string PrettifyBacktrace(void** frames, int num_frames) {
  StackFrameResolver srf;
  string ret;

  FrameInfo fi;
  for (int i = 0; i < num_frames; i++) {
    srf.GetInfo(frames[i], &fi);
    StringAppendF(&ret, "%p ", frames[i]);
    strings::SubstituteAndAppend(&ret, "$0 at $1:$2\n",
                                 fi.function, fi.file, fi.line_number);
  }
  return ret;
}


struct SignalArg {
  Atomic32 signal_has_run;
  void* stack[16];
  int first_valid_frame;
  int num_frames;
};

static void SignalHandler(int signal, siginfo_t* info, void* ctx) {
  if (info->si_code != SI_QUEUE) {
    return;
  }
  ucontext_t *uc = reinterpret_cast<ucontext_t*>(ctx);

  SignalArg* arg = reinterpret_cast<SignalArg*>(info->si_value.sival_ptr);

  // TODO: use libunwind here instead
  arg->num_frames = backtrace(&arg->stack[0], arraysize(arg->stack));
  void* user_rip = (void *) uc->uc_mcontext.gregs[REG_RIP];
  arg->first_valid_frame = 0;
  for (int i = 0; i < arg->num_frames; i++) {
    if (arg->stack[i] == user_rip) {
      arg->first_valid_frame = i;
      break;
    }
  }

  Release_Store(&arg->signal_has_run, 1);
}

void InstallSignalHandler() {
  struct sigaction sa;
  sa.sa_sigaction = SignalHandler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_SIGINFO;

  PCHECK(sigaction(SIGUSR1, &sa, NULL) == 0);
}

Status AskForStackTraceViaSignal(pid_t tid, vector<void*>* frames) {
  static GoogleOnceType once = GOOGLE_ONCE_INIT;
  GoogleOnceInit(&once, InstallSignalHandler);

  SignalArg arg;
  arg.signal_has_run = 0;
  sigval val;
  val.sival_ptr = &arg;
  sigqueue(tid, SIGUSR1, val);
  while (Acquire_Load(&arg.signal_has_run) == 0) {
  }
  for (int i = arg.first_valid_frame; i < arg.num_frames; i++) {
    frames->push_back(arg.stack[i]);
  }
  return Status::OK();
}

Status AskForStackTrace(pid_t tid, vector<void*>* frames) {
  return AskForStackTraceViaSignal(tid, frames);
}


class ResultBuffer {
 public:
  ResultBuffer()
    : pid_(0) {
    frames_.reserve(1024);
  }

  ~ResultBuffer() {}

  bool StartTid(pid_t pid) {
    CHECK_EQ(pid_, 0);
    if (remaining() < 2) {
      return false;
    }

    frames_.push_back(pid);
    pid_ = pid;
    return true;
  }

  bool StopTid(pid_t pid) {
    CHECK_EQ(pid_, pid);
    CHECK_GT(remaining(), 0);

    frames_.push_back(0);
    pid_ = 0;
    return true;
  }

  bool PushFrame(void* addr) {
    if (remaining() <= 1) return false;
    frames_.push_back(reinterpret_cast<uintptr_t>(addr));
    return true;
  }

  void DumpTo(unordered_map<pid_t, vector<void*>* >* stacks) {
    vector<void*>* cur = NULL;
    BOOST_FOREACH(uintptr_t v, frames_) {
      if (cur != 0) {
        if (v == 0) {
          cur = NULL;
          continue;
        }
        cur->push_back(reinterpret_cast<void*>(v));
      } else {
        cur = new vector<void*>();
        InsertOrDie(stacks, static_cast<pid_t>(v), cur);
      }
    }
  }

 private:
  int remaining() const {
    return frames_.capacity() - frames_.size();
  }

  pid_t pid_;
  vector<uintptr_t> frames_;

  DISALLOW_COPY_AND_ASSIGN(ResultBuffer);
};

static bool DumpThread(pid_t tid, ResultBuffer* buf) {
  unw_addr_space_t as = unw_create_addr_space(&_UPT_accessors, 0);
  if (!as) {
    // Unable to init address space
    return false;
  }
  BOOST_SCOPE_EXIT((&as)) {
    unw_destroy_addr_space(as);
  } BOOST_SCOPE_EXIT_END;

  void* upt_info = _UPT_create(tid);
  if (!upt_info) {
    return false;
  }
  BOOST_SCOPE_EXIT((upt_info)) {
    _UPT_destroy(upt_info);
  } BOOST_SCOPE_EXIT_END;

  unw_cursor_t cursor;
  int ret = unw_init_remote(&cursor, as, upt_info);
  if (ret != 0) {
    return false;
  }

  do {
    unw_word_t ip;
    ret = unw_get_reg(&cursor, UNW_REG_IP, &ip);
    if (ret != 0) {
      return false;
    }
    if (!buf->PushFrame(reinterpret_cast<void*>(ip))) {
      return false;
    }
  } while (unw_step(&cursor) > 0);

  return true;
}

static int ThreadDumper(void *parameter,
                        int num_threads,
                        pid_t *thread_pids,
                        va_list ap) {
  ResultBuffer* buf = reinterpret_cast<ResultBuffer*>(parameter);

  for (int i = 0; i < num_threads; i++) {
    const pid_t tid = thread_pids[i];

    if (!buf->StartTid(tid)) {
      return 1;
    }

    DumpThread(tid, buf);

    buf->StopTid(tid);
  }

  ResumeAllProcessThreads(num_threads, thread_pids);
  return 0;
}


void CollectAllThreadStacks(unordered_map<pid_t, vector<void*>*>* stacks) {
  ResultBuffer buf;
  ListAllProcessThreads(&buf, ThreadDumper);

  buf.DumpTo(stacks);
}

}  // namespace kudu
