// Copyright (c) 2013, Cloudera, inc.

#include "kudu/tablet/maintenance_manager.h"

#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <errno.h>
#include <set>
#include <stdint.h>
#include <string>
#include <sys/sysinfo.h>
#include <tr1/memory>
#include <utility>

#include <gflags/gflags.h>

#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/io_priority.h"
#include "kudu/util/metrics.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"

using std::pair;
using std::tr1::shared_ptr;
using strings::Substitute;

DEFINE_int32(maintenance_manager_num_threads, 4,
       "Size of the maintenance manager thread pool.");
DEFINE_string(maintenance_manager_io_priority, "be/7",
       "IO priority for maintenance operations. Must be 'none', 'idle', or "
       "be/N with N between 0 (highest) and 7 (lowest)");
DEFINE_int32(maintenance_manager_polling_interval_ms, 250,
       "Polling interval for the maintenance manager scheduler, "
       "in milliseconds.");
DEFINE_int64(maintenance_manager_memory_limit, 0,
       "Maximum amount of memory this daemon should use.  0 for "
       "autosizing based on the total system memory.");
DEFINE_int64(maintenance_manager_max_ts_anchored_secs, 0,
       "We will try not to let entries sit in the write-ahead log for "
       "longer than this interval in milliseconds.");
DEFINE_int32(maintenance_manager_history_size, 8,
       "Number of completed operations the manager is keeping track of.");
DEFINE_bool(enable_maintenance_manager, true,
       "Enable the maintenance manager, runs compaction and tablet cleaning tasks.");

namespace kudu {

using kudu::tablet::MaintenanceManagerStatusPB;
using kudu::tablet::MaintenanceManagerStatusPB_CompletedOpPB;
using kudu::tablet::MaintenanceManagerStatusPB_MaintenanceOpPB;

MaintenanceOpStats::MaintenanceOpStats() {
  Clear();
}

void MaintenanceOpStats::Clear() {
  runnable = false;
  ram_anchored = 0;
  ts_anchored_secs = 0;
  perf_improvement = 0;
}

MaintenanceOp::MaintenanceOp(const std::string &name)
  : name_(name),
    running_(0) {
}

MaintenanceOp::~MaintenanceOp() {
  CHECK(!manager_.get()) << "You must unregister the " << name_
         << " Op before destroying it.";
}

void MaintenanceOp::Unregister() {
  CHECK(manager_.get()) << "Op " << name_ << " was never registered.";
  manager_->UnregisterOp(this);
}

const MaintenanceManager::Options MaintenanceManager::DEFAULT_OPTIONS = {
  0,
  0,
  0,
  0,
  0,
};

MaintenanceManager::MaintenanceManager(const Options& options)
  : num_threads_(options.num_threads <= 0 ?
      FLAGS_maintenance_manager_num_threads : options.num_threads),
    shutdown_(false),
    mem_target_(0),
    running_ops_(0),
    polling_interval_ms_(options.polling_interval_ms <= 0 ?
          FLAGS_maintenance_manager_polling_interval_ms :
          options.polling_interval_ms),
    memory_limit_(options.memory_limit <= 0 ?
          FLAGS_maintenance_manager_memory_limit : options.memory_limit),
    max_ts_anchored_secs_(options.max_ts_anchored_secs <= 0 ?
          FLAGS_maintenance_manager_max_ts_anchored_secs :
          options.max_ts_anchored_secs),
    completed_ops_count_(0) {
  CHECK_OK(ThreadPoolBuilder("MaintenanceMgr").set_min_threads(num_threads_)
               .set_max_threads(num_threads_).Build(&thread_pool_));
  uint32_t history_size = options.history_size == 0 ?
                          FLAGS_maintenance_manager_history_size :
                          options.history_size;
  completed_ops_.resize(history_size);
}

MaintenanceManager::~MaintenanceManager() {
  Shutdown();
}

Status MaintenanceManager::Init() {
  RETURN_NOT_OK(CalculateMemTarget(&mem_target_));
  LOG(INFO) << StringPrintf("MaintenanceManager: targeting memory size of %.6f GB",
                (static_cast<float>(mem_target_) / (1024.0 * 1024.0 * 1024.0)));
  RETURN_NOT_OK(Thread::Create("maintenance", "maintenance_scheduler",
      boost::bind(&MaintenanceManager::RunSchedulerThread, this),
      &monitor_thread_));
  return Status::OK();
}

void MaintenanceManager::Shutdown() {
  {
    boost::lock_guard<boost::mutex> guard(lock_);
    if (shutdown_) {
      return;
    }
    shutdown_ = true;
    cond_.notify_all();
  }
  if (monitor_thread_.get()) {
    CHECK_OK(ThreadJoiner(monitor_thread_.get()).Join());
    monitor_thread_.reset();
    thread_pool_->Shutdown();
  }
}

void MaintenanceManager::RegisterOp(MaintenanceOp* op) {
  boost::lock_guard<boost::mutex> guard(lock_);
  CHECK(!op->manager_.get()) << "Tried to register " << op->name()
          << ", but it was already registered.";
  pair<OpMapTy::iterator, bool> val
    (ops_.insert(OpMapTy::value_type(op, MaintenanceOpStats())));
  CHECK(val.second)
      << "Tried to register " << op->name()
      << ", but it already exists in ops_.";
  op->manager_ = shared_from_this();
  VLOG(1) << "Registered " << op->name();
}

void MaintenanceManager::UnregisterOp(MaintenanceOp* op) {
  {
    boost::unique_lock<boost::mutex> guard(lock_);
    CHECK(op->manager_.get() == this) << "Tried to unregister " << op->name()
          << ", but it is not currently registered with this maintenance manager.";
    OpMapTy::iterator iter = ops_.find(op);
    CHECK(iter != ops_.end()) << "Tried to unregister " << op->name()
        << ", but it was never registered";
    // While the op is running, wait for it to be finished.
    if (iter->first->running_ > 0) {
      VLOG(1) << "Waiting for op " << op->name() << " to finish so "
            << "we can unregister it.";
    }
    while (iter->first->running_ > 0) {
      op->cond_.wait(guard);
      iter = ops_.find(op);
      CHECK(iter != ops_.end()) << "Tried to unregister " << op->name()
          << ", but another thread unregistered it while we were "
          << "waiting for it to complete";
    }
    ops_.erase(iter);
  }
  LOG(INFO) << "Unregistered op " << op->name();
  // Remove the op's shared_ptr reference to us.  This might 'delete this'.
  op->manager_.reset();
}

void MaintenanceManager::RunSchedulerThread() {
  boost::posix_time::milliseconds
        polling_interval(polling_interval_ms_);

  boost::unique_lock<boost::mutex> guard(lock_);
  boost::system_time cur_time;
  next_schedule_time_ = boost::get_system_time() + polling_interval;
  while (true) {
    // Loop until we are shutting down or it is time to run another op.
    do {
      cond_.timed_wait(guard, next_schedule_time_);
      if (shutdown_) {
        VLOG(1) << "Shutting down maintenance manager.";
        return;
      }
      cur_time = boost::get_system_time();
    } while (cur_time < next_schedule_time_);

    // Find the best op.
    MaintenanceOp* op = FindBestOp();
    if (!op) {
      VLOG(2) << "No maintenance operations look worth doing.";
      next_schedule_time_ = cur_time + polling_interval;
      continue;
    }

    // Prepare the maintenance operation.
    op->running_++;
    running_ops_++;
    guard.unlock();
    bool ready = op->Prepare();
    guard.lock();
    if (!ready) {
      LOG(INFO) << "Prepare failed for " << op->name()
                << ".  Re-running scheduler.";
      op->running_--;
      op->cond_.notify_one();
      continue;
    }

    // Run the maintenance operation.
    Status s = thread_pool_->SubmitFunc(boost::bind(
          &MaintenanceManager::LaunchOp, this, op));
    CHECK(s.ok());
    next_schedule_time_ = cur_time + polling_interval;
  }
}

MaintenanceOp* MaintenanceManager::FindBestOp() {
  if (!FLAGS_enable_maintenance_manager) {
    VLOG(1) << "Maintenance manager is disabled. Doing nothing";
    return NULL;
  }
  size_t free_threads = num_threads_ - running_ops_;
  if (free_threads == 0) {
    VLOG(1) << "there are no free threads, so we can't run anything.";
    return NULL;
  }

  uint64_t mem_total = 0;
  uint64_t most_mem_anchored = 0;
  MaintenanceOp* most_mem_anchored_op = NULL;
  int32_t ts_anchored_secs = 0;
  MaintenanceOp* ts_anchored_secs_op = NULL;
  double best_perf_improvement = 0;
  MaintenanceOp* best_perf_improvement_op = NULL;
  BOOST_FOREACH(OpMapTy::value_type &val, ops_) {
    MaintenanceOp* op(val.first);
    MaintenanceOpStats& stats(val.second);
    // Update op stats.
    stats.Clear();
    op->UpdateStats(&stats);
    // Add anchored memory to the total.
    mem_total += stats.ram_anchored;
    if (stats.runnable) {
      if (stats.ram_anchored > most_mem_anchored) {
        most_mem_anchored_op = op;
        most_mem_anchored = stats.ram_anchored;
      }
      if (stats.ts_anchored_secs > ts_anchored_secs) {
        ts_anchored_secs_op = op;
        ts_anchored_secs = stats.ts_anchored_secs;
      }
      if ((!best_perf_improvement_op) ||
          (stats.perf_improvement > best_perf_improvement)) {
        best_perf_improvement_op = op;
        best_perf_improvement = stats.perf_improvement;
      }
    }
  }
  // Look at free memory.  If it is dangerously low, we must select something
  // that frees memory-- the op with the most anchored memory.
  if (mem_total > mem_target_) {
    if (!most_mem_anchored_op) {
      LOG(INFO) << "mem_total is at " << mem_total << ", whereas we are "
              << "targetting " << mem_target_ << ".  However, there are "
              << "no ops currently runnable which would free memory. ";
      return NULL;
    }
    VLOG(1) << "mem_total is at " << mem_total << ", whereas we are "
            << "targetting " << mem_target_ << ".  Running the op "
            << "which anchors the most memory: "
            << most_mem_anchored_op->name();
    return most_mem_anchored_op;
  }
  // At this point, we know memory pressure is not too high.
  // If our threadpool has more than one thread, we should leave a spare thread for future
  // memory emergencies that might happen in the future.
  if ((num_threads_ > 1) && (free_threads == 1)) {
    VLOG(1) << "Leaving a free thread in case memory pressure becomes "
            << "high in the future.";
    return NULL;
  }
  if (ts_anchored_secs > max_ts_anchored_secs_) {
    CHECK_NOTNULL(ts_anchored_secs_op);
    VLOG(1) << "Performing " << ts_anchored_secs_op->name() << ", "
               << "because it anchors a transaction ID which is "
               << ts_anchored_secs << "ms old, and "
               << "max_ts_anchored_secs_ = " << max_ts_anchored_secs_;
    return ts_anchored_secs_op;
  }
  if (best_perf_improvement_op) {
    if (best_perf_improvement > 0) {
      VLOG(1) << "Performing " << best_perf_improvement_op->name() << ", "
                 << "because it had the best perf_improvement score, "
                 << "at " << best_perf_improvement;
      return best_perf_improvement_op;
    }
  }
  return NULL;
}

void MaintenanceManager::LaunchOp(MaintenanceOp* op) {
  ScopedIOPriority ioprio(IOPriority::FromString(FLAGS_maintenance_manager_io_priority));

  MonoTime start_time(MonoTime::Now(MonoTime::FINE));
  op->RunningGauge()->Increment();
  LOG_TIMING(INFO, Substitute("running $0", op->name())) {
    op->Perform();
  }
  op->RunningGauge()->Decrement();
  MonoTime end_time(MonoTime::Now(MonoTime::FINE));
  MonoDelta delta(end_time.GetDeltaSince(start_time));
  boost::lock_guard<boost::mutex> guard(lock_);

  int duration = delta.ToSeconds();

  CompletedOp& completed_op = completed_ops_[completed_ops_count_ % completed_ops_.size()];
  completed_op.name = op->name();
  completed_op.duration_secs = duration;
  completed_op.start_mono_time = start_time;
  completed_ops_count_++;

  op->DurationHistogram()->Increment(duration);

  running_ops_--;
  op->running_--;
  op->cond_.notify_one();
}

Status MaintenanceManager::CalculateMemTarget(uint64_t* mem_target) {
  if (memory_limit_ > 0) {
    *mem_target = memory_limit_;
    return Status::OK();
  }
  uint64_t mem_total = 0;
  RETURN_NOT_OK(CalculateMemTotal(&mem_total));
  *mem_target = mem_total * 4;
  *mem_target /= 5;
  return Status::OK();
}

// TODO: put this into Env
Status MaintenanceManager::CalculateMemTotal(uint64_t* total) {
#ifdef __linux__
  struct sysinfo info;
  if (sysinfo(&info) < 0) {
    int ret = errno;
    return Status::IOError("sysinfo() failed",
                           ErrnoToString(ret), ret);
  }
  *total = info.totalram;
  return Status::OK();
#else
#error "please implement CalculateMemTotal for this platform"
#endif
}

void MaintenanceManager::GetMaintenanceManagerStatusDump(MaintenanceManagerStatusPB* out_pb) {
  DCHECK(out_pb != NULL);
  boost::lock_guard<boost::mutex> guard(lock_);
  MaintenanceOp* best_op = FindBestOp();
  BOOST_FOREACH(MaintenanceManager::OpMapTy::value_type& val, ops_) {
    MaintenanceManagerStatusPB_MaintenanceOpPB* op_pb = out_pb->add_registered_operations();
    MaintenanceOp* op(val.first);
    MaintenanceOpStats& stat(val.second);
    op_pb->set_name(op->name());
    op_pb->set_running(op->running());
    op_pb->set_runnable(stat.runnable);
    op_pb->set_ram_anchored_bytes(stat.ram_anchored);
    op_pb->set_ts_anchored_secs(stat.ts_anchored_secs);
    op_pb->set_perf_improvement(stat.perf_improvement);

    if (best_op == op) {
      out_pb->mutable_best_op()->CopyFrom(*op_pb);
    }
  }

  BOOST_FOREACH(const CompletedOp& completed_op, completed_ops_) {
    if (!completed_op.name.empty()) {
      MaintenanceManagerStatusPB_CompletedOpPB* completed_pb = out_pb->add_completed_operations();
      completed_pb->set_name(completed_op.name);
      completed_pb->set_duration_secs(completed_op.duration_secs);

      MonoDelta delta(MonoTime::Now(MonoTime::FINE).GetDeltaSince(completed_op.start_mono_time));
      completed_pb->set_secs_since_start(delta.ToSeconds());
    }
  }
}

} // namespace kudu
