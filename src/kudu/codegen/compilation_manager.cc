// Copyright 2014 Cloudera inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/codegen/compilation_manager.h"

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <cstdlib>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <string>

#include "kudu/codegen/code_cache.h"
#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/jit_wrapper.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/codegen/rowblock_converter.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/util/faststring.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"

DEFINE_bool(time_codegen, false, "Whether to print time that each code "
            "generation request took.");

METRIC_DEFINE_gauge_int64(server, code_cache_hits, "Codegen Cache Hits",
                          kudu::MetricUnit::kCacheHits,
                          "Number of codegen cache hits since start");
METRIC_DEFINE_gauge_int64(server, code_cache_queries, "Codegen Cache Queries",
                          kudu::MetricUnit::kCacheQueries,
                          "Number of codegen cache queries (hits + misses) "
                          "since start");
namespace kudu {
namespace codegen {

namespace {

// A CompilationTask is a ThreadPool's Runnable which, given a
// pair of schemas and a cache to refer to, will generate code pertaining
// to the two schemas and store it in the cache when run. The code generated
// is dependent on the templated traits:
// Traits::Input is the input given for compilation (must be copyable).
// Traits::Output is the output compilation type. Note that this should be
//   implicitly convertible to JITWrapper.
// Traits::EncodeKey takes Input and faststring* as a parameters and encodes
//   the appropriate key - has the form Status(const Input&, faststring*).
// Traits::Compile is the compilation function, which has the form
//   Status(CodeGenerator*, const Input&, scoped_refptr<Output>*)
// Traits::ToString should be a function of the form string(const Input&)
//   which stringifies the input for an error message.
// Traits::kTaskName is a string which describes the task.
template<class Traits>
class CompilationTask : public Runnable {
 public:
  // Requires that the cache and generator are valid for the lifetime
  // of this object.
  CompilationTask(const typename Traits::Input& input, CodeCache* cache,
                  CodeGenerator* generator)
    : input_(input),
      cache_(cache),
      generator_(generator) {}

  // Can only be run once.
  virtual void Run() {
    // We need to fail softly because the user could have just given
    // a malformed projection schema pair, but could be long gone by
    // now so there's nowhere to return the status to.
    WARN_NOT_OK(RunWithStatus(),
                "Failed compilation for " + Traits::ToString(input_) + ": ");
  }

 private:
  Status RunWithStatus() {
    faststring key;
    RETURN_NOT_OK(Traits::EncodeKey(input_, &key));

    // Check again to make sure we didn't compile it already.
    // This can occur if we request the same schema pair while the
    // first one's compiling.
    if (cache_->Lookup(key)) return Status::OK();

    scoped_refptr<typename Traits::Output> output;
    LOG_TIMING_IF(INFO, FLAGS_time_codegen, Traits::kTaskName) {
      RETURN_NOT_OK(Traits::Compile(generator_, input_, &output));
    }

    RETURN_NOT_OK(cache_->AddEntry(output));
    return Status::OK();
  }

  typename Traits::Input input_;
  CodeCache* const cache_;
  CodeGenerator* const generator_;

  DISALLOW_COPY_AND_ASSIGN(CompilationTask);
};

struct RowProjectorCompilationTaskTraits {
  struct Input {
    Schema base_;
    Schema proj_;
  };
  typedef RowProjectorFunctions Output;
  static Status EncodeKey(const Input& in, faststring* fs) {
    return Output::EncodeKey(in.base_, in.proj_, fs);
  }
  static Status Compile(CodeGenerator* gen, const Input& in,
                        scoped_refptr<Output>* out) {
    return gen->CompileRowProjector(in.base_, in.proj_, out);
  }
  static string ToString(const Input& in) {
    return "row projector (base schema " + in.base_.ToString() + ", projection schema"
      + in.proj_.ToString() + ")";
  }
  static const char* const kTaskName;
};

const char* const RowProjectorCompilationTaskTraits::kTaskName =
   "code-generate row projector";

struct RowBlockConverterCompilationTaskTraits {
  struct Input {
    Schema src_schema_;
    Schema dst_schema_;
  };
  typedef RowBlockConverterFunction Output;
  static Status EncodeKey(const Input& in, faststring* fs) {
    return Output::EncodeKey(in.src_schema_, in.dst_schema_, fs);
  }
  static Status Compile(CodeGenerator* gen, const Input& in,
                        scoped_refptr<Output>* out) {
    return gen->CompileRowBlockConverter(in.src_schema_, in.dst_schema_, out);
  }
  static string ToString(const Input& in) {
    return "RowBlock to PB converter (source schema " + in.src_schema_.ToString()
      + ", destination schema" + in.dst_schema_.ToString() + ")";
  }
  static const char* const kTaskName;
};

const char* const RowBlockConverterCompilationTaskTraits::kTaskName =
  "code-generate RowBlock to PB converter";

} // anonymous namespace

CompilationManager::CompilationManager()
  : cache_(kDefaultCacheCapacity),
    hit_counter_(0),
    query_counter_(0) {
  CHECK_OK(ThreadPoolBuilder("compiler_manager_pool")
           .set_min_threads(0)
           .set_max_threads(1)
           .set_idle_timeout(MonoDelta::FromMilliseconds(kThreadTimeoutMs))
           .Build(&pool_));
  // We call std::atexit after the implicit default construction of
  // generator_ to ensure static LLVM constants would not have been destructed
  // when the registered function is called (since this object is a singleton,
  // atexit will only be called once).
  CHECK(std::atexit(&CompilationManager::Shutdown) == 0)
    << "Compilation manager shutdown must be registered successfully with "
    << "std::atexit to be used.";
}

CompilationManager::~CompilationManager() {}

void CompilationManager::Wait() {
  pool_->Wait();
}

void CompilationManager::Shutdown() {
  GetSingleton()->pool_->Shutdown();
}

Status CompilationManager::StartInstrumentation(const scoped_refptr<MetricEntity>& metric_entity) {
  // Even though these function as counters, we use gauges instead, because
  // this is a singleton that is shared across multiple TS instances in a
  // minicluster setup. If we were to use counters, then we could not properly
  // register the same metric in multiple registries. Using a gauge which loads
  // an atomic int is a suitable workaround: each TS's registry ends up with a
  // unique gauge which reads the value of the singleton's integer.
  Callback<int64_t(void)> hits = Bind(&AtomicInt<int64_t>::Load,
                                      Unretained(&hit_counter_),
                                      kMemOrderNoBarrier);
  Callback<int64_t(void)> queries = Bind(&AtomicInt<int64_t>::Load,
                                         Unretained(&query_counter_),
                                         kMemOrderNoBarrier);
  metric_entity->NeverRetire(
      METRIC_code_cache_hits.InstantiateFunctionGauge(metric_entity, hits));
  metric_entity->NeverRetire(
      METRIC_code_cache_queries.InstantiateFunctionGauge(metric_entity, queries));
  return Status::OK();
}

bool CompilationManager::RequestRowProjector(const Schema* base_schema,
                                             const Schema* projection,
                                             gscoped_ptr<RowProjector>* out) {
  RowProjectorCompilationTaskTraits::Input in;
  in.base_ = *base_schema;
  in.proj_ = *projection;
  scoped_refptr<RowProjectorCompilationTaskTraits::Output> cached =
    MakeRequest<RowProjectorCompilationTaskTraits>(in);
  if (cached) {
    out->reset(new RowProjector(base_schema, projection, cached));
  }
  return cached;
}

bool CompilationManager::RequestRowBlockConverter(const Schema* src_schema,
                                                  const Schema* dst_schema,
                                                  gscoped_ptr<RowBlockConverter>* out) {
  RowBlockConverterCompilationTaskTraits::Input in;
  in.src_schema_ = *src_schema;
  in.dst_schema_ = *dst_schema;
  scoped_refptr<RowBlockConverterCompilationTaskTraits::Output> cached =
    MakeRequest<RowBlockConverterCompilationTaskTraits>(in);
  if (cached) {
    out->reset(new RowBlockConverter(src_schema, dst_schema, cached));
  }
  return cached;
}

template<class Traits>
scoped_refptr<typename Traits::Output> CompilationManager::MakeRequest(
  const typename Traits::Input& in) {

  faststring key;
  Status s = Traits::EncodeKey(in, &key);
  WARN_NOT_OK(s, StrCat(Traits::kTaskName, ": request failed"));
  if (!s.ok()) return scoped_refptr<typename Traits::Output>();
  query_counter_.Increment();

  scoped_refptr<typename Traits::Output> cached(
    down_cast<typename Traits::Output*>(cache_.Lookup(key).get()));

  // If not cached, add a request to compilation pool
  if (!cached) {
    shared_ptr<Runnable> task(
      new CompilationTask<Traits>(in, &cache_, &generator_));
    WARN_NOT_OK(pool_->Submit(task),
                StrCat(Traits::kTaskName, ": request failed"));
  } else {
    hit_counter_.Increment();
  }

  return cached;
}

} // namespace codegen
} // namespace kudu
