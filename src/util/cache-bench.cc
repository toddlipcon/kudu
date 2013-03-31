#include <boost/intrusive/list.hpp>

#include <sys/time.h>
#include <pthread.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include "cache.h"
#include "slice.h"
using kudu::Cache;
using kudu::Slice;

/* ===========================================================================
 */
#define NTHREADS                      16
#define NTESTS                        7000000
#define DEFAULT_CACHE_CAPACITY        512

#define __key(k, capacity)            (k % ((capacity) >> 0))

struct options {
  int verbose;
  size_t cache_capacity;
  Cache *cache;
};

struct random {
  unsigned int seed;
};

static void __deleter (const Slice& key, void* value) {
}

static uint64_t __time_micros (void) {
#if 0
    struct timeval now;
    gettimeofday(&now, NULL);
    return(now.tv_sec * 1000000U + now.tv_usec);
#else
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return(now.tv_sec * 1000000000U + now.tv_nsec);
#endif
}

static void __parse_opts (struct options *opts, int argc, char **argv) {
  int i;

  opts->verbose = 0;
  opts->cache_capacity = DEFAULT_CACHE_CAPACITY;
  for (i = 0; i < argc; ++i) {
    if (!strcmp(argv[i], "-c") && (i + 1) < argc) {
      opts->cache_capacity = strtoull(argv[++i], NULL, 10);
    } else if (!strcmp(argv[i], "-v")) {
      opts->verbose = 1;
    }
  }
}

static unsigned int __rand (struct random *rand) {
    unsigned int next = rand->seed;
    unsigned int result;

    next *= 1103515245;
    next += 12345;
    result = (next >> 16) & 2047;

    next *= 1103515245;
    next += 12345;
    result <<= 10;
    result ^= (next >> 16) & 1023;

    next *= 1103515245;
    next += 12345;
    result <<= 10;
    result ^= (next >> 16) & 1023;

    rand->seed = next;
    return(result);
}

static void *__test_cache_insert (void *arg) {
  struct options *opts = (struct options *)arg;
  struct random rnd;
  uint64_t st, et;
  unsigned int i;

  printf("[S] %lu __test_cache_insert\n", pthread_self());
  st = __time_micros();
  rnd.seed = 0x6a237a1f;
  for (i = 0; i < NTESTS; ++i) {
    uint64_t key = __key(__rand(&rnd), opts->cache_capacity);

    Cache::Handle *entry = opts->cache->Insert(
      Slice(reinterpret_cast<uint8_t *>(&key), 8), reinterpret_cast<void *>(key), 1, __deleter);
    opts->cache->Release(entry);
  }
  et = __time_micros();
  printf("[E] %lu __test_cache_insert %.2fsec\n", pthread_self(), (double)(et - st) / 1000000000.0f);

  return(NULL);
}

static void *__test_cache_lookup (void *arg) {
  struct options *opts = (struct options *)arg;
  struct random rnd;
  uint64_t st, et;
  unsigned int i;

  printf("[S] %lu __test_cache_lookup\n", pthread_self());
  st = __time_micros();
  rnd.seed = 0x6a237a1f;
  for (i = 0; i < NTESTS; ++i) {
    uint64_t key = __key(__rand(&rnd), opts->cache_capacity);

    Cache::Handle *entry = opts->cache->Lookup(Slice(reinterpret_cast<uint8_t *>(&key), 8));
    if (entry != NULL) {
      assert(entry != NULL);
      assert(key == reinterpret_cast<uint64_t>(opts->cache->Value(entry)));
      opts->cache->Release(entry);
    }
  }
  et = __time_micros();
  printf("[E] %lu __test_cache_lookup %.2fsec\n", pthread_self(), (double)(et - st) / 1000000000.0f);

  return(NULL);
}

void __run_threaded_tests (struct options *opts, void *(func) (void *)) {
  int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  pthread_t threads[NTHREADS];
  int i;

  printf("Testing %u threads\n", NTHREADS);

  for (i = 0; i < NTHREADS; ++i) {
    pthread_create(&(threads[i]), NULL, func, opts);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i % num_cores, &cpuset);
    pthread_setaffinity_np(threads[i], sizeof(cpu_set_t), &cpuset);
  }

  for (i = 0; i < NTHREADS; ++i)
    pthread_join(threads[i], NULL);
}

void __run_threaded_tests2 (struct options *opts,
                            void *(func) (void *),
                            void *(func2) (void *))
{
  int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  pthread_t threads[NTHREADS];
  int i;

  printf("Testing %u threads\n", NTHREADS);

  for (i = 0; i < (NTHREADS / 2); ++i) {
    pthread_create(&(threads[i]), NULL, func, opts);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i % num_cores, &cpuset);
    pthread_setaffinity_np(threads[i], sizeof(cpu_set_t), &cpuset);
  }

  for (; i < NTHREADS; ++i) {
    pthread_create(&(threads[i]), NULL, func2, opts);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i % num_cores, &cpuset);
    pthread_setaffinity_np(threads[i], sizeof(cpu_set_t), &cpuset);
  }

  for (i = 0; i < NTHREADS; ++i)
    pthread_join(threads[i], NULL);
}

int main (int argc, char **argv) {
  struct options opts;

  __parse_opts(&opts, argc, argv);

  Cache *cache = kudu::NewLRUCache(opts.cache_capacity);
  opts.cache = cache;

#if 1
  #if 0
    for (int i = 0; i < opts.cache_capacity; ++i) {
      uint64_t key = __key(i, opts.cache_capacity);
      Cache::Handle *entry = cache->Insert(
        Slice(reinterpret_cast<uint8_t *>(&key), 8), reinterpret_cast<void *>(key), 1, __deleter);
      cache->Release(entry);
    }
  #else
    __run_threaded_tests(&opts, __test_cache_insert);
    //__run_threaded_tests2(&opts, __test_cache_insert, __test_cache_lookup);
  #endif
  __run_threaded_tests(&opts, __test_cache_lookup);
#else
  __fill_from_stdin(&opts, &cache);
#endif

  return(0);
}