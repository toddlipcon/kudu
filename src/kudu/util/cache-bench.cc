// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/human_readable.h"
#include "kudu/util/cache.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {

static constexpr int kCacheCapacity = 1024 * 1024 * 1024;
static constexpr int kEntrySize = 8 * 1024;

class CacheBench : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    cache_.reset(NewLRUCache(DRAM_CACHE, kCacheCapacity * 2, "test-cache"));
  }

  void PopulateCache();

 protected:
  unique_ptr<Cache> cache_;
};

void CacheBench::PopulateCache() {
  for (uint32_t i = 0; i < kCacheCapacity / kEntrySize; i++) {
    char buf[sizeof(i)];
    memcpy(buf, &i, sizeof(i));
    Cache::PendingHandle* ph = cache_->Allocate(Slice(buf, sizeof(i)), kEntrySize, kEntrySize);
    cache_->Release(cache_->Insert(ph, nullptr));
  }
}

TEST_F(CacheBench, BenchReadOnly) {
  PopulateCache();
  const int kNumThreads = 16;
  const int kSecondsToRun = 4;
  vector<thread> threads;
  atomic<bool> done(false);
  atomic<int64_t> total_lookups(0);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&,i]() {
        Random r(i);
        int64_t lookups = 0;
        while (!done) {
          //uint32_t entry = r.Uniform(kCacheCapacity / kEntrySize);
          uint32_t entry = r.Skewed(17);
          char buf[sizeof(entry)];
          memcpy(buf, &entry, sizeof(entry));
          Cache::Handle* h = cache_->Lookup(Slice(buf, sizeof(entry)), Cache::EXPECT_IN_CACHE);
          CHECK(h) << entry;
          if (h) {
            cache_->Release(h);
          }
          lookups++;
        }
        total_lookups += lookups;
      });
  }
  SleepFor(MonoDelta::FromSeconds(kSecondsToRun));
  done = true;
  for (auto& t : threads) {
    t.join();
  }
  int64_t l_per_sec = total_lookups / kSecondsToRun;
  LOG(INFO) << "did " << l_per_sec << " (" << HumanReadableNum::ToString(l_per_sec) << ") lookups/sec";
}

} // namespace kudu
