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

#include <memory>

#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env_util.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using kudu::env_util::ReadFully;
using kudu::pb_util::ReadablePBContainerFile;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

// LogBlockManager opens two files per container, and CloseManyBlocksTest
// uses one container for each block. To simplify testing (i.e. no need to
// raise the ulimit on open files), the default is kept low.
DEFINE_int32(num_blocks_close, 500,
             "Number of blocks to simultaneously close in CloseManyBlocksTest");

DECLARE_uint64(log_container_preallocate_bytes);
DECLARE_uint64(log_container_max_size);

DECLARE_int64(fs_data_dirs_reserved_bytes);
DECLARE_int64(disk_reserved_bytes_free_for_testing);

DECLARE_int32(log_block_manager_full_disk_cache_seconds);
DECLARE_string(block_manager);

// Generic block manager metrics.
METRIC_DECLARE_gauge_uint64(block_manager_blocks_open_reading);
METRIC_DECLARE_gauge_uint64(block_manager_blocks_open_writing);
METRIC_DECLARE_counter(block_manager_total_writable_blocks);
METRIC_DECLARE_counter(block_manager_total_readable_blocks);
METRIC_DECLARE_counter(block_manager_total_bytes_written);
METRIC_DECLARE_counter(block_manager_total_bytes_read);

// Log block manager metrics.
METRIC_DECLARE_gauge_uint64(log_block_manager_bytes_under_management);
METRIC_DECLARE_gauge_uint64(log_block_manager_blocks_under_management);
METRIC_DECLARE_counter(log_block_manager_containers);
METRIC_DECLARE_counter(log_block_manager_full_containers);

// The LogBlockManager is only supported on Linux, since it requires hole punching.
#define RETURN_NOT_LOG_BLOCK_MANAGER() \
  do { \
    if (FLAGS_block_manager != "log") { \
      LOG(INFO) << "This platform does not use the log block manager by default. Skipping test."; \
      return; \
    } \
  } while (false)

namespace kudu {
namespace fs {

template <typename T>
class BlockManagerTest : public KuduTest {
 public:
  BlockManagerTest() :
    bm_(CreateBlockManager(scoped_refptr<MetricEntity>(),
                           shared_ptr<MemTracker>(),
                           { GetTestDataDirectory() })) {
  }

  virtual void SetUp() OVERRIDE {
    CHECK_OK(bm_->Create());
    CHECK_OK(bm_->Open());
  }

 protected:
  T* CreateBlockManager(const scoped_refptr<MetricEntity>& metric_entity,
                                   const shared_ptr<MemTracker>& parent_mem_tracker,
                                   const vector<string>& paths) {
    BlockManagerOptions opts;
    opts.metric_entity = metric_entity;
    opts.parent_mem_tracker = parent_mem_tracker;
    opts.root_paths = paths;
    return new T(env_.get(), opts);
  }

  Status ReopenBlockManager(const scoped_refptr<MetricEntity>& metric_entity,
                            const shared_ptr<MemTracker>& parent_mem_tracker,
                            const vector<string>& paths,
                            bool create) {
    // Blow away old memtrackers first.
    bm_.reset();
    bm_.reset(CreateBlockManager(metric_entity, parent_mem_tracker, paths));
    if (create) {
      RETURN_NOT_OK(bm_->Create());
    }
    return bm_->Open();
  }

  void RunMultipathTest(const vector<string>& paths);

  void RunLogMetricsTest();

  void RunLogContainerPreallocationTest();

  void RunMemTrackerTest();

  gscoped_ptr<T> bm_;
};

template <>
void BlockManagerTest<LogBlockManager>::SetUp() {
  RETURN_NOT_LOG_BLOCK_MANAGER();
  CHECK_OK(bm_->Create());
  CHECK_OK(bm_->Open());
}

template <>
void BlockManagerTest<FileBlockManager>::RunMultipathTest(const vector<string>& paths) {
  // Ensure that each path has an instance file and that it's well-formed.
  for (const string& path : paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    ASSERT_EQ(3, children.size());
    for (const string& child : children) {
      if (child == "." || child == "..") {
        continue;
      }
      PathInstanceMetadataPB instance;
      ASSERT_OK(pb_util::ReadPBContainerFromPath(env_.get(),
                                                 JoinPathSegments(path, child),
                                                 &instance));
    }
  }

  // Write ten blocks.
  const char* kTestData = "test data";
  for (int i = 0; i < 10; i++) {
    gscoped_ptr<WritableBlock> written_block;
    ASSERT_OK(bm_->CreateBlock(&written_block));
    ASSERT_OK(written_block->Append(kTestData));
    ASSERT_OK(written_block->Close());
  }

  // Each path should now have some additional block subdirectories. We
  // can't know for sure exactly how many (depends on the block IDs
  // generated), but this ensures that at least some change were made.
  for (const string& path : paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    ASSERT_GT(children.size(), 3);
  }
}

template <>
void BlockManagerTest<LogBlockManager>::RunMultipathTest(const vector<string>& paths) {
  // Write (3 * numPaths * 2) blocks, in groups of (numPaths * 2). That should
  // yield two containers per path.
  const char* kTestData = "test data";
  for (int i = 0; i < 3; i++) {
    ScopedWritableBlockCloser closer;
    for (int j = 0; j < paths.size() * 2; j++) {
      gscoped_ptr<WritableBlock> block;
      ASSERT_OK(bm_->CreateBlock(&block));
      ASSERT_OK(block->Append(kTestData));
      closer.AddBlock(std::move(block));
    }
    ASSERT_OK(closer.CloseBlocks());
  }

  // Verify the results: 7 children = dot, dotdot, instance file, and two
  // containers (two files per container).
  for (const string& path : paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    ASSERT_EQ(children.size(), 7);
  }
}

template <>
void BlockManagerTest<FileBlockManager>::RunLogMetricsTest() {
  LOG(INFO) << "Test skipped; wrong block manager";
}

static void CheckLogMetrics(const scoped_refptr<MetricEntity>& entity,
                            int bytes_under_management, int blocks_under_management,
                            int containers, int full_containers) {
  ASSERT_EQ(bytes_under_management, down_cast<AtomicGauge<uint64_t>*>(
                entity->FindOrNull(METRIC_log_block_manager_bytes_under_management)
                .get())->value());
  ASSERT_EQ(blocks_under_management, down_cast<AtomicGauge<uint64_t>*>(
                entity->FindOrNull(METRIC_log_block_manager_blocks_under_management)
                .get())->value());
  ASSERT_EQ(containers, down_cast<Counter*>(
                entity->FindOrNull(METRIC_log_block_manager_containers)
                .get())->value());
  ASSERT_EQ(full_containers, down_cast<Counter*>(
                entity->FindOrNull(METRIC_log_block_manager_full_containers)
                .get())->value());
}

template <>
void BlockManagerTest<LogBlockManager>::RunLogMetricsTest() {
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");
  ASSERT_OK(this->ReopenBlockManager(entity,
                                     shared_ptr<MemTracker>(),
                                     { GetTestDataDirectory() },
                                     false));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 0, 0, 0));

  // Lower the max container size so that we can more easily test full
  // container metrics.
  FLAGS_log_container_max_size = 1024;

  // One block --> one container.
  gscoped_ptr<WritableBlock> writer;
  ASSERT_OK(this->bm_->CreateBlock(&writer));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 0, 1, 0));

  // And when the block is closed, it becomes "under management".
  ASSERT_OK(writer->Close());
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 1, 1, 0));

  // Create 10 blocks concurrently. We reuse the existing container and
  // create 9 new ones. All of them get filled.
  BlockId saved_id;
  {
    Random rand(SeedRandom());
    ScopedWritableBlockCloser closer;
    for (int i = 0; i < 10; i++) {
      gscoped_ptr<WritableBlock> b;
      ASSERT_OK(this->bm_->CreateBlock(&b));
      if (saved_id.IsNull()) {
        saved_id = b->id();
      }
      uint8_t data[1024];
      for (int i = 0; i < sizeof(data); i += sizeof(uint32_t)) {
        data[i] = rand.Next();
      }
      b->Append(Slice(data, sizeof(data)));
      closer.AddBlock(std::move(b));
    }
    ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 1, 10, 0));

    // Only when the blocks are closed are the containers considered full.
    ASSERT_OK(closer.CloseBlocks());
    ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 10 * 1024, 11, 10, 10));
  }

  // Reopen the block manager and test the metrics. They're all based on
  // persistent information so they should be the same.
  MetricRegistry new_registry;
  scoped_refptr<MetricEntity> new_entity = METRIC_ENTITY_server.Instantiate(&new_registry, "test");
  ASSERT_OK(this->ReopenBlockManager(new_entity,
                                     shared_ptr<MemTracker>(),
                                     { GetTestDataDirectory() },
                                     false));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(new_entity, 10 * 1024, 11, 10, 10));

  // Delete a block. Its contents should no longer be under management.
  ASSERT_OK(this->bm_->DeleteBlock(saved_id));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(new_entity, 9 * 1024, 10, 10, 10));
}

template <>
void BlockManagerTest<FileBlockManager>::RunLogContainerPreallocationTest() {
  LOG(INFO) << "Test skipped; wrong block manager";
}

template <>
void BlockManagerTest<LogBlockManager>::RunLogContainerPreallocationTest() {
  // Create a block with some test data. This should also trigger
  // preallocation of the container, provided it's supported by the kernel.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Close());

  // Now reopen the block manager and create another block. More
  // preallocation, but it should be from the end of the previous block,
  // not from the end of the file.
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     { GetTestDataDirectory() },
                                     false));
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Close());

  // dot, dotdot, test metadata, instance file, and one container file pair.
  vector<string> children;
  ASSERT_OK(this->env_->GetChildren(GetTestDataDirectory(), &children));
  ASSERT_EQ(6, children.size());

  // If preallocation was done from the end of the file (rather than the
  // end of the previous block), the file's size would be twice the
  // preallocation amount. That would be wrong.
  //
  // Instead, we expect the size to either be 0 (preallocation isn't
  // supported) or equal to the preallocation amount.
  bool found = false;
  for (const string& child : children) {
    if (HasSuffixString(child, ".data")) {
      found = true;
      uint64_t size;
      ASSERT_OK(this->env_->GetFileSizeOnDisk(
          JoinPathSegments(GetTestDataDirectory(), child), &size));
      ASSERT_TRUE(size == 0 || size == FLAGS_log_container_preallocate_bytes);
    }
  }
  ASSERT_TRUE(found);
}

template <>
void BlockManagerTest<FileBlockManager>::RunMemTrackerTest() {
  shared_ptr<MemTracker> tracker = MemTracker::CreateTracker(-1, "test tracker");
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     tracker,
                                     { GetTestDataDirectory() },
                                     false));

  // The file block manager does not allocate memory for persistent data.
  int64_t initial_mem = tracker->consumption();
  ASSERT_EQ(initial_mem, 0);
  gscoped_ptr<WritableBlock> writer;
  ASSERT_OK(this->bm_->CreateBlock(&writer));
  ASSERT_OK(writer->Close());
  ASSERT_EQ(tracker->consumption(), initial_mem);
}

template <>
void BlockManagerTest<LogBlockManager>::RunMemTrackerTest() {
  shared_ptr<MemTracker> tracker = MemTracker::CreateTracker(-1, "test tracker");
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     tracker,
                                     { GetTestDataDirectory() },
                                     false));

  // The initial consumption should be non-zero due to the block map.
  int64_t initial_mem = tracker->consumption();
  ASSERT_GT(initial_mem, 0);

  // Allocating a persistent block should increase the consumption.
  gscoped_ptr<WritableBlock> writer;
  ASSERT_OK(this->bm_->CreateBlock(&writer));
  ASSERT_OK(writer->Close());
  ASSERT_GT(tracker->consumption(), initial_mem);
}

// What kinds of BlockManagers are supported?
#if defined(__linux__)
typedef ::testing::Types<FileBlockManager, LogBlockManager> BlockManagers;
#else
typedef ::testing::Types<FileBlockManager> BlockManagers;
#endif
TYPED_TEST_CASE(BlockManagerTest, BlockManagers);

// Test the entire lifecycle of a block.
TYPED_TEST(BlockManagerTest, EndToEndTest) {
  // Create a block.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));

  // Write some data to it.
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Close());

  // Read the data back.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  uint64_t sz;
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(test_data.length(), sz);
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);

  // Delete the block.
  ASSERT_OK(this->bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());
}

// Test that we can still read from an opened block after deleting it
// (even if we can't open it again).
TYPED_TEST(BlockManagerTest, ReadAfterDeleteTest) {
  // Write a new block.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Close());

  // Open it for reading, then delete it. Subsequent opens should fail.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(this->bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());

  // But we should still be able to read from the opened block.
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);
}

TYPED_TEST(BlockManagerTest, CloseTwiceTest) {
  // Create a new block and close it repeatedly.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Close());
  ASSERT_OK(written_block->Close());

  // Open it for reading and close it repeatedly.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(read_block->Close());
  ASSERT_OK(read_block->Close());
}

TYPED_TEST(BlockManagerTest, CloseManyBlocksTest) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Not running in slow-tests mode";
    return;
  }

  // Disable preallocation for this test as it creates many containers.
  FLAGS_log_container_preallocate_bytes = 0;

  Random rand(SeedRandom());
  vector<WritableBlock*> dirty_blocks;
  ElementDeleter deleter(&dirty_blocks);
  LOG(INFO) << "Creating " <<  FLAGS_num_blocks_close << " blocks";
  for (int i = 0; i < FLAGS_num_blocks_close; i++) {
    // Create a block.
    gscoped_ptr<WritableBlock> written_block;
    ASSERT_OK(this->bm_->CreateBlock(&written_block));

    // Write 64k bytes of random data into it.
    uint8_t data[65536];
    for (int i = 0; i < sizeof(data); i += sizeof(uint32_t)) {
      data[i] = rand.Next();
    }
    written_block->Append(Slice(data, sizeof(data)));

    dirty_blocks.push_back(written_block.release());
  }

  LOG_TIMING(INFO, Substitute("closing $0 blocks", FLAGS_num_blocks_close)) {
    ASSERT_OK(this->bm_->CloseBlocks(dirty_blocks));
  }
}

// We can't really test that FlushDataAsync() "works", but we can test that
// it doesn't break anything.
TYPED_TEST(BlockManagerTest, FlushDataAsyncTest) {
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
}

TYPED_TEST(BlockManagerTest, WritableBlockStateTest) {
  gscoped_ptr<WritableBlock> written_block;

  // Common flow: CLEAN->DIRTY->CLOSED.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_EQ(WritableBlock::CLEAN, written_block->state());
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_EQ(WritableBlock::DIRTY, written_block->state());
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_EQ(WritableBlock::DIRTY, written_block->state());
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test FLUSHING->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_EQ(WritableBlock::FLUSHING, written_block->state());
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test CLEAN->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test FlushDataAsync() no-op.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_EQ(WritableBlock::FLUSHING, written_block->state());

  // Test DIRTY->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
}

TYPED_TEST(BlockManagerTest, AbortTest) {
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());

  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());
}

TYPED_TEST(BlockManagerTest, PersistenceTest) {
  // Create three blocks:
  // 1. Empty.
  // 2. Non-empty.
  // 3. Deleted.
  gscoped_ptr<WritableBlock> written_block1;
  gscoped_ptr<WritableBlock> written_block2;
  gscoped_ptr<WritableBlock> written_block3;
  ASSERT_OK(this->bm_->CreateBlock(&written_block1));
  ASSERT_OK(written_block1->Close());
  ASSERT_OK(this->bm_->CreateBlock(&written_block2));
  string test_data = "test data";
  ASSERT_OK(written_block2->Append(test_data));
  ASSERT_OK(written_block2->Close());
  ASSERT_OK(this->bm_->CreateBlock(&written_block3));
  ASSERT_OK(written_block3->Append(test_data));
  ASSERT_OK(written_block3->Close());
  ASSERT_OK(this->bm_->DeleteBlock(written_block3->id()));

  // Reopen the block manager. This may read block metadata from disk.
  //
  // The existing block manager is left open, which proxies for the process
  // having crashed without cleanly shutting down the block manager. The
  // on-disk metadata should still be clean.
  gscoped_ptr<BlockManager> new_bm(this->CreateBlockManager(
      scoped_refptr<MetricEntity>(),
      MemTracker::CreateTracker(-1, "other tracker"),
      { GetTestDataDirectory() }));
  ASSERT_OK(new_bm->Open());

  // Test that the state of all three blocks is properly reflected.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(new_bm->OpenBlock(written_block1->id(), &read_block));
  uint64_t sz;
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(0, sz);
  ASSERT_OK(read_block->Close());
  ASSERT_OK(new_bm->OpenBlock(written_block2->id(), &read_block));
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(test_data.length(), sz);
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);
  ASSERT_OK(read_block->Close());
  ASSERT_TRUE(new_bm->OpenBlock(written_block3->id(), nullptr)
              .IsNotFound());
}

TYPED_TEST(BlockManagerTest, MultiPathTest) {
  // Recreate the block manager with three paths.
  vector<string> paths;
  for (int i = 0; i < 3; i++) {
    paths.push_back(this->GetTestPath(Substitute("path$0", i)));
  }
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     paths,
                                     true));

  ASSERT_NO_FATAL_FAILURE(this->RunMultipathTest(paths));
}

static void CloseHelper(ReadableBlock* block) {
  CHECK_OK(block->Close());
}

// Tests that ReadableBlock::Close() is thread-safe and idempotent.
TYPED_TEST(BlockManagerTest, ConcurrentCloseReadableBlockTest) {
  gscoped_ptr<WritableBlock> writer;
  ASSERT_OK(this->bm_->CreateBlock(&writer));
  ASSERT_OK(writer->Close());

  gscoped_ptr<ReadableBlock> reader;
  ASSERT_OK(this->bm_->OpenBlock(writer->id(), &reader));

  vector<scoped_refptr<Thread> > threads;
  for (int i = 0; i < 100; i++) {
    scoped_refptr<Thread> t;
    ASSERT_OK(Thread::Create("test", Substitute("t$0", i),
                             &CloseHelper, reader.get(), &t));
    threads.push_back(t);
  }
  for (const scoped_refptr<Thread>& t : threads) {
    t->Join();
  }
}

static void CheckMetrics(const scoped_refptr<MetricEntity>& metrics,
                         int blocks_open_reading, int blocks_open_writing,
                         int total_readable_blocks, int total_writable_blocks,
                         int total_bytes_read, int total_bytes_written) {
  ASSERT_EQ(blocks_open_reading, down_cast<AtomicGauge<uint64_t>*>(
                metrics->FindOrNull(METRIC_block_manager_blocks_open_reading).get())->value());
  ASSERT_EQ(blocks_open_writing, down_cast<AtomicGauge<uint64_t>*>(
                metrics->FindOrNull(METRIC_block_manager_blocks_open_writing).get())->value());
  ASSERT_EQ(total_readable_blocks, down_cast<Counter*>(
                metrics->FindOrNull(METRIC_block_manager_total_readable_blocks).get())->value());
  ASSERT_EQ(total_writable_blocks, down_cast<Counter*>(
                metrics->FindOrNull(METRIC_block_manager_total_writable_blocks).get())->value());
  ASSERT_EQ(total_bytes_read, down_cast<Counter*>(
                metrics->FindOrNull(METRIC_block_manager_total_bytes_read).get())->value());
  ASSERT_EQ(total_bytes_written, down_cast<Counter*>(
                metrics->FindOrNull(METRIC_block_manager_total_bytes_written).get())->value());
}

TYPED_TEST(BlockManagerTest, MetricsTest) {
  const string kTestData = "test data";
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");
  ASSERT_OK(this->ReopenBlockManager(entity,
                                     shared_ptr<MemTracker>(),
                                     { GetTestDataDirectory() },
                                     false));
  ASSERT_NO_FATAL_FAILURE(CheckMetrics(entity, 0, 0, 0, 0, 0, 0));

  for (int i = 0; i < 3; i++) {
    gscoped_ptr<WritableBlock> writer;
    gscoped_ptr<ReadableBlock> reader;

    // An open writer. Also reflected in total_writable_blocks.
    ASSERT_OK(this->bm_->CreateBlock(&writer));
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 0, 1, i, i + 1,
        i * kTestData.length(), i * kTestData.length()));

    // Block is no longer opened for writing, but its data
    // is now reflected in total_bytes_written.
    ASSERT_OK(writer->Append(kTestData));
    ASSERT_OK(writer->Close());
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 0, 0, i, i + 1,
        i * kTestData.length(), (i + 1) * kTestData.length()));

    // An open reader.
    ASSERT_OK(this->bm_->OpenBlock(writer->id(), &reader));
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 1, 0, i + 1, i + 1,
        i * kTestData.length(), (i + 1) * kTestData.length()));

    // The read is reflected in total_bytes_read.
    Slice data;
    gscoped_ptr<uint8_t[]> scratch(new uint8_t[kTestData.length()]);
    ASSERT_OK(reader->Read(0, kTestData.length(), &data, scratch.get()));
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 1, 0, i + 1, i + 1,
        (i + 1) * kTestData.length(), (i + 1) * kTestData.length()));

    // The reader is now gone.
    ASSERT_OK(reader->Close());
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 0, 0, i + 1, i + 1,
        (i + 1) * kTestData.length(), (i + 1) * kTestData.length()));
  }
}

TYPED_TEST(BlockManagerTest, LogMetricsTest) {
  ASSERT_NO_FATAL_FAILURE(this->RunLogMetricsTest());
}

TYPED_TEST(BlockManagerTest, LogContainerPreallocationTest) {
  ASSERT_NO_FATAL_FAILURE(this->RunLogContainerPreallocationTest());
}

TYPED_TEST(BlockManagerTest, MemTrackerTest) {
  ASSERT_NO_FATAL_FAILURE(this->RunMemTrackerTest());
}

// LogBlockManager-specific tests.
class LogBlockManagerTest : public BlockManagerTest<LogBlockManager> {
};

// Regression test for KUDU-1190, a crash at startup when a block ID has been
// reused.
TEST_F(LogBlockManagerTest, TestReuseBlockIds) {
  RETURN_NOT_LOG_BLOCK_MANAGER();
  vector<BlockId> block_ids;

  // Create 4 containers, with the first four block IDs in the random sequence.
  {
    ScopedWritableBlockCloser closer;
    for (int i = 0; i < 4; i++) {
      gscoped_ptr<WritableBlock> writer;
      ASSERT_OK(bm_->CreateBlock(&writer));
      block_ids.push_back(writer->id());
      closer.AddBlock(std::move(writer));
    }
    ASSERT_OK(closer.CloseBlocks());
  }

  // Create one more block, which should reuse the first container.
  {
    gscoped_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(&writer));
    ASSERT_OK(writer->Close());
  }

  ASSERT_EQ(4, bm_->available_containers_.size());

  // Delete the original blocks.
  for (const BlockId& b : block_ids) {
    ASSERT_OK(bm_->DeleteBlock(b));
  }

  // Reset the random seed and re-create new blocks which should reuse the same
  // block IDs. This isn't allowed in current versions of Kudu, but older versions
  // could produce this situation, and we still need to handle it on startup.
  bm_->next_block_id_.Store(1);
  for (int i = 0; i < 4; i++) {
    gscoped_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(&writer));
    ASSERT_EQ(writer->id(), block_ids[i]);
    ASSERT_OK(writer->Close());
  }

  // Now we have 4 containers with the following metadata:
  //   1: CREATE(1) CREATE (5) DELETE(1) CREATE(4)
  //   2: CREATE(2) DELETE(2) CREATE(1)
  //   3: CREATE(3) DELETE(3) CREATE(2)
  //   4: CREATE(4) DELETE(4) CREATE(3)

  // Re-open the block manager and make sure it can deal with this case where
  // block IDs have been reused.
  ASSERT_OK(ReopenBlockManager(scoped_refptr<MetricEntity>(),
                               shared_ptr<MemTracker>(),
                               { GetTestDataDirectory() },
                               false));
}

// Test partial record at end of metadata file. See KUDU-1377.
// The idea behind this test is that we should tolerate one partial record at
// the end of a given container metadata file, since we actively append a
// record to a container metadata file when a new block is created or deleted.
// A system crash or disk-full event can result in a partially-written metadata
// record. Ignoring a trailing, partial (not corrupt) record is safe, so long
// as we only consider a container valid if there is at most one trailing
// partial record. If any other metadata record is somehow incomplete or
// corrupt, we consider that an error and the entire container is considered
// corrupted.
//
// Note that we rely on filesystem integrity to ensure that we do not lose
// trailing, fsync()ed metadata.
TEST_F(LogBlockManagerTest, TestMetadataTruncation) {
  RETURN_NOT_LOG_BLOCK_MANAGER();

  // Create several blocks.
  vector<BlockId> created_blocks;
  BlockId last_block_id;
  for (int i = 0; i < 4; i++) {
    gscoped_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(&writer));
    last_block_id = writer->id();
    created_blocks.push_back(last_block_id);
    ASSERT_OK(writer->Close());
  }
  ASSERT_EQ(4, bm_->CountBlocksForTests());
  gscoped_ptr<ReadableBlock> block;
  ASSERT_OK(bm_->OpenBlock(last_block_id, &block));
  ASSERT_OK(block->Close());

  // Start corrupting the metadata file in different ways.

  string path = LogBlockManager::ContainerPathForTests(bm_->available_containers_.front());
  string metadata_path = path + LogBlockManager::kContainerMetadataFileSuffix;

  uint64_t good_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &good_meta_size));

  // First, add an extra byte to the end of the metadata file. This makes the
  // trailing "record" of the metadata file corrupt, but doesn't cause data
  // loss. The result is that the container will automatically truncate the
  // metadata file back to its correct size.
  {
    RWFileOptions opts;
    opts.mode = Env::OPEN_EXISTING;
    gscoped_ptr<RWFile> file;
    ASSERT_OK(env_->NewRWFile(opts, metadata_path, &file));
    ASSERT_OK(file->Truncate(good_meta_size + 1));
  }

  uint64_t cur_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  ASSERT_EQ(good_meta_size + 1, cur_meta_size);

  // Reopen the metadata file. We will still see all of our blocks. The size of
  // the metadata file will be restored back to its previous value.
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     { GetTestDataDirectory() },
                                     false));
  ASSERT_EQ(4, bm_->CountBlocksForTests());
  ASSERT_OK(bm_->OpenBlock(last_block_id, &block));
  ASSERT_OK(block->Close());

  // Check that the file was truncated back to its previous size by the system.
  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  ASSERT_EQ(good_meta_size, cur_meta_size);

  // Delete the first block we created. This necessitates writing to the
  // metadata file of the originally-written container, since we append a
  // delete record to the metadata.
  ASSERT_OK(bm_->DeleteBlock(created_blocks[0]));
  ASSERT_EQ(3, bm_->CountBlocksForTests());

  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  good_meta_size = cur_meta_size;

  // Add a new block, increasing the size of the container metadata file.
  {
    gscoped_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(&writer));
    last_block_id = writer->id();
    created_blocks.push_back(last_block_id);
    ASSERT_OK(writer->Close());
  }
  ASSERT_EQ(4, bm_->CountBlocksForTests());
  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  ASSERT_GT(cur_meta_size, good_meta_size);
  uint64_t prev_good_meta_size = good_meta_size; // Store previous size.
  good_meta_size = cur_meta_size;

  // Now, truncate the metadata file so that we lose the last valid record.
  // This will result in the loss of a block record, therefore we will observe
  // data loss, however it will look like a failed partial write.
  {
    RWFileOptions opts;
    opts.mode = Env::OPEN_EXISTING;
    gscoped_ptr<RWFile> file;
    ASSERT_OK(env_->NewRWFile(opts, metadata_path, &file));
    ASSERT_OK(file->Truncate(good_meta_size - 1));
  }

  // Reopen the truncated metadata file. We will not find all of our blocks.
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     { GetTestDataDirectory() },
                                     false));

  // Because the last record was a partial record on disk, the system should
  // have assumed that it was an incomplete write and truncated the metadata
  // file back to the previous valid record. Let's verify that that's the case.
  good_meta_size = prev_good_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  ASSERT_EQ(good_meta_size, cur_meta_size);

  ASSERT_EQ(3, bm_->CountBlocksForTests());
  Status s = bm_->OpenBlock(last_block_id, &block);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Can't find block");

  // Add a new block, increasing the size of the container metadata file.
  {
    gscoped_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(&writer));
    last_block_id = writer->id();
    created_blocks.push_back(last_block_id);
    ASSERT_OK(writer->Close());
  }

  ASSERT_EQ(4, bm_->CountBlocksForTests());
  ASSERT_OK(bm_->OpenBlock(last_block_id, &block));
  ASSERT_OK(block->Close());

  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  ASSERT_GT(cur_meta_size, good_meta_size);
  good_meta_size = cur_meta_size;

  // Ensure that we only ever created a single container.
  ASSERT_EQ(1, bm_->all_containers_.size());
  ASSERT_EQ(1, bm_->available_containers_.size());

  // Find location of 2nd record in metadata file and corrupt it.
  // This is an unrecoverable error because it's in the middle of the file.
  gscoped_ptr<RandomAccessFile> meta_file;
  ASSERT_OK(env_->NewRandomAccessFile(metadata_path, &meta_file));
  ReadablePBContainerFile pb_reader(std::move(meta_file));
  ASSERT_OK(pb_reader.Open());
  BlockRecordPB record;
  ASSERT_OK(pb_reader.ReadNextPB(&record));
  uint64_t offset = pb_reader.offset();

  uint64_t latest_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &latest_meta_size));
  ASSERT_OK(env_->NewRandomAccessFile(metadata_path, &meta_file));
  Slice result;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[latest_meta_size]);
  ASSERT_OK(ReadFully(meta_file.get(), 0, latest_meta_size, &result, scratch.get()));
  string data = result.ToString();
  // Flip the high bit of the length field, which is a 4-byte little endian
  // unsigned integer. This will cause the length field to represent a large
  // value and also cause the length checksum not to validate.
  data[offset + 3] ^= 1 << 7;
  gscoped_ptr<WritableFile> writable_meta;
  ASSERT_OK(env_->NewWritableFile(metadata_path, &writable_meta));
  ASSERT_OK(writable_meta->Append(data));
  ASSERT_OK(writable_meta->Close());

  // Now try to reopen the container.
  // This should look like a bad checksum, and it's not recoverable.
  s = this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                               shared_ptr<MemTracker>(),
                               { GetTestDataDirectory() },
                               false);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");
}

TEST_F(LogBlockManagerTest, TestDiskSpaceCheck) {
  RETURN_NOT_LOG_BLOCK_MANAGER();

  FLAGS_log_block_manager_full_disk_cache_seconds = 0; // Don't cache device fullness.

  FLAGS_fs_data_dirs_reserved_bytes = 1; // Keep at least 1 byte reserved in the FS.
  FLAGS_disk_reserved_bytes_free_for_testing = 0;
  FLAGS_log_container_preallocate_bytes = 100;

  vector<BlockId> created_blocks;
  gscoped_ptr<WritableBlock> writer;
  Status s = bm_->CreateBlock(&writer);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "All data directories are full");

  FLAGS_disk_reserved_bytes_free_for_testing = 101;
  ASSERT_OK(bm_->CreateBlock(&writer));

  FLAGS_disk_reserved_bytes_free_for_testing = 0;
  s = bm_->CreateBlock(&writer);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();

  ASSERT_OK(writer->Close());
}

} // namespace fs
} // namespace kudu
