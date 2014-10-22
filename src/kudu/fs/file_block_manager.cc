// Copyright (c) 2014, Cloudera, inc.

#include "kudu/fs/file_block_manager.h"

#include <boost/foreach.hpp>
#include <string>
#include <vector>

#include "kudu/fs/block_id-inl.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

DEFINE_bool(block_coalesce_close, true,
            "Coalesce synchronization of data during CloseBlocks()");
DECLARE_bool(enable_data_block_fsync);

namespace kudu {
namespace fs {

////////////////////////////////////////////////////////////
// FileWritableBlock
////////////////////////////////////////////////////////////

FileWritableBlock::FileWritableBlock(FileBlockManager* block_manager,
                                     const BlockId& block_id,
                                     const shared_ptr<WritableFile>& writer) :
  block_manager_(block_manager),
  block_id_(block_id),
  writer_(writer),
  state_(CLEAN),
  bytes_appended_(0) {
}

FileWritableBlock::~FileWritableBlock() {
  WARN_NOT_OK(Close(), Substitute("Failed to close block $0",
                                  block_id_.ToString()));
}

Status FileWritableBlock::Close() {
  return Close(SYNC);
}

Status FileWritableBlock::Abort() {
  RETURN_NOT_OK(Close(NO_SYNC));
  return block_manager()->DeleteBlock(id());
}

BlockManager* FileWritableBlock::block_manager() const {
  return block_manager_;
}

const BlockId& FileWritableBlock::id() const {
  return block_id_;
}

Status FileWritableBlock::Append(const Slice& data) {
  DCHECK(state_ == CLEAN || state_ == DIRTY)
      << "Invalid state: " << state_;

  RETURN_NOT_OK(writer_->Append(data));
  state_ = DIRTY;
  bytes_appended_ += data.size();
  return Status::OK();
}

Status FileWritableBlock::FlushDataAsync() {
  DCHECK(state_ == CLEAN || state_ == DIRTY || state_ == FLUSHING)
      << "Invalid state: " << state_;
  if (state_ == DIRTY) {
    VLOG(3) << "Flushing block " << id();
    RETURN_NOT_OK(writer_->Flush(WritableFile::FLUSH_ASYNC));
  }

  state_ = FLUSHING;
  return Status::OK();
}

size_t FileWritableBlock::BytesAppended() const {
  return bytes_appended_;
}

WritableBlock::State FileWritableBlock::state() const {
  return state_;
}

Status FileWritableBlock::Close(SyncMode mode) {
  if (state_ == CLOSED) {
    return Status::OK();
  }

  Status sync;
  if (mode == SYNC &&
      (state_ == DIRTY || state_ == FLUSHING) &&
      FLAGS_enable_data_block_fsync) {
    // Safer to synchronize data first, then metadata.
    VLOG(3) << "Syncing block " << id();
    sync = writer_->Sync();
    if (sync.ok()) {
      sync = block_manager_->SyncMetadata(block_id_);
    }
    WARN_NOT_OK(sync, Substitute("Failed to sync when closing block $0",
                                 block_id_.ToString()));
  }
  Status close = writer_->Close();

  state_ = CLOSED;
  writer_.reset();

  // Prefer the result of Close() to that of Sync().
  return !close.ok() ? close : sync;
}

////////////////////////////////////////////////////////////
// FileReadableBlock
////////////////////////////////////////////////////////////

FileReadableBlock::FileReadableBlock(const BlockId& block_id,
                                     const shared_ptr<RandomAccessFile>& reader) :
  block_id_(block_id),
  reader_(reader),
  closed_(false) {
}

FileReadableBlock::~FileReadableBlock() {
  Close();
}

Status FileReadableBlock::Close() {
  if (closed_) {
    return Status::OK();
  }

  closed_ = true;
  reader_.reset();
  return Status::OK();
}

const BlockId& FileReadableBlock::id() const {
  return block_id_;
}

Status FileReadableBlock::Size(size_t* sz) const {
  DCHECK(!closed_);

  return reader_->Size(sz);
}

Status FileReadableBlock::Read(uint64_t offset, size_t length,
                               Slice* result, uint8_t* scratch) const {
  DCHECK(!closed_);

  return env_util::ReadFully(reader_.get(), offset, length, result, scratch);
}

////////////////////////////////////////////////////////////
// FileBlockManager
////////////////////////////////////////////////////////////

Status FileBlockManager::CreateBlockDir(const BlockId& block_id, vector<string>* created_dirs) {
  CHECK(!block_id.IsNull());
  DCHECK(env_->FileExists(root_path_));

  bool path0_created;
  string path0 = JoinPathSegments(root_path_, block_id.hash0());
  RETURN_NOT_OK(CreateDirIfMissing(path0, &path0_created));

  bool path1_created;
  string path1 = JoinPathSegments(path0, block_id.hash1());
  RETURN_NOT_OK(CreateDirIfMissing(path1, &path1_created));

  bool path2_created;
  string path2 = JoinPathSegments(path1, block_id.hash2());
  RETURN_NOT_OK(CreateDirIfMissing(path2, &path2_created));

  if (path2_created) {
    created_dirs->push_back(path1);
  }
  if (path1_created) {
    created_dirs->push_back(path0);
  }
  if (path0_created) {
    created_dirs->push_back(root_path_);
  }
  return Status::OK();
}

string FileBlockManager::GetBlockPath(const BlockId& block_id) const {
  CHECK(!block_id.IsNull());
  string path = root_path_;
  path = JoinPathSegments(path, block_id.hash0());
  path = JoinPathSegments(path, block_id.hash1());
  path = JoinPathSegments(path, block_id.hash2());
  path = JoinPathSegments(path, block_id.ToString());
  return path;
}

Status FileBlockManager::CreateDirIfMissing(const string& path, bool* created) {
  Status s = env_->CreateDir(path);
  if (created) {
    if (s.ok()) {
      *created = true;
    } else if (s.IsAlreadyPresent()) {
      *created = false;
    }
  }
  return s.IsAlreadyPresent() ? Status::OK() : s;
}

Status FileBlockManager::SyncMetadata(const BlockId& block_id) {
  CHECK(!block_id.IsNull());

  string path0 = JoinPathSegments(root_path_, block_id.hash0());
  string path1 = JoinPathSegments(path0, block_id.hash1());
  string path2 = JoinPathSegments(path1, block_id.hash2());

  // Figure out what directories to sync. Order is important.
  vector<string> to_sync;
  {
    lock_guard<simple_spinlock> l(&lock_);
    if (dirty_dirs_.erase(path2)) {
      to_sync.push_back(path2);
    }
    if (dirty_dirs_.erase(path1)) {
      to_sync.push_back(path1);
    }
    if (dirty_dirs_.erase(path0)) {
      to_sync.push_back(path0);
    }
    if (dirty_dirs_.erase(root_path_)) {
      to_sync.push_back(root_path_);
    }
  }

  // Sync them.
  BOOST_FOREACH(const string& s, to_sync) {
    RETURN_NOT_OK(env_->SyncDir(s));
  }
  return Status::OK();
}

void FileBlockManager::CreateBlock(const BlockId& block_id,
                                   const string& path,
                                   const vector<string>& created_dirs,
                                   const shared_ptr<WritableFile>& writer,
                                   const CreateBlockOptions& opts,
                                   gscoped_ptr<WritableBlock>* block) {
  VLOG(1) << "Creating new block " << block_id.ToString() << " at " << path;

  {
    // Update dirty_dirs_ with those provided as well as the block's
    // directory, which may not have been created but is definitely dirty
    // (because we added a file to it).
    lock_guard<simple_spinlock> l(&lock_);
    BOOST_FOREACH(const string& created, created_dirs) {
      dirty_dirs_.insert(created);
    }
    dirty_dirs_.insert(DirName(path));
  }

  block->reset(new FileWritableBlock(this, block_id, writer));
}

FileBlockManager::FileBlockManager(Env* env,
                                   const string& root_path) :
  env_(env),
  root_path_(root_path),
  rng_(time(NULL)),
  next_block_id_high_(0),
  next_block_id_low_(0) {
}

FileBlockManager::~FileBlockManager() {
}

Status FileBlockManager::Create() {
  return env_->CreateDir(root_path_);
}

Status FileBlockManager::Open() {
  return env_->FileExists(root_path_) ? Status::OK() : Status::NotFound(
      Substitute("FileBlockManager at $0 not found", root_path_));
}

Status FileBlockManager::CreateAnonymousBlock(const CreateBlockOptions& opts,
                                              gscoped_ptr<WritableBlock>* block) {
  string path;
  vector<string> created_dirs;
  Status s;
  BlockId block_id;
  shared_ptr<WritableFile> writer;

  // Repeat in case of block id collisions (unlikely).
  bool first_attempt = true;
  do {
    if (!first_attempt) {
      // If we collide, jump to a new area of key space and allocate sequentially from there.
      RandomizeNextBlockId();
    }
    GenerateBlockId(&block_id);
    first_attempt = false;

    created_dirs.clear();
    RETURN_NOT_OK(CreateBlockDir(block_id, &created_dirs));
    path = GetBlockPath(block_id);
    WritableFileOptions wr_opts;
    wr_opts.overwrite_existing = false;
    s = env_util::OpenFileForWrite(wr_opts, env_, path, &writer);
  } while (PREDICT_FALSE(s.IsAlreadyPresent()));
  if (s.ok()) {
    CreateBlock(block_id, path, created_dirs, writer, opts, block);
  }
  return s;
}

void FileBlockManager::RandomizeNextBlockId() {
  lock_guard<simple_spinlock> l(&id_gen_lock_);
  next_block_id_high_ = rng_.Next64();
  next_block_id_low_ = rng_.Next64();
}

void FileBlockManager::GenerateBlockId(BlockId* id) {
  lock_guard<simple_spinlock> l(&id_gen_lock_);
  id->SetId(StringPrintf("%08"PRIx64"%08"PRIx64, next_block_id_low_, next_block_id_high_));
  if (++next_block_id_low_ == 0) {
    ++next_block_id_high_;
  }
}

Status FileBlockManager::CreateAnonymousBlock(gscoped_ptr<WritableBlock>* block) {
  return CreateAnonymousBlock(CreateBlockOptions(), block);
}

Status FileBlockManager::CreateNamedBlock(const CreateBlockOptions& opts,
                                          const BlockId& block_id,
                                          gscoped_ptr<WritableBlock>* block) {
  string path = GetBlockPath(block_id);
  VLOG(1) << "Creating new block with predetermined id "
          << block_id.ToString() << " at " << path;

  vector<string> created_dirs;
  RETURN_NOT_OK(CreateBlockDir(block_id, &created_dirs));
  shared_ptr<WritableFile> writer;
  WritableFileOptions wr_opts;
  wr_opts.overwrite_existing = false;
  RETURN_NOT_OK(env_util::OpenFileForWrite(wr_opts, env_, path, &writer));
  CreateBlock(block_id, path, created_dirs, writer, opts, block);
  return Status::OK();
}

Status FileBlockManager::CreateNamedBlock(const BlockId& block_id,
                                          gscoped_ptr<WritableBlock>* block) {
  return CreateNamedBlock(CreateBlockOptions(), block_id, block);
}

Status FileBlockManager::OpenBlock(const BlockId& block_id,
                                   gscoped_ptr<ReadableBlock>* block) {
  string path = GetBlockPath(block_id);
  VLOG(1) << "Opening block with id " << block_id.ToString() << " at " << path;

  shared_ptr<RandomAccessFile> reader;
  RETURN_NOT_OK(env_util::OpenFileForRandom(env_, path, &reader));
  block->reset(new FileReadableBlock(block_id, reader));
  return Status::OK();
}

Status FileBlockManager::DeleteBlock(const BlockId& block_id) {
  string path = GetBlockPath(block_id);
  RETURN_NOT_OK(env_->DeleteFile(path));
  if (FLAGS_enable_data_block_fsync) {
    WARN_NOT_OK(env_->SyncDir(DirName(path)),
                "Failed to sync parent directory when deleting block");
  }

  // The block's directory hierarchy is left behind. We could prune it if
  // it's empty, but that's racy and leaving it isn't much overhead.

  return Status::OK();
}

Status FileBlockManager::CloseBlocks(const vector<WritableBlock*>& blocks) {
  VLOG(3) << "Closing " << blocks.size() << " blocks";
  if (FLAGS_block_coalesce_close) {
    // Ask the kernel to begin writing out each block's dirty data. This is
    // done up-front to give the kernel opportunities to coalesce contiguous
    // dirty pages.
    BOOST_FOREACH(WritableBlock* block, blocks) {
      RETURN_NOT_OK(block->FlushDataAsync());
    }
  }

  // Now close each block, waiting for each to become durable.
  BOOST_FOREACH(WritableBlock* block, blocks) {
    RETURN_NOT_OK(block->Close());
  }
  return Status::OK();
}

} // namespace fs
} // namespace kudu
