// Copyright (c) 2014, Cloudera, inc.

#include "kudu/util/faulty_env.h"

namespace kudu {


class FaultyWritableFile : public WritableFile {
 public:
  FaultyWritableFile(FaultyEnv* env,
                     gscoped_ptr<WritableFile> wrapped)
    : env_(env),
      wrapped_(wrapped.Pass()) {
  }
  virtual ~FaultyWritableFile() {}

  // Pre-allocates 'size' bytes for the file in the underlying filesystem.
  // size bytes are added to the current pre-allocated size or to the current
  // offset, whichever is bigger. In no case is the file truncated by this
  // operation.
  virtual Status PreAllocate(uint64_t size) {
    env_->MaybeCrash();
    return wrapped_->PreAllocate(size);
  }

  virtual Status Append(const Slice& data) {
    env_->MaybeCrash();
    return wrapped_->Append(data);
  }

  // If possible, uses scatter-gather I/O to efficiently append
  // multiple buffers to a file. Otherwise, falls back to regular I/O.
  //
  // For implementation specific quirks and details, see comments in
  // implementation source code (e.g., env_posix.cc)
  virtual Status AppendVector(const std::vector<Slice>& data_vector) {
    env_->MaybeCrash();
    return wrapped_->AppendVector(data_vector);
  }

  virtual Status Close() {
    env_->MaybeCrash();
    return wrapped_->Close();
  }

  virtual Status Flush() {
    env_->MaybeCrash();
    return wrapped_->Flush();
  }

  virtual Status Sync() {
    env_->MaybeCrash();
    return wrapped_->Flush();
  }

  virtual Status SyncParentDir() {
    env_->MaybeCrash();
    return wrapped_->SyncParentDir();
  }

  virtual uint64_t Size() const {
    env_->MaybeCrash();
    return wrapped_->Size();
  }

  // Returns a string representation of the file suitable for debugging.
  virtual std::string ToString() const {
    return wrapped_->ToString();
  }

 private:
  FaultyEnv* env_;
  gscoped_ptr<WritableFile> wrapped_;

  DISALLOW_COPY_AND_ASSIGN(FaultyWritableFile);
};


FaultyEnv::FaultyEnv(Env* target, int crash_after_operations)
  : target_(target),
    crash_after_operations_(crash_after_operations),
    op_count_(0) {
}

FaultyEnv::~FaultyEnv() {
}

Status FaultyEnv::NewSequentialFile(const std::string& f, SequentialFile** r) {
  return target_->NewSequentialFile(f, r);
}

Status FaultyEnv::NewRandomAccessFile(const std::string& f, RandomAccessFile** r) {
  return target_->NewRandomAccessFile(f, r);
}

Status FaultyEnv::NewWritableFile(const std::string& f, WritableFile** r) {
  MutexLock l(lock_);
  MaybeCrash();
  Status s = target_->NewWritableFile(f, r);
  if (s.ok()) {
    *r = new FaultyWritableFile(this, gscoped_ptr<WritableFile>(*r));
  }
  return s;
}

Status FaultyEnv::NewWritableFile(const WritableFileOptions& o,
                       const std::string& f,
                       WritableFile** r) {
  MutexLock l(lock_);
  MaybeCrash();
  Status s = target_->NewWritableFile(o, f, r);
  if (s.ok()) {
    *r = new FaultyWritableFile(this, gscoped_ptr<WritableFile>(*r));
  }
  return s;
}

Status FaultyEnv::NewTempWritableFile(const WritableFileOptions& o, const std::string& t,
                           std::string* f, gscoped_ptr<WritableFile>* r) {
  MutexLock l(lock_);
  MaybeCrash();
  gscoped_ptr<WritableFile> real;
  Status s = target_->NewTempWritableFile(o, t, f, &real);
  if (s.ok()) {
    r->reset(new FaultyWritableFile(this, real.Pass()));
  }
  return s;
}

bool FaultyEnv::FileExists(const std::string& f) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->FileExists(f);
}

Status FaultyEnv::GetChildren(const std::string& dir, std::vector<std::string>* r) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->GetChildren(dir, r);
}

Status FaultyEnv::DeleteFile(const std::string& f) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->DeleteFile(f);
}

Status FaultyEnv::CreateDir(const std::string& d) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->CreateDir(d);
}

Status FaultyEnv::SyncDir(const std::string& d) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->SyncDir(d);
}

Status FaultyEnv::DeleteDir(const std::string& d) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->DeleteDir(d);
}

Status FaultyEnv::DeleteRecursively(const std::string& d) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->DeleteRecursively(d);
}

Status FaultyEnv::GetFileSize(const std::string& f, uint64_t* s) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->GetFileSize(f, s);
}

Status FaultyEnv::RenameFile(const std::string& s, const std::string& t) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->RenameFile(s, t);
}

Status FaultyEnv::LockFile(const std::string& f, FileLock** fl) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->LockFile(f, fl);
}

Status FaultyEnv::UnlockFile(FileLock* fl) {
  MutexLock l(lock_);
  MaybeCrash();
  return target_->UnlockFile(fl);
}

void FaultyEnv::MaybeCrash() {
  if (op_count_++ == crash_after_operations_) {
    LOG(FATAL) << "Injected crash";
  }
}

} // namespace kudu
