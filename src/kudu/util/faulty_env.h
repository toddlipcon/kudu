// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_UTIL_FAULTY_ENV_H
#define KUDU_UTIL_FAULTY_ENV_H

#include "kudu/gutil/macros.h"
#include "kudu/util/env.h"
#include "kudu/util/mutex.h"

namespace kudu {

class FaultyEnv : public Env {
 public:
  FaultyEnv(Env* target, int crash_after_operations);
  virtual ~FaultyEnv();

  Status NewSequentialFile(const std::string& f, SequentialFile** r) OVERRIDE;
  Status NewRandomAccessFile(const std::string& f, RandomAccessFile** r) OVERRIDE;
  Status NewWritableFile(const std::string& f, WritableFile** r) OVERRIDE;
  Status NewWritableFile(const WritableFileOptions& o,
                         const std::string& f,
                         WritableFile** r) OVERRIDE ;
  Status NewTempWritableFile(const WritableFileOptions& o, const std::string& t,
                             std::string* f, gscoped_ptr<WritableFile>* r);
  bool FileExists(const std::string& f) OVERRIDE;
  Status GetChildren(const std::string& dir, std::vector<std::string>* r) OVERRIDE;
  Status DeleteFile(const std::string& f) OVERRIDE;
  Status CreateDir(const std::string& d) OVERRIDE;
  Status SyncDir(const std::string& d) OVERRIDE;
  Status DeleteDir(const std::string& d) OVERRIDE;
  Status DeleteRecursively(const std::string& d) OVERRIDE;
  Status GetFileSize(const std::string& f, uint64_t* s) OVERRIDE;
  Status RenameFile(const std::string& s, const std::string& t) OVERRIDE;
  Status LockFile(const std::string& f, FileLock** l) OVERRIDE;
  Status UnlockFile(FileLock* l) OVERRIDE;

  virtual Status GetTestDirectory(std::string* path) OVERRIDE {
    return target_->GetTestDirectory(path);
  }
  uint64_t NowMicros() OVERRIDE {
    return target_->NowMicros();
  }
  void SleepForMicroseconds(int micros) OVERRIDE {
    target_->SleepForMicroseconds(micros);
  }
  uint64_t gettid() OVERRIDE {
    return target_->gettid();
  }
  Status GetExecutablePath(std::string* path) OVERRIDE {
    return target_->GetExecutablePath(path);
  }

  void MaybeCrash();

 private:
  Env* const target_;
  const int crash_after_operations_;

  Mutex lock_;
  int64_t op_count_;

  DISALLOW_COPY_AND_ASSIGN(FaultyEnv);
};

} // namespace kudu
#endif /* KUDU_UTIL_FAULTY_ENV_H */
