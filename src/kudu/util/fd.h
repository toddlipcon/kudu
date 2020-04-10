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
#pragma once

#include <errno.h>
#include <unistd.h>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"

namespace kudu {

class FileDescriptor {
 public:
  FileDescriptor() : fd_(-1) {
  }
  explicit FileDescriptor(int fd)
      : fd_(fd) {
  }
  FileDescriptor(FileDescriptor&& other) noexcept
      : fd_(other.fd_) {
    other.fd_ = -1;
  }

  FileDescriptor& operator=(FileDescriptor&& other) {
    WARN_NOT_OK(Close(), "could not close fd");
    fd_ = other.Release();
    return *this;
  }

  int Release() {
    int ret = fd_;
    fd_ = -1;
    return ret;
  }

  ~FileDescriptor() {
    WARN_NOT_OK(Close(), "could not close fd");
  }

  Status Close() {
    if (fd_ < 0) return Status::OK();
    int err;
    RETRY_ON_EINTR(err, close(fd_));
    if (PREDICT_FALSE(err != 0)) {
      return Status::IOError(ErrnoToString(errno));
    }
    fd_ = -1;
    return Status::OK();
  }

  bool is_valid() const {
    return fd_ != -1;
  }

  int get() const {
    return fd_;
  }

 private:
  int fd_;
  DISALLOW_COPY_AND_ASSIGN(FileDescriptor);
};

} // namespace kudu
