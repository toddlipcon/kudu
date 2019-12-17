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

#include "kudu/gutil/macros.h"
#include "kudu/client/client.h"
#include "kudu/util/mutex.h"

#include <string>


namespace kudu {
namespace tsdb {


class SequenceGenerator {
 public:
  explicit SequenceGenerator(client::sp::shared_ptr<client::KuduClient> client,
                             std::string sequence_name)
      : client_(std::move(client)),
        name_(std::move(sequence_name)) {
  }
  ~SequenceGenerator();

  Status Init();

  Status Next(int64_t* id) {
    MutexLock l(lock_);
    if (next_ == reserved_until_) {
      RETURN_NOT_OK(ReserveChunk());
    }
    *id = next_++;
    return Status::OK();
  }

 private:
  static constexpr const char * const kTableName = "sequences";

  Status CreateTable();
  Status ReserveChunk();
  Status GetMaxReservedFromTable(int64_t* max_reserved);


  const client::sp::shared_ptr<client::KuduClient> client_;
  const std::string name_;

  client::sp::shared_ptr<client::KuduTable> table_;

  Mutex lock_;
  int64_t next_ = 0;
  int64_t reserved_until_ = 0;
};


} // namespace tsdb
} // namespace kudu
