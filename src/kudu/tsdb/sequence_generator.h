// Copyright (C) 2020 Cloudera, inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
