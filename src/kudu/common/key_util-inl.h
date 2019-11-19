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

#include "kudu/common/key_util.h"
#include "kudu/common/row.h"

namespace kudu {
namespace key_util {

// Increments a key with the provided column indices to the smallest key which
// is greater than the current key.
template<typename ColIdxIter>
bool IncrementKey(ColIdxIter first,
                  ColIdxIter last,
                  ContiguousRow* row,
                  Arena* arena) {
  for (auto col_idx_it = std::prev(last);
       std::distance(first, col_idx_it) >= 0;
       std::advance(col_idx_it, -1)) {
    if (IncrementCell(row->schema()->column(*col_idx_it),
                      row->mutable_cell_ptr(*col_idx_it), arena)) {
      return true;
    }
  }
  return false;
}

} // namespace key_util
} // namespace kudu
