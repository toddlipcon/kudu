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

// Utility functions for working with the primary key portion of a row.

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/port.h"

namespace kudu {

class Arena;
class ColumnPredicate;
class ColumnSchema;
class ContiguousRow;

namespace key_util {

// Pushes lower bound key predicates into the row. Returns the number of pushed
// predicates. Unpushed predicate columns will be set to the minimum value
// (unless no predicates are pushed at all).
int PushLowerBoundKeyPredicates(
    const std::vector<int32_t>& col_idxs,
    const std::unordered_map<std::string, ColumnPredicate>& predicates,
    ContiguousRow* row,
    Arena* arena);

// Pushes upper bound key predicates into the row. Returns the number of pushed
// predicates. Unpushed predicate columns will be set to the minimum value
// (unless no predicates are pushed at all).
int PushUpperBoundKeyPredicates(
    const std::vector<int32_t>& col_idxs,
    const std::unordered_map<std::string, ColumnPredicate>& predicates,
    ContiguousRow* row,
    Arena* arena);

// Pushes lower bound key predicates into the row. Returns the number of pushed
// predicates. Unpushed predicate columns will be set to the minimum value
// (unless no predicates are pushed at all).
int PushLowerBoundPrimaryKeyPredicates(
    const std::unordered_map<std::string, ColumnPredicate>& predicates,
    ContiguousRow* row,
    Arena* arena);

// Pushes upper bound key predicates into the row. Returns the number of pushed
// predicates. Unpushed predicate columns will be set to the minimum value
// (unless no predicates are pushed at all).
int PushUpperBoundPrimaryKeyPredicates(
    const std::unordered_map<std::string, ColumnPredicate>& predicates,
    ContiguousRow* row,
    Arena* arena);

} // namespace key_util
} // namespace kudu
