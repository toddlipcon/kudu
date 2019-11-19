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

#include "kudu/common/key_predicates.h"

#include <cmath>
#include <cstring>
#include <iterator>
#include <limits>
#include <ostream>
#include <string>
#include <tuple>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <glog/logging.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/key_util.h"
#include "kudu/common/key_util-inl.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"

using std::nextafter;
using std::numeric_limits;
using std::string;
using std::tuple;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace key_util {

namespace {

template<typename ColIdxIter>
void SetKeyToMinValues(ColIdxIter first, ColIdxIter last, ContiguousRow* row) {
  for (auto col_idx_it = first; col_idx_it != last; std::advance(col_idx_it, 1)) {
    DCHECK_LE(0, *col_idx_it);
    const ColumnSchema& col = row->schema()->column(*col_idx_it);
    col.type_info()->CopyMinValue(row->mutable_cell_ptr(*col_idx_it));
  }
}


template<typename ColIdxIter>
int PushLowerBoundKeyPredicates(ColIdxIter first,
                                ColIdxIter last,
                                const unordered_map<string, ColumnPredicate>& predicates,
                                ContiguousRow* row,
                                Arena* arena) {
  const Schema& schema = *CHECK_NOTNULL(row->schema());
  int pushed_predicates = 0;

  // Step 1: copy predicates into the row in key column order, stopping after
  // the first missing predicate.

  bool break_loop = false;
  for (auto col_idx_it = first; !break_loop && col_idx_it < last; std::advance(col_idx_it, 1)) {
    const ColumnSchema& column = schema.column(*col_idx_it);
    const ColumnPredicate* predicate = FindOrNull(predicates, column.name());
    if (predicate == nullptr) break;
    size_t size = column.type_info()->size();

    switch (predicate->predicate_type()) {
      case PredicateType::InBloomFilter: // Lower in InBloomFilter processed as lower in Range.
      case PredicateType::Range:
        if (predicate->raw_lower() == nullptr) {
          break_loop = true;
          break;
        }
        // Fall through.
      case PredicateType::Equality:
        memcpy(row->mutable_cell_ptr(*col_idx_it), predicate->raw_lower(), size);
        pushed_predicates++;
        break;
      case PredicateType::IsNotNull: // Fallthrough intended
      case PredicateType::IsNull:
        break_loop = true;
        break;
      case PredicateType::InList:
        // Since the InList predicate is a sorted vector of values, the first
        // value provides an inclusive lower bound that can be pushed.
        DCHECK(!predicate->raw_values().empty());
        memcpy(row->mutable_cell_ptr(*col_idx_it), predicate->raw_values().front(), size);
        pushed_predicates++;
        break;
      case PredicateType::None:
        LOG(FATAL) << "NONE predicate can not be pushed into key";
    }
  }

  // If no predicates were pushed, no need to do any more work.
  if (pushed_predicates == 0) { return 0; }

  // Step 2: Fill the remaining columns without predicates with the min value.
  SetKeyToMinValues(std::next(first, pushed_predicates), last, row);
  return pushed_predicates;
}

template<typename ColIdxIter>
int PushUpperBoundKeyPredicates(ColIdxIter first,
                                ColIdxIter last,
                                const unordered_map<string, ColumnPredicate>& predicates,
                                ContiguousRow* row,
                                Arena* arena) {

  const Schema& schema = *CHECK_NOTNULL(row->schema());
  int pushed_predicates = 0;

  // Step 1: copy predicates into the row in key column order, stopping after the first missing
  // predicate or range predicate which can't be transformed to an inclusive upper bound.
  bool break_loop = false;
  bool is_inclusive_bound = true;
  for (auto col_idx_it = first; !break_loop && col_idx_it < last; std::advance(col_idx_it, 1)) {
    const ColumnSchema& column = schema.column(*col_idx_it);
    const ColumnPredicate* predicate = FindOrNull(predicates, column.name());
    if (predicate == nullptr) break;
    size_t size = column.type_info()->size();
    switch (predicate->predicate_type()) {
      case PredicateType::Equality:
        memcpy(row->mutable_cell_ptr(*col_idx_it), predicate->raw_lower(), size);
        pushed_predicates++;
        break;
      case PredicateType::InBloomFilter:  // Upper in InBloomFilter processed as upper in Range.
      case PredicateType::Range:
        if (predicate->raw_upper() != nullptr) {
          memcpy(row->mutable_cell_ptr(*col_idx_it), predicate->raw_upper(), size);
          pushed_predicates++;
          // Try to decrease because upper bound is exclusive.
          if (!TryDecrementCell(row->schema()->column(*col_idx_it),
                                row->mutable_cell_ptr(*col_idx_it))) {
            is_inclusive_bound = false;
            break_loop = true;
          }
        } else {
          break_loop = true;
        }
        break;
      case PredicateType::IsNotNull: // Fallthrough intended
      case PredicateType::IsNull:
        break_loop = true;
        break;
      case PredicateType::InList:
        // Since the InList predicate is a sorted vector of values, the last
        // value provides an inclusive upper bound that can be pushed.
        DCHECK(!predicate->raw_values().empty());
        memcpy(row->mutable_cell_ptr(*col_idx_it), predicate->raw_values().back(), size);
        pushed_predicates++;
        break;
      case PredicateType::None:
        LOG(FATAL) << "NONE predicate can not be pushed into key";
    }
  }

  // If no predicates were pushed, no need to do any more work.
  if (pushed_predicates == 0) { return 0; }

  // Step 2: If the upper bound is inclusive, increment it to become exclusive.
  if (is_inclusive_bound) {
    if (!IncrementKey(first, std::next(first, pushed_predicates), row, arena)) {
      // If the increment fails then this bound is is not constraining the keyspace.
      return 0;
    }
  }

  // Step 3: Fill the remaining columns without predicates with the min value.
  SetKeyToMinValues(std::next(first, pushed_predicates), last, row);
  return pushed_predicates;
}

} // anonymous namespace

int PushLowerBoundKeyPredicates(const vector<int32_t>& col_idxs,
                                const unordered_map<string, ColumnPredicate>& predicates,
                                ContiguousRow* row,
                                Arena* arena) {
  return PushLowerBoundKeyPredicates(col_idxs.begin(), col_idxs.end(), predicates, row, arena);
}

int PushUpperBoundKeyPredicates(const vector<int32_t>& col_idxs,
                                const unordered_map<string, ColumnPredicate>& predicates,
                                ContiguousRow* row,
                                Arena* arena) {
  return PushUpperBoundKeyPredicates(col_idxs.begin(), col_idxs.end(), predicates, row, arena);
}

int PushLowerBoundPrimaryKeyPredicates(const unordered_map<string, ColumnPredicate>& predicates,
                                       ContiguousRow* row,
                                       Arena* arena) {
  int32_t num_pk_cols = row->schema()->num_key_columns();
  return PushLowerBoundKeyPredicates(boost::make_counting_iterator(0),
                                     boost::make_counting_iterator(num_pk_cols),
                                     predicates,
                                     row,
                                     arena);
}

int PushUpperBoundPrimaryKeyPredicates(const unordered_map<string, ColumnPredicate>& predicates,
                                       ContiguousRow* row,
                                       Arena* arena) {
  int32_t num_pk_cols = row->schema()->num_key_columns();
  return PushUpperBoundKeyPredicates(boost::make_counting_iterator(0),
                                     boost::make_counting_iterator(num_pk_cols),
                                     predicates,
                                     row,
                                     arena);
}

}
}
