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

#include "kudu/common/partition_pruner.h"

#include <algorithm>
#include <boost/optional.hpp>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "kudu/common/key_encoder.h"
#include "kudu/common/key_util.h"
#include "kudu/common/partition.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"

using boost::optional;
using std::distance;
using std::find;
using std::get;
using std::iota;
using std::lower_bound;
using std::make_tuple;
using std::memcpy;
using std::min;
using std::move;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace {

// Returns true if the partition schema's range columns are a prefix of the
// primary key columns.
bool AreRangeColumnsPrefixOfPrimaryKey(const Schema& schema,
                                       const vector<ColumnId>& range_columns) {
  CHECK(range_columns.size() <= schema.num_key_columns());
  for (int32_t col_idx = 0; col_idx < range_columns.size(); col_idx++) {
    if (schema.column_id(col_idx) != range_columns[col_idx]) {
      return false;
    }
  }
  return true;
}

// Translates the scan primary key bounds into range keys. This should only be
// used when the range columns are a prefix of the primary key columns.
void EncodeRangeKeysFromPrimaryKeyBounds(const Schema& schema,
                                         const ScanSpec& scan_spec,
                                         size_t num_range_columns,
                                         string* range_key_start,
                                         string* range_key_end) {
  if (scan_spec.lower_bound_key() == nullptr && scan_spec.exclusive_upper_bound_key() == nullptr) {
    // Don't bother if there are no lower and upper PK bounds
    return;
  }

  if (num_range_columns == schema.num_key_columns()) {
    // The range columns are the primary key columns, so the range key is the
    // primary key.
    if (scan_spec.lower_bound_key() != nullptr) {
      *range_key_start = scan_spec.lower_bound_key()->encoded_key().ToString();
    }
    if (scan_spec.exclusive_upper_bound_key() != nullptr) {
      *range_key_end = scan_spec.exclusive_upper_bound_key()->encoded_key().ToString();
    }
  } else {
    // The range columns are a prefix of the primary key columns. Copy
    // the column values over to a row, and then encode the row as a range key.

    vector<int32_t> col_idxs(num_range_columns);
    iota(col_idxs.begin(), col_idxs.end(), 0);

    // TODO: here assuming that PK starts at the front of the key.
    unique_ptr<uint8_t[]> buf(new uint8_t[schema.key_byte_size()]);
    ContiguousRow row(&schema, buf.get());

    if (scan_spec.lower_bound_key() != nullptr) {
      for (int32_t idx : col_idxs) {
        memcpy(row.mutable_cell_ptr(idx),
               scan_spec.lower_bound_key()->raw_keys()[idx],
               schema.column(idx).type_info()->size());
      }
      key_util::EncodeKey(col_idxs, row, range_key_start);
    }

    if (scan_spec.exclusive_upper_bound_key() != nullptr) {
      for (int32_t idx : col_idxs) {
        memcpy(row.mutable_cell_ptr(idx),
               scan_spec.exclusive_upper_bound_key()->raw_keys()[idx],
               schema.column(idx).type_info()->size());
      }
      key_util::EncodeKey(col_idxs, row, range_key_end);
    }
  }
}

// Push the scan predicates into the range keys.
void EncodeRangeKeysFromPredicates(const Schema& schema,
                                   const unordered_map<string, ColumnPredicate>& predicates,
                                   const vector<ColumnId>& range_columns,
                                   string* range_key_start,
                                   string* range_key_end) {
  // Find the column indexes of the range columns.
  vector<int32_t> col_idxs;
  col_idxs.reserve(range_columns.size());
  for (ColumnId column : range_columns) {
    int32_t col_idx = schema.find_column_by_id(column);
    CHECK(col_idx != Schema::kColumnNotFound);
    CHECK(schema.is_key_column(col_idx));
    col_idxs.push_back(col_idx);
  }

  // Arenas must be at least the minimum chunk size, and we require at least
  // enough space for the range key columns.
  Arena arena(max<size_t>(Arena::kMinimumChunkSize, schema.key_byte_size()), 4096);
  uint8_t* buf = static_cast<uint8_t*>(CHECK_NOTNULL(arena.AllocateBytes(schema.key_byte_size())));
  ContiguousRow row(&schema, buf);

  if (key_util::PushLowerBoundKeyPredicates(col_idxs, predicates, &row, &arena) > 0) {
    key_util::EncodeKey(col_idxs, row, range_key_start);
  }

  if (key_util::PushUpperBoundKeyPredicates(col_idxs, predicates, &row, &arena) > 0) {
    key_util::EncodeKey(col_idxs, row, range_key_end);
  }
}
} // anonymous namespace

void PartitionPruner::Init(const Schema& schema,
                           const PartitionSchema& partition_schema,
                           const ScanSpec& scan_spec) {
  // If we can already short circuit the scan we don't need to bother with
  // partition pruning. This also allows us to assume some invariants of the
  // scan spec, such as no None predicates and that the lower bound PK < upper
  // bound PK.
  if (scan_spec.CanShortCircuit()) { return; }

  // Build a set of partition key ranges which cover the tablets necessary for
  // the scan.
  //
  // Example predicate sets and resulting partition key ranges, based on the
  // following tablet schema:
  //
  // CREATE TABLE t (a INT32, b INT32, c INT32) PRIMARY KEY (a, b, c)
  // DISTRIBUTE BY RANGE (c)
  //               HASH (a) INTO 2 BUCKETS
  //               HASH (b) INTO 3 BUCKETS;
  //
  // Assume that hash(0) = 0 and hash(2) = 2.
  //
  // | Predicates | Partition Key Ranges                                   |
  // +------------+--------------------------------------------------------+
  // | a = 0      | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
  // | b = 2      |                                                        |
  // | c = 0      |                                                        |
  // +------------+--------------------------------------------------------+
  // | a = 0      | [(bucket=0, bucket=2), (bucket=0, bucket=3))           |
  // | b = 2      |                                                        |
  // +------------+--------------------------------------------------------+
  // | a = 0      | [(bucket=0, bucket=0, c=0), (bucket=0, bucket=0, c=1)) |
  // | c = 0      | [(bucket=0, bucket=1, c=0), (bucket=0, bucket=1, c=1)) |
  // |            | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
  // +------------+--------------------------------------------------------+
  // | b = 2      | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
  // | c = 0      | [(bucket=1, bucket=2, c=0), (bucket=1, bucket=2, c=1)) |
  // +------------+--------------------------------------------------------+
  // | a = 0      | [(bucket=0), (bucket=1))                               |
  // +------------+--------------------------------------------------------+
  // | b = 2      | [(bucket=0, bucket=2), (bucket=0, bucket=3))           |
  // |            | [(bucket=1, bucket=2), (bucket=1, bucket=3))           |
  // +------------+--------------------------------------------------------+
  // | c = 0      | [(bucket=0, bucket=0, c=0), (bucket=0, bucket=0, c=1)) |
  // |            | [(bucket=0, bucket=1, c=0), (bucket=0, bucket=1, c=1)) |
  // |            | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
  // |            | [(bucket=1, bucket=0, c=0), (bucket=1, bucket=0, c=1)) |
  // |            | [(bucket=1, bucket=1, c=0), (bucket=1, bucket=1, c=1)) |
  // |            | [(bucket=1, bucket=2, c=0), (bucket=1, bucket=2, c=1)) |
  // +------------+--------------------------------------------------------+
  // | None       | [(), ())                                               |
  //
  // If the partition key is considered as a sequence of the hash bucket
  // components and a range component, then a few patterns emerge from the
  // examples above:
  //
  // 1) The partition keys are truncated after the final constrained component
  //    (hash bucket components are constrained when the scan is limited to a
  //    single bucket via equality predicates on that component, while range
  //    components are constrained if they have an upper or lower bound via
  //    range or equality predicates on that component).
  //
  // 2) If the final constrained component is a hash bucket, then the
  //    corresponding bucket in the upper bound is incremented in order to make
  //    it an exclusive key.
  //
  // 3) The number of partition key ranges in the result is equal to the product
  //    of the number of buckets of each unconstrained hash component which come
  //    before a final constrained component. If there are no unconstrained hash
  //    components, then the number of partition key ranges is one.

  // Step 1: Build the range portion of the partition key.
  string range_lower_bound;
  string range_upper_bound;
  const vector<ColumnId>& range_columns = partition_schema.range_schema_.column_ids;
  if (!range_columns.empty()) {
    if (AreRangeColumnsPrefixOfPrimaryKey(schema, range_columns)) {
      EncodeRangeKeysFromPrimaryKeyBounds(schema,
                                          scan_spec,
                                          range_columns.size(),
                                          &range_lower_bound,
                                          &range_upper_bound);
    } else {
      EncodeRangeKeysFromPredicates(schema,
                                    scan_spec.predicates(),
                                    range_columns,
                                    &range_lower_bound,
                                    &range_upper_bound);
    }
  }

  // Step 2: Create the hash bucket portion of the partition key.

  // The list of hash buckets per hash component, or none if the component is
  // not constrained.
  vector<optional<uint32_t>> hash_buckets;
  hash_buckets.reserve(partition_schema.hash_bucket_schemas_.size());
  for (int hash_idx = 0; hash_idx < partition_schema.hash_bucket_schemas_.size(); hash_idx++) {
    const auto& hash_bucket_schema = partition_schema.hash_bucket_schemas_[hash_idx];
    string encoded_columns;
    bool can_prune = true;
    for (int col_offset = 0; col_offset < hash_bucket_schema.column_ids.size(); col_offset++) {
      const ColumnSchema& column = schema.column_by_id(hash_bucket_schema.column_ids[col_offset]);
      const ColumnPredicate* predicate = FindOrNull(scan_spec.predicates(), column.name());
      if (predicate == nullptr || predicate->predicate_type() != PredicateType::Equality) {
        can_prune = false;
        break;
      }

      const KeyEncoder<string>& encoder = GetKeyEncoder<string>(column.type_info());
      encoder.Encode(predicate->raw_lower(),
                     col_offset + 1 == hash_bucket_schema.column_ids.size(),
                     &encoded_columns);
    }
    if (can_prune) {
      hash_buckets.push_back(partition_schema.BucketForEncodedColumns(encoded_columns,
                                                                      hash_bucket_schema));
    } else {
      hash_buckets.push_back(boost::none);
    }
  }

  // The index of the final constrained component in the partition key.
  int constrained_index;
  if (!range_lower_bound.empty() || !range_upper_bound.empty()) {
    // The range component is constrained.
    constrained_index = partition_schema.hash_bucket_schemas_.size();
  } else {
    // Search the hash bucket constraints from right to left, looking for the
    // first constrained component.
    constrained_index = partition_schema.hash_bucket_schemas_.size() -
                        distance(hash_buckets.rbegin(),
                                 find_if(hash_buckets.rbegin(),
                                         hash_buckets.rend(),
                                         [] (const optional<uint32_t>& x) { return x; }));
  }

  // Build up a set of partition key ranges out of the hash components.
  //
  // Each constrained hash component simply appends its bucket number to the
  // partition key ranges (possibly incrementing the upper bound by one bucket
  // number if this is the final constraint, see note 2 in the example above).
  //
  // Each unconstrained hash component results in creating a new partition key
  // range for each bucket of the hash component.
  vector<tuple<string, string>> partition_key_ranges(1);
  const KeyEncoder<string>& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));
  for (int hash_idx = 0; hash_idx < constrained_index; hash_idx++) {
    // This is the final partition key component if this is the final constrained
    // bucket, and the range upper bound is empty. In this case we need to
    // increment the bucket on the upper bound to convert from inclusive to
    // exclusive.
    bool is_last = hash_idx + 1 == constrained_index && range_upper_bound.empty();

    if (hash_buckets[hash_idx]) {
      // This hash component is constrained by equality predicates to a single
      // hash bucket.
      uint32_t bucket = *hash_buckets[hash_idx];
      uint32_t bucket_upper = is_last ? bucket + 1 : bucket;
      for (auto& partition_key_range : partition_key_ranges) {
        hash_encoder.Encode(&bucket, &get<0>(partition_key_range));
        hash_encoder.Encode(&bucket_upper, &get<1>(partition_key_range));
      }
    } else {
      const auto& hash_bucket_schema = partition_schema.hash_bucket_schemas_[hash_idx];
      // Add a partition key range for each possible hash bucket.
      vector<tuple<string, string>> new_partition_key_ranges;
      for (const auto& partition_key_range : partition_key_ranges) {
        for (uint32_t bucket = 0; bucket < hash_bucket_schema.num_buckets; bucket++) {
          uint32_t bucket_upper = is_last ? bucket + 1 : bucket;
          string lower = get<0>(partition_key_range);
          string upper = get<1>(partition_key_range);
          hash_encoder.Encode(&bucket, &lower);
          hash_encoder.Encode(&bucket_upper, &upper);
          new_partition_key_ranges.push_back(make_tuple(move(lower), move(upper)));
        }
      }
      partition_key_ranges.swap(new_partition_key_ranges);
    }
  }

  // Step 3: append the (possibly empty) range bounds to the partition key ranges.
  for (auto& range : partition_key_ranges) {
    get<0>(range).append(range_lower_bound);
    get<1>(range).append(range_upper_bound);
  }

  // Step 4: remove all partition key ranges past the scan spec's upper bound partition key.
  if (!scan_spec.exclusive_upper_bound_partition_key().empty()) {
    for (auto range = partition_key_ranges.rbegin();
         range != partition_key_ranges.rend();
         range++) {
      if (!get<1>(*range).empty() &&
          scan_spec.exclusive_upper_bound_partition_key() >= get<1>(*range)) {
        break;
      }
      if (scan_spec.exclusive_upper_bound_partition_key() <= get<0>(*range)) {
        partition_key_ranges.pop_back();
      } else {
        get<1>(*range) = scan_spec.exclusive_upper_bound_partition_key();
      }
    }
  }

  // Step 5: Reverse the order of the partition key ranges, so that it is
  // efficient to remove the partition key ranges from the vector in ascending order.
  partition_key_ranges_.resize(partition_key_ranges.size());
  move(partition_key_ranges.rbegin(), partition_key_ranges.rend(), partition_key_ranges_.begin());

  // Step 6: Remove all partition key ranges before the scan spec's lower bound partition key.
  if (!scan_spec.lower_bound_partition_key().empty()) {
    RemovePartitionKeyRange(scan_spec.lower_bound_partition_key());
  }
}

bool PartitionPruner::HasMorePartitionKeyRanges() const {
  return !partition_key_ranges_.empty();
}

const string& PartitionPruner::NextPartitionKey() const {
  CHECK(HasMorePartitionKeyRanges());
  return get<0>(partition_key_ranges_.back());
}

void PartitionPruner::RemovePartitionKeyRange(const string& upper_bound) {
  if (upper_bound.empty()) {
    partition_key_ranges_.clear();
    return;
  }

  for (auto range = partition_key_ranges_.rbegin();
       range != partition_key_ranges_.rend();
       range++) {
    if (upper_bound <= get<0>(*range)) { break; }
    if (get<1>(*range).empty() || upper_bound < get<1>(*range)) {
      get<0>(*range) = upper_bound;
    } else {
      partition_key_ranges_.pop_back();
    }
  }
}

bool PartitionPruner::ShouldPrune(const Partition& partition) const {
  // range is an iterator that points to the first partition key range which
  // overlaps or is greater than the partition.
  auto range = lower_bound(partition_key_ranges_.rbegin(), partition_key_ranges_.rend(), partition,
    [] (const tuple<string, string>& scan_range, const Partition& partition) {
      // return true if scan_range < partition
      const string& scan_upper = get<1>(scan_range);
      return !scan_upper.empty() && scan_upper <= partition.partition_key_start();
    });

  return range == partition_key_ranges_.rend() ||
         (!partition.partition_key_end().empty() &&
          partition.partition_key_end() <= get<0>(*range));
}

string PartitionPruner::ToString(const Schema& schema,
                                 const PartitionSchema& partition_schema) const {
  vector<string> strings;
  for (auto range = partition_key_ranges_.rbegin();
       range != partition_key_ranges_.rend();
       range++) {
    strings.push_back(strings::Substitute(
          "[($0), ($1))",
          get<0>(*range).empty() ? "<start>" :
              partition_schema.PartitionKeyDebugString(get<0>(*range), schema),
          get<1>(*range).empty() ? "<end>" :
              partition_schema.PartitionKeyDebugString(get<1>(*range), schema)));
  }

  return JoinStrings(strings, ", ");
}

} // namespace kudu
