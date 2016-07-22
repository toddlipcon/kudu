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

#include "kudu/common/partition.h"

#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/hash_util.h"

using std::pair;
using std::set;
using std::string;
using std::vector;

namespace kudu {

using google::protobuf::RepeatedPtrField;
using strings::Substitute;
using strings::SubstituteAndAppend;

// The encoded size of a hash bucket in a partition key.
static const size_t kEncodedBucketSize = sizeof(uint32_t);

Slice Partition::range_key_start() const {
  return range_key(partition_key_start());
}

Slice Partition::range_key_end() const {
  return range_key(partition_key_end());
}

Slice Partition::range_key(const string& partition_key) const {
  size_t hash_size = kEncodedBucketSize * hash_buckets().size();
  if (partition_key.size() > hash_size) {
    Slice s = Slice(partition_key);
    s.remove_prefix(hash_size);
    return s;
  } else {
    return Slice();
  }
}

void Partition::ToPB(PartitionPB* pb) const {
  pb->Clear();
  pb->mutable_hash_buckets()->Reserve(hash_buckets_.size());
  for (int32_t bucket : hash_buckets()) {
    pb->add_hash_buckets(bucket);
  }
  pb->set_partition_key_start(partition_key_start());
  pb->set_partition_key_end(partition_key_end());
}

void Partition::FromPB(const PartitionPB& pb, Partition* partition) {
  partition->hash_buckets_.clear();
  partition->hash_buckets_.reserve(pb.hash_buckets_size());
  for (int32_t hash_bucket : pb.hash_buckets()) {
    partition->hash_buckets_.push_back(hash_bucket);
  }

  partition->partition_key_start_ = pb.partition_key_start();
  partition->partition_key_end_ = pb.partition_key_end();
}

namespace {
// Extracts the column IDs from a protobuf repeated field of column identifiers.
Status ExtractColumnIds(const RepeatedPtrField<PartitionSchemaPB_ColumnIdentifierPB>& identifiers,
                        const Schema& schema,
                        vector<ColumnId>* column_ids) {
    column_ids->reserve(identifiers.size());
    for (PartitionSchemaPB_ColumnIdentifierPB identifier : identifiers) {
      switch (identifier.identifier_case()) {
        case PartitionSchemaPB_ColumnIdentifierPB::kId: {
          ColumnId column_id(identifier.id());
          if (schema.find_column_by_id(column_id) == Schema::kColumnNotFound) {
            return Status::InvalidArgument("unknown column id", identifier.DebugString());
          }
          column_ids->push_back(column_id);
          continue;
        }
        case PartitionSchemaPB_ColumnIdentifierPB::kName: {
          int32_t column_idx = schema.find_column(identifier.name());
          if (column_idx == Schema::kColumnNotFound) {
            return Status::InvalidArgument("unknown column", identifier.DebugString());
          }
          column_ids->push_back(schema.column_id(column_idx));
          continue;
        }
        default: return Status::InvalidArgument("unknown column", identifier.DebugString());
      }
    }
    return Status::OK();
}
// Sets a repeated field of column identifiers to the provided column IDs.
void SetColumnIdentifiers(const vector<ColumnId>& column_ids,
                          RepeatedPtrField<PartitionSchemaPB_ColumnIdentifierPB>* identifiers) {
    identifiers->Reserve(column_ids.size());
    for (ColumnId column_id : column_ids) {
      identifiers->Add()->set_id(column_id);
    }
}
} // namespace

Status PartitionSchema::FromPB(const PartitionSchemaPB& pb,
                               const Schema& schema,
                               PartitionSchema* partition_schema) {
  partition_schema->Clear();

  for (const PartitionSchemaPB_HashBucketSchemaPB& hash_bucket_pb : pb.hash_bucket_schemas()) {
    HashBucketSchema hash_bucket;
    RETURN_NOT_OK(ExtractColumnIds(hash_bucket_pb.columns(), schema, &hash_bucket.column_ids));

    // Hashing is column-order dependent, so sort the column_ids to ensure that
    // hash components with the same columns hash consistently. This is
    // important when deserializing a user-supplied partition schema during
    // table creation; after that the columns should remain in sorted order.
    std::sort(hash_bucket.column_ids.begin(), hash_bucket.column_ids.end());

    hash_bucket.seed = hash_bucket_pb.seed();
    hash_bucket.num_buckets = hash_bucket_pb.num_buckets();
    partition_schema->hash_bucket_schemas_.push_back(hash_bucket);
  }

  if (pb.has_range_schema()) {
    const PartitionSchemaPB_RangeSchemaPB& range_pb = pb.range_schema();
    RETURN_NOT_OK(ExtractColumnIds(range_pb.columns(), schema,
                                   &partition_schema->range_schema_.column_ids));
  } else {
    // Fill in the default range partition (PK columns).
    // like the sorting above, this should only happen during table creation
    // while deserializing the user-provided partition schema.
    for (int column_idx : schema.key_column_indexes()) {
      partition_schema->range_schema_.column_ids.push_back(schema.column_id(column_idx));
    }
  }

  return partition_schema->Validate(schema);
}

void PartitionSchema::ToPB(PartitionSchemaPB* pb) const {
  pb->Clear();
  pb->mutable_hash_bucket_schemas()->Reserve(hash_bucket_schemas_.size());
  for (const HashBucketSchema& hash_bucket : hash_bucket_schemas_) {
    PartitionSchemaPB_HashBucketSchemaPB* hash_bucket_pb = pb->add_hash_bucket_schemas();
    SetColumnIdentifiers(hash_bucket.column_ids, hash_bucket_pb->mutable_columns());
    hash_bucket_pb->set_num_buckets(hash_bucket.num_buckets);
    hash_bucket_pb->set_seed(hash_bucket.seed);
  }

  SetColumnIdentifiers(range_schema_.column_ids, pb->mutable_range_schema()->mutable_columns());
}

Status PartitionSchema::EncodeKey(const KuduPartialRow& row, string* buf) const {
  const KeyEncoder<string>& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));

  for (const HashBucketSchema& hash_bucket_schema : hash_bucket_schemas_) {
    int32_t bucket;
    RETURN_NOT_OK(BucketForRow(row, hash_bucket_schema, &bucket));
    hash_encoder.Encode(&bucket, buf);
  }

  return EncodeColumns(row, range_schema_.column_ids, buf);
}

Status PartitionSchema::EncodeKey(const ConstContiguousRow& row, string* buf) const {
  const KeyEncoder<string>& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));

  for (const HashBucketSchema& hash_bucket_schema : hash_bucket_schemas_) {
    int32_t bucket;
    RETURN_NOT_OK(BucketForRow(row, hash_bucket_schema, &bucket));
    hash_encoder.Encode(&bucket, buf);
  }

  return EncodeColumns(row, range_schema_.column_ids, buf);
}

Status PartitionSchema::EncodeRangeKey(const KuduPartialRow& row,
                                       const Schema& schema,
                                       string* key) const {
  DCHECK(key->empty());
  bool contains_no_columns = true;
  for (int column_idx = 0; column_idx < schema.num_columns(); column_idx++) {
    const ColumnSchema& column = schema.column(column_idx);
    if (row.IsColumnSet(column_idx)) {
      if (std::find(range_schema_.column_ids.begin(),
                    range_schema_.column_ids.end(),
                    schema.column_id(column_idx)) != range_schema_.column_ids.end()) {
        contains_no_columns = false;
      } else {
        return Status::InvalidArgument(
            "split rows may only contain values for range partitioned columns", column.name());
      }
    }
  }

  if (contains_no_columns) {
    return Status::OK();
  }
  return EncodeColumns(row, range_schema_.column_ids, key);
}

Status PartitionSchema::EncodeRangeSplits(const vector<KuduPartialRow>& split_rows,
                                          const Schema& schema,
                                          vector<string>* splits) const {
  DCHECK(splits->empty());
  for (const KuduPartialRow& row : split_rows) {
    string split;
    RETURN_NOT_OK(EncodeRangeKey(row, schema, &split));
    if (split.empty()) {
        return Status::InvalidArgument(
            "split rows must contain a value for at least one range partition column");
    }
    splits->emplace_back(std::move(split));
  }

  std::sort(splits->begin(), splits->end());
  auto unique_end = std::unique(splits->begin(), splits->end());
  if (unique_end != splits->end()) {
    return Status::InvalidArgument("duplicate split row", RangeKeyDebugString(*unique_end, schema));
  }
  return Status::OK();
}

Status PartitionSchema::EncodeRangeBounds(const vector<pair<KuduPartialRow,
                                                            KuduPartialRow>>& range_bounds,
                                          const Schema& schema,
                                          vector<pair<string, string>>* range_partitions) const {
  DCHECK(range_partitions->empty());
  if (range_bounds.empty()) {
    range_partitions->emplace_back("", "");
    return Status::OK();
  }

  for (const auto& bound : range_bounds) {
    string lower;
    string upper;
    RETURN_NOT_OK(EncodeRangeKey(bound.first, schema, &lower));
    RETURN_NOT_OK(EncodeRangeKey(bound.second, schema, &upper));

    if (!lower.empty() && !upper.empty() && lower >= upper) {
      return Status::InvalidArgument(
          "range bound has lower bound equal to or above the upper bound",
          strings::Substitute("lower bound: ($0), upper bound: ($1)",
                              bound.first.ToString(),
                              bound.second.ToString()));
    }
    range_partitions->emplace_back(std::move(lower), std::move(upper));
  }

  // Check that the range bounds are non-overlapping
  std::sort(range_partitions->begin(), range_partitions->end());
  for (int i = 0; i < range_partitions->size() - 1; i++) {
    const string& first_upper = range_partitions->at(i).second;
    const string& second_lower = range_partitions->at(i + 1).first;

    if (first_upper.empty() || second_lower.empty() || first_upper > second_lower) {
      return Status::InvalidArgument(
          "overlapping range bounds",
          strings::Substitute("first upper bound: ($0), second lower bound: ($1)",
                              RangeKeyDebugString(first_upper, schema),
                              RangeKeyDebugString(second_lower, schema)));
    }
  }

  return Status::OK();
}

Status PartitionSchema::SplitRangeBounds(const Schema& schema,
                                         vector<string> splits,
                                         vector<pair<string, string>>* bounds) const {
  int expected_bounds = std::max(1UL, bounds->size()) + splits.size();

  vector<pair<string, string>> new_bounds;
  new_bounds.reserve(expected_bounds);

  // Iterate through the sorted bounds and sorted splits, splitting the bounds
  // as appropriate and adding them to the result list ('new_bounds').

  auto split = splits.begin();
  for (auto& bound : *bounds) {
    string& lower = bound.first;
    const string& upper = bound.second;

    for (; split != splits.end() && (upper.empty() || *split <= upper); split++) {
      if (!lower.empty() && *split < lower) {
        return Status::InvalidArgument("split out of bounds", RangeKeyDebugString(*split, schema));
      } else if (lower == *split || upper == *split) {
        return Status::InvalidArgument("split matches lower or upper bound",
                                       RangeKeyDebugString(*split, schema));
      }
      // Split the current bound. Add the lower section to the result list,
      // and continue iterating on the upper section.
      new_bounds.emplace_back(std::move(lower), *split);
      lower = std::move(*split);
    }

    new_bounds.emplace_back(std::move(lower), std::move(upper));
  }

  if (split != splits.end()) {
    return Status::InvalidArgument("split out of bounds", RangeKeyDebugString(*split, schema));
  }

  bounds->swap(new_bounds);
  CHECK_EQ(expected_bounds, bounds->size());
  return Status::OK();
}

Status PartitionSchema::CreatePartitions(const vector<KuduPartialRow>& split_rows,
                                         const vector<pair<KuduPartialRow,
                                                           KuduPartialRow>>& range_bounds,
                                         const Schema& schema,
                                         vector<Partition>* partitions) const {
  const KeyEncoder<string>& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));

  // Create a partition per hash bucket combination.
  *partitions = vector<Partition>(1);
  for (const HashBucketSchema& bucket_schema : hash_bucket_schemas_) {
    vector<Partition> new_partitions;
    // For each of the partitions created so far, replicate it
    // by the number of buckets in the next hash bucketing component
    for (const Partition& base_partition : *partitions) {
      for (int32_t bucket = 0; bucket < bucket_schema.num_buckets; bucket++) {
        Partition partition = base_partition;
        partition.hash_buckets_.push_back(bucket);
        hash_encoder.Encode(&bucket, &partition.partition_key_start_);
        hash_encoder.Encode(&bucket, &partition.partition_key_end_);
        new_partitions.push_back(partition);
      }
    }
    partitions->swap(new_partitions);
  }

  unordered_set<int> range_column_idxs;
  for (ColumnId column_id : range_schema_.column_ids) {
    int column_idx = schema.find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument(Substitute("range partition column ID $0 "
                                                "not found in table schema.", column_id));
    }
    if (!InsertIfNotPresent(&range_column_idxs, column_idx)) {
      return Status::InvalidArgument("duplicate column in range partition",
                                     schema.column(column_idx).name());
    }
  }

  vector<pair<string, string>> bounds;
  vector<string> splits;
  RETURN_NOT_OK(EncodeRangeBounds(range_bounds, schema, &bounds));
  RETURN_NOT_OK(EncodeRangeSplits(split_rows, schema, &splits));
  RETURN_NOT_OK(SplitRangeBounds(schema, std::move(splits), &bounds));

  // Create a partition per range bound and hash bucket combination.
  vector<Partition> new_partitions;
  for (const Partition& base_partition : *partitions) {
    for (const auto& bound : bounds) {
      Partition partition = base_partition;
      partition.partition_key_start_.append(bound.first);
      partition.partition_key_end_.append(bound.second);
      new_partitions.push_back(partition);
    }
  }
  partitions->swap(new_partitions);

  // Note: the following discussion and logic only takes effect when the table's
  // partition schema includes at least one hash bucket component, and the
  // absolute upper and/or absolute lower range bound is unbounded.
  //
  // At this point, we have the full set of partitions built up, but each
  // partition only covers a finite slice of the partition key-space. Some
  // operations involving partitions are easier (pruning, client meta cache) if
  // it can be assumed that the partition keyspace does not have holes.
  //
  // In order to 'fill in' the partition key space, the absolute first and last
  // partitions are extended to cover the rest of the lower and upper partition
  // range by clearing the start and end partition key, respectively.
  //
  // When the table has two or more hash components, there will be gaps in
  // between partitions at the boundaries of the component ranges. Similar to
  // the absolute start and end case, these holes are filled by clearing the
  // partition key beginning at the hash component. For a concrete example,
  // see PartitionTest::TestCreatePartitions.
  for (Partition& partition : *partitions) {
    if (partition.range_key_start().empty()) {
      for (int i = partition.hash_buckets().size() - 1; i >= 0; i--) {
        if (partition.hash_buckets()[i] != 0) {
          break;
        }
        partition.partition_key_start_.erase(kEncodedBucketSize * i);
      }
    }
    if (partition.range_key_end().empty()) {
      for (int i = partition.hash_buckets().size() - 1; i >= 0; i--) {
        partition.partition_key_end_.erase(kEncodedBucketSize * i);
        int32_t hash_bucket = partition.hash_buckets()[i] + 1;
        if (hash_bucket != hash_bucket_schemas_[i].num_buckets) {
          hash_encoder.Encode(&hash_bucket, &partition.partition_key_end_);
          break;
        }
      }
    }
  }

  return Status::OK();
}

template<typename Row>
Status PartitionSchema::PartitionContainsRowImpl(const Partition& partition,
                                                 const Row& row,
                                                 bool* contains) const {
  CHECK_EQ(partition.hash_buckets().size(), hash_bucket_schemas_.size());
  for (int i = 0; i < hash_bucket_schemas_.size(); i++) {
    const HashBucketSchema& hash_bucket_schema = hash_bucket_schemas_[i];
    int32_t bucket;
    RETURN_NOT_OK(BucketForRow(row, hash_bucket_schema, &bucket));

    if (bucket != partition.hash_buckets()[i]) {
      *contains = false;
      return Status::OK();
    }
  }

  string range_partition_key;
  RETURN_NOT_OK(EncodeColumns(row, range_schema_.column_ids, &range_partition_key));

  // If all of the hash buckets match, then the row is contained in the
  // partition if the row is gte the lower bound; and if there is no upper
  // bound, or the row is lt the upper bound.
  *contains = (Slice(range_partition_key).compare(partition.range_key_start()) >= 0)
           && (partition.range_key_end().empty()
                || Slice(range_partition_key).compare(partition.range_key_end()) < 0);

  return Status::OK();
}

Status PartitionSchema::PartitionContainsRow(const Partition& partition,
                                             const KuduPartialRow& row,
                                             bool* contains) const {
  return PartitionContainsRowImpl(partition, row, contains);
}

Status PartitionSchema::PartitionContainsRow(const Partition& partition,
                                             const ConstContiguousRow& row,
                                             bool* contains) const {
  return PartitionContainsRowImpl(partition, row, contains);
}


Status PartitionSchema::DecodeRangeKey(Slice* encoded_key,
                                       KuduPartialRow* row,
                                       Arena* arena) const {
  ContiguousRow cont_row(row->schema(), row->row_data_);
  for (int i = 0; i < range_schema_.column_ids.size(); i++) {

    if (encoded_key->empty()) {
      // This can happen when decoding partition start and end keys, since they
      // are truncated to simulate absolute upper and lower bounds.
      continue;
    }

    int32_t column_idx = row->schema()->find_column_by_id(range_schema_.column_ids[i]);
    const ColumnSchema& column = row->schema()->column(column_idx);
    const KeyEncoder<faststring>& key_encoder = GetKeyEncoder<faststring>(column.type_info());
    bool is_last = i == (range_schema_.column_ids.size() - 1);

    // Decode the column.
    RETURN_NOT_OK_PREPEND(key_encoder.Decode(encoded_key,
                                             is_last,
                                             arena,
                                             cont_row.mutable_cell_ptr(column_idx)),
                          Substitute("Error decoding partition key range component '$0'",
                                     column.name()));
    // Mark the column as set.
    BitmapSet(row->isset_bitmap_, column_idx);
  }
  if (!encoded_key->empty()) {
    return Status::InvalidArgument("unable to fully decode partition key range components");
  }
  return Status::OK();
}

// Decodes a slice of a partition key into the buckets. The slice is modified to
// remove the hash components.
Status PartitionSchema::DecodeHashBuckets(Slice* encoded_key,
                                          vector<int32_t>* buckets) const {
  size_t hash_components_size = kEncodedBucketSize * hash_bucket_schemas_.size();
  if (encoded_key->size() < hash_components_size) {
    return Status::InvalidArgument(
        Substitute("expected encoded hash key to be at least $0 bytes (only found $1)",
                   hash_components_size, encoded_key->size()));
  }
  for (const auto& schema : hash_bucket_schemas_) {
    (void) schema; // quiet unused variable warning
    uint32_t big_endian;
    memcpy(&big_endian, encoded_key->data(), sizeof(uint32_t));
    buckets->push_back(BigEndian::ToHost32(big_endian));
    encoded_key->remove_prefix(sizeof(uint32_t));
  }

  return Status::OK();
}

string PartitionSchema::PartitionDebugString(const Partition& partition,
                                             const Schema& schema) const {
  string s;

  if (!partition.hash_buckets().empty()) {
    vector<string> components;
    for (int32_t bucket : partition.hash_buckets()) {
      components.push_back(Substitute("$0", bucket));
    }
    s.append("hash buckets: (");
    s.append(JoinStrings(components, ", "));
    if (!range_schema_.column_ids.empty()) {
      s.append("), ");
    } else {
      s.append(")");
    }
  }

  if (!range_schema_.column_ids.empty()) {
    Arena arena(1024, 128 * 1024);
    KuduPartialRow start_row(&schema);
    KuduPartialRow end_row(&schema);

    s.append("range: [(");

    vector<string> start_components;
    Slice encoded_range_key_start = partition.range_key_start();
    Status status;
    status = DecodeRangeKey(&encoded_range_key_start, &start_row, &arena);
    if (status.ok()) {
      AppendRangeDebugStringComponentsOrString(start_row, "<start>", &start_components);
      s.append(JoinStrings(start_components, ", "));
    } else {
      s.append(Substitute("<decode-error: $0>", status.ToString()));
    }
    s.append("), (");

    vector<string> end_components;
    Slice encoded_range_key_end = partition.range_key_end();
    status = DecodeRangeKey(&encoded_range_key_end, &end_row, &arena);
    if (status.ok()) {
      AppendRangeDebugStringComponentsOrString(end_row, "<end>", &end_components);
      s.append(JoinStrings(end_components, ", "));
    } else {
      s.append(Substitute("<decode-error: $0>", status.ToString()));
    }
    s.append("))");
  }

  return s;
}

void PartitionSchema::AppendRangeDebugStringComponentsOrString(const KuduPartialRow& row,
                                                               const StringPiece default_string,
                                                               vector<string>* components) const {
  ConstContiguousRow const_row(row.schema(), row.row_data_);

  for (ColumnId column_id : range_schema_.column_ids) {
    string column;
    int32_t column_idx = row.schema()->find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      components->push_back("<unknown-column>");
      continue;
    }
    const ColumnSchema& column_schema = row.schema()->column(column_idx);

    if (!row.IsColumnSet(column_idx)) {
      components->push_back(default_string.as_string());
      break;
    } else {
      column_schema.DebugCellAppend(const_row.cell(column_idx), &column);
    }

    components->push_back(column);
  }
}

void PartitionSchema::AppendRangeDebugStringComponentsOrMin(const KuduPartialRow& row,
                                                            vector<string>* components) const {
  ConstContiguousRow const_row(row.schema(), row.row_data_);

  for (ColumnId column_id : range_schema_.column_ids) {
    string column;
    int32_t column_idx = row.schema()->find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      components->push_back("<unknown-column>");
      continue;
    }
    const ColumnSchema& column_schema = row.schema()->column(column_idx);

    if (!row.IsColumnSet(column_idx)) {
      uint8_t min_value[kLargestTypeSize];
      column_schema.type_info()->CopyMinValue(&min_value);
      SimpleConstCell cell(&column_schema, &min_value);
      column_schema.DebugCellAppend(cell, &column);
    } else {
      column_schema.DebugCellAppend(const_row.cell(column_idx), &column);
    }

    components->push_back(column);
  }
}

string PartitionSchema::RowDebugString(const ConstContiguousRow& row) const {
  vector<string> components;

  for (const HashBucketSchema& hash_bucket_schema : hash_bucket_schemas_) {
    int32_t bucket;
    Status s = BucketForRow(row, hash_bucket_schema, &bucket);
    if (s.ok()) {
      components.push_back(Substitute("bucket=$0", bucket));
    } else {
      components.push_back(Substitute("<bucket-error: $0>", s.ToString()));
    }
  }

  for (ColumnId column_id : range_schema_.column_ids) {
    string column;
    int32_t column_idx = row.schema()->find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      components.push_back("<unknown-column>");
      break;
    }
    row.schema()->column(column_idx).DebugCellAppend(row.cell(column_idx), &column);
    components.push_back(column);
  }

  return JoinStrings(components, ", ");
}

string PartitionSchema::RowDebugString(const KuduPartialRow& row) const {
  vector<string> components;

  for (const HashBucketSchema& hash_bucket_schema : hash_bucket_schemas_) {
    int32_t bucket;
    Status s = BucketForRow(row, hash_bucket_schema, &bucket);
    if (s.ok()) {
      components.push_back(Substitute("bucket=$0", bucket));
    } else {
      components.push_back(Substitute("<bucket-error: $0>", s.ToString()));
    }
  }

  AppendRangeDebugStringComponentsOrMin(row, &components);

  return JoinStrings(components, ", ");
}

string PartitionSchema::PartitionKeyDebugString(const string& key, const Schema& schema) const {
  Slice encoded_key = key;

  vector<string> components;

  if (!hash_bucket_schemas_.empty()) {
    vector<int32_t> buckets;
    Status s = DecodeHashBuckets(&encoded_key, &buckets);
    if (!s.ok()) {
      return Substitute("<hash-decode-error: $0>", s.ToString());
    }
    for (int32_t bucket : buckets) {
      components.push_back(Substitute("bucket=$0", bucket));
    }
  }

  if (!range_schema_.column_ids.empty()) {
    Arena arena(1024, 128 * 1024);
    KuduPartialRow row(&schema);

    Status s = DecodeRangeKey(&encoded_key, &row, &arena);
    if (!s.ok()) {
      return Substitute("<range-decode-error: $0>", s.ToString());
    }

    AppendRangeDebugStringComponentsOrMin(row, &components);
  }

  return JoinStrings(components, ", ");
}

string PartitionSchema::RangeKeyDebugString(const string& range_key, const Schema& schema) const {
  Arena arena(1024, 128 * 1024);
  KuduPartialRow row(&schema);
  vector<string> components;

  Slice encoded_key(range_key);
  Status s = DecodeRangeKey(&encoded_key, &row, &arena);
  if (!s.ok()) {
    return Substitute("<range-decode-error: $0>", s.ToString());
  }
  AppendRangeDebugStringComponentsOrMin(row, &components);
  return JoinStrings(components, ", ");
}

namespace {
// Converts a list of column IDs to a string with the column names seperated by
// a comma character.
string ColumnIdsToColumnNames(const Schema& schema,
                              const vector<ColumnId> column_ids) {
  vector<string> names;
  for (ColumnId column_id : column_ids) {
    names.push_back(schema.column(schema.find_column_by_id(column_id)).name());
  }

  return JoinStrings(names, ", ");
}
} // namespace

string PartitionSchema::DebugString(const Schema& schema) const {
  vector<string> component_types;

  if (!hash_bucket_schemas_.empty()) {
    vector<string> hash_components;
    for (const HashBucketSchema& hash_bucket_schema : hash_bucket_schemas_) {
      string component;
      component.append(Substitute("(bucket count: $0", hash_bucket_schema.num_buckets));
      if (hash_bucket_schema.seed != 0) {
        component.append(Substitute(", seed: $0", hash_bucket_schema.seed));
      }
      component.append(Substitute(", columns: [$0])",
                                  ColumnIdsToColumnNames(schema, hash_bucket_schema.column_ids)));
      hash_components.push_back(component);
    }
    component_types.push_back(Substitute("hash bucket components: [$0]",
                                         JoinStrings(hash_components, ", ")));
  }

  if (!range_schema_.column_ids.empty()) {
    component_types.push_back(Substitute("range columns: [$0]",
                                         ColumnIdsToColumnNames(schema, range_schema_.column_ids)));
  }
  return JoinStrings(component_types, ", ");
}

string PartitionSchema::DisplayString(const Schema& schema) const {
  string display_string;

  if (!hash_bucket_schemas_.empty()) {
    display_string.append("Hash bucket schemas:\n");
    for (const HashBucketSchema& hash_bucket_schema : hash_bucket_schemas_) {
      display_string.append("  Key columns:\n");
      for (const ColumnId& col_id : hash_bucket_schema.column_ids) {
        const ColumnSchema& col = schema.column_by_id(col_id);
        SubstituteAndAppend(&display_string, "    $0 $1\n", col.name(), col.type_info()->name());
      }
      SubstituteAndAppend(&display_string, "  Bucket count: $0\n", hash_bucket_schema.num_buckets);
      if (hash_bucket_schema.seed != 0) {
        SubstituteAndAppend(&display_string, "  Seed: $0\n", hash_bucket_schema.seed);
      }
      display_string.append("\n");
    }
  }

  if (!range_schema_.column_ids.empty()) {
    display_string.append("Range columns:\n");
    for (const ColumnId& col_id : range_schema_.column_ids) {
      const ColumnSchema& col = schema.column_by_id(col_id);
      SubstituteAndAppend(&display_string, "  $0 $1\n", col.name(), col.type_info()->name());
    }
  }
  return display_string;
}

bool PartitionSchema::Equals(const PartitionSchema& other) const {
  if (this == &other) return true;

  // Compare range component.
  if (range_schema_.column_ids != other.range_schema_.column_ids) return false;

  // Compare hash bucket components.
  if (hash_bucket_schemas_.size() != other.hash_bucket_schemas_.size()) return false;
  for (int i = 0; i < hash_bucket_schemas_.size(); i++) {
    if (hash_bucket_schemas_[i].seed != other.hash_bucket_schemas_[i].seed) return false;
    if (hash_bucket_schemas_[i].num_buckets
        != other.hash_bucket_schemas_[i].num_buckets) return false;
    if (hash_bucket_schemas_[i].column_ids
        != other.hash_bucket_schemas_[i].column_ids) return false;
  }

  return true;
}

// Encodes the specified primary key columns of the supplied row into the buffer.
Status PartitionSchema::EncodeColumns(const ConstContiguousRow& row,
                                      const vector<ColumnId>& column_ids,
                                      string* buf) {
  for (int i = 0; i < column_ids.size(); i++) {
    ColumnId column_id = column_ids[i];
    int32_t column_idx = row.schema()->find_column_by_id(column_id);
    const TypeInfo* type = row.schema()->column(column_idx).type_info();
    GetKeyEncoder<string>(type).Encode(row.cell_ptr(column_idx), i + 1 == column_ids.size(), buf);
  }
  return Status::OK();
}

// Encodes the specified primary key columns of the supplied row into the buffer.
Status PartitionSchema::EncodeColumns(const KuduPartialRow& row,
                                      const vector<ColumnId>& column_ids,
                                      string* buf) {
  for (int i = 0; i < column_ids.size(); i++) {
    int32_t column_idx = row.schema()->find_column_by_id(column_ids[i]);
    CHECK(column_idx != Schema::kColumnNotFound);
    const TypeInfo* type_info = row.schema()->column(column_idx).type_info();
    const KeyEncoder<string>& encoder = GetKeyEncoder<string>(type_info);

    if (PREDICT_FALSE(!row.IsColumnSet(column_idx))) {
      uint8_t min_value[kLargestTypeSize];
      type_info->CopyMinValue(min_value);
      encoder.Encode(min_value, i + 1 == column_ids.size(), buf);
    } else {
      ContiguousRow cont_row(row.schema(), row.row_data_);
      encoder.Encode(cont_row.cell_ptr(column_idx), i + 1 == column_ids.size(), buf);
    }
  }
  return Status::OK();
}

int32_t PartitionSchema::BucketForEncodedColumns(const string& encoded_key,
                                                 const HashBucketSchema& hash_bucket_schema) {
  uint64_t hash = HashUtil::MurmurHash2_64(encoded_key.data(),
                                           encoded_key.length(),
                                           hash_bucket_schema.seed);
  return hash % static_cast<uint64_t>(hash_bucket_schema.num_buckets);
}

template<typename Row>
Status PartitionSchema::BucketForRow(const Row& row,
                                     const HashBucketSchema& hash_bucket_schema,
                                     int32_t* bucket) {
  string buf;
  RETURN_NOT_OK(EncodeColumns(row, hash_bucket_schema.column_ids, &buf));
  uint64_t hash = HashUtil::MurmurHash2_64(buf.data(), buf.length(), hash_bucket_schema.seed);
  *bucket = hash % static_cast<uint64_t>(hash_bucket_schema.num_buckets);
  return Status::OK();
}

//------------------------------------------------------------
// Template instantiations: We instantiate all possible templates to avoid linker issues.
// see: https://isocpp.org/wiki/faq/templates#separate-template-fn-defn-from-decl
//------------------------------------------------------------

template
Status PartitionSchema::BucketForRow(const KuduPartialRow& row,
                                     const HashBucketSchema& hash_bucket_schema,
                                     int32_t* bucket);

template
Status PartitionSchema::BucketForRow(const ConstContiguousRow& row,
                                     const HashBucketSchema& hash_bucket_schema,
                                     int32_t* bucket);

void PartitionSchema::Clear() {
  hash_bucket_schemas_.clear();
  range_schema_.column_ids.clear();
}

Status PartitionSchema::Validate(const Schema& schema) const {
  set<ColumnId> hash_columns;
  for (const PartitionSchema::HashBucketSchema& hash_schema : hash_bucket_schemas_) {
    if (hash_schema.num_buckets < 2) {
      return Status::InvalidArgument("must have at least two hash buckets");
    }

    if (hash_schema.column_ids.size() < 1) {
      return Status::InvalidArgument("must have at least one hash column");
    }

    for (ColumnId hash_column : hash_schema.column_ids) {
      if (!hash_columns.insert(hash_column).second) {
        return Status::InvalidArgument("hash bucket schema components must not "
                                       "contain columns in common");
      }
      int32_t column_idx = schema.find_column_by_id(hash_column);
      if (column_idx == Schema::kColumnNotFound) {
        return Status::InvalidArgument("must specify existing columns for hash "
                                       "bucket partition components");
      } else if (!schema.is_key_column(column_idx)) {
        return Status::InvalidArgument("must specify only primary key columns for "
                                       "hash bucket partition components");
      }
    }
  }

  for (ColumnId column_id : range_schema_.column_ids) {
    int32_t column_idx = schema.find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument("must specify existing columns for range "
                                     "partition component");
    } else if (!schema.is_key_column(column_idx)) {
      return Status::InvalidArgument("must specify only primary key columns for "
                                     "range partition component");
    }
  }

  return Status::OK();
}

} // namespace kudu
