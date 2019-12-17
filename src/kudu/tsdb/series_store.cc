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

#include <algorithm>
#include <unordered_map>
#include <vector>
#include <string>

#include "kudu/tsdb/series_store.h"

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tsdb/influx_wire_protocol.h"
#include "kudu/tsdb/series_id.h"
#include "kudu/util/rw_mutex.h"

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSession;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using std::pair;

using strings::Substitute;

namespace kudu {
namespace tsdb {

SeriesStoreImpl::~SeriesStoreImpl() = default;

Status SeriesStoreImpl::Init() {
  auto create_and_open =
      [this](const char* table_name,
             const std::function<Status()>& creator,
             client::sp::shared_ptr<KuduTable>* table) {
        Status s = client_->OpenTable(table_name, table);
        if (s.IsNotFound()) {
          RETURN_NOT_OK_PREPEND(creator(), Substitute("could not create table $0", table_name));
          s = client_->OpenTable(table_name, table);
        }
        RETURN_NOT_OK_PREPEND(s, Substitute("could not open table $0", table_name));
        return Status::OK();
      };
  RETURN_NOT_OK(create_and_open(kSeriesByIdTableName,
                                std::bind(&SeriesStoreImpl::CreateSeriesByIdTable, this),
                                &series_by_id_table_));

  RETURN_NOT_OK(create_and_open(kIdBySeriesTableName,
                                std::bind(&SeriesStoreImpl::CreateIdBySeriesTable, this),
                                &id_by_series_table_));


  RETURN_NOT_OK(create_and_open(kTagIndexTableName,
                                std::bind(&SeriesStoreImpl::CreateTagIndexTable, this),
                                &tag_index_table_));


  RETURN_NOT_OK_PREPEND(series_id_gen_.Init(), "could not init sequence generator");

  return Status::OK();
}

Status SeriesStoreImpl::CreateSeriesByIdTable() {
  KuduSchemaBuilder b;
  b.AddColumn("series_id")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("encoded_tags")->Type(KuduColumnSchema::BINARY)->NotNull();
  KuduSchema schema;
  RETURN_NOT_OK_PREPEND(b.Build(&schema), "could not create series schema");

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  RETURN_NOT_OK_PREPEND(
      table_creator->table_name(kSeriesByIdTableName)
      .schema(&schema)
      .set_range_partition_columns({})
      .Create(),
      "could not create table");
  return Status::OK();
}

Status SeriesStoreImpl::CreateIdBySeriesTable() {
  KuduSchemaBuilder b;
  b.AddColumn("encoded_tags")->Type(KuduColumnSchema::BINARY)->NotNull()->PrimaryKey();
  b.AddColumn("series_id")->Type(KuduColumnSchema::INT32)->NotNull();
  KuduSchema schema;
  RETURN_NOT_OK_PREPEND(b.Build(&schema), "could not create series schema");

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  RETURN_NOT_OK_PREPEND(
      table_creator->table_name(kIdBySeriesTableName)
      .schema(&schema)
      .set_range_partition_columns({})
      .Create(),
      "could not create table");
  return Status::OK();
}

Status SeriesStoreImpl::CreateTagIndexTable() {
  KuduSchemaBuilder b;
  b.AddColumn("measurement")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("tag_key")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("tag_value")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("series_id")->Type(KuduColumnSchema::INT32)
      ->Encoding(client::KuduColumnStorageAttributes::BP128)
      ->NotNull();
  // TODO: change order of measurement here?
  b.SetPrimaryKey({"measurement", "tag_key", "tag_value", "series_id"});
  KuduSchema schema;
  RETURN_NOT_OK_PREPEND(b.Build(&schema), "could not create series schema");

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  RETURN_NOT_OK_PREPEND(
      table_creator->table_name(kTagIndexTableName)
      .schema(&schema)
      .set_range_partition_columns({})
      .Create(),
      "could not create table");
  return Status::OK();
}


string SeriesStoreImpl::EncodeSeries(
    StringPiece measurement_name,
    const vector<pair<StringPiece, StringPiece>>& tags) {
  auto tags_sorted = tags;
  string encoded;
  std::sort(tags_sorted.begin(), tags_sorted.end());
  measurement_name.AppendToString(&encoded);
  for (const auto& p : tags_sorted) {
    encoded.push_back(',');
    p.first.AppendToString(&encoded);
    encoded.push_back('=');
    p.second.AppendToString(&encoded);
  }
  return encoded;
}

Status SeriesStoreImpl::LookupSeries(const string& encoded,
                                 int32_t* series_id) {
  KuduScanner scanner(id_by_series_table_.get());
  CHECK_OK(scanner.SetProjectedColumnNames({"series_id"}));
  RETURN_NOT_OK(scanner.AddConjunctPredicate(
      id_by_series_table_->NewComparisonPredicate(
          "encoded_tags", KuduPredicate::EQUAL, KuduValue::CopyString(encoded))));
  RETURN_NOT_OK_PREPEND(scanner.Open(), "could not scan series table");
  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    RETURN_NOT_OK_PREPEND(scanner.NextBatch(&batch), "could not fetch batch from series table");
    for (const auto& row : batch) {
      CHECK_OK(row->GetInt32("series_id", series_id));
      VLOG(1) << "Found id " << *series_id << " for series " << encoded;
      return Status::OK();
    }
  }
  return Status::NotFound("series not found");
}

Status SeriesStoreImpl::FindOrCreateSeries(
    StringPiece measurement_name,
    const vector<pair<StringPiece, StringPiece>>& tags,
    int32_t* series_id) {

  std::string encoded = EncodeSeries(measurement_name, tags);
  {
    shared_lock<RWMutex> l(cache_lock_);
    if (int32_t* in_cache = FindOrNull(cache_, encoded)) {
      *series_id = *in_cache;
      return Status::OK();
    }
  }
  RETURN_NOT_OK(FindOrCreateSeriesUncached(measurement_name, tags, series_id));
  {
    std::lock_guard<RWMutex> l(cache_lock_);
    cache_.emplace(std::move(encoded), *series_id);
  }
  return Status::OK();
}

Status SeriesStoreImpl::FindSeriesWithTag(StringPiece measurement_name,
                                      StringPiece tag_key,
                                      std::vector<std::pair<std::string, int32_t>>* results) {
  KuduScanner scanner(tag_index_table_.get());
  RETURN_NOT_OK(scanner.AddConjunctPredicate(
      tag_index_table_->NewComparisonPredicate(
          "measurement", KuduPredicate::EQUAL, KuduValue::CopyString(measurement_name))));
  RETURN_NOT_OK(scanner.AddConjunctPredicate(
      tag_index_table_->NewComparisonPredicate(
          "tag_key", KuduPredicate::EQUAL, KuduValue::CopyString(tag_key))));
  RETURN_NOT_OK(scanner.SetProjectedColumnNames({"series_id", "tag_value"}));
  RETURN_NOT_OK_PREPEND(scanner.Open(), "could not scan tag index table");

  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    RETURN_NOT_OK_PREPEND(scanner.NextBatch(&batch), "could not fetch batch from series table");
    for (const auto& row : batch) {
      int32_t series_id;
      CHECK_OK(row->GetInt32(0, &series_id));
      Slice tag_value;
      CHECK_OK(row->GetString(1, &tag_value));
      results->emplace_back(tag_value.ToString(), series_id);
    }
  }
  return Status::OK();
}

Status SeriesStoreImpl::FindTagKeys(StringPiece measurement_name,
                                std::vector<std::string>* tag_keys) {
  KuduScanner scanner(tag_index_table_.get());
  RETURN_NOT_OK(scanner.AddConjunctPredicate(
      tag_index_table_->NewComparisonPredicate(
          "measurement", KuduPredicate::EQUAL, KuduValue::CopyString(measurement_name))));
  RETURN_NOT_OK(scanner.SetProjectedColumnNames({"tag_key"}));
  RETURN_NOT_OK_PREPEND(scanner.Open(), "could not scan tag index table");

  tag_keys->clear();
  string prev;
  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    RETURN_NOT_OK_PREPEND(scanner.NextBatch(&batch), "could not fetch batch from series table");
    for (const auto& row : batch) {
      Slice key;
      CHECK_OK(row->GetString(0, &key));
      if (key != prev) {
        // We expect good "clustering" of results -- duplicate values show up in
        // long runs. But, since we don't use a fault-tolerant scan, we aren't
        // guaranteed of this, and we still sort and unique once more at the end.
        tag_keys->emplace_back(key.ToString());
        prev = key.ToString();
      }
    }
  }

  std::sort(tag_keys->begin(), tag_keys->end());
  tag_keys->erase(std::unique(tag_keys->begin(), tag_keys->end()), tag_keys->end());
  return Status::OK();
}


Status SeriesStoreImpl::FindMatchingSeries(StringPiece measurement_name,
                                       StringPiece tag_key,
                                       StringPiece tag_value,
                                       std::vector<int32_t>* ids) {
  KuduScanner scanner(tag_index_table_.get());
  RETURN_NOT_OK(scanner.AddConjunctPredicate(
      tag_index_table_->NewComparisonPredicate(
          "measurement", KuduPredicate::EQUAL, KuduValue::CopyString(measurement_name))));
  RETURN_NOT_OK(scanner.AddConjunctPredicate(
      tag_index_table_->NewComparisonPredicate(
          "tag_key", KuduPredicate::EQUAL, KuduValue::CopyString(tag_key))));
  RETURN_NOT_OK(scanner.AddConjunctPredicate(
      tag_index_table_->NewComparisonPredicate(
          "tag_value", KuduPredicate::EQUAL, KuduValue::CopyString(tag_value))));
  RETURN_NOT_OK(scanner.SetProjectedColumnNames({"series_id"}));
  RETURN_NOT_OK_PREPEND(scanner.Open(), "could not scan tag index table");

  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    RETURN_NOT_OK_PREPEND(scanner.NextBatch(&batch), "could not fetch batch from series table");
    for (const auto& row : batch) {
      int32_t series_id;
      CHECK_OK(row->GetInt32(0, &series_id));
      ids->push_back(series_id);
    }
  }
  return Status::OK();
}


Status SeriesStoreImpl::FindAllSeries(StringPiece measurement_name,
                                      const vector<string>& fetch_tags,
                                      std::vector<SeriesIdWithTags>* results) {
  KuduScanner scanner(id_by_series_table_.get());

  string lower_bound = StrCat(measurement_name, ",");
  string upper_bound = StrCat(measurement_name, "-"); // ',' + 1
  RETURN_NOT_OK(scanner.AddConjunctPredicate(
      id_by_series_table_->NewComparisonPredicate(
          "encoded_tags", KuduPredicate::GREATER_EQUAL, KuduValue::CopyString(lower_bound))));
  RETURN_NOT_OK(scanner.AddConjunctPredicate(
      id_by_series_table_->NewComparisonPredicate(
          "encoded_tags", KuduPredicate::LESS, KuduValue::CopyString(upper_bound))));
  RETURN_NOT_OK(scanner.SetProjectedColumnNames({"series_id", "encoded_tags"}));
  RETURN_NOT_OK_PREPEND(scanner.Open(), "could not scan tag index table");

  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    RETURN_NOT_OK_PREPEND(scanner.NextBatch(&batch), "could not fetch batch from series table");
    for (const auto& row : batch) {
      int32_t series_id;
      CHECK_OK(row->GetInt32(0, &series_id));
      Slice encoded_tags;
      CHECK_OK(row->GetBinary(1, &encoded_tags));

      SeriesIdWithTags::TagMap tags;
      if (!fetch_tags.empty()) {
        // TODO(todd) factor out this code somewhere for better testing.
        using TagPair = pair<StringPiece, StringPiece>;
        vector<TagPair> row_tags;
        StringPiece measurement;
        if (!ParseSeries(StringPiece(reinterpret_cast<const char*>(encoded_tags.data()), encoded_tags.size()),
                         &measurement,
                         &row_tags)) {
          return Status::Corruption("unparseable series in database", encoded_tags);
        }
        tags.reserve(fetch_tags.size());
        for (const auto& tag : fetch_tags) {
          auto range = std::equal_range(
              row_tags.begin(), row_tags.end(), TagPair{tag, ""},
              [](const TagPair& a, const TagPair& b) {
                return a.first < b.first;
              });
          if (range.first != range.second) {
            tags.emplace(tag, range.first->second.ToString());
          }
        }
      }

      results->emplace_back(series_id, std::move(tags));
    }
  }
  return Status::OK();
}


Status SeriesStoreImpl::FindOrCreateSeriesUncached(
    StringPiece measurement_name,
    const vector<pair<StringPiece, StringPiece>>& tags,
    int32_t* series_id) {

  std::string encoded = EncodeSeries(measurement_name, tags);

  while (true) {
    // Look up in table.
    Status s = LookupSeries(encoded, series_id);
    if (s.ok()) return Status::OK();
    if (!s.IsNotFound()) {
      return s;
    }

    // If we didn't find one, we need to insert. Generate an ID.
    int64_t id_to_write_64;
    RETURN_NOT_OK(series_id_gen_.Next(&id_to_write_64));
    int32_t id_to_write = id_to_write_64;
    CHECK_EQ(id_to_write_64, id_to_write) << "id overflow"; // TODO do we need 64 bit ids?

    LOG(INFO) << "trying to insert new series id " << id_to_write;

    // Insert into the tables.
    auto session = client_->NewSession();
    CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    session->SetTimeoutMillis(60000);

    {
      KuduInsert* insert = id_by_series_table_->NewInsert();
      CHECK_OK(insert->mutable_row()->SetBinaryCopy("encoded_tags", encoded));
      CHECK_OK(insert->mutable_row()->SetInt32("series_id", id_to_write));
      // TODO(todd) add a flag "indexed" or something so that we can recover from
      // a failure in the middle of this process.
      s = session->Apply(insert);
      if (!s.ok()) {
        vector<KuduError*> errors;
        bool overflow;
        session->GetPendingErrors(&errors, &overflow);
        if (!errors.empty()) {
          s = errors[0]->status();
        }
        for (auto* e : errors) {
          delete e;
        }
      }
      if (s.IsAlreadyPresent()) {
        LOG(INFO) << "conflicted inserting series " << encoded << ": retrying...";
        continue;
      }
      RETURN_NOT_OK_PREPEND(s, "failed to insert new series into id_by_series");
    }

    CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

    // Then the series_by_id.
    {
      auto* insert = series_by_id_table_->NewInsert();
      CHECK_OK(insert->mutable_row()->SetInt32("series_id", id_to_write));
      CHECK_OK(insert->mutable_row()->SetBinaryCopy("encoded_tags", encoded));
      s = session->Apply(insert);
      RETURN_NOT_OK_PREPEND(s, Substitute("failed to insert new series id $0 into series_by_id", id_to_write));
    }

    // Then the tag index.
    {
      for (const auto& p : tags) {
        const auto& key = p.first;
        const auto& val = p.second;

        auto* insert = tag_index_table_->NewInsert();
        CHECK_OK(insert->mutable_row()->SetStringCopy("measurement", measurement_name));
        CHECK_OK(insert->mutable_row()->SetStringCopy("tag_key", key));
        CHECK_OK(insert->mutable_row()->SetStringCopy("tag_value", val));
        CHECK_OK(insert->mutable_row()->SetInt32("series_id", id_to_write));
        s = session->Apply(insert);
        RETURN_NOT_OK_PREPEND(s, Substitute("failed to insert tag index entry ($0, $1, $2)", measurement_name, key, val));
      }
    }

    RETURN_NOT_OK_PREPEND(session->Flush(), "failed to write index entries");
    CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));


    VLOG(1) << "Assigned id " << id_to_write << " to series " << encoded;
    *series_id = id_to_write;
    return Status::OK();
  }

  return Status::OK();
}

} // namespace tsdb
} // namespace kudu
