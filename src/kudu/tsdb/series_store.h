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

#include <unordered_map>
#include <string>
#include <utility>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/status.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/tsdb/sequence_generator.h"

namespace kudu {
namespace tsdb {

struct SeriesIdWithTags;

class SeriesStore {
 public:
  virtual ~SeriesStore() = default;
  virtual Status FindMatchingSeries(StringPiece measurement_name,
                                    StringPiece tag_key,
                                    StringPiece tag_value,
                                    std::vector<int32_t>* ids) = 0;

  virtual Status FindAllSeries(StringPiece measurement_name,
                               const std::vector<std::string>& fetch_tags,
                               std::vector<SeriesIdWithTags>* results) = 0;
};

class SeriesStoreImpl : public SeriesStore {
 public:
    explicit SeriesStoreImpl(client::sp::shared_ptr<client::KuduClient> client)
      : client_(client),
        series_id_gen_(client, "series") {
  }
  ~SeriesStoreImpl();

  Status Init();

  // Find or create a time series with the given set of tags.
  // This is thread-safe and guarantees that, for a given set of tag k/v pairs,
  // exactly one identifier will be assigned.
  Status FindOrCreateSeries(StringPiece measurement_name,
                            const std::vector<std::pair<StringPiece, StringPiece>>& tags,
                            int32_t* series_id);

  // Find all series which have the given tag key/value pair.
  Status FindMatchingSeries(StringPiece measurement_name,
                            StringPiece tag_key,
                            StringPiece tag_value,
                            std::vector<int32_t>* ids) override;

  // Find all series that have the given tag. Returns a list of the corresponding
  // tag values and their series IDs.
  Status FindSeriesWithTag(StringPiece measurement_name,
                           StringPiece tag_key,
                           std::vector<std::pair<std::string, int32_t>>* results);

  // Return the unique tag keys that are used for the given measurement.
  // The resulting vector is guaranteed to be in sorted order.
  Status FindTagKeys(StringPiece measurement_name,
                     std::vector<std::string>* tag_keys);

  // Find all series for the given measurement, also fetching the specified
  // tags for those series.
  Status FindAllSeries(StringPiece measurement_name,
                       const std::vector<std::string>& fetch_tags,
                       std::vector<SeriesIdWithTags>* results) override;

 private:
  constexpr static const char* const kSeriesByIdTableName = "series_by_id";
  constexpr static const char* const kIdBySeriesTableName = "id_by_series";
  constexpr static const char* const kTagIndexTableName = "tag_index";

  Status CreateSeriesByIdTable();
  Status CreateIdBySeriesTable();
  Status CreateTagIndexTable();

  static std::string EncodeSeries(StringPiece measurement_name,
                                  const std::vector<std::pair<StringPiece, StringPiece>>& tags);
  Status LookupSeries(const std::string& encoded, int32_t* series_id);
  Status FindOrCreateSeriesUncached(
      StringPiece measurement_name,
      const std::vector<std::pair<StringPiece, StringPiece>>& tags,
      int32_t* series_id);

  client::sp::shared_ptr<client::KuduClient> client_;
  client::sp::shared_ptr<client::KuduTable> series_by_id_table_;
  client::sp::shared_ptr<client::KuduTable> id_by_series_table_;
  client::sp::shared_ptr<client::KuduTable> tag_index_table_;
  SequenceGenerator series_id_gen_;

  RWMutex cache_lock_;
  std::unordered_map<std::string, int32_t> cache_; // TODO use an LRU
};

} // namespace  tsdb
} // namespace kudu
