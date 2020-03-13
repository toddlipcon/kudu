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
