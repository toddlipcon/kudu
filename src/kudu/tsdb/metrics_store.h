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
#include "kudu/tsdb/ql/exec.h"
#include "kudu/tsdb/ql/influxql.h"
#include "kudu/tsdb/influx_wire_protocol.h"
#include "kudu/util/status.h"
#include "kudu/util/rw_mutex.h"

namespace kudu {
namespace tsdb {

struct ProjectionInfo;

class MetricsColumnSource {
 public:
  virtual Status GetColumnsForMeasurement(
      StringPiece measurement,
      std::map<std::string, client::KuduColumnSchema::DataType>* cols) = 0;
};

class MetricsStore : public MetricsColumnSource {
 public:

  MetricsStore(client::sp::shared_ptr<client::KuduClient> client);
  virtual ~MetricsStore();

  Status Init();

  Status Write(SeriesId series_id, const InfluxMeasurement& measurement,
               client::KuduSession* session);

  Status Read(StringPiece metric_name,
              SeriesId series_id, int64_t start_time, int64_t end_time,
              const std::vector<StringPiece>& project,
              const std::vector<influxql::Predicate>& preds,
              influxql::QContext* ctx,
              influxql::TSBlockConsumer* consumer);

  Status GetColumnsForMeasurement(
      StringPiece measurement,
      std::map<std::string, client::KuduColumnSchema::DataType>* cols) override;

  client::KuduClient* client() {
    return client_.get();
  }

 private:
  constexpr static const char* const kTablePrefix = "metrics.";

  Status FindOrCreateTableAndEnsureSchema(const InfluxMeasurement& measurement,
                                          client::sp::shared_ptr<client::KuduTable>* table);
  Status FindTable(StringPiece metric_name,
                   client::sp::shared_ptr<client::KuduTable>* table);
  Status CreateTable(const InfluxMeasurement& measurement);

  static Status ReadFromScanner(client::KuduScanner* scanner,
                                const ProjectionInfo& proj_info,
                                influxql::QContext* ctx,
                                influxql::TSBlockConsumer* consumer);
  static Status ReadFromScannerColumnar(client::KuduScanner* scanner,
                                        const ProjectionInfo& proj_info,
                                        influxql::QContext* ctx,
                                        influxql::TSBlockConsumer* consumer);

  client::sp::shared_ptr<client::KuduClient> client_;

  RWMutex table_lock_;
  std::unordered_map<std::string, client::sp::shared_ptr<client::KuduTable>> tables_;

  const client::KuduColumnStorageAttributes::EncodingType int_encoding_;
};

} // namespace  tsdb
} // namespace kudu
