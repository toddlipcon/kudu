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
#include "kudu/tsdb/ql/exec.h"
#include "kudu/tsdb/ql/influxql.h"
#include "kudu/tsdb/influx_wire_protocol.h"
#include "kudu/util/status.h"
#include "kudu/util/rw_mutex.h"

namespace kudu {
namespace tsdb {

class MetricsColumnSource {
 public:
  virtual Status GetColumnsForMeasurement(
      StringPiece measurement,
      std::map<std::string, client::KuduColumnSchema::DataType>* cols) = 0;
};

class MetricsStore : public MetricsColumnSource {
 public:

  MetricsStore(client::sp::shared_ptr<client::KuduClient> client)
      : client_(client) {
  }
  virtual ~MetricsStore();

  Status Init();

  Status Write(SeriesId series_id, const InfluxMeasurement& measurement,
               client::KuduSession* session);

  Status Read(StringPiece metric_name,
              SeriesId series_id, int64_t start_time, int64_t end_time,
              const std::vector<StringPiece>& project,
              const std::vector<influxql::Predicate>& preds,
              std::vector<int64_t>* result_timess,
              std::vector<InfluxVec>* result_vals);

  Status GetColumnsForMeasurement(
      StringPiece measurement,
      std::map<std::string, client::KuduColumnSchema::DataType>* cols) override;

 private:
  constexpr static const char* const kTablePrefix = "metrics.";

  Status FindOrCreateTableAndEnsureSchema(const InfluxMeasurement& measurement,
                                          client::sp::shared_ptr<client::KuduTable>* table);
  Status FindTable(StringPiece metric_name,
                   client::sp::shared_ptr<client::KuduTable>* table);
  Status CreateTable(const InfluxMeasurement& measurement);

  client::sp::shared_ptr<client::KuduClient> client_;

  RWMutex table_lock_;
  std::unordered_map<std::string, client::sp::shared_ptr<client::KuduTable>> tables_;

};

} // namespace  tsdb
} // namespace kudu
