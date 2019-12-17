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

#include <string>
#include <vector>
#include <utility>

#include <boost/variant.hpp>

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"
#include "kudu/gutil/strings/stringpiece.h"

namespace kudu {
namespace tsdb {

using SeriesId = int32_t;

using InfluxVal = boost::variant<double, int64_t>;
enum InfluxValTypeIndexes {
  kValTypeDouble = 0,
  kValTypeInt64,
};

using FieldSet = std::vector<std::pair<StringPiece, InfluxVal>>;

// References memory in the InfluxBatch which contains it.
struct InfluxMeasurement {
  StringPiece metric_name;
  int64_t timestamp_us;
  std::vector<std::pair<StringPiece, StringPiece>> tags;
  FieldSet fields;
};

struct InfluxBatch {
 public:
  // https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/
  Status Parse(std::string s);

  std::vector<InfluxMeasurement> measurements;

 private:
  std::string backing_data_;
};

bool ParseSeries(StringPiece series,
                 StringPiece* measurement,
                 std::vector<std::pair<StringPiece, StringPiece>>* tags);

enum class TimestampFormat {
  NS,
  MS,
  US,
  RFC3339
};
Status ParseTimestampFormat(StringPiece format_str, TimestampFormat* format);

} // namespace tsdb
} // namespace kudu
