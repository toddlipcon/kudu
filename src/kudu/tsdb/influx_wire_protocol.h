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
