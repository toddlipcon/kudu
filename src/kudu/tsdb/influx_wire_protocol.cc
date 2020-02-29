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


#include "kudu/tsdb/influx_wire_protocol.h"

#include <memory>
#include <utility>
#include <vector>
#include <array>

#include <boost/variant.hpp>

#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/status.h"

using std::array;
using std::vector;
using std::pair;
using std::string;
using strings::Split;

namespace kudu {
namespace tsdb {

namespace {

// This hand-coded parser is more than twice as fast vs using Split
// (better inlining?)
template<class F>
bool ParseKVPairs(StringPiece str, const F& func) {
  while (!str.empty()) {
    ssize_t pos = str.find('=');
    if (PREDICT_FALSE(pos == StringPiece::npos)) {
      return false;
    }
    StringPiece k = str.substr(0, pos);
    str.remove_prefix(pos + 1);

    pos = str.find(',');
    StringPiece v;
    if (PREDICT_FALSE(pos == StringPiece::npos)) {
      v = str;
      str = "";
    } else {
      v = str.substr(0, pos);
      str.remove_prefix(pos + 1);
    }

    if (!func(k, v)) {
      return false;
    }
  }
  return true;
}

int CountChars(StringPiece str,
               char delim) {
  int i = 0;
  for (char c : str) {
    if (c == delim) {
      i++;
    }
  }
  return i;
}

} // anonymous namespace

bool ParseSeries(StringPiece series,
                 StringPiece* measurement,
                 vector<pair<StringPiece, StringPiece>>* tags) {
  int pos = series.find(',');
  if (PREDICT_FALSE(pos == StringPiece::npos)) {
    return false;
  }
  *measurement = series.substr(0, pos);
  series.remove_prefix(pos + 1);
  return ParseKVPairs(series, [&](StringPiece k, StringPiece v) {
                                tags->emplace_back(k, v);
                                return true;
                              });
}

Status InfluxBatch::Parse(std::string s) {
  backing_data_ = std::move(s);
  for (StringPiece l : Split(backing_data_, "\n")) {
    if (l.empty()) continue;

    array<StringPiece, 3> parts = Split(l, strings::delimiter::Limit(" ", 2));
    auto series = parts[0];
    auto fields_str = parts[1];
    auto ts_str = parts[2];

    // parse ts
    int64_t ts;
    if (!safe_strto64(ts_str.ToString(), &ts)) {
      LOG(WARNING) << "could not parse input line: " << s;
      return Status::InvalidArgument("invalid ts", ts_str);
    }

    // parse tags
    vector<pair<StringPiece, StringPiece>> tags;
    tags.reserve(CountChars(series, ',') + 1);

    StringPiece measurement;
    if (!ParseSeries(series, &measurement, &tags)) {
      return Status::InvalidArgument("invalid series", series);
    }

    // parse fields
    vector<pair<StringPiece, InfluxVal>> fields;
    fields.reserve(CountChars(fields_str, ',') + 1);
    bool bad_fields = ParseKVPairs(
        fields_str,
        [&](StringPiece k, StringPiece v) {
          InfluxVal val;
          string str;
          double double_val;
          int64_t int_val;

          if (v.size() > 1 && v[v.size() - 1] == 'i' &&
              safe_strto64(v.data(), v.size() - 1, &int_val)) {
            val = int_val;
          } else if (safe_strtod(v.as_string(), &double_val)) {
            val = double_val;
          } else {
            return false;
          }
          fields.emplace_back(k, std::move(val));
          return true;
        });
    if (PREDICT_FALSE(!bad_fields)) {
      return Status::InvalidArgument("invalid field format", fields_str);
    }

    InfluxMeasurement m;
    m.metric_name = measurement;
    m.tags = std::move(tags);
    m.fields = std::move(fields);
    m.timestamp_us = ts / 1000; // TODO(todd) parse units
    measurements.emplace_back(std::move(m));
  }
  return Status::OK();
}

Status ParseTimestampFormat(StringPiece format_str, TimestampFormat* format) {
  if (format_str == "ns" || format_str == "n" || format_str == "") {
    *format = TimestampFormat::NS;
  } else if (format_str == "rfc3339") {
    *format = TimestampFormat::RFC3339;
  } else if (format_str == "ms") {
    *format = TimestampFormat::MS;
  } else if (format_str == "us") {
    *format = TimestampFormat::US;
  } else {
    return Status::NotSupported("invalid timestamp format", format_str.ToString());
  }
  return Status::OK();
}

} // namespace tsdb
} // namespace kudu    backing_data_ = std::move(s);
