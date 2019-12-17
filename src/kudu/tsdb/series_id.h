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

#include <boost/container/flat_map.hpp>
#include <string>
#include <vector>

namespace kudu {
namespace tsdb {

struct SeriesIdWithTags {
  using TagMap = boost::container::flat_map<std::string, std::string>;
  SeriesIdWithTags(int32_t id, TagMap tags)
      : series_id(id), tags(std::move(tags)) {
  }

  std::string ToString() const;

  int32_t series_id;
  TagMap tags;
};

} // namespace tsdb
} // namespace kudu
