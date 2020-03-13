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
