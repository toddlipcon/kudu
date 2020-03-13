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

#include "kudu/tsdb/series_id.h"

#include <string>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"

using std::string;
using strings::Substitute;

namespace kudu {
namespace tsdb {

string SeriesIdWithTags::ToString() const {
  return Substitute("{id=$0, tags={$1}}", series_id,
                    JoinKeysAndValuesIterator(tags.begin(), tags.end(), "=", ","));
}

} // namespace tsdb
} // namespace kudu
