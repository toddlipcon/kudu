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

#include "kudu/gutil/macros.h"

#include <unordered_map>
#include <vector>
#include <string>

#include <glog/logging.h>
#include <libdivide.h>

#include "kudu/client/schema.h"
#include "kudu/tsdb/ql/exec.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tsdb {

namespace influxql {
struct Expr;

class Bucketer {
 public:
  // start_time: the inclusive lowest timestamp to be bucketed
  // end_time: the inclusive highest timestamp to be bucketed
  // granularity: the size of each bucket
  Bucketer(int64_t start_time,
           int64_t end_time,
           int64_t granularity)
      : granularity_(granularity),
        divider_(granularity),
        base_bucket_(start_time / granularity),
        end_bucket_(end_time / granularity) {

    DCHECK_LE(start_time, end_time);
    DCHECK_GT(granularity, 0);

    int num_buckets = end_bucket_ - base_bucket_ + 1;
    bucket_times_.resize(num_buckets);
    for (int i = 0; i < num_buckets; i++) {
      bucket_times_[i] = (i + base_bucket_) * granularity_;
    }
  }

  const std::vector<int64_t>& bucket_times() const {
    return bucket_times_;
  }

  int num_buckets() const {
    return bucket_times_.size();
  }

  inline int bucket(int64_t ts) const {
    int ret = ts / divider_ - base_bucket_;
    DCHECK_GE(ret, 0);
    DCHECK_LT(ret, bucket_times_.size());
    return ret;
  }

 private:
  const int64_t granularity_;
  const libdivide::divider<int64_t> divider_;

  const int64_t base_bucket_;
  const int64_t end_bucket_;

  std::vector<int64_t> bucket_times_;
};

struct AggSpec {
  std::string func_name;
  std::string col_name;
  client::KuduColumnSchema::DataType col_type;
};

Status CreateMultiAggExpressionEvaluator(
    QContext* ctx,
    const std::vector<AggSpec>& aggs,
    Bucketer bucketer,
    TSBlockConsumer* downstream,
    std::unique_ptr<TSBlockConsumer>* eval);

} // namespace influxql
} // namespace tsdb
} // namespace kudu
