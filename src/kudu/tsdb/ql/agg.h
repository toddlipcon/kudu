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
