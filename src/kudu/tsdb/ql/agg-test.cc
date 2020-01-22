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

#include "kudu/tsdb/ql/agg.h"
#include "kudu/util/test_util.h"
#include "kudu/util/test_macros.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>
#include <glog/stl_logging.h>

using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tsdb {
namespace influxql {

class AggTest : public KuduTest {
};


TEST_F(AggTest, TestBucketer) {
  Bucketer b(100, 199, 10);
  ASSERT_EQ(10, b.num_buckets());
  ASSERT_EQ(0, b.bucket(100));
  ASSERT_EQ(0, b.bucket(105));
  ASSERT_EQ(0, b.bucket(109));
  ASSERT_EQ(1, b.bucket(110));
  ASSERT_EQ(1, b.bucket(111));
  ASSERT_EQ(9, b.bucket(199));
}

TEST_F(AggTest, TestMaxAgg) {
  const int kMinTs = 100;
  const int kMaxTs = 199;

  TSBlock block;
  vector<int64_t> cells;
  for (int ts = kMinTs; ts <= kMaxTs; ts++) {
    block.times.emplace_back(ts);
    cells.emplace_back(ts % 10);
  }
  block.AddColumn("usage", InfluxVec::WithNoNulls(std::move(cells)));

  BlockBuffer result_consumer;

  unique_ptr<TSBlockConsumer> agg;
  ASSERT_OK(CreateMultiAggExpressionEvaluator(
      {{"mean", "usage"},
       {"max", "usage"}},
      Bucketer(kMinTs, kMaxTs, 10),
      &result_consumer,
      &agg));
  ASSERT_OK(agg->Consume(&block));
  ASSERT_OK(agg->Finish());

  TSBlock result_block = result_consumer.TakeSingleResult();
  LOG(INFO) << result_block.times;
  LOG(INFO) << *result_block.column("max_usage").data_as<int64_t>();
  LOG(INFO) << *result_block.column("mean_usage").data_as<double>();
  ASSERT_THAT(result_block.times, testing::ElementsAre(
      100, 110, 120, 130, 140, 150, 160, 170, 180, 190));
  ASSERT_THAT(*result_block.column("max_usage").data_as<int64_t>(),
              testing::ElementsAre(9, 9, 9, 9, 9, 9, 9, 9, 9, 9));
  ASSERT_THAT(*result_block.column("mean_usage").data_as<double>(),
              testing::ElementsAre(4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5));

}

} // namespace influxql
} // namespace tsdb
} // namespace kudu
