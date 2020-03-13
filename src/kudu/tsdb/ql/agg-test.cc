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

#include "kudu/tsdb/ql/agg.h"
#include "kudu/tsdb/ql/qcontext.h"
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

  QContext ctx(nullptr, nullptr);
  auto block = ctx.NewTSBlock();
  vector<int64_t> cells;
  for (int ts = kMinTs; ts <= kMaxTs; ts++) {
    block->times.emplace_back(ts);
    cells.emplace_back(ts % 10);
  }
  block->AddColumn("usage", InfluxVec::WithNoNulls(std::move(cells)));

  BlockBuffer result_consumer;

  unique_ptr<TSBlockConsumer> agg;
  ASSERT_OK(CreateMultiAggExpressionEvaluator(
      &ctx,
      {{"mean", "usage", client::KuduColumnSchema::DataType::INT64},
       {"max", "usage", client::KuduColumnSchema::DataType::INT64}},
      Bucketer(kMinTs, kMaxTs, 10),
      &result_consumer,
      &agg));
  ASSERT_OK(agg->Consume(std::move(block)));
  ASSERT_OK(agg->Finish());

  auto result_block = result_consumer.TakeSingleResult();
  LOG(INFO) << result_block->times;
  LOG(INFO) << *result_block->column("max_usage").data_as<int64_t>();
  LOG(INFO) << *result_block->column("mean_usage").data_as<double>();
  ASSERT_THAT(result_block->times, testing::ElementsAre(
      100, 110, 120, 130, 140, 150, 160, 170, 180, 190));
  ASSERT_THAT(*result_block->column("max_usage").data_as<int64_t>(),
              testing::ElementsAre(9, 9, 9, 9, 9, 9, 9, 9, 9, 9));
  ASSERT_THAT(*result_block->column("mean_usage").data_as<double>(),
              testing::ElementsAre(4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5));

}

} // namespace influxql
} // namespace tsdb
} // namespace kudu
