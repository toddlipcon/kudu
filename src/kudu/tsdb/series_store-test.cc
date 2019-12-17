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

#include <algorithm>
#include <gtest/gtest.h>
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/test_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/tsdb/series_id.h"
#include "kudu/tsdb/series_store.h"

using std::pair;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tsdb {

class SeriesStoreTest : public ExternalMiniClusterITestBase {
};

TEST_F(SeriesStoreTest, TestStore) {
  StartCluster();
  SeriesStoreImpl store(client_);
  ASSERT_OK(store.Init());

  // Create a series.
  int32_t series_1;
  ASSERT_OK(store.FindOrCreateSeries("cpu",
      {{"hostname", "host1"},
       {"rack", "rack1"}}, &series_1));
  // Fetch back the same series that was created.
  {
    int32_t series_retrieved;
    ASSERT_OK(store.FindOrCreateSeries(
        "cpu",
        {{"hostname", "host1"},
         {"rack", "rack1"}}, &series_retrieved));
    ASSERT_EQ(series_1, series_retrieved);
  }

  // Create a new one with some overlapping but some different tags.
  // It should be assigned a different identifier.
  int32_t series_2;
  ASSERT_OK(store.FindOrCreateSeries("cpu",
      {{"hostname", "host2"},
       {"rack", "rack1"}}, &series_2));
  ASSERT_NE(series_1, series_2);

  // Fetch back the series.
  {
    vector<int32_t> ids;
    ASSERT_OK(store.FindMatchingSeries("cpu", "hostname", "host2", &ids));
    ASSERT_EQ(1, ids.size());
    EXPECT_EQ(series_2, ids[0]);
  }
  {
    vector<int32_t> ids;
    ASSERT_OK(store.FindMatchingSeries("cpu", "rack", "rack1", &ids));
    ASSERT_EQ(2, ids.size());
    std::sort(ids.begin(), ids.end());
    EXPECT_EQ(series_1, ids[0]);
    EXPECT_EQ(series_2, ids[1]);
  }
  {
    vector<pair<string, int32_t>> hosts_with_ids;
    ASSERT_OK(store.FindSeriesWithTag("cpu", "hostname", &hosts_with_ids));
    std::sort(hosts_with_ids.begin(), hosts_with_ids.end());
    ASSERT_EQ(2, hosts_with_ids.size());
    ASSERT_EQ(hosts_with_ids[0].first, "host1");
    ASSERT_EQ(hosts_with_ids[0].second, series_1);
    ASSERT_EQ(hosts_with_ids[1].first, "host2");
    ASSERT_EQ(hosts_with_ids[1].second, series_2);
  }

  // Get all tag keys.
  vector<string> keys;
  ASSERT_OK(store.FindTagKeys("cpu", &keys));
  ASSERT_EQ(2, keys.size());
  ASSERT_EQ(keys[0], "hostname");
  ASSERT_EQ(keys[1], "rack");

  // Get all series for this measurement.
  {
    vector<SeriesIdWithTags> series;
    ASSERT_OK(store.FindAllSeries("cpu", {"hostname"}, &series));
    ASSERT_EQ(2, series.size());
    EXPECT_EQ(Substitute("{id=$0, tag_values=[host1]}", series_1), series[0].ToString());
    EXPECT_EQ(Substitute("{id=$0, tag_values=[host2]}", series_2), series[1].ToString());
  }
  {
    vector<SeriesIdWithTags> series;
    ASSERT_OK(store.FindAllSeries("cpu", {"hostname","rack"}, &series));
    ASSERT_EQ(2, series.size());
    EXPECT_EQ(Substitute("{id=$0, tag_values=[host1,rack1]}", series_1), series[0].ToString());
    EXPECT_EQ(Substitute("{id=$0, tag_values=[host2,rack1]}", series_2), series[1].ToString());
  }
  {
    vector<SeriesIdWithTags> series;
    ASSERT_OK(store.FindAllSeries("cpu", {"rack","hostname"}, &series));
    ASSERT_EQ(2, series.size());
    EXPECT_EQ(Substitute("{id=$0, tag_values=[rack1,host1]}", series_1), series[0].ToString());
    EXPECT_EQ(Substitute("{id=$0, tag_values=[rack1,host2]}", series_2), series[1].ToString());
  }
  {
    vector<SeriesIdWithTags> series;
    ASSERT_OK(store.FindAllSeries("cpu", {"rack","missingtag", "hostname"}, &series));
    ASSERT_EQ(2, series.size());
    EXPECT_EQ(Substitute("{id=$0, tag_values=[rack1,,host1]}", series_1), series[0].ToString());
    EXPECT_EQ(Substitute("{id=$0, tag_values=[rack1,,host2]}", series_2), series[1].ToString());
  }
}


} // namespace tsdb
} // namespace kudu
