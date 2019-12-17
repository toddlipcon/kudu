# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


add_library(kudu_tsdb_common
  influx_wire_protocol.cc
  series_id.cc)

target_link_libraries(kudu_tsdb_common
  gutil
  kudu_util
  glog)

add_library(kudu_tsdb
  influx_wire_protocol.cc
  metrics_store.cc
  series_store.cc
  sequence_generator.cc
  tsdb.cc)

target_link_libraries(kudu_tsdb
  gutil
  kudu_util
  glog
  dl
  webserver
  kudu_client
  kudu_tsdb_common)

add_library(kudu_influxql
  ql/analysis.cc
  ql/agg.cc
  ql/exec.cc
  ql/expr.cc
  ql/influxql.cc
  ql/planner.cc )

target_link_libraries(kudu_influxql
  gutil
  kudu_util
  kudu_tsdb_common
  glog
  dl)

add_executable(kudu-tsdbd tsdbd.cc)
target_link_libraries(kudu-tsdbd
  kudu_tsdb kudu_influxql)

SET_KUDU_TEST_LINK_LIBS(kudu_tsdb)
ADD_KUDU_TEST(influx_wire_protocol-test)

SET_KUDU_TEST_LINK_LIBS(kudu_tsdb itest_util)
ADD_KUDU_TEST(series_store-test)

SET_KUDU_TEST_LINK_LIBS(kudu_influxql)
ADD_KUDU_TEST(ql/agg-test)
ADD_KUDU_TEST(ql/influxql-test)

