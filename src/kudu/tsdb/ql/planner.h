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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/macros.h"

#include "kudu/util/status.h"
#include "kudu/tsdb/metrics_store.h"
#include "kudu/tsdb/ql/exec.h"
#include "kudu/tsdb/ql/qcontext.h"

using std::unique_ptr;

namespace kudu {
namespace tsdb {

struct SeriesIdWithTags;

namespace influxql {

struct AnalyzedSelectStmt;
struct Expr;

class SeriesSelector {
 public:
  virtual ~SeriesSelector() = default;
  virtual Status Execute(QContext* ctx,
                         std::vector<SeriesIdWithTags>* series) = 0;

  virtual std::string ToString() const = 0;
};

using TSBlockConsumerFactory = std::function<Status(TSBlockConsumer* downstream, std::unique_ptr<TSBlockConsumer>* consumer)>;

class Planner {
 public:
  explicit Planner(QContext* ctx) : ctx_(ctx) {}
  ~Planner();

  Status PlanSeriesSelector(const AnalyzedSelectStmt* asel,
                            SeriesSelector** selector);

  Status PlanSelectExpressions(const AnalyzedSelectStmt* asel,
                               TSBlockConsumerFactory* factory);

  Status PlanPredicates(const AnalyzedSelectStmt* asel,
                        std::vector<Predicate>* predicates);

 private:
  QContext* const ctx_;

  Status ConvertExprToSelector(const AnalyzedSelectStmt* asel,
                               const Expr* expr,
                               SeriesSelector** selector);

  Status ConvertBinaryExpr(const Expr* expr, Predicate* predicate);
  Status ConvertTagEqualityExpr(const Expr* expr,
                                std::string* tag_key,
                                std::string* tag_val);

  Status CreateSingleTagSelector(const AnalyzedSelectStmt* asel,
                                 std::string tag_key,
                                 std::vector<std::string> tag_values,
                                 SeriesSelector** selector);

  Status PlanSelectAggregate(const AnalyzedSelectStmt* asel,
                             TSBlockConsumerFactory* factory);
  Status PlanProjection(const AnalyzedSelectStmt* asel,
                        TSBlockConsumerFactory* factory);

  DISALLOW_COPY_AND_ASSIGN(Planner);
};

} // namespace influxql
} // namespace tsdb
} // namespace kudu
