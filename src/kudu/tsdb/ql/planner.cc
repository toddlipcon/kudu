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

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tsdb/series_id.h"
#include "kudu/tsdb/series_store.h"
#include "kudu/tsdb/ql/agg.h"
#include "kudu/tsdb/ql/analysis.h"
#include "kudu/tsdb/ql/planner.h"
#include "kudu/tsdb/ql/expr.h"

#include <limits>
#include <vector>
#include <string>
#include <unordered_map>

using std::pair;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tsdb {
namespace influxql {

// Select series that match various values of a given tag key.
// equivalent to 'tag IN (...)'
class SingleTagSeriesSelector : public SeriesSelector {
 public:
  SingleTagSeriesSelector(const AnalyzedSelectStmt* stmt,
                          std::string tag_key,
                          vector<std::string> tag_values,
                          bool return_tag_values)
      : measurement_(stmt->stmt->from_.measurement),
        tag_key_(std::move(tag_key)),
        tag_values_(uniquify(std::move(tag_values))),
        return_tag_values_(return_tag_values) {
  }

  ~SingleTagSeriesSelector() = default;

  Status Execute(QContext* ctx, std::vector<SeriesIdWithTags>* series) override {
    // TODO(todd) enable parallelism.
    series->clear();
    for (const auto& tag_value : tag_values_) {
      SeriesIdWithTags::TagMap tag_map;
      if (return_tag_values_) {
        tag_map = {{tag_key_, tag_value}};
      }
      vector<int32_t> ids;
      RETURN_NOT_OK(ctx->series_store()->FindMatchingSeries(measurement_, tag_key_, tag_value, &ids));
      for (int32_t id : ids) {
        series->emplace_back(id, tag_map);
      }
    }
    return Status::OK();
  }

  string ToString() const override {
    return Substitute("SingleTagSeriesSelector($0, $1, [$2])",
                      measurement_, tag_key_, JoinStrings(tag_values_, ","));
  }

 private:
  // We make the tag values unique so that if someone passes the same value
  // twice we don't end up yielding the results twice.
  static vector<string> uniquify(vector<string> v) {
    v.erase(std::unique(v.begin(), v.end()), v.end());
    return v;
  }

  const std::string measurement_;
  const std::string tag_key_;
  const vector<std::string> tag_values_;
  const bool return_tag_values_;
};

class AllSeriesSelector : public SeriesSelector {
 public:
  AllSeriesSelector(string measurement, vector<string> fetch_tag_keys)
      : measurement_(std::move(measurement)),
        fetch_tag_keys_(std::move(fetch_tag_keys)) {
  }
  ~AllSeriesSelector() = default;

  Status Execute(QContext* ctx, std::vector<SeriesIdWithTags>* series) override {
    return ctx->series_store()->FindAllSeries(measurement_, fetch_tag_keys_, series);
  }

  string ToString() const override {
    return Substitute("AllSeriesSelector($0)", measurement_);
  }

 private:
  const string measurement_;
  const vector<string> fetch_tag_keys_;
};


Planner::~Planner() = default;

Status Planner::PlanSeriesSelector(const AnalyzedSelectStmt* asel,
                                   SeriesSelector** selector) {
  // TODO(todd) if a tag shows up in the select clause, need to also
  // materialize it in series selection.

  const auto& exprs = asel->predicates->tag;
  if (exprs.empty()) {
    *selector = ctx_->Alloc<AllSeriesSelector>(
        asel->stmt->from_.measurement,
        asel->dimensions->tag_keys);
    return Status::OK();
  }
  if (exprs.size() != 1) {
    return Status::InvalidArgument("unable to process multiple conjunct tag conditions");
  }
  Expr* expr = exprs[0];
  RETURN_NOT_OK(ConvertExprToSelector(asel, expr, selector));
  return Status::OK();
}

Status Planner::ConvertExprToSelector(const AnalyzedSelectStmt* asel,
                                      const Expr* expr,
                                      SeriesSelector** selector) {
  if (auto* be = expr->As<BooleanExpr>()) {
    if (be->mode_ != BooleanExpr::DISJUNCTION) {
      return Status::InvalidArgument("unable to process tag conjunction", expr->ToQL());
    }

    unordered_map<string, vector<string>> key_vals;
    for (Expr* e : be->exprs_) {
      string tag_key, tag_val;
      RETURN_NOT_OK(ConvertTagEqualityExpr(e, &tag_key, &tag_val));
      key_vals[tag_key].emplace_back(std::move(tag_val));
    }
    if (key_vals.size() != 1) {
      return Status::InvalidArgument("unable to process disjunction across multiple tags");
    }

    const auto& tag_key = key_vals.begin()->first;
    const auto& tag_values = key_vals.begin()->second;
    return CreateSingleTagSelector(asel, tag_key, tag_values, selector);
  }

  if (auto* be = expr->As<BinaryExpr>()) {
    string tag_key, tag_val;
    RETURN_NOT_OK(ConvertTagEqualityExpr(be, &tag_key, &tag_val));
    return CreateSingleTagSelector(asel, tag_key, vector<string>{tag_val}, selector);
  }

  return Status::InvalidArgument("could not handle tag subexpression", expr->ToQL());
}

Status Planner::CreateSingleTagSelector(const AnalyzedSelectStmt* asel,
                                        string tag_key,
                                        vector<string> tag_values,
                                        SeriesSelector** selector) {
  bool return_tag_values;

  const auto& dim_tags = asel->dimensions->tag_keys;
  if (dim_tags.size() == 1 && dim_tags[0] == tag_key) {
    return_tag_values = true;
  } else if (dim_tags.empty()) {
    return_tag_values = false;
  } else {
    return Status::NotSupported("cannot current combine a where on one tag with a group by on another");
  }
  *selector = ctx_->Alloc<SingleTagSeriesSelector>(asel, tag_key, std::move(tag_values), return_tag_values);
  return Status::OK();
}

Status Planner::ConvertTagEqualityExpr(const Expr* expr,
                                       string* tag_key,
                                       string* tag_val) {

  Predicate pred;
  RETURN_NOT_OK(ConvertBinaryExpr(expr, &pred));
  string* str_val = boost::get<string>(&pred.val);
  if (pred.op != "=" || !str_val) {
    return Status::InvalidArgument("can only handle tag conditions the form (tag = 'val')",
                                   expr->ToQL());
  }
  *tag_key = std::move(pred.field);
  *tag_val = std::move(*str_val);
  return Status::OK();
}

Status Planner::ConvertBinaryExpr(const Expr* expr,
                                  Predicate* pred) {
  auto* be = expr->As<BinaryExpr>();
  if (!be) {
    return Status::InvalidArgument("expected binary comparison expression", expr->ToQL());
  }

  auto* tag_fieldref = be->l_->As<FieldRefExpr>();
  if (!tag_fieldref) {
    return Status::NotSupported("can only express binary comparisons with a field on the left", be->ToQL());
  }

  pred->field = tag_fieldref->field_;
  pred->op = be->op_;
  if (auto* str_lit = be->r_->As<StringLiteralExpr>()) {
    pred->val = str_lit->val_;
  } else if (auto* int_lit = be->r_->As<IntLiteralExpr>()) {
    pred->val = int_lit->val_;
  } else if (auto* double_lit = be->r_->As<DoubleLiteralExpr>()) {
    pred->val = double_lit->val_;
  } else {
    return Status::NotSupported("expression not supported (expected int or string literal)", be->r_->ToQL());
  }
  return Status::OK();
}

Status Planner::PlanSelectExpressions(const AnalyzedSelectStmt* asel,
                                      TSBlockConsumerFactory* factory) {
  if (asel->is_aggregate) {
    return PlanSelectAggregate(asel, factory);
  } else {
    return PlanProjection(asel, factory);
  }
}

Status Planner::PlanProjection(const AnalyzedSelectStmt* asel,
                               TSBlockConsumerFactory* factory) {
  vector<string> fields;
  for (const Expr* expr : *asel->select_exprs) {
    auto* fieldref = expr->As<FieldRefExpr>();
    if (!fieldref){
      return Status::NotSupported("can only plan selection of simple fields", expr->ToQL());
    }
    fields.emplace_back(fieldref->field_);
  }
  *factory = [=](TSBlockConsumer* downstream, unique_ptr<TSBlockConsumer>* eval) {
               return CreateProjectionEvaluator(fields, downstream, eval);
             };
  return Status::OK();
}

Status Planner::PlanSelectAggregate(const AnalyzedSelectStmt* asel,
                                    TSBlockConsumerFactory* factory) {
  CHECK(asel->is_aggregate);

  vector<pair<string, string>> aggs_and_fields;
  for (const Expr* expr : *asel->select_exprs) {
    auto* func = expr->As<CallExpr>();
    if (!func || !func->analysis->is_aggregate_function) {
      return Status::NotSupported("can only plan aggregate functions in the SELECT clause",
                                  expr->ToQL());
    }
    if (func->args_.size() != 1) {
      return Status::InvalidArgument("missing aggregate arg", expr->ToQL());
    }
    auto* fieldref = func->args_[0]->As<FieldRefExpr>();
    if (!fieldref){
      return Status::NotSupported("can only plan aggregates over simple fields", expr->ToQL());
    }
    aggs_and_fields.emplace_back(func->func_, fieldref->field_);
  }


  Bucketer bucketer(asel->time_range->min_us.value_or(0),
                    asel->time_range->max_us.value_or(asel->now_us),
                    asel->dimensions->time_granularity_us.value_or(std::numeric_limits<int64_t>::max()));

  *factory = [=](TSBlockConsumer* downstream, unique_ptr<TSBlockConsumer>* eval) {
               return CreateMultiAggExpressionEvaluator(aggs_and_fields, bucketer, downstream, eval);
             };
  return Status::OK();
}

Status Planner::PlanPredicates(
    const AnalyzedSelectStmt* asel,
    vector<Predicate>* predicates) {

  predicates->clear();
  for (const auto* expr : asel->predicates->metric) {
    Predicate pred;
    RETURN_NOT_OK(ConvertBinaryExpr(expr, &pred));
    predicates->emplace_back(std::move(pred));
  }

  return Status::OK();
}


} // namespace influxql
} // namespace tsdb
} // namespace kudu
