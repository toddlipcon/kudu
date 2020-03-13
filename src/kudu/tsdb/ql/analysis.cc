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

#include <algorithm>
#include <limits>
#include <unordered_set>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tsdb/metrics_store.h"
#include "kudu/tsdb/ql/analysis.h"
#include "kudu/tsdb/ql/expr.h"
#include "kudu/tsdb/ql/qcontext.h"

using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tsdb {
namespace influxql {

string AnalyzedDimensions::ToString() const {
  return Substitute("{tag keys: [$0], time interval: $1}",
                    JoinStrings(tag_keys, ","),
                    time_granularity_us ? *time_granularity_us: -1);
}

string TimeRange::ToString() const {
  return Substitute("[$0, $1]",
                    min_us ? *min_us : 0,
                    max_us ? *max_us : std::numeric_limits<int64_t>::max());
}

Analyzer::~Analyzer() = default;

Status Analyzer::AnalyzeSelectStmt(SelectStmt* stmt, AnalyzedSelectStmt** asel) {
  *asel = ctx_->Alloc<AnalyzedSelectStmt>(stmt);
  RETURN_NOT_OK(AnalyzeFromClause(*asel));
  RETURN_NOT_OK(AnalyzeWhereClause(*asel));
  RETURN_NOT_OK(PlacePredicates(*asel));
  RETURN_NOT_OK(AnalyzeDimensions(*asel));
  RETURN_NOT_OK(AnalyzeTimeRange(*asel));
  RETURN_NOT_OK(AnalyzeSelectList(*asel));
  return Status::OK();
}

Status Analyzer::AnalyzeFromClause(AnalyzedSelectStmt* stmt) {
  AnalyzedFromClause afc;
  Status s = ctx_->metrics_column_source()->GetColumnsForMeasurement(
      stmt->stmt->from_.measurement, &afc.column_types);
  if (s.IsNotFound()) {
    return Status::NotFound("measurement not found", stmt->stmt->from_.measurement);
  }
  RETURN_NOT_OK(s);
  stmt->from_clause = std::move(afc);
  return Status::OK();
}

Status Analyzer::CheckFieldRefsInSelect(AnalyzedSelectStmt* stmt, Expr* expr) {
  if (expr->analysis->is_aggregate_function) {
    return Status::OK();
  }
  if (auto* fr = expr->As<FieldRefExpr>()) {
    const auto& valid_keys = stmt->dimensions->tag_keys;
    if (std::find(valid_keys.begin(), valid_keys.end(), fr->field_) == valid_keys.end()) {
      return Status::InvalidArgument(Substitute("field reference to $0 is not a grouped dimension", fr->ToQL()));
    }
  }
  return expr->MapChildExprs([&](Expr* child) {
                               return CheckFieldRefsInSelect(stmt, child);
                             });
}

Status Analyzer::AnalyzeSelectList(AnalyzedSelectStmt* stmt) {
  CHECK(stmt->dimensions != boost::none) << "Must AnalyzeDimensions";
  CHECK(stmt->selected_fields == boost::none) << "already analyzed";

  bool has_aggregates = false;

  // Expand any '*' references in the select clause to all of the
  // columns of the table in the FROM clause.
  vector<Expr*> expanded_select_exprs;
  for (auto* expr : stmt->stmt->select_exprs_) {
    if (expr->IsA<StarExpr>()) {
      for (const auto& p : stmt->from_clause->column_types) {
        expanded_select_exprs.push_back(ctx_->Alloc<FieldRefExpr>(p.first));
      }
    } else {
      expanded_select_exprs.push_back(expr);
    }
  }

  for (auto* expr : expanded_select_exprs) {
    RETURN_NOT_OK(AnalyzeExpr(expr, ExprAnalysisContext(stmt, ExprAnalysisContext::SELECT)));
    has_aggregates |= expr->analysis->has_aggregates;
  }

  if (has_aggregates) {
    for (auto* expr : expanded_select_exprs) {
      RETURN_NOT_OK(CheckFieldRefsInSelect(stmt, expr));
    }
  }

  unordered_set<StringPiece> fields;
  for (auto* expr : expanded_select_exprs) {
    RETURN_NOT_OK(expr->WalkTreeTopDown(
        [&](Expr* e) {
          auto* fr = e->As<FieldRefExpr>();
          if (fr && e->analysis->references & ExprAnalysis::REF_METRICS) {
            fields.emplace(fr->field_);
          }
          return Status::OK();
        }));
  }
  stmt->select_exprs = std::move(expanded_select_exprs);
  stmt->selected_fields.emplace(fields.begin(), fields.end());
  stmt->is_aggregate = has_aggregates;

  return Status::OK();
}

Status Analyzer::AnalyzeWhereClause(AnalyzedSelectStmt* stmt) {
  if (!stmt->stmt->where_) return Status::OK();
  return AnalyzeExpr(stmt->stmt->where_, ExprAnalysisContext(stmt, ExprAnalysisContext::WHERE));
}


// where (taga = 'foo' or tagb = 'bar') and time > 3
//
// AND                     #ref(tag,series)
//   OR                    # ref(tag)
//     taga = foo          # ref(tag)
//     tagb = bar          # ref(tag)
//   time > 3
//
// visit bottom-up: mark nodes whether they or any children access tag, series
// top-down: for conjunctions, move any expr that only references tag to tag selection,
//  remove from conjunction. similarly separate metrics filters.
// expect to not have any "mixed" when done.
Status Analyzer::AnalyzeExpr(Expr* node, const ExprAnalysisContext& ctx) {
  if (node->analysis != boost::none) return Status::OK();

  ExprAnalysis ea;

  bool has_child_aggregates = false;
  RETURN_NOT_OK(node->MapChildExprs(
      [&](Expr* child) {
        RETURN_NOT_OK(AnalyzeExpr(child, ctx));
        ea.references |= child->analysis->references;
        has_child_aggregates |= child->analysis->has_aggregates | child->analysis->is_aggregate_function;
        return Status::OK();
      }));

  // Analyze function calls to determine if this is an aggregate expression.
  if (auto* e = node->As<CallExpr>()) {
    // TODO(todd) some kind of function registry
    if (e->func_ == "max" || e->func_ == "mean") {
      if (e->args_.size() != 1) {
        return Status::InvalidArgument("expected 1 argument", e->func_);
      }
      ea.is_aggregate_function = true;
    } else if (e->func_ == "time") {
      if (ctx.clause_type != ExprAnalysisContext::GROUP_BY) {
        return Status::InvalidArgument("time() function may only be used in a GROUP BY clause");
      }
      // otherwise handled in AnalyzeDimension
    } else {
      return Status::InvalidArgument("unknown function", e->func_);
    }

    if (ea.is_aggregate_function && has_child_aggregates) {
      return Status::InvalidArgument("aggregate function calls cannot be nested", node->ToQL());
    }
    if (ea.is_aggregate_function && ctx.clause_type != ExprAnalysisContext::SELECT) {
      return Status::InvalidArgument("aggregate function may only be in the SELECT list", node->ToQL());
    }
  }
  if (auto* e = node->As<FieldRefExpr>()) {
    if (e->field_ == "time") {
      ea.references = ExprAnalysis::REF_TIME;
    } else if (ContainsKey(ctx.stmt->from_clause->column_types, e->field_)) {
      ea.references = ExprAnalysis::REF_METRICS;
    } else {
      ea.references = ExprAnalysis::REF_TAGS;
    }
  }
  ea.has_aggregates = ea.is_aggregate_function || has_child_aggregates;
  node->analysis = ea;
  return Status::OK();
}

Status Analyzer::AnalyzeDimensions(AnalyzedSelectStmt* stmt) {
  ExprAnalysisContext ctx(stmt, ExprAnalysisContext::GROUP_BY);
  AnalyzedDimensions dims;
  for (auto* expr : stmt->stmt->group_by_) {
    RETURN_NOT_OK(AnalyzeExpr(expr, ctx));

    if (const auto* field = expr->As<FieldRefExpr>()) {
      if (field->field_ == "time") {
        return Status::InvalidArgument("must group by time with an interval argument");
      }
      if (field->analysis->references != ExprAnalysis::REF_TAGS) {
        return Status::InvalidArgument(Substitute(
            "cannot group by expression $0 (may only group by tag dimensions)",
            field->ToQL()));
      }
      dims.tag_keys.emplace_back(field->field_);
      continue;
    }

    if (auto* call = expr->As<CallExpr>()) {
      if (call->func_ != "time") {
        return Status::InvalidArgument(Substitute("may not group by function $0", call->func_));
      }
      if (call->args_.size() != 1) {
        // TODO(todd) influxql supports a multi-argument time() grouping with some offset.
        return Status::InvalidArgument("group by time(...) must have a single argument");
      }
      auto* dur = call->args_[0]->As<DurationLiteralExpr>();
      if (!dur) {
        return Status::InvalidArgument("unexpected argument to time(...): expected interval",
                                       call->args_[0]->ToQL());
      }

      if (dims.time_granularity_us) {
        return Status::InvalidArgument("time() grouping specified multiple times");
      }

      dims.time_granularity_us = dur->ToMicroseconds();
      continue;
    }

    return Status::InvalidArgument("unexpected expression in GROUP BY", expr->ToQL());
  }

  stmt->dimensions.emplace(std::move(dims));
  return Status::OK();
}

Status Analyzer::PlacePredicates(AnalyzedSelectStmt* stmt) {
  PlacedPredicates preds;

  if (stmt->stmt->where_ != nullptr) {
    RETURN_NOT_OK(PlacePredicates(stmt->stmt->where_, &preds));
  }
  stmt->predicates.emplace(std::move(preds));
  return Status::OK();
}

Status Analyzer::PlacePredicates(Expr* predicate, PlacedPredicates* preds) {
  CHECK(predicate->analysis != boost::none) << "unanalyzed predicate: "<< predicate->ToQL();
  if (predicate->analysis->references == ExprAnalysis::REF_TAGS) {
    preds->tag.push_back(predicate);
    return Status::OK();
  }
  if (predicate->analysis->references == ExprAnalysis::REF_METRICS) {
    preds->metric.push_back(predicate);
    return Status::OK();
  }
  if (predicate->analysis->references == ExprAnalysis::REF_TIME) {
    preds->time.push_back(predicate);
    return Status::OK();
  }
  if (predicate->analysis->references == ExprAnalysis::REF_CONST) {
    return Status::InvalidArgument("cannot handle const predicates", predicate->ToQL());
  }

  // If an expression references both tags and metrics,
  // it needs to be a conjunction that we can evaluate separately
  auto* be = predicate->As<BooleanExpr>();
  if (!be || be->mode_ != BooleanExpr::CONJUNCTION) {
    return Status::InvalidArgument("cannot evaluate expression across metric values and tags",
                                   predicate->ToQL());
  }
  for (auto* e : be->exprs_) {
    RETURN_NOT_OK(PlacePredicates(e, preds));
  }
  return Status::OK();
}

bool Analyzer::ParseRFC3339(StringPiece s, int64_t* us) {
  if (s.size() != 20) {
    return false;
  }
  char buf[21];
  memcpy(buf, s.data(), 20);
  buf[20] = '\0';
  struct tm time;
  // Parse format: 2016-01-01T21:09:04Z
  // TODO(todd) influx supports some other formats too.
  const char* rem = strptime(buf, "%Y-%m-%dT%H:%M:%SZ", &time);
  if (rem == nullptr || *rem != '\0') {
    return false;
  }

  time_t unixtime = mkgmtime(&time);
  *us = unixtime * 1000000;
  return true;
}

Status ParseTimeExpr(const LiteralExpr* time_expr, int64_t* time_us) {
  if (auto* lit = time_expr->As<IntLiteralExpr>()) {
    // Int literals are in nanoseconds.
    *time_us = lit->val_ / 1000;
    return Status::OK();
  }

  if (auto* lit = time_expr->As<StringLiteralExpr>()) {
    if (Analyzer::ParseRFC3339(lit->val_, time_us)) {
      return Status::OK();
    }
  }

  return Status::InvalidArgument(Substitute("cannot parse time: $0", time_expr->ToQL()));
}

Status Analyzer::AnalyzeTimeRange(AnalyzedSelectStmt* stmt) {
  CHECK(stmt->time_range == boost::none);
  CHECK(stmt->predicates != boost::none);
  TimeRange tr;
  RETURN_NOT_OK(AnalyzeTimeRange(stmt->predicates->time, &tr));
  stmt->time_range.emplace(std::move(tr));
  return Status::OK();
}

Status Analyzer::AnalyzeTimeRange(const std::vector<Expr*>& time_predicates,
                                  TimeRange* time_range) {

  auto update_max = [&](int64_t t) {
                      time_range->max_us = time_range->max_us ? std::min(*time_range->max_us, t) : t;
                    };
  auto update_min = [&](int64_t t) {
                      time_range->min_us = time_range->min_us ? std::max(*time_range->min_us, t) : t;
                    };

  for (const auto* e : time_predicates) {
    if (auto* be = e->As<BinaryExpr>()) {
      const auto* l = be->l_->As<FieldRefExpr>();
      const auto* r = be->r_->As<LiteralExpr>();
      if (l && l->field_ == "time") {
        int64_t time_us = 0;
        RETURN_NOT_OK(ParseTimeExpr(r, &time_us));

        if (be->op_ == "<=") {
          update_max(time_us);
        } else if (be->op_ == "<") {
          update_max(time_us - 1);
        } else if (be->op_ == "=") {
          update_max(time_us);
          update_min(time_us);
        } else if (be->op_ == ">") {
          update_min(time_us + 1);
        } else if (be->op_ == ">=") {
          update_min(time_us);
        } else {
          return Status::InvalidArgument("unexpected time comparison operator", be->op_);
        }
      }
    } else if (auto* boolexp = e->As<BooleanExpr>()) {
      if (boolexp->mode_ == BooleanExpr::CONJUNCTION) {
        RETURN_NOT_OK(AnalyzeTimeRange(boolexp->exprs_, time_range));
      }
    } else {
      return Status::InvalidArgument("cannot analyze time predicate", e->ToQL());
    }
  }
  return Status::OK();
}

AnalyzedSelectStmt::AnalyzedSelectStmt(SelectStmt* stmt)
    : stmt(stmt),
      now_us(GetCurrentTimeMicros()) {
}

} // namespace influxql
} // namespace tsdb
} // namespace kudu
