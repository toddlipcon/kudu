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

#include <unordered_set>
#include <vector>

#include <boost/optional.hpp>

#include "kudu/client/schema.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"


namespace kudu {
namespace tsdb {
namespace influxql {

struct AnalyzedSelectStmt;
struct Expr;
struct SelectStmt;
class QContext;

struct AnalyzedDimensions {
  std::vector<std::string> tag_keys;
  boost::optional<int64_t> time_granularity_us;

  std::string ToString() const;
};

struct TimeRange {
  boost::optional<int64_t> min_us;
  boost::optional<int64_t> max_us;

  std::string ToString() const;
};

struct ExprAnalysisContext {
  enum ClauseType {
    SELECT,
    WHERE,
    GROUP_BY
  };

  ExprAnalysisContext(AnalyzedSelectStmt* stmt,
                      ClauseType t)
      : stmt(stmt), clause_type(t) {
  }

  AnalyzedSelectStmt* const stmt;
  const ClauseType clause_type;
};

struct PlacedPredicates {
  std::vector<Expr*> tag;
  std::vector<Expr*> metric;
  std::vector<Expr*> time;
};

struct AnalyzedFromClause {
  std::map<std::string, client::KuduColumnSchema::DataType> column_types;
};

struct AnalyzedSelectStmt {
  explicit AnalyzedSelectStmt(SelectStmt* stmt);

  SelectStmt* const stmt;

  // Populated by AnalyzeFromClause.
  boost::optional<AnalyzedFromClause> from_clause;

  // Populated by PlacePredicates.
  boost::optional<PlacedPredicates> predicates;

  // Populated by AnalyzeTimeRange.
  boost::optional<TimeRange> time_range;

  // Populated by AnalyzeDimensions.
  boost::optional<AnalyzedDimensions> dimensions;

  // Populated by AnalyzeSelectList.
  boost::optional<std::vector<StringPiece>> selected_fields;
  boost::optional<std::vector<Expr*>> select_exprs;

  int64_t now_us;
  bool is_aggregate;
};

class Analyzer {
 public:
  explicit Analyzer(QContext* ctx)
      : ctx_(ctx) {
  }
  ~Analyzer();

  Status AnalyzeSelectStmt(SelectStmt* stmt, AnalyzedSelectStmt** asel);

  // TODO(todd) make all the below private with test friend.

  Status AnalyzeFromClause(AnalyzedSelectStmt* stmt);
  Status AnalyzeSelectList(AnalyzedSelectStmt* stmt);
  Status AnalyzeWhereClause(AnalyzedSelectStmt* stmt);

  Status AnalyzeExpr(Expr* node, const ExprAnalysisContext& ctx);

  // Analyze the where clause predicate 'predicate' and extract
  // conjunctions against each of tags, metrics, and time.
  Status PlacePredicates(AnalyzedSelectStmt* stmt);

  Status AnalyzeTimeRange(AnalyzedSelectStmt* stmt);

  Status AnalyzeDimensions(AnalyzedSelectStmt* stmt);


  static bool ParseRFC3339(StringPiece s, int64_t* us);

 private:
  Status AnalyzeTimeRange(const std::vector<Expr*>& time_predicates,
                          TimeRange* time_range);
  Status PlacePredicates(Expr* predicate, PlacedPredicates* preds);

  // Check that all field references in expressions are either aggregates or grouping
  // column (dimension) references.
  Status CheckFieldRefsInSelect(AnalyzedSelectStmt* stmt, Expr* expr);

  QContext* const ctx_;

  DISALLOW_COPY_AND_ASSIGN(Analyzer);
};

} // namespace influxql
} // namespace tsdb
} // namespace kudu
