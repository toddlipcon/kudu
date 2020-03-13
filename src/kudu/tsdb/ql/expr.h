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

#include <string>
#include <vector>

#include "kudu/tsdb/ql/influxql.h"

namespace kudu {
namespace tsdb {
namespace influxql {

struct ExprAnalysis {
  void AppendToString(const std::string& indent, std::string* str);

  // What types of fields this expr and its descendents reference.
  // Note that this is a bitmask: an expr may reference several of these
  // (eg "WHERE time > 10 and hostname = 'foo' and cpu > 10")
  uint32_t references = 0;
  enum {
    // Only constants.
    REF_CONST = 0,
    // Tags values.
    REF_TAGS = 1 << 0,
    // The special 'time' field.
    REF_TIME = 1 << 1,
    // Fields of metrics.
    REF_METRICS = 1 << 2,
  };

  // Whether this node or any descendent is an aggregate function.
  bool has_aggregates = false;

  // Whether this node itself is an aggregate function call.
  bool is_aggregate_function = false;
};


struct Expr : public AstNode {
  Status WalkTreeTopDown(const std::function<Status(Expr*)>& f) {
    RETURN_NOT_OK(f(this));
    return MapChildExprs([&](Expr* e) {
                           return e->WalkTreeTopDown(f);
                         });
  }

  virtual Status MapChildExprs(const std::function<Status(Expr*)>& f) {
    return Status::OK();
  }

  virtual std::string ToQL() const = 0;

  boost::optional<ExprAnalysis> analysis;
};

struct LiteralExpr : public Expr {
};

struct IntLiteralExpr : public LiteralExpr {
  explicit IntLiteralExpr(int64_t val) : val_(val) {}
  const int64_t val_;
  void AppendToString(const std::string& indent, std::string* str) override;
  std::string ToQL() const override;
};

struct DoubleLiteralExpr : public LiteralExpr {
  explicit DoubleLiteralExpr(double val) : val_(val) {}
  const double val_;
  void AppendToString(const std::string& indent, std::string* str) override;
  std::string ToQL() const override;
};

struct DurationLiteralExpr : public LiteralExpr {
  explicit DurationLiteralExpr(int64_t val, std::string unit)
      : val_(val),
        unit_(unit) {
  }
  void AppendToString(const std::string& indent, std::string* str) override;
  std::string ToQL() const override;

  int64_t ToMicroseconds() const;
  
  const int64_t val_;
  const std::string unit_;
};

struct StringLiteralExpr : public LiteralExpr {
  // TODO(todd) handle escaping?
  explicit StringLiteralExpr(std::string val) : val_(val) {}

  void AppendToString(const std::string& indent, std::string* str) override;
  std::string ToQL() const override;

  const std::string val_;
};

struct StarExpr : public Expr {
  std::string ToQL() const override;
  void AppendToString(const std::string& indent, std::string* str) override;
};

struct FieldRefExpr : public Expr {
  explicit FieldRefExpr(std::string f) : field_(std::move(f)) {}

  void AppendToString(const std::string& indent, std::string* str) override;
  std::string ToQL() const override;

  const std::string field_;
};


struct CallExpr : public Expr {
  explicit CallExpr(std::string func, std::vector<Expr*> args) :
      func_(std::move(func)),
      args_(std::move(args)) {
  }
  Status MapChildExprs(const std::function<Status(Expr*)>& f) override {
    for (Expr* e : args_) {
      RETURN_NOT_OK(f(e));
    }
    return Status::OK();
  }
  void AppendToString(const std::string& indent, std::string* str) override;
  std::string ToQL() const override;

  const std::string func_;
  const std::vector<Expr*> args_;
};

struct BinaryExpr : public Expr {
  BinaryExpr(Expr* l, Expr* r, std::string op)
      : l_(l), r_(r), op_(std::move(op)) {
  }
  Status MapChildExprs(const std::function<Status(Expr*)>& f) override {
    RETURN_NOT_OK(f(l_));
    RETURN_NOT_OK(f(r_));
    return Status::OK();
  }
  void AppendToString(const std::string& indent, std::string* str) override;
  std::string ToQL() const override;

  Expr* const l_;
  Expr* const r_;
  const std::string op_;
};

struct BooleanExpr : public Expr {
  enum Mode {
    CONJUNCTION,
    DISJUNCTION
  };

  BooleanExpr(Mode mode, std::vector<Expr*> exprs)
      : mode_(mode), exprs_(std::move(exprs)) {
    CHECK_GT(exprs_.size(), 1);
  }
  Status MapChildExprs(const std::function<Status(Expr*)>& f) override {
    for (Expr* e : exprs_) {
      RETURN_NOT_OK(f(e));
    }
    return Status::OK();
  }

  void AppendToString(const std::string& indent, std::string* str) override;
  std::string ToQL() const override;

  const Mode mode_;
  const std::vector<Expr*> exprs_;
};

} // namespace influxql
} // namespace tsdb
} // namespace kudu
