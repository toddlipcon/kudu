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
