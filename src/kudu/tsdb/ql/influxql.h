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

#include <memory>
#include <string>
#include <vector>

#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <glog/logging.h>

#include "kudu/util/status.h"

namespace kudu {
namespace tsdb {
namespace influxql {

struct Predicate {
  std::string field;
  std::string op;
  boost::variant<int64_t, std::string, double> val;
};


struct Expr;
class QContext;

struct AstNode {
  virtual ~AstNode() = default;

  template<class T>
  bool IsA() const {
    return dynamic_cast<const T*>(this) != nullptr;
  }

  template<class T>
  T* As() {
    return dynamic_cast<T*>(this);
  }
  template<class T>
  const T* As() const {
    return dynamic_cast<const T*>(this);
  }

  virtual void AppendToString(const std::string& indent,
                              std::string* str) = 0;

};

struct FromClause {
  std::string measurement;
};

struct SelectStmt {
  std::vector<Expr*> select_exprs_;
  std::vector<Expr*> group_by_;
  FromClause from_;
  Expr* where_ = nullptr;
};

class Parser {
 public:
  explicit Parser(QContext* ctx);
  ~Parser();

  Status ParseSelectStatement(const std::string& q, SelectStmt** sel);

 private:
  QContext* ctx_;
};


} // namespace influxql
} // namespace tsdb
} // namespace kudu
