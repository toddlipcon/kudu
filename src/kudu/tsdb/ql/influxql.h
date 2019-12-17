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

#include <memory>
#include <string>
#include <vector>

#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <glog/logging.h>

#include "kudu/util/status.h"

namespace peg {
class parser;
} // namespace peg

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
  std::unique_ptr<peg::parser> parser_;
};


} // namespace influxql
} // namespace tsdb
} // namespace kudu
