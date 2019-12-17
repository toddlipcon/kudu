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

#include "kudu/tsdb/ql/expr.h"

#include <memory>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

using std::string;
using std::vector;

namespace kudu {
namespace tsdb {
namespace influxql {

void ExprAnalysis::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, "References: [");
  const char* sep = "";
  if (references & REF_TAGS) {
    StrAppend(str, "tags");
    sep = ",";
  }
  if (references & REF_METRICS) {
    StrAppend(str, sep, "metrics");
  }
  if (references & REF_TIME) {
    StrAppend(str, sep, "time");
  }
  StrAppend(str, "]\n");
}



string DurationLiteralExpr::ToQL() const {
  return StrCat(val_, unit_);
}

void DurationLiteralExpr::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, val_, unit_, "\n");
  if (analysis) {
    analysis->AppendToString(indent + "  ", str);
  }
}

int64_t DurationLiteralExpr::ToMicroseconds() const {
  if (unit_ == "u" || unit_ == "µ") {
    return val_;
  } else if (unit_ == "ms") {
    return val_ * 1000;
  } else if (unit_ == "s") {
    return val_ * 1000000L;
  } else if (unit_ == "m") {
    return val_ * 1000000L * 60;
  } else if (unit_ == "h") {
    return val_ * 1000000L * 60 * 60;
  } else if (unit_ == "d") {
    return val_ * 1000000L * 60 * 60 * 24;
  } else if (unit_ == "w") {
    return val_ * 1000000L * 60 * 60 * 24 * 7;
  }
  LOG(FATAL) << "unhandled unit: " << unit_;
  }


string StringLiteralExpr::ToQL() const {
  return StrCat("'", val_, "'");
}

void StringLiteralExpr::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, val_, "\n");
  if (analysis) {
    analysis->AppendToString(indent + "  ", str);
  }
}


string IntLiteralExpr::ToQL() const {
  return StrCat(val_);
}

void IntLiteralExpr::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, val_, "\n");
  if (analysis) {
    analysis->AppendToString(indent + "  ", str);
  }
}

string DoubleLiteralExpr::ToQL() const {
  return StrCat(val_);
}

void DoubleLiteralExpr::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, val_, "\n");
  if (analysis) {
    analysis->AppendToString(indent + "  ", str);
  }
}


string StarExpr::ToQL() const {
  return "*";
}

void StarExpr::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, "Star\n");
  if (analysis) {
    analysis->AppendToString(indent + "  ", str);
  }
}

string FieldRefExpr::ToQL() const {
  return field_;
}

void FieldRefExpr::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, "FieldRef: ", field_, "\n");
  if (analysis) {
    analysis->AppendToString(indent + "  ", str);
  }
}


string BinaryExpr::ToQL() const {
  return StrCat("(", l_->ToQL(), " ", op_, " ", r_->ToQL(), ")");
}

void BinaryExpr::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, "BinaryExpr:", op_,  "\n");
  if (analysis) {
    analysis->AppendToString(indent + "  ", str);
  }
  StrAppend(str, indent, "  Operands:\n");
  l_->AppendToString(indent + "    ", str);
  r_->AppendToString(indent + "    ", str);
}


string BooleanExpr::ToQL() const {
  const char* delim = mode_ == DISJUNCTION ? " OR " : " AND ";
  return StrCat("(", JoinMapped(exprs_, [](Expr* e) { return e->ToQL(); }, delim), ")");
}

void BooleanExpr::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, "BooleanExpr: ", mode_ == DISJUNCTION ? "OR" : "AND", "\n");
  if (analysis) {
    analysis->AppendToString(indent + "  ", str);
  }
  StrAppend(str, indent, "  Exprs:\n");
  for (auto* e : exprs_) {
    e->AppendToString(indent + "    ", str);
  }
}


string CallExpr::ToQL() const {
  return StrCat(func_, "(",
                JoinMapped(args_, [](Expr* e) { return e->ToQL(); }, ","),
                ")");
}

void CallExpr::AppendToString(const string& indent, string* str) {
  StrAppend(str, indent, "CallExpr: ", func_,  "\n");
  if (analysis) {
    analysis->AppendToString(indent + "  ", str);
  }
  StrAppend(str, indent, "  Args:\n");
  for (auto* e : args_) {
    e->AppendToString(indent + "    ", str);
  }
}


} // namespace influxql
} // namespace tsdb
} // namespace kudu
