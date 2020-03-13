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
