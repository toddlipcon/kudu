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

#include "kudu/tsdb/ql/influxql.h"

#include <mutex>
#include <iostream>
#include <memory>
#include <vector>
#include <cstdlib>

#include "peglib.h"
#include <glog/logging.h>

#include "kudu/tsdb/ql/expr.h"
#include "kudu/tsdb/ql/qcontext.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"
#include "kudu/util/memory/arena.h"

using peg::Ast;
using peg::Parser;
using strings::Substitute;
using std::shared_ptr;
using std::string;
using std::vector;

namespace kudu {
namespace tsdb {
namespace influxql {

namespace {

static const char* kGrammar = R"(
SelectStmt <- 'select'i Fields FromClause WhereClause? GroupByClause? ';'?

WhereClause <- 'where'i Expr

Fields <- Field (',' Field)*
Field <- Expr Alias? / Star
Alias <- 'AS' Identifier

FromClause <- 'from'i Measurement
GroupByClause <- 'group by'i Dimensions

Dimension <- Expr
Dimensions <- Dimension (',' Dimension)*

# Expressions
VarRef <- Measurement
FuncCall <- Identifier '(' FuncArgs? ')'
FuncArgs <- Expr (',' Expr)*
Expr0 <- '(' Expr ')' / FuncCall / VarRef / StringLiteral / DurationLiteral / DoubleLiteral / IntLiteral
Product <- Expr0 (ProductOp Product)?
Sum <- Product (AddOrSubtractOp Sum)?
Comparison <- Sum (ComparisonOp Sum)?
MaybeNegation <- Comparison / ('not'i MaybeNegation)
Conjunction <- MaybeNegation ('and'i Conjunction)*
Disjunction <- Conjunction ('or' Disjunction)*
Expr <- Disjunction
ComparisonOp <-  <'<=' / '<' / '=' / '>=' / '>'>
AddOrSubtractOp <- <'+' / '-'>
ProductOp <- <'*' / '/' / '%'>

# Identifiers
Measurement     <- MeasurementName
MeasurementName <- Identifier
UnquotedIdentifier <- < [a-zA-Z][a-zA-Z0-9_]* >
QuotedIdentifier <- StringLiteral
Identifier <- UnquotedIdentifier / QuotedIdentifier

# Literals
doublequote <- '"'
quote <- '\''
backslash <- '\\'

StringLiteral <- quote <(!quote Char)*> quote
Char   <- backslash ( doublequote  # '\' Escapes
                        / quote
                        / backslash
                        / [bfnrt]
                        / [0-2][0-7][0-7]
                        / [0-7][0-7]?
                        / 'x' Hex Hex
                        / 'u' Hex Hex Hex Hex
                        / 'U' Hex Hex Hex Hex Hex Hex Hex Hex
                        )
             / . # Or any char, really
Hex     <- [0-9a-fA-F]

DurationLiteral <- IntLiteral DurationUnit
IntLiteral <- <( '+' / '-' )?[1-9][0-9]*>

DoubleLiteral <- <IntLiteral '.' [0-9]+>

# If adding a new unit, update DurationLiteralExpr::ToMicroseconds()
# accordingly.
DurationUnit <- 'u' / 'µ' / 'ms' / 's' / 'm' / 'h' / 'd' / 'w'

Star <- '*'

%whitespace <- [ \t\r\n]*
)";


// Converts a peglib AST into our own parse tree
class AstConverter {
 public:
  explicit AstConverter(QContext* ctx)
      : ctx_(ctx) {
  }

  Status ConvertSelect(const std::shared_ptr<Ast>& ast,
                       SelectStmt** sel) {
    const auto& opt = peg::AstOptimizer(
        true, {"Fields", "FromClause", "WhereClause", "FuncArgs",
               "GroupByClause", "Dimensions"}).optimize(ast);
    VLOG(2) << peg::ast_to_s(opt);
    *sel = ctx_->Alloc<SelectStmt>();
    return ParseSelect(opt, *sel);
  }

 private:
  QContext* ctx_;

  static Status CheckAstName(const shared_ptr<Ast>& ast, const string& expected) {
    if (ast->name != expected) {
      return ParseError(ast, Substitute("unexpected ast node $0, expected $1",
                                        ast->name, expected));
    }
    return Status::OK();
  }

  static Status CheckNodeCount(const shared_ptr<Ast>& ast, int n) {
    if (ast->nodes.size() != n) {
      return ParseError(ast, Substitute("expected $0 children of node $1",
                                        n, ast->name));
    }
    return Status::OK();
  }

  Status ParseFields(const shared_ptr<Ast>& ast, vector<Expr*>* fields) {
    RETURN_NOT_OK(CheckAstName(ast, "Fields"));
    if (ast->nodes.empty()) {
      return ParseError(ast, "expected at least one field");
    }
    return ParseChildrenAsExprs(ast, fields);
  }

  Status ParseChildrenAsExprs(const shared_ptr<Ast>& parent,
                              vector<Expr*>* exprs) {
    exprs->clear();
    for (const auto& n : parent->nodes) {
      Expr* e;
      RETURN_NOT_OK(ParseExpr(n, &e));
      exprs->push_back(e);
    }
    return Status::OK();
  }

  Status ParseExpr(const shared_ptr<Ast>& ast, Expr** expr) {
    // ============================================================
    // Identifier.
    // ============================================================
    if (ast->name == "UnquotedIdentifier") {
      *expr = ctx_->Alloc<FieldRefExpr>(ast->token);
      return Status::OK();
    }

    if (ast->name == "Star") {
      *expr = ctx_->Alloc<StarExpr>();
      return Status::OK();
    }

    // ============================================================
    // Function call.
    // ============================================================
    if (ast->name == "FuncCall") {
      vector<Expr*> exprs;
      RETURN_NOT_OK(ParseChildrenAsExprs(ast->nodes[1], &exprs));
      *expr = ctx_->Alloc<CallExpr>(ast->nodes[0]->token, std::move(exprs));
      return Status::OK();
    }

    // ============================================================
    // Binary operators.
    // ============================================================
    if (ast->name == "Sum" ||
        ast->name == "Product" ||
        ast->name == "Comparison") {
      RETURN_NOT_OK(CheckNodeCount(ast, 3));
      Expr* l;
      Expr* r;
      RETURN_NOT_OK(ParseExpr(ast->nodes[0], &l));
      RETURN_NOT_OK(ParseExpr(ast->nodes[2], &r));
      *expr = ctx_->Alloc<BinaryExpr>(l, r, ast->nodes[1]->token);
      return Status::OK();
    }

    // ============================================================
    // AND/OR
    // ============================================================
    if (ast->name == "Conjunction" ||
        ast->name == "Disjunction") {
      auto mode = (ast->name == "Conjunction") ? BooleanExpr::CONJUNCTION : BooleanExpr::DISJUNCTION;
      vector<Expr*> exprs;
      RETURN_NOT_OK(ParseChildrenAsExprs(ast, &exprs));

      // Flatten structures like 'a AND (b AND c)' into a single conjunction/disjunction.
      vector<Expr*> flattened_exprs;
      for (auto* e : exprs) {
        auto* be = e->As<BooleanExpr>();
        if (be && be->mode_ == mode) {
          for (Expr* sub_expr : be->exprs_) {
            flattened_exprs.push_back(sub_expr);
          }
        } else {
          flattened_exprs.push_back(e);
        }
      }
      *expr = ctx_->Alloc<BooleanExpr>(
          mode,
          std::move(flattened_exprs));
      return Status::OK();
    }

    // ============================================================
    // Literals
    // ============================================================
    if (ast->name == "IntLiteral") {
      int64_t val;
      if (!safe_strto64(ast->token, &val)) {
        return ParseError(ast, "bad int literal");
      }
      *expr = ctx_->Alloc<IntLiteralExpr>(val);
      return Status::OK();
    }

    if (ast->name == "DoubleLiteral") {
      double val;
      if (!safe_strtod(ast->token, &val)) {
        return ParseError(ast, "bad int literal");
      }
      *expr = ctx_->Alloc<DoubleLiteralExpr>(val);
      return Status::OK();
    }

    if (ast->name == "DurationLiteral") {
      RETURN_NOT_OK(CheckNodeCount(ast, 2));
      RETURN_NOT_OK(CheckAstName(ast->nodes[0], "IntLiteral"));
      RETURN_NOT_OK(CheckAstName(ast->nodes[1], "DurationUnit"));
      int64_t val;
      if (!safe_strto64(ast->nodes[0]->token, &val)) {
        return ParseError(ast, "bad int literal");
      }
      *expr = ctx_->Alloc<DurationLiteralExpr>(val, ast->nodes[1]->token);
      return Status::OK();
    }

    if (ast->name == "StringLiteral") {
      *expr = ctx_->Alloc<StringLiteralExpr>(ast->token);
      return Status::OK();
    }

    return ParseError(ast, Substitute("unexpected expression AST node $0", ast->name));
  }

  Status ParseFrom(const shared_ptr<Ast>& ast, FromClause* ret) {
    RETURN_NOT_OK(CheckNodeCount(ast, 1));
    const auto& expr = ast->nodes[0];
    RETURN_NOT_OK(CheckAstName(expr, "UnquotedIdentifier"));
    ret->measurement = expr->token;
    return Status::OK();
  }

  Status ParseSelect(const shared_ptr<Ast>& ast, SelectStmt* ret) {
    for (const auto& n : ast->nodes) {
      if (n->name == "Fields") {
        RETURN_NOT_OK(ParseFields(n, &ret->select_exprs_));
      } else if (n->name == "FromClause") {
        RETURN_NOT_OK(ParseFrom(n, &ret->from_));
      } else if (n->name == "WhereClause") {
        RETURN_NOT_OK(CheckNodeCount(n, 1));
        RETURN_NOT_OK(ParseExpr(n->nodes[0], &ret->where_));
      } else if (n->name == "GroupByClause") {
        RETURN_NOT_OK(CheckNodeCount(n, 1));
        RETURN_NOT_OK(ParseChildrenAsExprs(n->nodes[0], &ret->group_by_));
      } else {
        return ParseError(n, Substitute("unexpected child $0 of select statement", n->name));
      }
    }
    return Status::OK();
  }

  static Status ParseError(const shared_ptr<Ast>& ast, const string& str) {
    return Status::InvalidArgument(Substitute(
        "parse error at $0:$1 (token '$2'): $3",
        ast->line, ast->column, ast->token, str));
  }
};


peg::parser* GetParser() {
  static std::once_flag once;
  static peg::parser* parser;
  std::call_once(once, []() {
                         parser = new peg::parser();
                         CHECK(parser->load_grammar(kGrammar));
                         parser->enable_ast();
                       });
  return parser;
}

} // anonymous namespace

Parser::Parser(QContext* ctx)
    : ctx_(ctx) {
  parser_.reset(new peg::parser(*GetParser()));
  parser_->log = [](size_t line, size_t col, const string& msg) {
                   LOG(WARNING) << line << ":" << col << ": " << msg;
                 };
}

Parser::~Parser() {
}

Status Parser::ParseSelectStatement(const string& q, SelectStmt** sel) {
  std::shared_ptr<Ast> ast;
  if (!parser_->parse(q.c_str(), ast)) {
    return Status::InvalidArgument("failed to parse");
  }
  return AstConverter(ctx_).ConvertSelect(ast, sel);
}



} // namespace influxql
} // namespace tsdb
} // namespace kudu
