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

#include "kudu/tsdb/ql/influxql.h"

#include <mutex>
#include <iostream>
#include <memory>
#include <vector>
#include <cstdlib>
#include <any>
#include <glog/logging.h>
#include <sparsehash/dense_hash_set>
#include <tao/pegtl.hpp>
#include <tao/pegtl/analyze.hpp>
#include <tao/pegtl/contrib/parse_tree.hpp>
#include <tao/pegtl/contrib/parse_tree_to_dot.hpp>

#include "kudu/tsdb/ql/ast_node.h"
#include "kudu/tsdb/ql/expr.h"
#include "kudu/tsdb/ql/qcontext.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/array_view.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/memory/arena.h"

using strings::Substitute;
using std::shared_ptr;
using std::string;
using std::vector;

namespace pegtl = tao::pegtl;

namespace tao {
namespace TAO_PEGTL_NAMESPACE {
namespace influxql {

struct tok_group : istring<'g', 'r', 'o', 'u', 'p'> {};
struct tok_by : istring<'b', 'y'> {};
struct tok_as : istring<'a', 's'> {};
struct tok_from : istring<'f', 'r', 'o', 'm'> {};
struct tok_where : istring<'w', 'h', 'e', 'r', 'e'> {};
struct tok_and : istring<'a', 'n', 'd'> {};
struct tok_or : istring<'o', 'r'> {};
struct tok_select : istring<'s', 'e', 'l', 'e', 'c', 't'> {};
struct tok_not : istring<'n', 'o', 't'> {};
struct tok_star : one<'*'> {};

struct comma : one<','> {};
struct sp : space {};
struct sps : star<sp> {};
struct dot : one< '.' > {};

struct comparison_op : sor< string<'<', '='>,
                            string<'<'>,
                            string<'='>,
                            string<'>', '='>,
                            string<'>'>> {};
struct add_or_subtract_op : sor< string<'+'>, string<'-'> > {};
struct product_op : sor<one<'*'>, one<'/'>, one<'%'>> {};

struct duration_unit : sor< one<'u'>,
// TODO                            one<'µ'>, 
                            string<'m', 's'>,
                            one<'m'>,
                            one<'h'>,
                            one<'d'>,
                            one<'w'> > {};

template< typename E >
struct exponent : opt_must< E, opt< one< '+', '-' > >, plus< digit > > {};

template< typename D, typename E >
struct numeral_three : seq< if_must< one< '.' >, plus< D > >, exponent< E > > {};
template< typename D, typename E >
struct numeral_two : seq< plus< D >, opt< one< '.' >, star< D > >, exponent< E > > {};
template< typename D, typename E >
struct numeral_one : sor< numeral_two< D, E >, numeral_three< D, E > > {};

struct decimal : numeral_one< digit, one< 'e', 'E' > > {};
struct hexadecimal : if_must< istring< '0', 'x' >, numeral_one< xdigit, one< 'p', 'P' > > > {};
struct numeral : sor< hexadecimal, decimal > {};

struct odigit : range< '0', '7' > {};
struct hex_escape : if_must< one< 'x', 'X' >, xdigit, xdigit > {};
struct oct_escape : if_must< odigit, odigit, odigit > {};
struct char_escape : one< 'a', 'b', 'f', 'n', 'r', 't', 'v', '\\', '\'', '"' > {};
struct escape : if_must< one< '\\' >, hex_escape, oct_escape, char_escape > {};
struct char_value : sor< escape, not_one< '\n', '\0' > > {};  // NOTE: No need to exclude '\' from not_one<>, see escape rule.
template< char Q >
struct str_impl : if_must< one< Q >, until< one< Q >, char_value > > {};
struct str_lit : sor< str_impl< '\'' >, str_impl< '"' > > {};

struct duration_lit : seq<numeral, sps, duration_unit> {};

struct expr;

struct var_ref : identifier {};
struct func_args : list_must<expr, comma, sp> {};
struct func_call : seq<identifier, sps, one<'('>, sps, func_args, one<')'>> {};

struct expr0 : sor<seq<one<'('>, sps, expr, sps, one<')'>>,
                   func_call,
                   var_ref,
                   str_lit,
                   duration_lit,
                   numeral> {};
struct product : seq<expr0, sps, star<seq<product_op, product>>> {};

struct sum : seq<product, sps, star<seq<add_or_subtract_op, sps, sum>>> {};
struct comparison : seq<sum, sps, star<seq<comparison_op, sps, sum>>> {};
struct maybe_negation : sor<comparison, seq<tok_not, sps, maybe_negation>> {};
struct conjunction : seq<maybe_negation, sps, star<seq<tok_and, sps, conjunction>>> {};
struct disjunction : seq<conjunction, sps, star<seq<tok_or, sps, disjunction>>> {};
struct expr : seq<disjunction> {};


struct alias : seq<tok_as, sps, identifier> {};
struct field : sor<seq<expr, opt<alias>>, tok_star> {};
struct fields : list_must<field, comma, sp> {};

struct measurement : identifier {};

struct from_clause : seq<tok_from, sps, measurement> {};
struct where_clause : seq<tok_where, sps, expr> {};

struct dimensions : list_must<expr, comma, sp> {};
struct groupby_clause : seq<tok_group, sps, tok_by, sps, dimensions> {};

struct select_stmt : seq<tok_select, sps,
                         fields, sps,
                         from_clause, sps,
                         opt<where_clause>,sps,
                         opt<groupby_clause>, sps,
                         opt<string<';'>>> {};

struct fold_one_keep_content : parse_tree::apply<fold_one_keep_content> {
            template< typename Node, typename... States >
            static void transform( std::unique_ptr< Node >& n, States&&... st ) noexcept( noexcept( n->children.size(), n->Node::remove_content( st... ) ) )
            {
               if( n->children.size() == 1 ) {
                  n = std::move( n->children.front() );
               }
            }
};

template<typename Rule>
using selector = parse_tree::selector<Rule,
   parse_tree::store_content::on<
     func_call,
     var_ref,
     func_args,
     fields,
     duration_lit,
     select_stmt,
     from_clause,
     where_clause,
     groupby_clause,
     numeral,
     str_lit,
     identifier,
     duration_unit,
     comparison_op,
     tok_not,
     tok_star,
     add_or_subtract_op,
     product_op,
     measurement>,
   fold_one_keep_content::on<
     expr,
     expr0,
     disjunction,
     conjunction,
     product,
     sum,
     maybe_negation,
     comparison>>;



}
}
}

#ifdef ENABLE_NODE_POOLING

namespace tao {
namespace TAO_PEGTL_NAMESPACE {
namespace parse_tree {

namespace internal {

template<>
struct state<influxql::ast_node>
{
  using Node = influxql::ast_node;
  std::vector< std::unique_ptr< Node > > stack;
  std::vector< std::unique_ptr< Node > > pool;
  int pool_hits=0, pool_misses=0;
  size_t max_stack_size = 0;

  state()
  {
    emplace_back();
  }

  ~state() {
    VLOG(3) << "hits: " << pool_hits << " misses: " << pool_misses
            << " max stack: " << max_stack_size;
  }

  void emplace_back()
  {
    std::unique_ptr<Node> inst;
    if (!pool.empty()) {
      inst = std::move(pool.back());
      pool.pop_back();
      inst->reset(&pool);
      pool_hits++;
    } else {
      inst.reset(new Node);
      pool_misses++;
    }
    stack.emplace_back(std::move(inst));
    max_stack_size = std::max(stack.size(), max_stack_size);
  }

  std::unique_ptr< Node >& back() noexcept
  {
    assert( !stack.empty() );
    return stack.back();
  }

  void pop_back() noexcept
  {
    assert( !stack.empty() );
    auto popped = std::move(stack.back());
    if (popped) {
      pool.emplace_back(std::move(popped));
    }
    stack.pop_back();
  }
};

}
}
}
}

#endif

namespace kudu {
namespace tsdb {
namespace influxql {

namespace {

namespace pegql = ::pegtl::influxql;

using AstNode = std::unique_ptr< pegql::ast_node >;

// Converts a pegtl AST into our own parse tree
class AstConverter {
 public:
  AstConverter(QContext* ctx, StringPiece input)
      : ctx_(ctx),
        input_(input){
  }

  Status ConvertSelect(const AstNode& ast,
                       SelectStmt** sel) {
    *sel = ctx_->Alloc<SelectStmt>();
    return ParseSelect(ast, *sel);
  }

 private:
  QContext* ctx_;

  template<class T>
  Status CheckAstType(const AstNode& ast) {
    if (!ast->is<T>()) {
      return ParseError(ast, Substitute(
          "unexpected ast node $0, expected $1",
          ast->name(),
          TAO_PEGTL_NAMESPACE::internal::demangle(typeid(T).name())));
    }
    return Status::OK();
  }

  Status CheckNodeCount(const AstNode& ast, int n) {
    if (ast->children.size() != n) {
      return ParseError(ast, Substitute("expected $0 children of node $1, got $2",
                                        n, ast->name(), ast->children.size()));
    }
    return Status::OK();
  }

  Status ParseFields(const AstNode& ast, vector<Expr*>* fields) {
    RETURN_NOT_OK(CheckAstType<pegql::fields>(ast));
    if (ast->children.empty()) {
      return ParseError(ast, "expected at least one field");
    }
    return ParseChildrenAsExprs(ast, fields);
  }

  Status ParseChildrenAsExprs(const AstNode& parent,
                              vector<Expr*>* exprs) {
    exprs->clear();
    for (const auto& n : parent->children) {
      Expr* e;
      RETURN_NOT_OK(ParseExpr(n, &e));
      exprs->push_back(e);
    }
    return Status::OK();
  }

  Status ParseExpr(const AstNode& ast, Expr** expr) {
    // ============================================================
    // Identifier.
    // ============================================================
    if (ast->is<pegql::var_ref>()) {
      *expr = ctx_->Alloc<FieldRefExpr>(ast->string());
      return Status::OK();
    }

    if (ast->is<pegql::tok_star>()) {
      *expr = ctx_->Alloc<StarExpr>();
      return Status::OK();
    }

    // ============================================================
    // Function call.
    // ============================================================
    if (ast->is<pegql::func_call>()) {
      RETURN_NOT_OK(CheckNodeCount(ast, 2));
      vector<Expr*> exprs;
      RETURN_NOT_OK(ParseChildrenAsExprs(ast->children[1], &exprs));
      *expr = ctx_->Alloc<CallExpr>(ast->children[0]->string(), std::move(exprs));
      return Status::OK();
    }

    // ============================================================
    // Binary operators.
    // ============================================================
    if (ast->is<pegql::sum>() ||
        ast->is<pegql::product>() ||
        ast->is<pegql::comparison>()) {
      RETURN_NOT_OK(CheckNodeCount(ast, 3));
      Expr* l;
      Expr* r;
      RETURN_NOT_OK(ParseExpr(ast->children[0], &l));
      RETURN_NOT_OK(ParseExpr(ast->children[2], &r));
      *expr = ctx_->Alloc<BinaryExpr>(l, r, ast->children[1]->string());
      return Status::OK();
    }

    // ============================================================
    // AND/OR
    // ============================================================
    if (ast->is<pegql::conjunction>() ||
        ast->is<pegql::disjunction>()) {
      auto mode = ast->is<pegql::conjunction>() ?
                   BooleanExpr::CONJUNCTION : BooleanExpr::DISJUNCTION;
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
    if (ast->is<pegql::numeral>()) {
      int64_t i_val;
      if (safe_strto64(ast->string_piece().data(), ast->string_piece().size(), &i_val)) {
        *expr = ctx_->Alloc<IntLiteralExpr>(i_val);
        return Status::OK();
      }
      double d_val;
      if (safe_strtod(ast->string(), &d_val)) {
        *expr = ctx_->Alloc<DoubleLiteralExpr>(d_val);
        return Status::OK();
      }
      return ParseError(ast, "bad numeral literal");
    }

    if (ast->is<pegql::duration_lit>()) {
      RETURN_NOT_OK(CheckNodeCount(ast, 2));
      RETURN_NOT_OK(CheckAstType<pegql::numeral>(ast->children[0]));
      RETURN_NOT_OK(CheckAstType<pegql::duration_unit>(ast->children[1]));
      int64_t val;
      if (!safe_strto64(ast->children[0]->string_piece().data(),
                        ast->children[0]->string_piece().size(), &val)) {
        return ParseError(ast, "bad int literal in duration"); // TODO is '2.5m' supported?
      }
      *expr = ctx_->Alloc<DurationLiteralExpr>(val, ast->children[1]->string());
      return Status::OK();
    }

    if (ast->is<pegql::str_lit>()) {
      auto sp = ast->string_piece();
      // Remove the leading and trailing quote characters.
      CHECK_GE(sp.size(), 2);
      CHECK_EQ(sp[0], sp[sp.size() - 1]);
      sp.remove_prefix(1);
      sp.remove_suffix(1);
      string unescaped, err;
      if (!strings::CUnescape(sp, &unescaped, &err)) {
        return ParseError(ast, Substitute("illegal string literal: $0", err));
      }
      *expr = ctx_->Alloc<StringLiteralExpr>(std::move(unescaped));
      return Status::OK();
    }

    return ParseError(ast, Substitute("unexpected expression AST node $0", ast->name()));
  }

  Status ParseFrom(const AstNode& ast, FromClause* ret) {
    RETURN_NOT_OK(CheckNodeCount(ast, 1));
    const auto& expr = ast->children[0];
    RETURN_NOT_OK(CheckAstType<pegql::measurement>(expr));
    ret->measurement = expr->string();
    return Status::OK();
  }

  Status ParseSelect(const AstNode& ast, SelectStmt* ret) {
    if (ast->is_root() && ast->children.size() == 1) {
      return ParseSelect(ast->children[0], ret);
    }
    for (const auto& n : ast->children) {
      if (n->is<pegql::fields>()) {
        RETURN_NOT_OK(ParseFields(n, &ret->select_exprs_));
      } else if (n->is<pegql::from_clause>()) {
        RETURN_NOT_OK(ParseFrom(n, &ret->from_));
      } else if (n->is<pegql::where_clause>()) {
        RETURN_NOT_OK(CheckNodeCount(n, 1));
        RETURN_NOT_OK(ParseExpr(n->children[0], &ret->where_));
      } else if (n->is<pegql::groupby_clause>()) {
        RETURN_NOT_OK(ParseChildrenAsExprs(n, &ret->group_by_));
      } else {
        return ParseError(n, Substitute("unexpected child $0 of select statement", n->name()));
      }
    }
    return Status::OK();
  }

  Status ParseError(const AstNode& ast, const string& err_str) {
    int err_begin = ast->begin().byte;
    int ctx_begin = std::max<int>(0, err_begin - 10);
    int err_end = ast->has_content() ? ast->end().byte : err_begin;
    int ctx_end = std::min<int>(err_end + 10, input_.size());

    StringPiece ctx(input_, ctx_begin, ctx_end - ctx_begin);
    string tildes;
    tildes.append(err_begin - ctx_begin, ' ');
    if (err_end != err_begin) {
      tildes.append(err_end - err_begin, '~');
    } else {
      tildes.push_back('^');
    }

    return Status::InvalidArgument(Substitute(
        "parse error at  $0:$1 (token '$2'): $3\n$4\n$5",
        ast->begin().line, ast->begin().byte_in_line,
        ast->has_content() ? ast->string_piece() : "",
        err_str,
        ctx,
        tildes));
  }

  StringPiece input_;
};


} // anonymous namespace

Parser::Parser(QContext* ctx)
    : ctx_(ctx) {
}

Parser::~Parser() {
}

Status Parser::ParseSelectStatement(const string& q, SelectStmt** sel) {
  pegtl::memory_input<> in(q, __FILE__);
  using only_select = pegtl::must<pegql::select_stmt, pegtl::eof>;
  auto ast = pegtl::parse_tree::parse<only_select,
                                      pegql::ast_node,
                                      pegql::selector>(in);
  if (!ast) {
    return Status::InvalidArgument("failed to parse");
  }
  Status s = AstConverter(ctx_, q).ConvertSelect(ast, sel);
  if (!s.ok()) {
    pegtl::parse_tree::print_dot(LOG(INFO), *ast);

    LOG(INFO) << "full AST:";
    pegtl::memory_input<> in2(q, __FILE__);
    auto full_ast = pegtl::parse_tree::parse<only_select, pegql::ast_node>(in2);
    pegtl::parse_tree::print_dot(LOG(INFO), *full_ast);
  }
  return s;
}



} // namespace influxql
} // namespace tsdb
} // namespace kudu
