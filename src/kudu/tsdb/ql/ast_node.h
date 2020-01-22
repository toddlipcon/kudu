#pragma once

#include <cassert>
#include <memory>
#include <typeindex>
#include <tao/pegtl/config.hpp>
#include <tao/pegtl/memory_input.hpp>
#include <tao/pegtl/position.hpp>

#include <boost/container/small_vector.hpp>

namespace tao {
namespace pegtl {
namespace influxql {

// this seems to improve performance about 15%, but is it worth the complexity?
#ifdef ENABLE_BOOST_SMALL_VECTOR

struct ast_node {

  using node_t = ast_node;
  using children_t = boost::container::small_vector< std::unique_ptr< node_t >, 2 >;
  children_t children;

  std::type_index id = std::type_index( typeid( void ) );
  std::string source;

  TAO_PEGTL_NAMESPACE::internal::iterator m_begin;
  TAO_PEGTL_NAMESPACE::internal::iterator m_end;

  // each node will be default constructed
  ast_node() = default;

  // no copy/move is necessary
  // (nodes are always owned/handled by a std::unique_ptr)
  ast_node( const ast_node& ) = delete;
  ast_node( ast_node&& ) = delete;

  ~ast_node() = default;

  // no assignment either
  ast_node& operator=( const ast_node& ) = delete;
  ast_node& operator=( ast_node&& ) = delete;

  bool is_root() const noexcept
  {
    return id == typeid( void );
  }

  template< typename U >
  bool is() const noexcept
  {
    return id == typeid( U );
  }

  std::string name() const
  {
    assert( !is_root() );
    return TAO_PEGTL_NAMESPACE::internal::demangle( id.name() );
  }

  position begin() const
  {
    return position( m_begin, source );
  }

  position end() const
  {
    return position( m_end, source );
  }

  bool has_content() const noexcept
  {
    return m_end.data != nullptr;
  }

  std::string string() const
  {
    assert( has_content() );
    return std::string( m_begin.data, m_end.data );
  }

  // Compatibility, remove with 3.0.0
  std::string content() const
  {
    return string();
  }

  template< tracking_mode P = tracking_mode::eager, typename Eol = eol::lf_crlf >
  memory_input< P, Eol > as_memory_input() const
  {
    assert( has_content() );
    return { m_begin.data, m_end.data, source, m_begin.byte, m_begin.line, m_begin.byte_in_line };
  }

  template< typename... States >
  void remove_content( States&&... /*unused*/ ) noexcept
  {
    m_end.reset();
  }

  // all non-root nodes are initialized by calling this method
  template< typename Rule, typename Input, typename... States >
  void start( const Input& in, States&&... /*unused*/ )
  {
    id = typeid( Rule );
    source = in.source();
    m_begin = TAO_PEGTL_NAMESPACE::internal::iterator( in.iterator() );
  }

  // if parsing of the rule succeeded, this method is called
  template< typename Rule, typename Input, typename... States >
  void success( const Input& in, States&&... /*unused*/ ) noexcept
  {
    m_end = TAO_PEGTL_NAMESPACE::internal::iterator( in.iterator() );
  }

  // if parsing of the rule failed, this method is called
  template< typename Rule, typename Input, typename... States >
  void failure( const Input& /*unused*/, States&&... /*unused*/ ) noexcept
  {
  }

  // if parsing succeeded and the (optional) transform call
  // did not discard the node, it is appended to its parent.
  // note that "child" is the node whose Rule just succeeded
  // and "*this" is the parent where the node should be appended.
  template< typename... States >
  void emplace_back( std::unique_ptr< node_t >&& child, States&&... /*unused*/ )
  {
    assert( child );
    children.emplace_back( std::move( child ) );
  }

  StringPiece string_piece() const {
    return { m_begin.data, static_cast<int>(m_end.data - m_begin.data) };
  }

  void reset(std::vector< std::unique_ptr< ast_node > >* pool) {
    for (auto& c : children) {
      pool->emplace_back(std::move(c));
    }
    children.clear();
    source.clear();
    id = std::type_index(typeid(void));
  }
};
#else

struct ast_node : parse_tree::basic_node<ast_node> {
  StringPiece string_piece() const {
    return { m_begin.data, static_cast<int>(m_end.data - m_begin.data) };
  }

  void reset(std::vector< std::unique_ptr< ast_node > >* pool) {
    for (auto& c : children) {
      pool->emplace_back(std::move(c));
    }
    children.clear();
    source.clear();
    id = std::type_index(typeid(void));
  }
};

#endif


} // namespace influxql
} // namespace pegtl
} // namespace tao
