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
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/memory/arena.h"
#include "kudu/tsdb/ql/exec.h"

#include <type_traits>

namespace kudu {
namespace tsdb {

class SeriesStore;
class MetricsColumnSource;

namespace influxql {

// Context for compilation and execution of a single query.
//
// This handles memory allocation, and may add things like profiling,
// stats, etc.
class QContext {
 private:
  struct DtorNodeBase;

 public:
  explicit QContext(SeriesStore* series_store,
                    MetricsColumnSource* column_source)
      : series_store_(series_store),
        column_source_(column_source),
        arena_(1024) {
  }
  ~QContext();

  // Allocate an object from the query's arena. If the type has a non-trivial
  // destructor, it will be registered for invocation when the query ends.
  template<class T, class ... Args>
  T* Alloc(Args&&... args);

  SeriesStore* series_store() {
    return series_store_;
  }

  MetricsColumnSource* metrics_column_source() {
    return column_source_;
  }

  Arena* arena() { return &arena_; }

  void Reset() {
    ClearDestructors();
    arena_.Reset();
  }

  scoped_refptr<TSBlock> NewTSBlock() {
    return scoped_refptr<TSBlock>(new TSBlock());
  }


 private:
  SeriesStore* const series_store_;
  MetricsColumnSource* const column_source_;

  Arena arena_;
  DtorNodeBase* dtor_head_ = nullptr;

  std::vector<scoped_refptr<TSBlock>> block_pool_;

  struct DtorNodeBase {
    virtual void destruct() = 0;
    DtorNodeBase* next = nullptr;
  };
  template<class T>
  struct DtorNode : public DtorNodeBase {
    explicit DtorNode(T* obj) : obj_(DCHECK_NOTNULL(obj)) {}
    void destruct() override {
      obj_->~T();
    }

    T* obj_;
  };

  void ClearDestructors();
};


template<class T, class ... Args>
inline T* QContext::Alloc(Args&&... args) {
  void *mem = arena_.AllocateBytesAligned(sizeof(T), alignof(T));
  if (mem == NULL) throw std::bad_alloc();
  T* ret = new (mem) T(std::forward<Args>(args)...);

  if (!std::is_trivially_destructible<T>::value) {
    auto* dtor = arena_.NewObject<DtorNode<T>>(ret);
    dtor->next = dtor_head_;
    dtor_head_ = dtor;
  }
  return ret;
}

inline QContext::~QContext() {
  ClearDestructors();
}

inline void QContext::ClearDestructors() {
  while (dtor_head_) {
    dtor_head_->destruct();
    dtor_head_ = dtor_head_->next;
  }
}


} // namespace influxql
} // namespace tsdb
} // namespace kudu
