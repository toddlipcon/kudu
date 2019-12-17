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
#include "kudu/util/memory/arena.h"
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
      : arena_(1024),
        series_store_(series_store),
        column_source_(column_source) {
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

 private:
  Arena arena_;
  DtorNodeBase* dtor_head_ = nullptr;

  SeriesStore* const series_store_;
  MetricsColumnSource* const column_source_;

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
  while (dtor_head_) {
    dtor_head_->destruct();
    dtor_head_ = dtor_head_->next;
  }
}


} // namespace influxql
} // namespace tsdb
} // namespace kudu
