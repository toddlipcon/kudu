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

#include <unordered_map>
#include <vector>
#include <boost/variant.hpp>

#include <glog/logging.h>

#include "kudu/gutil/bits.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/array_view.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tsdb {

template<class T>
class MaybeOwnedArrayView {
 public:
  static MaybeOwnedArrayView<T> AllocAndZero(size_t size) {
    T* data = new T[size];
    memset(data, 0, sizeof(T) * size);
    return Owning(data, size);
  }

  static MaybeOwnedArrayView<T> Owning(T* data, size_t size) {
    return MaybeOwnedArrayView<T>(data, size, true);
  }
  static MaybeOwnedArrayView<T> ViewOf(T* data, size_t size) {
    return MaybeOwnedArrayView<T>(data, size, false);
  }
  static MaybeOwnedArrayView<T> ViewOf(const MaybeOwnedArrayView<T>& other) {
    return MaybeOwnedArrayView<T>(other.data(), other.size(), false);
  }

  MaybeOwnedArrayView() : owned_(false) {
  }

  MaybeOwnedArrayView(MaybeOwnedArrayView<T>&& other) noexcept
      : view_(other.view_),
        owned_(other.owned_) {
    other.owned_ = false;
  }

  MaybeOwnedArrayView& operator=(MaybeOwnedArrayView<T>&& other) {
    if (&other == this) return *this;

    Reset();
    view_ = other.view_;
    owned_ = other.owned_;
    other.owned_ = false;
    other.Reset();

    return *this;
  }

  ~MaybeOwnedArrayView() {
    Reset();
  }

  void Reset() {
    if (owned_) {
      delete [] view_.data();
    }
    view_ = {};
    owned_ = false;
  }

  T* data() const {
    return view_.data();
  }

  size_t size() const {
    return view_.size();
  }

  T& operator[](size_t index) {
    return view_[index];
  }

  const T& operator[](size_t index) const {
    return view_[index];
  }

 private:
  MaybeOwnedArrayView(T* data, size_t size, bool owned)
      : view_(data, size),
        owned_(owned) {
  }

  ArrayView<T> view_;
  bool owned_;
};

struct InfluxVec {
  bool has_nulls = false;
  boost::variant<MaybeOwnedArrayView<double>,
                 MaybeOwnedArrayView<int64_t>> data;
  MaybeOwnedArrayView<uint8_t> null_bitmap;

  InfluxVec() {}

  static InfluxVec ViewOf(const InfluxVec& other) {
    InfluxVec ret;
    ret.has_nulls = other.has_nulls;
    ret.null_bitmap = MaybeOwnedArrayView<uint8_t>::ViewOf(other.null_bitmap);
    boost::apply_visitor([&](auto& v){
                           using T = std::remove_reference_t<decltype(v)>;
                           ret.data = T::ViewOf(v);
                         }, other.data);
    return ret;
  }
  
  template<class T>
  InfluxVec(MaybeOwnedArrayView<T> cells, MaybeOwnedArrayView<uint8_t> null_bitmap)
      : data(std::move(cells)),
        null_bitmap(std::move(null_bitmap)) {
    CHECK_EQ(BitmapSize(cells.size()), null_bitmap.size());
    has_nulls = !BitmapIsAllZero(null_bitmap.data(), 0, cells.size());
  }

  template<class T>
  static InfluxVec WithNoNulls(MaybeOwnedArrayView<T> cells) {
    InfluxVec ret;
    ret.null_bitmap = MaybeOwnedArrayView<uint8_t>::AllocAndZero(BitmapSize(cells.size()));
    ret.has_nulls = false;
    ret.data = std::move(cells);
    return ret;
  }
  template<class T>
  static InfluxVec Empty() {
    return InfluxVec(MaybeOwnedArrayView<T>(), {});
  }

  template<class T>
  const MaybeOwnedArrayView<T>* data_as() const {
    return boost::get<MaybeOwnedArrayView<T>>(&data);
  }

  template<class T>
  MaybeOwnedArrayView<T>* data_as() {
    return boost::get<MaybeOwnedArrayView<T>>(&data);
  }

  bool null_at_index(int i) const {
    return has_nulls && BitmapTest(null_bitmap.data(), i);
  }

  template<class T>
  void set(int i, T val) {
    (*data_as<T>())[i] = val;
    BitmapClear(null_bitmap.data(), i);
  }

  void set(int i, std::nullptr_t val) {
    BitmapSet(null_bitmap.data(), i);
    has_nulls = true;
  }

  void Reset(int n_rows) {
    has_nulls = false;
    boost::apply_visitor([&](auto& v){
                           using T = std::remove_reference_t<decltype(v[0])>;
                           v = MaybeOwnedArrayView<T>::Owning(new T[n_rows], n_rows);
                         }, data);
    null_bitmap = MaybeOwnedArrayView<uint8_t>::Owning(
        new uint8_t[BitmapSize(n_rows)], n_rows);
  }

};

namespace influxql {

class QContext;

struct TSBlock : public RefCounted<TSBlock> {
  InfluxVec& column(const std::string& name) {
    auto it = column_indexes.find(name);
    if (it == column_indexes.end()) {
      AddColumn(name, {});
      return columns.back();
    }
    return columns[it->second];
  }

  const InfluxVec* column_ptr_or_null(const std::string& name) const {
    auto it = column_indexes.find(name);
    if (it == column_indexes.end()) {
      return nullptr;
    }
    return &columns[it->second];
  }

  void AddColumn(std::string name, InfluxVec vals) {
    int index = columns.size();
    columns.emplace_back(std::move(vals));
    column_indexes.emplace(name, index);
    column_names.emplace_back(std::move(name));
  }

  void Clear() {
    column_indexes.clear();
    columns.clear();
    column_names.clear();
    times = {};
  }

  void Reset(int n_rows) {
    times = MaybeOwnedArrayView<int64_t>::Owning(new int64_t[n_rows], n_rows);
    for (auto& c : columns) {
      c.Reset(n_rows);
    }
  }


  MaybeOwnedArrayView<int64_t> times;
  std::unordered_map<std::string, int> column_indexes;
  std::vector<InfluxVec> columns;
  std::vector<std::string> column_names;
};

class TSBlockConsumer {
 public:
  virtual Status Consume(scoped_refptr<const TSBlock> block) = 0;
  virtual Status Finish() { return Status::OK(); }

  virtual ~TSBlockConsumer() = default;
};

class BlockBuffer : public TSBlockConsumer {
 public:
  Status Consume(scoped_refptr<const TSBlock> block) override {
    blocks_.emplace_back(std::move(block));
    return Status::OK();
  }

  scoped_refptr<const TSBlock> TakeSingleResult() {
    CHECK_EQ(blocks_.size(), 1);
    auto ret = std::move(blocks_[0]);
    blocks_.clear();
    return ret;
  }

  std::vector<scoped_refptr<const TSBlock>> TakeResults() {
    return std::move(blocks_);
  }

 private:
  std::vector<scoped_refptr<const TSBlock>> blocks_;
};

Status CreateProjectionEvaluator(
    QContext* ctx,
    std::vector<std::string> fields,
    TSBlockConsumer* downstream,
    std::unique_ptr<TSBlockConsumer>* eval);


} // namespace influxql
} // namespace tsdb
} // namespace kudu
