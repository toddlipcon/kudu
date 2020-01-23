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
#include "kudu/util/status.h"

namespace kudu {
namespace tsdb {

struct InfluxVec {
  bool has_nulls = false;
  boost::variant<std::vector<double>, std::vector<int64_t>> data;
  std::vector<bool> nulls;

  InfluxVec() {}

  template<class T>
  InfluxVec(std::vector<T> cells, std::vector<bool> nulls)
      : data(std::move(cells)),
        nulls(std::move(nulls)) {
    CHECK_EQ(cells.size(), nulls.size());
    has_nulls = false;
    for (auto b : nulls) {
      has_nulls |= b;
    }
  }

  template<class T>
  static InfluxVec WithNoNulls(std::vector<T> cells) {
    InfluxVec ret;
    ret.nulls.assign(cells.size(), false);
    ret.has_nulls = false;
    ret.data = std::move(cells);
    return ret;
  }

  template<class T>
  static InfluxVec Empty() {
    return WithNoNulls<T>({});
  }

  template<class T>
  const std::vector<T>* data_as() const {
    return boost::get<std::vector<T>>(&data);
  }

  template<class T>
  std::vector<T>* data_as() {
    return boost::get<std::vector<T>>(&data);
  }

  bool null_at_index(int i) const {
    return has_nulls && nulls[i];
  }

  template<class T>
  void set(int i, T val) {
    (*data_as<T>())[i] = val;
    nulls[i] = false;
  }

  void set(int i, std::nullptr_t val) {
    nulls[i] = true;
    has_nulls = true;
  }

  void Reset(int n_rows) {
    has_nulls = false;
    boost::apply_visitor([&](auto& v){
                           v.resize(n_rows);
                         }, data);
    nulls.resize(n_rows);
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
    times.clear();
  }

  void Reset(int n_rows) {
    times.resize(n_rows);
    for (auto& c : columns) {
      c.Reset(n_rows);
    }
  }


  std::vector<int64_t> times;
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
