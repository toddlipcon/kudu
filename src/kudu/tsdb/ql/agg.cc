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

#include "kudu/tsdb/ql/agg.h"

#include <memory>
#include <utility>
#include <string>
#include <vector>

#include <boost/variant.hpp>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/util/status.h"

using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tsdb {
namespace influxql {

struct MaxTraits {
  using IntermediateType = int64_t;
  using InputType = int64_t;
  using OutputType = int64_t;
  static constexpr const char* const name = "max";

  static void Combine(IntermediateType* old_val, InputType new_val) {
    *old_val = std::max(*old_val, new_val);
  }

  static vector<OutputType> Finish(vector<IntermediateType> intermediate) {
    return intermediate;
  }
};

struct MeanTraits {
  struct IntermediateType {
    int64_t total = 0;
    int64_t count = 0;
  };
  using InputType = int64_t;
  using OutputType = double;
  static constexpr const char* const name = "mean";

  static void Combine(IntermediateType* old_val, InputType new_val) {
    old_val->total += new_val;
    old_val->count++;
  }

  static vector<OutputType> Finish(vector<IntermediateType> intermediate) {
    vector<OutputType> ret(intermediate.size());
    for (int i = 0; i < intermediate.size(); i++) {
      ret[i] = static_cast<double>(intermediate[i].total) / intermediate[i].count;
    }
    return ret;
  }
};

class Aggregator {
 public:
  virtual ~Aggregator() = default;
  virtual Status Consume(const TSBlock& block,
                         const vector<int>& bucket_indexes) = 0;
  virtual InfluxVec TakeResults() = 0;
  virtual string name() const = 0;
};

template<class Traits>
class AggregatorImpl : public Aggregator {
 public:
  AggregatorImpl(string col_name, const Bucketer& bucketer)
      : col_name_(std::move(col_name)),
        intermediate_vals_(bucketer.num_buckets()) {
  }

  string name() const override {
    return Traits::name;
  }

  Status Consume(const TSBlock& block,
                 const vector<int>& bucket_indexes) override {
    CHECK(!done_);
    const InfluxVec* vec = block.column_ptr_or_null(col_name_);
    if (!vec) {
      return Status::RuntimeError("missing column", col_name_);
    }
    const vector<int64_t>* src_vals = boost::get<vector<int64_t>>(vec);
    if (!src_vals) {
      return Status::RuntimeError("wrong column type", col_name_);
    }

    int n = block.times.size();
    DCHECK_EQ(src_vals->size(), n);
    DCHECK_EQ(bucket_indexes.size(), n);
    DoMergeAgg(bucket_indexes.data(), src_vals->data(), intermediate_vals_.data(), n);
    return Status::OK();
  }

  InfluxVec TakeResults() override {
    CHECK(!done_);
    done_ = true;
    return Traits::Finish(std::move(intermediate_vals_));
  }

 private:
  template<class T>
  void DoMergeAgg(const int* __restrict__ bucket_indexes,
                  const T* __restrict__ src_vals,
                  typename Traits::IntermediateType* __restrict__ intermediate_vals,
                  int n) {
#pragma unroll(4)
    for (int i = 0; i < n; i++) {
      int bucket = *bucket_indexes++;
      Traits::Combine(&intermediate_vals[bucket], src_vals[i]);
    }
  }


  const string col_name_;

  vector<typename Traits::IntermediateType> intermediate_vals_;
  bool done_ = false;
};

class MultiAggExpressionEvaluator : public TSBlockConsumer {
 public:
  MultiAggExpressionEvaluator(const vector<std::pair<string, string>>& agg_and_field,
                              Bucketer bucketer,
                              TSBlockConsumer* downstream )
      : bucketer_(std::move(bucketer)),
        downstream_(CHECK_NOTNULL(downstream)) {
    for (const auto& p : agg_and_field) {
      const auto& agg_name = p.first;
      const auto& col_name = p.second;

      // TODO(todd) implement mean
      if (agg_name == "max") {
        aggs_.emplace_back(col_name, new AggregatorImpl<MaxTraits>(col_name, bucketer_));
      } else if (agg_name == "mean") {
        aggs_.emplace_back(col_name, new AggregatorImpl<MeanTraits>(col_name, bucketer_));
      } else {
        LOG(FATAL) << "unknown agg: " << agg_name; // TODO move to Init()
      }
    }
  }

  Status Consume(TSBlock* block) override {
    // TODO(todd) move this to persist across calls instead of reallocating.
    vector<int> bucket_indexes;
    bucket_indexes.resize(block->times.size());
    for (int i = 0; i < block->times.size(); i++) {
      bucket_indexes[i] = bucketer_.bucket(block->times[i]);
    }

    for (const auto& p : aggs_) {
      const auto& agg = p.second;
      RETURN_NOT_OK(agg->Consume(*block, bucket_indexes));
    }
    return Status::OK();
  }

  Status Finish() override {
    CHECK(!done_);
    TSBlock out_block;
    // All input blocks have been consumed. Finalize aggregates and
    // output.
    out_block.times = bucketer_.bucket_times();
    for (const auto& p : aggs_) {
      out_block.AddColumn(StrCat(p.second->name(), "_", p.first),
                          p.second->TakeResults());
    }
    RETURN_NOT_OK(downstream_->Consume(&out_block));
    RETURN_NOT_OK(downstream_->Finish());
    done_ = true;
    return Status::OK();
  }

 private:

  const Bucketer bucketer_;
  TSBlockConsumer* const downstream_;
  vector<pair<string, unique_ptr<Aggregator>>> aggs_;

  bool done_ = false;
};

Status CreateMultiAggExpressionEvaluator(
    const std::vector<std::pair<string, string>>& aggs,
    Bucketer bucketer,
    TSBlockConsumer* downstream,
    std::unique_ptr<TSBlockConsumer>* eval) {
  eval->reset(new MultiAggExpressionEvaluator(aggs, std::move(bucketer), downstream));
  return Status::OK();
}

} // namespace influxql
} // namespace tsdb
} // namespace kudu
