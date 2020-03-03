
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

#include <map>
#include <memory>
#include <utility>
#include <string>
#include <vector>

#include <boost/variant.hpp>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/tsdb/ql/qcontext.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tsdb {
namespace influxql {

template<typename ValueType>
struct MaxTraits {
  using IntermediateType = ValueType;
  using InputType = ValueType;
  using OutputType = ValueType;
  static constexpr const char* const name = "max";

  static void Combine(IntermediateType* old_val, InputType new_val) {
    *old_val = std::max<ValueType>(*old_val, new_val);
  }

  static MaybeOwnedArrayView<OutputType> Finish(vector<IntermediateType> intermediate) {
    return MaybeOwnedArrayView<OutputType>::ViewOf(&intermediate[0], intermediate.size());
  }
};

template<typename ValueType>
struct MeanTraits {
  struct IntermediateType {
    ValueType total = 0;
    int64_t count = 0;
  };
  using InputType = ValueType;
  using OutputType = double;
  static constexpr const char* const name = "mean";

  static void Combine(IntermediateType* old_val, InputType new_val) {
    old_val->total += new_val;
    old_val->count++;
  }

  static MaybeOwnedArrayView<OutputType> Finish(vector<IntermediateType> intermediate) {
    auto ret = MaybeOwnedArrayView<OutputType>::Owning(new OutputType[intermediate.size()],
                                                       intermediate.size());;
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
  static unique_ptr<Aggregator> Create(string col_name, const Bucketer& bucketer) {
    return unique_ptr<Aggregator>(new AggregatorImpl(std::move(col_name), bucketer));
  }

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
    const auto* src_vals = vec->data_as<typename Traits::InputType>();
    if (!src_vals) {
      return Status::RuntimeError("wrong column type", col_name_);
    }

    int n = block.times.size();
    DCHECK_EQ(src_vals->size(), n);
    DCHECK_EQ(bucket_indexes.size(), n);
    if (vec->has_nulls) {
      DCHECK_EQ(vec->non_null_bitmap.size(), BitmapSize(n));
      DoMergeAgg<typename Traits::InputType, true>(
          bucket_indexes.data(),
          src_vals->data(),
          vec->non_null_bitmap.data(),
          intermediate_vals_.data(), n);
    } else {
      DoMergeAgg<typename Traits::InputType, false>(
          bucket_indexes.data(),
          src_vals->data(),
          nullptr,
          intermediate_vals_.data(), n);
    }
    return Status::OK();
  }

  InfluxVec TakeResults() override {
    CHECK(!done_);
    done_ = true;
    auto ret_av = Traits::Finish(std::move(intermediate_vals_));
    return InfluxVec::WithNoNulls(std::move(ret_av));
  }

 private:
  template<class T, bool HAS_NULLS>
  void DoMergeAgg(const int* __restrict__ bucket_indexes,
                  const T* __restrict__ src_vals,
                  const uint8_t* __restrict__ non_null_bitmap,
                  typename Traits::IntermediateType* __restrict__ intermediate_vals,
                  int n) {
#pragma unroll(4)
    for (int i = 0; i < n; i++) {
      if (HAS_NULLS && !BitmapTest(non_null_bitmap, i)) continue;
      int bucket = *bucket_indexes++;
      Traits::Combine(&intermediate_vals[bucket], src_vals[i]);
    }
  }


  const string col_name_;

  vector<typename Traits::IntermediateType> intermediate_vals_;
  bool done_ = false;
};

class AggFactory {
 public:
  AggFactory() {
    aggs_.emplace(MapKey{"max", client::KuduColumnSchema::DataType::INT64},
                  AggregatorImpl<MaxTraits<int64_t>>::Create);
    aggs_.emplace(MapKey{"max", client::KuduColumnSchema::DataType::DOUBLE},
                  AggregatorImpl<MaxTraits<double>>::Create);

    aggs_.emplace(MapKey{"mean", client::KuduColumnSchema::DataType::INT64},
                  AggregatorImpl<MeanTraits<int64_t>>::Create);
    aggs_.emplace(MapKey{"mean", client::KuduColumnSchema::DataType::DOUBLE},
                  AggregatorImpl<MeanTraits<double>>::Create);
  }

  unique_ptr<Aggregator> CreateAgg(
      const string& agg_name, client::KuduColumnSchema::DataType type,
      string col_name, const Bucketer& bucketer) {
    FactoryFunc* func = FindOrNull(aggs_, {agg_name, type});
    if (!func) return nullptr;
    return (*func)(col_name, bucketer);
  }
 private:
  using MapKey = pair<string, client::KuduColumnSchema::DataType>;
  using FactoryFunc = std::function<unique_ptr<Aggregator>(string col_name, const Bucketer& bucketer)>;
  std::map<MapKey, FactoryFunc> aggs_;
};

class MultiAggExpressionEvaluator : public TSBlockConsumer {
 public:
  MultiAggExpressionEvaluator(QContext* ctx,
                              const vector<AggSpec>& agg_specs,
                              Bucketer bucketer,
                              TSBlockConsumer* downstream )
      : ctx_(ctx),
        bucketer_(std::move(bucketer)),
        downstream_(CHECK_NOTNULL(downstream)) {
    static AggFactory* agg_factory = new AggFactory();
    for (const auto& spec : agg_specs) {
      auto agg = agg_factory->CreateAgg(spec.func_name, spec.col_type, spec.col_name, bucketer_);
      if (!agg) {
        LOG(FATAL) << "unknown agg: " << spec.func_name << "(type " << spec.col_type << ")"; // TODO move to Init()
      }
      aggs_.emplace_back(spec.col_name, std::move(agg));
    }
  }

  Status Consume(scoped_refptr<const TSBlock> block) override {
    // TODO(todd) move this to persist across calls instead of reallocating.
    vector<int> bucket_indexes;
    bucket_indexes.resize(block->times.size());
    const int64_t* ts = &block->times[0];
    int* __restrict__ bucket = &bucket_indexes[0];
    int n_times = block->times.size();
    while (n_times--) {
      *bucket++ = bucketer_.bucket(*ts++);
    }

    for (const auto& p : aggs_) {
      const auto& agg = p.second;
      RETURN_NOT_OK(agg->Consume(*block.get(), bucket_indexes));
    }
    return Status::OK();
  }

  Status Finish() override {
    CHECK(!done_);
    scoped_refptr<TSBlock> out_block = ctx_->NewTSBlock();
    // All input blocks have been consumed. Finalize aggregates and
    // output.
    const auto& times = bucketer_.bucket_times();
    out_block->times = MaybeOwnedArrayView<int64_t>::ViewOf(
        const_cast<int64_t*>(times.data()),
        times.size());
    for (const auto& p : aggs_) {
      out_block->AddColumn(StrCat(p.second->name(), "_", p.first),
                          p.second->TakeResults());
    }
    RETURN_NOT_OK(downstream_->Consume(std::move(out_block)));
    RETURN_NOT_OK(downstream_->Finish());
    done_ = true;
    return Status::OK();
  }

 private:
  QContext* const ctx_;
  const Bucketer bucketer_;
  TSBlockConsumer* const downstream_;
  vector<pair<string, unique_ptr<Aggregator>>> aggs_;

  bool done_ = false;
};

Status CreateMultiAggExpressionEvaluator(
    QContext* ctx,
    const vector<AggSpec>& aggs,
    Bucketer bucketer,
    TSBlockConsumer* downstream,
    std::unique_ptr<TSBlockConsumer>* eval) {
  eval->reset(new MultiAggExpressionEvaluator(ctx, aggs, std::move(bucketer), downstream));
  return Status::OK();
}

} // namespace influxql
} // namespace tsdb
} // namespace kudu

