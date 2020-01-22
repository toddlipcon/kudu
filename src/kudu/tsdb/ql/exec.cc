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

#include <string>
#include <vector>
#include "kudu/tsdb/ql/exec.h"

using std::vector;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace tsdb {
namespace influxql {

class Projector : public TSBlockConsumer {
 public:
  Projector(vector<string> fields, TSBlockConsumer* downstream)
      : fields_(fields),
        downstream_(downstream) {
  }

  Status Consume(TSBlock* block) override {
    TSBlock projected;
    for (const auto& f : fields_) {
      const auto* src_col = block->column_ptr_or_null(f);
      if (!src_col) {
        return Status::RuntimeError("missing input column to projection", f);
      }
      // TODO(todd): copy-on-write would make sense for column data
      projected.AddColumn(f, *src_col);
    }
    // TODO(todd) copy-on-write
    projected.times = std::move(block->times);
    return downstream_->Consume(&projected);
  }

  Status Finish() override {
    return downstream_->Finish();
  }

 private:
  const vector<string> fields_;
  TSBlockConsumer* const downstream_;
};

Status CreateProjectionEvaluator(
    std::vector<std::string> fields,
    TSBlockConsumer* downstream,
    std::unique_ptr<TSBlockConsumer>* eval) {
  eval->reset(new Projector(std::move(fields), downstream));
  return Status::OK();
}


} // namespace influxql
} // namespace tsdb
} // namespace kudu
