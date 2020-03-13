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

#include <string>
#include <vector>

#include "kudu/tsdb/ql/exec.h"
#include "kudu/tsdb/ql/qcontext.h"
#include "kudu/util/scoped_cleanup.h"

using std::vector;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace tsdb {
namespace influxql {

class Projector : public TSBlockConsumer {
 public:
  Projector(QContext* ctx, vector<string> fields, TSBlockConsumer* downstream)
      : ctx_(ctx),
        fields_(fields),
        downstream_(downstream) {
  }

  Status Consume(scoped_refptr<const TSBlock> block) override {
    auto projected = ctx_->NewTSBlock();

    for (const auto& f : fields_) {
      const auto* src_col = block->column_ptr_or_null(f);
      if (!src_col) {
        return Status::RuntimeError("missing input column to projection", f);
      }
      // TODO(todd): copy-on-write would make sense for column data
      // TODO(todd): could move assuming the src col was only referenced once.
      projected->AddColumn(f, InfluxVec::ViewOf(*src_col));
    }
    projected->times = MaybeOwnedArrayView<int64_t>::ViewOf(block->times);
    return downstream_->Consume(std::move(projected));
  }

  Status Finish() override {
    return downstream_->Finish();
  }

 private:
  QContext* const ctx_;
  const vector<string> fields_;
  TSBlockConsumer* const downstream_;
};

Status CreateProjectionEvaluator(
    QContext* ctx,
    std::vector<std::string> fields,
    TSBlockConsumer* downstream,
    std::unique_ptr<TSBlockConsumer>* eval) {
  eval->reset(new Projector(ctx, std::move(fields), downstream));
  return Status::OK();
}


} // namespace influxql
} // namespace tsdb
} // namespace kudu
