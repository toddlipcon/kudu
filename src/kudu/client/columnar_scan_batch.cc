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

#include "kudu/client/columnar_scan_batch.h"

#include "kudu/client/scanner-internal.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace client {

KuduColumnarScanBatch::KuduColumnarScanBatch()
    : data_(new KuduColumnarScanBatch::Data()) {
}

KuduColumnarScanBatch::~KuduColumnarScanBatch() {
  delete data_;
}

/// @return The number of rows in this batch.
int KuduColumnarScanBatch::NumRows() const {
  return data_->resp_data_.num_rows();
}

Status KuduColumnarScanBatch::GetDataForColumn(int idx, Slice* data) const {
  RETURN_NOT_OK(data_->CheckColumnIndex(idx));
  RETURN_NOT_OK(data_->controller_.GetInboundSidecar(
      data_->resp_data_.columns(idx).data_sidecar(),
      data));
  // TODO(todd) rewrite string pointers
  return Status::OK();
}

Status KuduColumnarScanBatch::GetNonNullBitmapForColumn(int idx, Slice* data) const {
  RETURN_NOT_OK(data_->CheckColumnIndex(idx));
  const auto& col = data_->resp_data_.columns(idx);
  if (!col.has_non_null_bitmap_sidecar()) {
    return Status::NotFound("column is not nullable");
  }
  return data_->controller_.GetInboundSidecar(col.non_null_bitmap_sidecar(), data);
}

} // namespace client
} // namespace kudu
