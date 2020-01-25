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

// NOTE: using stdint.h instead of cstdint because this file is supposed
//       to be processed by a compiler lacking C++11 support.
#include <stdint.h>

#include <cstddef>
#include <string>

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif

#include "kudu/util/kudu_export.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
class Schema;

namespace tools {
class ReplicaDumper;
class TableScanner;
} // namespace tools

namespace client {
class KuduSchema;

// TODO: docs
class KUDU_EXPORT KuduColumnarScanBatch {
 public:
  KuduColumnarScanBatch();
  ~KuduColumnarScanBatch();

  /// @return The number of rows in this batch.
  int NumRows() const;

  Status GetDataForColumn(int idx, Slice* data) const;
  Status GetNonNullBitmapForColumn(int idx, Slice* data) const;

 private:
  class KUDU_NO_EXPORT Data;
  friend class KuduScanner;

  Data* data_;
  DISALLOW_COPY_AND_ASSIGN(KuduColumnarScanBatch);
};


} // namespace client
} // namespace kudu
