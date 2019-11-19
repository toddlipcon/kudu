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

#include "kudu/cfile/prefix_key.h"

#include <glog/logging.h>

namespace kudu {
namespace cfile {

using std::string;

size_t CommonPrefixLength(const Slice& slice_a, const Slice& slice_b) {
  // This implementation is modeled after strings::fastmemcmp_inlined().
  int len = std::min(slice_a.size(), slice_b.size());
  const uint8_t* a = slice_a.data();
  const uint8_t* b = slice_b.data();
  const uint8_t* a_limit = a + len;

  const size_t sizeof_uint64 = sizeof(uint64_t);
  // Move forward 8 bytes at a time until finding an unequal portion.
  while (a + sizeof_uint64 <= a_limit &&
         UNALIGNED_LOAD64(a) == UNALIGNED_LOAD64(b)) {
    a += sizeof_uint64;
    b += sizeof_uint64;
  }

  // Same, 4 bytes at a time.
  const size_t sizeof_uint32 = sizeof(uint32_t);
  while (a + sizeof_uint32 <= a_limit &&
         UNALIGNED_LOAD32(a) == UNALIGNED_LOAD32(b)) {
    a += sizeof_uint32;
    b += sizeof_uint32;
  }

  // Now one byte at a time. We could do a 2-bytes-at-a-time loop,
  // but we're following the example of fastmemcmp_inlined(). The benefit of
  // 2-at-a-time likely doesn't outweigh the cost of added code size.
  while (a < a_limit &&
         *a == *b) {
    a++;
    b++;
  }

  return a - slice_a.data();
}

void GetSeparatingKey(const Slice& left, Slice* right) {
  DCHECK_LE(left.compare(*right), 0);
  size_t cpl = CommonPrefixLength(left, *right);
  right->truncate(cpl == right->size() ? cpl : cpl + 1);
}

} // namespace cfile
} // namespace kudu
