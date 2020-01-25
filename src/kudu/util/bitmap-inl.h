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

#include "kudu/gutil/bits.h"

namespace kudu {

template<bool SET, class F>
inline void ForEachBit(const uint8_t* bitmap,
                       int n_bits,
                       const F& func) {
  int rem = n_bits;
  int base_idx = 0;
  while (rem >= 64) {
    uint64_t w = UnalignedLoad<int64_t>(bitmap);
    if (!SET) {
      w = ~w;
    }
    bitmap += sizeof(uint64_t);

    // Within each word, keep flipping the least significant non-zero bit and adding
    // the bit index to the output until none are set.
    //
    // Get the count up front so that the loop can be unrolled without dependencies.
    // The 'tzcnt' instruction that's generated here has a latency of 3 so unrolling
    // and avoiding any cross-iteration dependencies is beneficial.
    int tot_count = __builtin_popcountll(w);
#pragma unroll(3)
    while (tot_count--) {
      func(base_idx + Bits::FindLSBSetNonZero64(w));
      w &= w - 1;
    }
    base_idx += 64;
    rem -= 64;
  }

  while (rem > 0) {
    uint8_t b = *bitmap++;
    if (!SET) {
      b = ~b;
    }
    while (b) {
      int idx = base_idx + Bits::FindLSBSetNonZero(b);
      if (idx >= n_bits) break;
      func(idx);
      b &= b - 1;
    }
    base_idx += 8;
    rem -= 8;
  }
}

template<class F>
inline void ForEachSetBit(const uint8_t* __restrict__ bitmap,
                          int n_bits,
                          const F& func) {
  ForEachBit<true, F>(bitmap, n_bits, func);
}

template<class F>
inline void ForEachUnsetBit(const uint8_t* __restrict__ bitmap,
                            int n_bits,
                            const F& func) {
  ForEachBit<false, F>(bitmap, n_bits, func);
}

} // namespace kudu
