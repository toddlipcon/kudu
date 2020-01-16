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

#include <algorithm>
#include <limits>
#include <cstdint>
#include <utility>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/util/random.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/cfile/bitpacking.h"
#include "kudu/cfile/simd_bitpacking.h"
#include "kudu/cfile/bitpacking-internal.h"
#include "kudu/cfile/bp128_block.h"

using std::pair;
using std::string;
using std::vector;

namespace kudu {
namespace cfile {

class BitPackingTest : public KuduTest {
 protected:
  void DoTest(const vector<int64_t>& ints, const vector<uint64_t>& expected_diffs,
              int expected_num_bits) {
    vector<uint64_t> diffs(ints.size());
    using BP = Bitpacking<int64_t>;
    BP::PageMeta meta;
    BP::PreparePage(ints.data(), ints.size(), /*delta_encode=*/false, diffs.data(), &meta);
    ASSERT_EQ(diffs, expected_diffs);
    ASSERT_EQ(meta.reference_val, *std::min_element(ints.begin(), ints.end()));
    ASSERT_EQ(meta.num_bits, expected_num_bits);
  }

  void RoundTripScalar(const vector<uint32_t>& ints, int bits) {
    vector<uint32_t> packed(ints.size());
    bp::fastpackwithoutmask(ints.data(), packed.data(), bits);

    vector<uint32_t> ints_restored(ints.size());
    bp::fastunpack(packed.data(), ints_restored.data(), bits);
    ASSERT_EQ(ints, ints_restored);
  }

  template<typename INT>
  void RoundTripSimd(const vector<INT>& ints, int bits) {
    __m128i packed[ints.size()/4];
    bp::simd_fastpackwithoutmask(ints.data(), packed, bits);

    vector<INT> ints_restored(ints.size());
    bp::simd_fastunpack(packed, ints_restored.data(), bits);
    ASSERT_EQ(ints, ints_restored);
  }
};


TEST_F(BitPackingTest, TestPreparePage) {
  DoTest({2,2,2}, {0,0,0}, 0);
  DoTest({3,2,3}, {1,0,1}, 1);
  DoTest({3,2,4}, {1,0,2}, 2);
  DoTest({3,2,5}, {1,0,3}, 2);
  DoTest({3,2,6}, {1,0,4}, 3);
}

vector<int64_t> Multiply(vector<int64_t> v, int64_t m) {
  for (auto& i : v) i *= m;
  return v;
}
vector<int64_t> Add(vector<int64_t> v, int64_t x) {
  for (auto& i : v) i += x;
  return v;
}
vector<int64_t> RandomVec(int n, int64_t max) {
  Random rng(SeedRandom());;
  vector<int64_t> ret(n);
  for (auto& i : ret) {
    i = rng.Uniform(max);
  }
  return ret;
}
vector<int64_t> CumSum(vector<int64_t> v) {
  for (int i = 1; i < v.size(); i++) {
    v[i] += v[i - 1];
  }
  return v;
}
vector<int64_t> Sequence(int n) {
  vector<int64_t> ret(n);
  for (int i = 0; i < n; i++) {
    ret[i] = i;
  }
  return ret;
}
vector<int64_t> Zeros(int n) {
  vector<int64_t> ret(n);
  return ret;
}

TEST_F(BitPackingTest, TestPatterns) {
  // TODO templatize and test int32
  vector<pair<string, vector<int64_t>>> patterns;
  patterns.emplace_back("constant_zero", Zeros(128));
  patterns.emplace_back("constant", Add(Zeros(128), 1000));
  patterns.emplace_back("constant_max32", Add(Zeros(128), std::numeric_limits<int32_t>::max()));
  patterns.emplace_back("constant_max64", Add(Zeros(128), std::numeric_limits<int64_t>::max()));
  patterns.emplace_back("32bit_nums", Add(Sequence(128), 1L<<31));
  patterns.emplace_back("64bit_nums", Add(Sequence(128), 1L<<63));
  patterns.emplace_back("32bit_gaps", Multiply(Sequence(128), (1L<<32) - 1));
  patterns.emplace_back("random_32bit", RandomVec(128, std::numeric_limits<int32_t>::max()));
  patterns.emplace_back("random_64bit", RandomVec(128, std::numeric_limits<int64_t>::max()));
  patterns.emplace_back("sequential", Sequence(128));
  patterns.emplace_back("sequential_from_base", Add(Sequence(128), 1000));
  patterns.emplace_back("reverse_sequential", Multiply(Sequence(128), -1));
  patterns.emplace_back("reverse_sequential_from_base", Add(Multiply(Sequence(128), -1), 1000));
  patterns.emplace_back("clustered", Add(RandomVec(128, 50), 1000));
  patterns.emplace_back("increasing", Add(CumSum(RandomVec(128, 100)), 1000));
  patterns.emplace_back("constant_gaps",
                        Add(Multiply(Sequence(128), 10), 12345L));
  // Simulate timestamps collected exactly every 10 seconds, stored in microseconds
  patterns.emplace_back("timestamps_constant_gaps",
                        Add(Multiply(Sequence(128), 10L*1000000000L), 1576871767198152000L));
  
  for (const auto & pattern_pair : patterns) {
    const auto& pattern_name = pattern_pair.first;
    const auto& ints = pattern_pair.second;
    LOG(INFO) << pattern_name << ": " << ints;
  }

  for (const auto & pattern_pair : patterns) {
    const auto& pattern_name = pattern_pair.first;
    const auto& ints = pattern_pair.second;
    SCOPED_TRACE(pattern_name);
    for (bool delta_encode : {false, true}) {
      SCOPED_TRACE(delta_encode);
      faststring buf;
      buf.resize(128 * sizeof(int64_t));
      Bitpacking<int64_t>::PageMeta meta;
      int n = Bitpacking<int64_t>::CompressFullPage(ints.data(), delta_encode, buf.data(), &meta);
      buf.resize(n);

      LOG(INFO) << pattern_name << ": encoded size "
                << (delta_encode ? "with":"without")
                << " delta encoding: " << buf.size();

      // Initialize the output vector to random data so that we detect if we didn't
      // properly overwrite the memory.
      vector<int64_t> ints_restored = RandomVec(128, std::numeric_limits<int64_t>::max());
      Bitpacking<int64_t>::UncompressFullPage(buf.data(), ints_restored.data(), meta);
      EXPECT_EQ(ints, ints_restored);
    }
  }
}

TEST_F(BitPackingTest, TestCompressAndUncompressPage) {

  const auto& ints = Add(Sequence(128), 1000);

  for (bool delta_encode : {false, true}) {
    faststring buf;
    buf.resize(128 * sizeof(int64_t));
    Bitpacking<int64_t>::PageMeta meta;
    int n = Bitpacking<int64_t>::CompressFullPage(ints.data(), delta_encode, buf.data(), &meta);
    buf.resize(n);

    if (delta_encode) {
      ASSERT_EQ(1000, meta.reference_val);
      ASSERT_EQ(1, meta.min_delta); // every delta is 1
      ASSERT_EQ(0, meta.num_bits); // all deltas are equal
    } else {
      // Our range was ints from 1000 to 1127. So, we should be able to represent that with
      // the base (1000) and 7-bit offsets.
      ASSERT_EQ(1000, meta.reference_val);
      ASSERT_EQ(7, meta.num_bits);
      ASSERT_EQ(n, meta.num_bits * 128 / 8);
    }

    vector<int64_t> ints_restored(128);
    Bitpacking<int64_t>::UncompressFullPage(buf.data(), ints_restored.data(), meta);
    ASSERT_EQ(ints, ints_restored);
  }
}

TEST_F(BitPackingTest, BenchmarkPrefixSum) {
  vector<uint32_t> offsets;
  vector<uint8_t> bit_widths;
  for (int i = 0; i < 256; i++) {
    bit_widths.push_back(1);
  }
  bit_widths.reserve(bit_widths.size() + 32);

  for (volatile int i = 0; i < 1000000; i++) {
    Bitpacking<int64_t>::ComputePageOffsets(bit_widths, &offsets);
  }
}



TEST_F(BitPackingTest, TestPack) {
  Random rng(1);

  for (bool scalar : {false, true}) {
    SCOPED_TRACE(scalar);
    for (int bits = 1; bits <= 31; bits++) {
      SCOPED_TRACE(bits);
      int num_ints = scalar ? 32 : 128;

      vector<uint32_t> ints;
      uint32_t mask = (1 << bits) - 1;
      for (int i = 0; i < num_ints; i++) {
        ints.push_back(rng.Next() & mask);
      }

      if (scalar) {
        RoundTripScalar(ints, bits);
      } else {
        RoundTripSimd(ints, bits);
      }
    }
  }
}

TEST_F(BitPackingTest, Test64BitInts) {
  Random rng(1);
  for (int bits = 1; bits <= 32; bits++) {
    SCOPED_TRACE(bits);
    int num_ints = 128;
    vector<uint64_t> ints;
    uint64_t mask = (1 << bits) - 1;
    for (int i = 0; i < num_ints; i++) {
      ints.push_back(rng.Next64() & mask);
    }
    // only SIMD supported for 64-bit
    RoundTripSimd(ints, bits);
  }
}

}  // namespace cfile
}  // namespace kudu

