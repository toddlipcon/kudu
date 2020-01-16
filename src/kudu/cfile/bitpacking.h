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

#include <cstdint>
#include <glog/stl_logging.h>
#include <type_traits>

#include <immintrin.h>
#include <xmmintrin.h>

#include "kudu/util/alignment.h"
#include "kudu/util/array_view.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/bits.h"

#include "kudu/cfile/bitpacking-internal.h"
#include "kudu/cfile/simd_bitpacking.h"

namespace kudu {
namespace cfile {

template<class UnsignedCppType>
void Unpack4Scalar(const uint32_t* __restrict__ src,
                   UnsignedCppType* __restrict__ dst,
                   int num_bits) {
  bp::fastunpack(src, &dst[0], num_bits);
  src += num_bits;
  bp::fastunpack(src, &dst[32], num_bits);
  src += num_bits;
  bp::fastunpack(src, &dst[64], num_bits);
  src += num_bits;
  bp::fastunpack(src, &dst[96], num_bits);
}

template<>
void Unpack4Scalar(const uint32_t* __restrict__ src,
                   uint16_t* __restrict__ dst,
                   int num_bits) {
  LOG(FATAL) << "trying to unpack " << num_bits << " into uint16_t";
}

template<>
void Unpack4Scalar(const uint32_t* __restrict__ src,
                   uint8_t* __restrict__ dst,
                   int num_bits) {
  LOG(FATAL) << "trying to unpack " << num_bits << " into uint16_t";
}

template<class UnsignedCppType>
void Pack4Scalar(const UnsignedCppType* __restrict__ diffs,
                 uint32_t* __restrict__ dst,
                 int num_bits) {
  bp::fastpackwithoutmask(&diffs[0], dst, num_bits);
  dst += num_bits;
  bp::fastpackwithoutmask(&diffs[32], dst, num_bits);
  dst += num_bits;
  bp::fastpackwithoutmask(&diffs[64], dst, num_bits);
  dst += num_bits;
  bp::fastpackwithoutmask(&diffs[96], dst, num_bits);
}
template<>
void Pack4Scalar(const uint16_t* __restrict__ diffs,
                 uint32_t* __restrict__ dst,
                 int num_bits) {
  LOG(FATAL) << "int16 should always use simd path";
}
template<>
void Pack4Scalar(const uint8_t* __restrict__ diffs,
                 uint32_t* __restrict__ dst,
                 int num_bits) {
  LOG(FATAL) << "int8 should always use simd path";
}

template<typename CppType>
struct Bitpacking {
  using UnsignedCppType = typename std::make_unsigned<CppType>::type;

  struct PageMeta {
    // For frame-of-reference, the minimum value in the page.
    // For delta encoding, the first value in the page.
    CppType reference_val;

    // The minimum of the deltas. Deltas are encoded relative to this value.
    // Only set for delta encoding.
    CppType min_delta;
    int num_bits;
    bool delta_encoded;
  };

  static constexpr int kPageSize = 128;

  static void ComputePageOffsets(const ArrayView<uint8_t>& bit_widths,
                                 std::vector<uint32_t>* offsets) {
    int num_pages = bit_widths.size();
    offsets->resize(num_pages);
#if 0
    // TODO(todd) this doesn't work yet, and is slower, too. need to futz with it.

    // "cumsum16" algorithm from http://people.cs.uchicago.edu/~hajiang/paper/damon2018_sboost.pdf
    // This operates on 16 16-bit integers at a time. Our input is 8-bit but after summing may
    // be 16-bit integers, so we promote when loading into simd registers.
    const auto shift16 = _mm256_set1_epi64x(16);
    const auto mask16 = _mm256_set1_epi32(0xffff);
    const auto inv16 =  _mm256_setr_epi16(0xF0E, 0xD0C, 0xB0A,
                                          0x908, 0x706, 0x504, 0x302, 0x100, 0xF0E, 0xD0C, 0xB0A, 0x908,
                                          0x706, 0x504, 0x302, 0x100);
    for (int i = 0; i < num_pages; i += 16) {
      __m256i b = _mm256_loadu2_m128i((__m128i*)&bit_widths[i], (__m128i*)&bit_widths[i+8]);
      b = _mm256_unpacklo_epi8(_mm256_setzero_si256(), b);
      auto bp = _mm256_bslli_epi128(b, 2);
      auto s1 = _mm256_hadd_epi16(b, bp);
      auto s2 = _mm256_sllv_epi64(s1, shift16); // why using sllv instead of slli?
      auto s3 = _mm256_hadd_epi16(s1, s2);
      auto s4 = _mm256_and_si256(s3, mask16);
      auto result = _mm256_hadd_epi16(s3, s4);
      auto ordered = _mm256_shuffle_epi8(result, inv16);
      uint16_t offsets_16[16];
      _mm256_storeu_si256((__m256i*)offsets_16, ordered);
      for (int j = 0; j < 16; j++) {
        (*offsets)[i+j] = offsets_16[j];
      }
    }
#elif 0
    uint32_t off = 0;
    int i;
    for (i = 0; i < KUDU_ALIGN_DOWN(num_pages, 16); i += 16) {
      auto x = _mm_loadu_si128((__m128i*)&bit_widths[i]);
      x = _mm_add_epi8(x, _mm_srli_si128(x, 1));
      x = _mm_add_epi8(x, _mm_srli_si128(x, 2));
      x = _mm_add_epi8(x, _mm_srli_si128(x, 4));
      x = _mm_add_epi8(x, _mm_srli_si128(x, 8));
      // x is now the prefix sum of the current block of 16 widths.
      // promote to 16-bits
      auto hi_16 = _mm_unpackhi_epi8(x, _mm_setzero_si128());
      auto lo_16 = _mm_unpacklo_epi8(x, _mm_setzero_si128());

      // now promote to 32-bits
      auto hi_32_hi = _mm_unpackhi_epi16(hi_16, _mm_setzero_si128());
      auto hi_32_lo = _mm_unpacklo_epi16(hi_16, _mm_setzero_si128());
      auto lo_32_hi = _mm_unpackhi_epi16(lo_16, _mm_setzero_si128());
      auto lo_32_lo = _mm_unpacklo_epi16(lo_16, _mm_setzero_si128());
      _mm_storeu_si128((__m128i*)&(*offsets)[i], hi_32_hi);
      _mm_storeu_si128((__m128i*)&(*offsets)[i+4], hi_32_lo);
      _mm_storeu_si128((__m128i*)&(*offsets)[i+8], lo_32_hi);
      _mm_storeu_si128((__m128i*)&(*offsets)[i+12], lo_32_lo);
      // TODO(todd) add offsets from previous group
    }
#else
    uint32_t off = 0;
    for (int i = 0; i < num_pages; i++) {
      (*offsets)[i] = off;
      // We use the MSB of the width to encode whether a page is delta-encoded.
      // Mask that off.
      off += (bit_widths[i] & 0x7f) * kPageSize / 8;
    }
#endif
  }

  static CppType ComputeMin(const CppType* page, int page_len) {
    CppType min_val = page[0];
    for (int i = 1; i < page_len; i++) {
      min_val = std::min<CppType>(min_val, page[i]);
    }
    return min_val;
  }

  static CppType ComputeDeltas(const CppType* page, int page_len, CppType* __restrict__ deltas) {
    // Compute deltas.
    deltas[0] = 0;
    for (int i = 1; i < page_len; i++) {
      deltas[i] = page[i] - page[i - 1];
    }
    return page[0];
  }

  static void SubtractFOR(const CppType* page, int page_len, CppType min_val, UnsignedCppType* __restrict__ diffs) {
    // Compute deltas.
    for (int i = 0; i < page_len; i++) {
      diffs[i] = page[i] - min_val;
    }
  }

  static void PreparePage(const CppType* page, int page_len,
                          bool delta_encode,
                          UnsignedCppType* __restrict__ diffs,
                          PageMeta* meta) {
    DCHECK_GT(page_len, 0);
    memset(meta, 0, sizeof(*meta));
    if (delta_encode && page_len > 1) {
      meta->delta_encoded = true;
      // TODO add tests for page_len = 1
      CppType deltas[page_len];
      meta->reference_val = ComputeDeltas(page, page_len, deltas);
      meta->min_delta = ComputeMin(&deltas[1], page_len - 1);
      SubtractFOR(deltas, page_len, meta->min_delta, diffs);
      diffs[0] = 0;
    } else {
      meta->delta_encoded = false;
      meta->reference_val = ComputeMin(page, page_len);
      SubtractFOR(page, page_len, meta->reference_val, diffs);
    }

    // Prepare a buffer for bitpacking and determine the number of required
    // bits.
    UnsignedCppType diff_mask = 0;
    for (int i = 0; i < page_len; i++) {
      diff_mask |= diffs[i];
    }
    meta->num_bits = 0;
    if (diff_mask != 0) {
      meta->num_bits = Bits::Log2Floor64(diff_mask) + 1;
    }
  }

  static int num_pages(int num_ints) {
    return KUDU_ALIGN_UP(num_ints, kPageSize) / kPageSize;
  }

  // 'out': must have space for 128 uncompressed ints.
  // Return the number of bytes appended to 'out'.
  static int CompressFullPage(const CppType* __restrict__ ints,
                              bool delta_encode,
                              uint8_t* __restrict__ dst,
                              PageMeta* meta) {
    UnsignedCppType diffs[kPageSize];
    PreparePage(ints, kPageSize, delta_encode, diffs, meta);
    DCHECK_LE(meta->num_bits, 8 * sizeof(CppType));
    if (meta->num_bits <= 32) {
      bp::simd_fastpackwithoutmask(diffs, reinterpret_cast<__m128i*>(dst), meta->num_bits);
    } else {
      // TODO(todd) put a flag indicating the non-SIMD layout, so that if we implement a SIMD
      // accelerated 64-bit packer, we have an easy evolution to it.
      Pack4Scalar(diffs, reinterpret_cast<uint32_t*>(dst), meta->num_bits);
    }
    return meta->num_bits * 128 / 8;
  }

  static int UncompressFullPage(const uint8_t* __restrict__ src,
                                CppType* __restrict__ dst,
                                const PageMeta& meta) {
    // TODO(todd): make sure this is validated elsewhere with error return.
    DCHECK(meta.num_bits <= 8 * sizeof(CppType));
    auto* dst_unsigned = reinterpret_cast<UnsignedCppType*>(dst);
    for (int i = 0; i < kPageSize; i++) { dst_unsigned[i] = 0; }
    if (meta.num_bits <= 32) {
      bp::simd_fastunpack(reinterpret_cast<const __m128i*>(src),
                          dst_unsigned,
                          meta.num_bits);
    } else {
      const uint32_t* src_tmp = reinterpret_cast<const uint32_t*>(src);
      Unpack4Scalar(src_tmp, dst_unsigned, meta.num_bits);
    }
    if (meta.delta_encoded) {
      // Consider the original sequence:
      //
      //   100 110 121 130 ...
      //
      // In this case, first_val = 0 and the real deltas are:
      //
      //   0   10  11  9
      //
      // If we calculated the min_delta across all the deltas, we would always determine
      // a min_delta = 0 for this common case, which would prevent us from better bitpacking.
      // So, we actually calculate the frame of reference ignoring the first value. In this case,
      // the reference value would be 9, resulting in the bit-packed data:
      //
      //   0   1   2   0
      //
      // So, when we restore the frame-of-reference encoded deltas, we need to make sure to
      // explicitly restore the first val, and only restore deltas for later values.
      dst[0] = meta.reference_val;
      CppType min_delta_local = meta.min_delta; // ensure no aliasing
      for (int i = 1; i < kPageSize; i++) {
        dst[i] = dst[i - 1] + dst[i] + min_delta_local;
      }
    } else {
      for (int i = 0; i < kPageSize; i++) {
        dst[i] += meta.reference_val;
      }
    }
    return meta.num_bits * 128 / 8;
  }

};




} // namespace cfile
} // namespace kudu
