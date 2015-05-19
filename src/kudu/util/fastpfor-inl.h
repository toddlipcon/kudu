// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_FASTPFOR_INL_H
#define KUDU_UTIL_FASTPFOR_INL_H

#include <boost/utility/binary.hpp>
#include <glog/logging.h>
#include <stdint.h>
#include <smmintrin.h>
#include <immintrin.h>

#include "kudu/util/faststring.h"

#include "kudu/util/simdbitpacking.h"
#include "kudu/util/usimdbitpacking.h"
#include "kudu/util/bitpackinghelpers.h"

namespace kudu {
namespace coding {

inline void AppendInt32GroupFastPFor(const uint32_t *source, size_t size,
                                     faststring *out, const uint32_t bit) {
  if (bit == 0) return;
  size_t old_size = out->size();
  out->resize(old_size + bit * size / 8);

  uint32_t *ptr = reinterpret_cast<uint32_t *>(&((*out)[old_size]));
  for(int k = 0; k < size; k+=128) {
    FastPForLib::SIMD_fastpack_32(source, reinterpret_cast<__m128i *>(ptr), bit);
    ptr += 4 * bit;
    source += 128;
  }
}

inline const uint32_t *DecodeInt32GroupFastPFor(const uint32_t *source, size_t size,
                                                uint32_t *out, const uint32_t bit) {
  if (bit == 0) {
    memset(static_cast<void *>(out), 0, size * sizeof(uint32_t));
  }
  else {
    for(int k = 0; k < size; k+=128) {
      FastPForLib::SIMD_fastunpack_32(reinterpret_cast<const __m128i *>(source), out, bit);
      source += 4 * bit;
      out += 128;
    }
  }
  return source;
}

inline uint32_t CalcOptimalEncodingCost(const uint32_t *source, size_t size, uint8_t &best_b,
                                        uint8_t &best_cexcept, uint8_t &max_b) {
  uint32_t freqs[33];
  for (uint32_t k = 0; k <= 32; ++k)
    freqs[k] = 0;
  for (uint32_t k = 0; k < size; ++k) {
    if (source[k])
      freqs[32 - __builtin_clz(source[k])]++;
  }

  best_b = 32;
  while (freqs[best_b] == 0)
    best_b--;
  max_b = best_b;
  uint32_t best_cost = best_b * size;
  uint32_t cexcept = 0;
  best_cexcept = static_cast<uint8_t>(cexcept);
  for (uint32_t b = best_b - 1; b < 32; --b) {
    cexcept += freqs[b + 1];
    uint32_t this_cost = cexcept * 8 + cexcept * (max_b - b) + b * size;
    if (this_cost < best_cost) {
      best_cost = this_cost;
      best_b = static_cast<uint8_t>(b);
      best_cexcept = static_cast<uint8_t>(cexcept);
    }
  }
  return 2 + (best_cexcept > 0 ? 1 :  0) + ((best_cost + 7) >> 3);
}

template<class STLContainer>
inline void PackWithoutMask_SIMD(STLContainer &source, faststring *out,
                                 const uint32_t bit) {
  const uint32_t num_inputs = static_cast<uint32_t>(source.size());

  size_t old_size = out->size();
  out->resize(old_size + 4 + (num_inputs + 31) / 32 * 4 * bit);
  uint32_t *ptr = reinterpret_cast<uint32_t *>(&((*out)[old_size]));

  *ptr++ = num_inputs;
  if (num_inputs == 0)
    return;

  source.resize((num_inputs + 31) / 32 * 32);

  uint32_t j = 0;
  for (; j + 128 <= num_inputs; j += 128) {
    FastPForLib::usimdpackwithoutmask(&source[j], reinterpret_cast<__m128i *>(ptr), bit);
    ptr += 4 * bit;
  }
  for(; j < num_inputs; j += 32) {
    FastPForLib::fastpackwithoutmask(&source[j], ptr, bit);
    ptr += bit;
  }

  source.resize(num_inputs);
  out->resize(old_size + 4 + num_inputs * bit / 8);
}

template<class STLContainer>
inline const uint32_t *Unpack_SIMD(const uint32_t *in, STLContainer & out,
                                   const uint32_t bit) {
  const uint32_t size = *in;
  ++in;
  out.resize((size + 32 - 1) / 32 * 32);
  uint32_t j = 0;

  for (; j + 128 <= size; j += 128) {
    FastPForLib::usimdunpack(reinterpret_cast<const __m128i *>(in), &out[j], bit);
    in += 4 * bit;
  }
  for(; j + 31 < size; j += 32) {
    FastPForLib::fastunpack(in, &out[j], bit);
    in += bit;
  }

  uint32_t buffer[32];
  uint32_t remaining = size - j;

  memcpy(buffer, in, (remaining * bit + 31) / 32 * sizeof(uint32_t));
  uint32_t * bpointer = buffer;
  in += (out.size() - j) / 32 * bit;
  for(; j < size; j += 32) {
    FastPForLib::fastunpack(bpointer, &out[j], bit);
    bpointer += bit;
  }

  in -= ( j - size ) * bit / 32;
  out.resize(size);
  return in;
}

} // namespace coding
} // namespace kudu

#endif
