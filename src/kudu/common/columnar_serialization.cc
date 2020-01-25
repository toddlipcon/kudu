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

#include "kudu/common/columnar_serialization.h"

#include "kudu/common/wire_protocol.h"

#include <time.h>
#include <immintrin.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/zp7.h"
#include "kudu/gutil/cpu.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/bit-util.h"
#include "kudu/util/bitmap-inl.h"
#include "kudu/util/faststring.h"

using boost::optional;
using std::vector;

namespace kudu {

namespace {

static void nt_memcpy(uint8_t* __restrict__ dst,
                      const uint8_t* __restrict__ src,
                      int size) {
  while (size >= 16) {
    __builtin_nontemporal_store(
        *reinterpret_cast<const __m128i*>(src),
        reinterpret_cast<__m128i*>(dst));
    size -= 16;
    dst += 16;
    src += 16;
  }
  while (size >= 8) {
    __builtin_nontemporal_store(
        *(reinterpret_cast<const uint64_t*>(src)),
        reinterpret_cast<uint64_t*>(dst));
    size -= 8;
    dst += 8;
    src += 8;
  }
while (size >= 4) {
  __builtin_nontemporal_store(
      *(reinterpret_cast<const uint32_t*>(src)),
        reinterpret_cast<uint32_t*>(dst));
    size -= 4;
    dst += 4;
    src += 4;
  }
  while (size >= 2) {
    __builtin_nontemporal_store(
        *(reinterpret_cast<const uint16_t*>(src)),
        reinterpret_cast<uint16_t*>(dst));
    size -= 2;
    dst += 2;
    src += 2;
  }
  while (size > 0) {
    __builtin_nontemporal_store(
        *(src), dst);
    size--;
    dst++;
    src++;
  }
}

template<int type_size>
ATTRIBUTE_NOINLINE
void ZeroNullValuesImpl(int n_sel_rows,
                        int dst_idx,
                        uint8_t* __restrict__ dst_values_buf,
                        uint8_t* __restrict__ non_null_bitmap) {
  int aligned_dst_idx = KUDU_ALIGN_DOWN(dst_idx, 8);
  int aligned_n_sel = n_sel_rows + (dst_idx - aligned_dst_idx);

  uint8_t* aligned_values_base = dst_values_buf + aligned_dst_idx * type_size;

  ForEachUnsetBit(non_null_bitmap + aligned_dst_idx/8,
                  aligned_n_sel,
                  [&](int position) {
                    // The position here is relative to our aligned bitmap.
                    memset(aligned_values_base + position * type_size, 0, type_size);
                  });
}


ATTRIBUTE_ALWAYS_INLINE
void ZeroNullValuesImpl(int type_size,
                    int n_sel_rows,
                    int dst_idx,
                    uint8_t* __restrict__ dst_values_buf,
                    uint8_t* __restrict__ dst_non_null_bitmap) {
  switch (type_size) {
#define CASE(size)                                                      \
    case size:                                                          \
      ZeroNullValuesImpl<size>(n_sel_rows, dst_idx, dst_values_buf, dst_non_null_bitmap); \
      break;
    CASE(1);
    CASE(2);
    CASE(4);
    CASE(8);
    CASE(16);
#undef CASE
    default:
      LOG(FATAL) << "bad size: " << type_size;
  }
}

template<class ...Args>
__attribute__((target("bmi")))
void ZeroNullValuesBmi(Args... args) {
  ZeroNullValuesImpl(std::forward<Args>(args)...);
}

template<class ...Args>
void ZeroNullValues(Args... args) {
  static bool use_bmi = base::CPU().has_bmi();
  if (use_bmi) {
    ZeroNullValuesBmi(std::forward<Args>(args)...);
  } else {
    ZeroNullValuesImpl(std::forward<Args>(args)...);
  }
}

namespace {
struct BitWriter {
  explicit BitWriter(uint8_t* dst) : dst_(dst) {
  }

  void Skip(int num_bits) {
    dst_ += num_bits / 8;
    bit_offset_ = num_bits % 8;
    buffered_values_ = *dst_ & ((1 << bit_offset_) - 1);
  }

  void Put(uint64_t v, int num_bits) {
    DCHECK_LE(num_bits, 64);
    buffered_values_ |= v << bit_offset_;
    bit_offset_ += num_bits;

    if (PREDICT_FALSE(bit_offset_ >= 64)) {
      memcpy(dst_, &buffered_values_, 8);
      buffered_values_ = 0;
      bit_offset_ -= 64;
      int shift = num_bits - bit_offset_;
      buffered_values_ = (shift >= 64) ? 0 : v >> shift;
      dst_ += 8;
    }
    DCHECK_LT(bit_offset_, 64);
  }

  void Flush() {
    while (bit_offset_ > 0) {
      *dst_++ = buffered_values_ & 0xff;
      buffered_values_ >>= 8;
      bit_offset_ -= 8;
    }
  }

  uint8_t* dst_;
  uint64_t buffered_values_ = 0;
  int bit_offset_ = 0;
};
}

template<class PextImpl>
void CopyNullBitmapImpl(PextImpl pext,
                    const uint8_t* __restrict__ non_null_bitmap,
                    const uint8_t* __restrict__ sel_bitmap,
                    int n_rows,
                    int dst_idx,
                    uint8_t* __restrict__ dst_non_null_bitmap) {
  BitWriter bw(dst_non_null_bitmap);

  bw.Skip(dst_idx);

  int num_64bit_words = n_rows / 64;
  for (int i = 0; i < num_64bit_words; i++) {
    uint64_t sel_mask = UnalignedLoad<uint64_t>(sel_bitmap + i * 8);
    int num_bits = __builtin_popcountll(sel_mask);
    
    uint64_t non_nulls = UnalignedLoad<uint64_t>(non_null_bitmap + i * 8);
    uint64_t extracted = pext(non_nulls, sel_mask);
    bw.Put(extracted, num_bits);
  }

  int rem_rows = n_rows % 64;
  non_null_bitmap += num_64bit_words * 8;
  sel_bitmap += num_64bit_words * 8;
  while (rem_rows > 0) {
    uint8_t non_nulls = *non_null_bitmap;
    uint8_t sel_mask = *sel_bitmap;

    uint64_t extracted = pext(non_nulls, sel_mask);
    int num_bits = __builtin_popcountl(sel_mask);
    bw.Put(extracted, num_bits);

    sel_bitmap++;
    non_null_bitmap++;
    rem_rows -= 8;
  }
  bw.Flush();
}

bool ShouldUsePext() {
  base::CPU cpu;
  // Even though recent AMD chips support pext, it's extremely slow.
  // The software implementation 'zp7' is much faster.
  return cpu.has_bmi2() && cpu.vendor_name() == "GenuineIntel";
}

template<class ...Args>
__attribute__((target("bmi2")))
void CopyNullBitmapBmi2(Args... args) {
  CopyNullBitmapImpl(_pext_u64, std::forward<Args>(args)...);
}

template<class ...Args>
void CopyNullBitmap(Args... args) {
  static bool use_pext = ShouldUsePext();
  if (use_pext) {
    CopyNullBitmapBmi2(std::forward<Args>(args)...);
  } else {
    CopyNullBitmapImpl(zp7_pext_64, std::forward<Args>(args)...);
  }
}

template<int type_size>
ATTRIBUTE_NOINLINE
void CopyRows(const uint16_t* __restrict__ sel_rows,
              int n_sel_rows,
              const uint8_t* __restrict__ src_buf,
              uint8_t* __restrict__ dst_buf)
{
  int rem = n_sel_rows;
  while (rem--) {
    int idx = *sel_rows++;
    memcpy(dst_buf, src_buf + idx * type_size, type_size);
    // TODO(todd) nt_memcpy is faster when alignment is right, but crashes when
    // unaligned.
    //nt_memcpy(dst_buf, src_buf + idx * type_size, type_size);
    dst_buf += type_size;
  }
  // TODO(todd): should we zero out nulls first or otherwise avoid
  // copying them?
}

/*
TODO: enable this AVX2 variant dynamically on intel chips after some
benchmarking

template<>
__attribute__((target("avx2")))
void CopyRows<4>(const int* __restrict__ sel_rows,
              int n_sel_rows,
              const uint8_t* __restrict__ src_buf,
              uint8_t* __restrict__ dst_buf)
{

  int rem = n_sel_rows;
  while (rem >= 8) {
    __m256i indexes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sel_rows));
    __m256i elems = _mm256_i32gather_epi32(src_buf, indexes, sizeof(int32_t));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst_buf), elems);
    dst_buf += 8 * sizeof(int32_t);
    sel_rows += 8;
    rem -= 8;
  }

  while (rem--) {
    int idx = *sel_rows++;
    memcpy(dst_buf, src_buf + idx * sizeof(int32_t), sizeof(int32_t));
    dst_buf += sizeof(int32_t);
  }
}
*/


template<int type_size>
ATTRIBUTE_NOINLINE
void CopyRows(const vector<uint16_t>& sel_rows,
              const uint8_t* __restrict__ src_buf,
              uint8_t* __restrict__ dst_buf) {
  CopyRows<type_size>(sel_rows.data(), sel_rows.size(), src_buf, dst_buf);
}

void CopySelectedRows(const vector<uint16_t>& sel_rows,
                      int type_size,
                      const uint8_t* __restrict__ src_buf,
                      uint8_t* __restrict__ dst_buf) {
  switch (type_size) {
    case 1:
      CopyRows<1>(sel_rows, src_buf, dst_buf);
      break;
    case 2:
      CopyRows<2>(sel_rows, src_buf, dst_buf);
      break;
    case 4:
      CopyRows<4>(sel_rows, src_buf, dst_buf);
      break;
    case 8:
      CopyRows<8>(sel_rows, src_buf, dst_buf);
      break;
    case 16:
      CopyRows<16>(sel_rows, src_buf, dst_buf);
      break;
    default:
      LOG(FATAL) << "unexpected type size: " << type_size;
  }
}

void RelocateSlicesToIndirect(uint8_t* dst_buf, int n_rows,
                              faststring* indirect) {
  Slice* dst_slices = reinterpret_cast<Slice*>(dst_buf);
  size_t total_size = 0;
  for (int i = 0; i < n_rows; i++) {
    total_size += dst_slices[i].size();
  }

  int old_size = indirect->size();
  indirect->EnsureRoomForAppend(total_size);
  indirect->resize(old_size + total_size);

  uint8_t* dst_base = indirect->data() + old_size;
  uint8_t* dst = dst_base;

  for (int i = 0; i < n_rows; i++) {
    Slice* s = &dst_slices[i];
    if (s->size()) {
      strings::memcpy_inlined(dst, s->data(), s->size());
    }
    *s = Slice(reinterpret_cast<const uint8_t*>(dst - dst_base), s->size());
    dst += s->size();
  }
}

int div_type_size(size_t s, size_t divisor) {
  switch (divisor) {
    case 1: return s;
    case 2: return s/2;
    case 4: return s/4;
    case 8: return s/8;
    case 16: return s/16;
    default: return s/divisor;
  }
}

ATTRIBUTE_NOINLINE
void CopySelectedRowsFromColumn(const ColumnBlock& cblock,
                                const optional<vector<uint16_t>>& sel_rows,
                                const uint8_t* sel_bitmap,
                                ColumnarSerializedBatch::Column* dst) {
  size_t type_size = cblock.type_info()->size();

  // Number of initial rows in the dst values and null_bitmap.
  DCHECK_EQ(dst->data.size() % type_size, 0);
  size_t initial_rows = div_type_size(dst->data.size(), type_size);
  int n_sel = sel_rows == boost::none ? cblock.nrows() : sel_rows->size();
  dst->data.resize_with_extra_capacity(dst->data.size() + type_size * n_sel);
  uint8_t* dst_buf = dst->data.data() + type_size * initial_rows;
  const uint8_t* src_buf = cblock.cell_ptr(0);

  if (sel_rows == boost::none) {
    // All rows selected: just memcpy
    // TODO(todd) use nt_memcpy
    memcpy(dst_buf, src_buf, type_size * n_sel);
  } else {
    CopySelectedRows(boost::get(sel_rows), type_size, src_buf, dst_buf);
  }

  if (cblock.is_nullable()) {
    DCHECK_EQ(dst->non_null_bitmap->size(), BitmapSize(initial_rows));
    dst->non_null_bitmap->resize_with_extra_capacity(BitmapSize(initial_rows + n_sel));
    CopyNullBitmap(cblock.null_bitmap(),
                   sel_bitmap,
                   cblock.nrows(),
                   initial_rows, dst->non_null_bitmap->data());
    ZeroNullValues(type_size, n_sel, initial_rows, dst_buf, dst->non_null_bitmap->data());
  }

  if (cblock.type_info()->physical_type() == BINARY) {
    RelocateSlicesToIndirect(dst_buf, n_sel, boost::get_pointer(dst->indirect_data));
  }
}

} // anonymous namespace

int SerializeRowBlockColumnar(
    const RowBlock& block,
    const Schema* projection_schema,
    ColumnarSerializedBatch* out) {
  DCHECK_GT(block.nrows(), 0);
  const Schema* tablet_schema = block.schema();

  if (projection_schema == nullptr) {
    projection_schema = tablet_schema;
  }

  optional<vector<uint16_t>> sel_rows;
  block.selection_vector()->GetSelectedRows(&sel_rows);

  if (out->columns.size() != projection_schema->num_columns()) {
    CHECK_EQ(out->columns.size(), 0);
    out->columns.reserve(projection_schema->num_columns());
    for (const auto& col : projection_schema->columns()) {
      out->columns.emplace_back();
      out->columns.back().data.reserve(1024 * 1024);
      if (col.type_info()->physical_type() == BINARY) {
        out->columns.back().indirect_data.emplace();
      }
      if (col.is_nullable()) {
        out->columns.back().non_null_bitmap.emplace();
      }
    }
  }

  int col_idx = 0;
  for (const auto& col : projection_schema->columns()) {
    int t_schema_idx = tablet_schema->find_column(col.name());
    const ColumnBlock& column_block = block.column_block(t_schema_idx);

    CopySelectedRowsFromColumn(column_block, sel_rows, block.selection_vector()->bitmap(),
                               &out->columns[col_idx]);
    col_idx++;
  }

  return sel_rows ? sel_rows->size() : block.nrows();
}


} // namespace kudu
