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
#include "kudu/common/rowblock.h"

#include <numeric>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/bits.h"
#include "kudu/gutil/port.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/bitmap-inl.h"

using std::vector;

namespace kudu {

SelectionVector::SelectionVector(size_t row_capacity)
  : bytes_capacity_(BitmapSize(row_capacity)),
    n_rows_(row_capacity),
    n_bytes_(bytes_capacity_),
    bitmap_(new uint8_t[n_bytes_]) {
  CHECK_GT(n_bytes_, 0);
  PadExtraBitsWithZeroes();
}

void SelectionVector::Resize(size_t n_rows) {
  if (PREDICT_FALSE(n_rows == n_rows_)) {
    return;
  }

  size_t new_bytes = BitmapSize(n_rows);
  CHECK_LE(new_bytes, bytes_capacity_);
  n_rows_ = n_rows;
  n_bytes_ = new_bytes;
  PadExtraBitsWithZeroes();
}

void SelectionVector::ClearToSelectAtMost(size_t max_rows) {
  if (max_rows < n_rows_) {
    BitmapIterator iter(&bitmap_[0], n_rows_);
    bool selected;
    size_t run_size;
    size_t end_idx = 0;
    // Adjust the end index until we have selected 'max_rows' rows.
    while ((run_size = iter.Next(&selected)) && max_rows > 0) {
      if (selected) {
        if (run_size >= max_rows) {
          end_idx += max_rows;
          break;
        }
        max_rows -= run_size;
      }
      end_idx += run_size;
    }
    // If the limit is reached, zero out the rest of the selection vector.
    if (n_rows_ > end_idx) {
      BitmapChangeBits(&bitmap_[0], end_idx, n_rows_ - end_idx, false);
    }
  }
}

namespace {
struct RunProducer {
  vector<int>* runs;
  int* dst;
  bool cur_val = true;
  int cur_count = 0;

  RunProducer(vector<int>* runs,
                       int capacity) : runs(runs) {
    runs->resize(capacity);
    dst = runs->data();
  }

  void Add(bool v, int n) {
    if (cur_val == v) {
      cur_count += n;
    } else {
      *dst++ = cur_count;
      cur_count = n;
      cur_val = v;
    }
  }

  void Flush() {
    if (cur_count > 0) {
      *dst++ = cur_count;
    }

    runs->resize(dst - runs->data());
  }
};

// Iterate over the bitmap one T at a time. T should be an unsigned
// integer type.
//
// This specializes for the case of runs of all ones or zeros at least
// as long as the 'T' type.
template<typename T>
const uint8_t* GetSelectedRunsLoop(const T* __restrict__ bitmap,
                                   int rem_bytes,
                                   RunProducer* rp) {
  constexpr T all_zeros = 0;
  constexpr T all_ones = ~all_zeros;

  for (int i = 0; i < rem_bytes / sizeof(T); i++) {
    T bm = *bitmap++;
    if (bm == all_ones) {
      rp->Add(true, sizeof(T) * 8);
    } else if (bm == all_zeros) {
      rp->Add(false, sizeof(T) * 8);
    } else {
      // TODO(todd): benchmark this approach against one that uses the "find first set bit"
      // intrinsics to find bit transitions, which would be O(number of transitions) instead of
      // O(number of bits).
      if (sizeof(T) > 1) {
        // If we have a 64-bit run that isn't all ones or zeros, fall through
        // to the 1-byte-at-a-time case since we still might have 1-byte runs,
        GetSelectedRunsLoop(reinterpret_cast<const uint8_t*>(&bm), sizeof(T), rp);
      } else {
        for (int i = 0; i < 8; i++) {
          rp->Add(bm & 1, 1);
          bm >>= 1;
        }
      }
    }
  }
  return reinterpret_cast<const uint8_t*>(bitmap);
}

} // anonymous namespace

void SelectionVector::GetSelectedRuns(vector<int>* runs) const {
  // TODO(todd): benchmark vs using BitmapIterator.
  RunProducer rp(runs, n_bytes_ * 8);

  const uint8_t* bitmap = &bitmap_[0];
  bitmap = GetSelectedRunsLoop(reinterpret_cast<const uint64_t*>(bitmap),
                               n_bytes_,
                               &rp);
  bitmap = GetSelectedRunsLoop(bitmap,
                               n_bytes_ % 8,
                               &rp);
  rp.Flush();

  // Since the last byte may be padded with zeros when the number of rows is not a
  // multiple of 8, we may need to remove them.
  size_t bits_in_last_byte = n_rows_ & 7;
  if (bits_in_last_byte > 0) {
    int n_padding = 8 - bits_in_last_byte;
    runs->back() -= n_padding;
    if (runs->back() == 0) {
      runs->pop_back();
    }
  }
}

// TODO(todd) this is a bit faster when implemented with target "bmi" enabled.
// Consider duplicating it and doing runtime switching.
static void GetSelectedRowsInternal(const uint8_t* __restrict__ bitmap,
                                    int n_bytes,
                                    uint16_t* __restrict__ dst) {
  ForEachSetBit(bitmap, n_bytes * 8,
                [&](int bit) {
                  *dst++ = bit;
                });
}

void SelectionVector::GetSelectedRows(boost::optional<vector<uint16_t>>* selected) const {
  CHECK_LE(n_rows_, std::numeric_limits<uint16_t>::max());
  int n_selected = CountSelected();
  if (n_selected == n_rows_) {
    *selected = boost::none;
    return;
  }

  if (*selected == boost::none) {
    selected->emplace();
  }
  auto* vec = boost::get_pointer(*selected);
  vec->resize(n_selected);
  if (n_selected == 0) {
    return;
  }
  GetSelectedRowsInternal(&bitmap_[0], n_bytes_, vec->data());
}


size_t SelectionVector::CountSelected() const {
  return Bits::Count(&bitmap_[0], n_bytes_);
}

bool SelectionVector::AnySelected() const {
  size_t rem = n_bytes_;
  const uint32_t *p32 = reinterpret_cast<const uint32_t *>(
    &bitmap_[0]);
  while (rem >= 4) {
    if (*p32 != 0) {
      return true;
    }
    p32++;
    rem -= 4;
  }

  const uint8_t *p8 = reinterpret_cast<const uint8_t *>(p32);
  while (rem > 0) {
    if (*p8 != 0) {
      return true;
    }
    p8++;
    rem--;
  }

  return false;
}

bool operator==(const SelectionVector& a, const SelectionVector& b) {
  if (a.nrows() != b.nrows()) {
    return false;
  }
  return BitmapEquals(a.bitmap(), b.bitmap(), a.nrows());
}

bool operator!=(const SelectionVector& a, const SelectionVector& b) {
  return !(a == b);
}

//////////////////////////////
// RowBlock
//////////////////////////////
RowBlock::RowBlock(const Schema* schema,
                   size_t nrows,
                   Arena *arena)
  : schema_(schema),
    columns_data_(schema->num_columns()),
    column_null_bitmaps_(schema->num_columns()),
    row_capacity_(nrows),
    nrows_(nrows),
    arena_(arena),
    sel_vec_(nrows) {
  CHECK_GT(row_capacity_, 0);

  size_t bitmap_size = BitmapSize(row_capacity_);
  for (size_t i = 0; i < schema->num_columns(); ++i) {
    const ColumnSchema& col_schema = schema->column(i);
    size_t col_size = row_capacity_ * col_schema.type_info()->size();
    columns_data_[i] = new uint8_t[col_size];

    if (col_schema.is_nullable()) {
      column_null_bitmaps_[i] = new uint8_t[bitmap_size];
    }
  }
}

RowBlock::~RowBlock() {
  for (uint8_t* column_data : columns_data_) {
    delete[] column_data;
  }
  for (uint8_t* bitmap_data : column_null_bitmaps_) {
    delete[] bitmap_data;
  }
}

void RowBlock::Resize(size_t n_rows) {
  if (PREDICT_FALSE(n_rows == nrows_)) {
    return;
  }

  CHECK_LE(n_rows, row_capacity_);
  nrows_ = n_rows;
  sel_vec_.Resize(n_rows);
}

} // namespace kudu
