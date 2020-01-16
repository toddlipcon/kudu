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
//
// Make use of bitshuffle and lz4 to encode the fixed size
// type blocks, such as UINT8, INT8, UINT16, INT16,
//                      UINT32, INT32, FLOAT, DOUBLE.
// Reference:
// https://github.com/kiyo-masui/bitshuffle.git
#pragma once

#include <sys/types.h>

#include <algorithm>
#include <cstring>
#include <cstdint>
#include <ostream>
#include <vector>

#include <glog/logging.h>

#include "kudu/cfile/bitshuffle_arch_wrapper.h"
#include "kudu/cfile/block_encodings.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/array_view.h"
#include "kudu/util/alignment.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace cfile {

template<DataType Type>
class BP128BlockBuilder final : public BlockBuilder {
 public:
  explicit BP128BlockBuilder(const WriterOptions* options)
      : count_(0),
        options_(options) {
    Reset();
  }

  void Reset() override;

  bool IsBlockFull() const override {
    // TODO(todd): perhaps when we see that the page is full, we should run
    // an encoding pass, and see if we actually got a good compression rate.
    // Right now we might be 64x smaller than the target page size.
    return rem_elem_capacity_ == 0;
  }

  int Add(const uint8_t* vals_void, size_t count) override;

  size_t Count() const override {
    return count_;
  }

  Status GetFirstKey(void* key) const override;

  Status GetLastKey(void* key) const override;

  void Finish(rowid_t ordinal_pos, std::vector<Slice>* slices) override;

 private:
  using CppType = typename TypeTraits<Type>::cpp_type;
  using UnsignedCppType = typename std::make_unsigned<CppType>::type;

  CppType cell(int idx) const {
    DCHECK_GE(idx, 0);
    return UnalignedLoad<CppType>(&data_[idx * size_of_type]);
  }

  // Length of a header.
  static const size_t kHeaderSize = sizeof(uint32_t) * 5;
  enum {
    size_of_type = TypeTraits<Type>::size,
    kPageSize = 128,
  };

  std::vector<CppType> data_;

  faststring buf_;
  faststring header_buf_;

  int count_;
  int rem_elem_capacity_;
  bool finished_;
  const WriterOptions* options_;
};


template<DataType Type>
class BP128BlockDecoder final : public BlockDecoder {
 public:
  explicit BP128BlockDecoder(Slice slice);

  virtual Status ParseHeader() override;

  virtual void SeekToPositionInBlock(uint pos) override;

  virtual Status SeekAtOrAfterValue(const void *value,
                                    bool *exact_match) override;

  virtual Status CopyNextValues(size_t *n, ColumnDataView *dst) override;

  virtual bool HasNext() const override;

  virtual size_t Count() const override;

  virtual size_t GetCurrentIndex() const override;

  virtual rowid_t GetFirstRowId() const override;

  virtual ~BP128BlockDecoder() = default;

 private:
  using CppType = typename TypeTraits<Type>::cpp_type;
  using UnsignedCppType = typename std::make_unsigned<CppType>::type;

  void LoadCurrentPageToBuffer();
  void LoadPage(int page, CppType* __restrict__ dst);

  static constexpr int kHeaderSize = 8;
  static constexpr int kPageSize = 128;

  const Slice data_;

  bool parsed_ = false;
  int num_elems_;
  rowid_t first_row_idx_;
  ArrayView<CppType> min_vals_;
  ArrayView<CppType> min_deltas_;

  // For each page, the bit width used to encode that page.
  // The MSB of these values encodes whether the page is delta-encoded.
  ArrayView<uint8_t> bit_widths_;
  uint8_t* data_start_;

  // Offsets within data_start_ for each page.
  std::vector<uint32_t> page_offsets_;

  // The index of the next int to be read.
  int cur_idx_;

  // The index that has been loaded into cur_page_data_.
  int cur_page_idx_ = -1;
  CppType cur_page_data_[kPageSize];
};

} // namespace cfile
} // namespace kudu
