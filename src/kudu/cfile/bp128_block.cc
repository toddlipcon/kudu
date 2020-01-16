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


#include "kudu/cfile/bp128_block.h"
#include "kudu/cfile/bitpacking.h"

#include "kudu/util/hexdump.h"
#include "kudu/gutil/strings/substitute.h"


#include <vector>

using std::vector;

namespace kudu {
namespace cfile {

template<DataType T>
void BP128BlockBuilder<T>::Reset() {
  auto block_size = options_->storage_attributes.cfile_block_size;
  count_ = 0;
  data_.clear();
  header_buf_.clear();
  buf_.clear();
  DCHECK_EQ(reinterpret_cast<uintptr_t>(data_.data()) & (alignof(CppType) - 1), 0)
      << "buffer must be naturally-aligned";
  finished_ = false;
  rem_elem_capacity_ = KUDU_ALIGN_UP(block_size / size_of_type, kPageSize);
  data_.resize(rem_elem_capacity_);
}

template<DataType T>
Status BP128BlockBuilder<T>::GetFirstKey(void* key) const {
  DCHECK(finished_);
  if (count_ == 0) {
    return Status::NotFound("no keys in data block");
  }
  memcpy(key, &data_[0], size_of_type);
  return Status::OK();
}

template<DataType T>
Status BP128BlockBuilder<T>::GetLastKey(void* key) const {
  DCHECK(finished_);
  if (count_ == 0) {
    return Status::NotFound("no keys in data block");
  }
  memcpy(key, &data_[count_ - 1], size_of_type);
  return Status::OK();
}

template<DataType T>
int BP128BlockBuilder<T>::Add(const uint8_t* vals_void, size_t count) {
  DCHECK(!finished_);
  int to_add = std::min<int>(rem_elem_capacity_, count);
  const auto* vals_ints = reinterpret_cast<const CppType*>(vals_void);
  for (int i = 0; i < to_add; i++) {
    data_[count_++] = *vals_ints++;
  }
  rem_elem_capacity_ -= to_add;
  return to_add;
}


template<DataType T>
void BP128BlockBuilder<T>::Finish(rowid_t ordinal_pos, std::vector<Slice>* slices) {
  DCHECK(data_.size() % kPageSize == 0);

  // TODO(todd) be intelligent about this.
  //
  // Delta encoding can use an extra bit relative to plain frame-of-reference in some
  // cases.
  // For example, consider the page:
  //
  //   Original data: {0, 1, 0, 1, ...}
  //   Frame-of-reference encoding: {0, 1, 0, 1, ...} min_val = 0
  //   Delta-encoding: {0, 1, -1, 1, -1}
  //   Frame-of-reference on delta: {1, 2, 0, 2, ...} min_val = -1
  //
  // FOR-encoding here only requires 1 bit per value, but delta-encoding requires
  // two bits per value.
  bool use_deltas = true;

  // Pad the last page with repeats of the last value. In contrast to padding
  // with 0s, this is guaranteed to not increase the required bit-width for the page.
  //
  // TODO(todd) consider using a different encoding on the last (partial) page so we only need
  // to deal with full pages. This can waste up to almost 1KB in the case of 127 x 64-bit values.
  int padding_needed = KUDU_ALIGN_UP(count_, kPageSize) - count_;
  for (int i = 0; i < padding_needed; i++) {
    data_[count_ + i] = data_[count_ - 1];
  }

  buf_.resize(data_.size() * sizeof(CppType));

  vector<CppType> min_vals;
  vector<CppType> min_deltas;
  vector<uint8_t> bit_sizes;
  uint8_t* dst = buf_.data();

  // Encode all the pages.
  // TODO(todd) last page should not use SIMD layout -- may waste significant space.
  for (int page_start = 0; page_start < count_; page_start += kPageSize) {
    typename Bitpacking<CppType>::PageMeta meta;
    int n = Bitpacking<CppType>::CompressFullPage(
        &data_[page_start], use_deltas, dst, &meta);
    dst += n;
    min_vals.push_back(meta.reference_val);
    bit_sizes.push_back(meta.num_bits | ((uint8_t)meta.delta_encoded << 7));
    if (use_deltas) {
      min_deltas.push_back(meta.min_delta);
    }
  }
  int page_data_size = dst - buf_.data();
  buf_.resize(page_data_size);
  const Slice page_data = Slice(buf_.data(), page_data_size);

  header_buf_.clear();
  PutFixed32(&header_buf_, ordinal_pos);
  PutFixed32(&header_buf_, count_);

  // TODO(todd) clean up this header with flags, compression, etc.
  PutFixed32(&header_buf_, use_deltas ? 1 : 0);

  header_buf_.append(min_vals.data(), sizeof(CppType) * min_vals.size());
  header_buf_.append(bit_sizes.data(), bit_sizes.size());
  if (use_deltas) {
    header_buf_.append(min_deltas.data(), sizeof(CppType) * min_deltas.size());
  }

  finished_ = true;
  *slices = { { header_buf_.data(), header_buf_.size() }, page_data };
}

////////////////////////////////////////////////////////////

template<DataType T>
BP128BlockDecoder<T>::BP128BlockDecoder(Slice slice)
    : data_(slice) {
}



template<DataType T>
Status BP128BlockDecoder<T>::ParseHeader() {
  // TODO: check sizes
  CHECK(!parsed_);
  const int kHeaderSize = 8;
  if (data_.size() < kHeaderSize) {
    return Status::Corruption(
      strings::Substitute("not enough bytes for header: string block header "
        "size ($0) less than minimum possible header length ($1)",
        data_.size(), kHeaderSize));
  }

  Slice decode_slice = data_;
  // Decode header.
  first_row_idx_ = DecodeFixed32(decode_slice.data());
  decode_slice.remove_prefix(4);

  num_elems_ = DecodeFixed32(decode_slice.data());
  decode_slice.remove_prefix(4);
  CHECK(num_elems_ >= 0);

  bool use_deltas = DecodeFixed32(decode_slice.data());
  decode_slice.remove_prefix(4);

  int num_pages = Bitpacking<CppType>::num_pages(num_elems_);

  min_vals_ = { reinterpret_cast<CppType*>(decode_slice.mutable_data()),
                (size_t)num_pages };
  decode_slice.remove_prefix(num_pages * sizeof(CppType));

  bit_widths_ = { reinterpret_cast<uint8_t*>(decode_slice.mutable_data()), (size_t)num_pages };
  decode_slice.remove_prefix(num_pages * sizeof(int8_t));

  if (use_deltas) {
    min_deltas_ = { reinterpret_cast<CppType*>(decode_slice.mutable_data()), (size_t)num_pages };
    decode_slice.remove_prefix(num_pages * sizeof(CppType));
  }

  Slice data_slice = decode_slice;
  data_start_ = data_slice.mutable_data();

  Bitpacking<CppType>::ComputePageOffsets(bit_widths_, &page_offsets_);

  parsed_ = true;
  cur_idx_ = 0;
  return Status::OK();
}


template<DataType T>
void BP128BlockDecoder<T>::SeekToPositionInBlock(uint pos) {
  CHECK(parsed_) << "Must call ParseHeader()";
  DCHECK_LE(pos, num_elems_);
  cur_idx_ = pos;
}

template<DataType T>
Status BP128BlockDecoder<T>::SeekAtOrAfterValue(const void *value, bool *exact_match) {
  CHECK(parsed_) << "Must call ParseHeader()";
  auto cpp_val = UnalignedLoad<CppType>(value);
  auto it = std::upper_bound(min_vals_.begin(), min_vals_.end(), cpp_val);

  // 'it' points to the first page which has a minimum (first) value > the search
  // value. The search value then must be in the prior page, if present.
  if (it == min_vals_.begin()) {
    // match on the first value.
    SeekToPositionInBlock(0);
    *exact_match = min_vals_[0] == cpp_val;
    return Status::OK();
  }

  // If the value exists, it must be in the prior page. Load that page.
  int page_idx = std::distance(min_vals_.begin(), it) - 1;
  int first_elem_idx = page_idx * kPageSize;
  SeekToPositionInBlock(first_elem_idx);
  LoadCurrentPageToBuffer();

  // Binary search within that page.
  int page_size = std::min(num_elems_ - first_elem_idx, int(kPageSize));
  CppType* page_end = &cur_page_data_[page_size];
  CppType* in_page_it = std::lower_bound(cur_page_data_, page_end, cpp_val);
  int in_page_idx = in_page_it - cur_page_data_;
  SeekToPositionInBlock(first_elem_idx + in_page_idx);
  if (in_page_idx == kPageSize) {
    // no elements in this page were >= the search value.
    //
    // Page N                     Page N+1
    // 0 1 2 3 4 5 .. kPageSize   0 1 2 3 4 5 6 ...
    //                          ^
    //                          \- search value falls here
    *exact_match = false;
  } else {
    *exact_match = *in_page_it == cpp_val;
  }
  if (cur_idx_ == num_elems_) {
    return Status::NotFound("after last key in block");
  }
  return Status::OK();
}

template<DataType T>
void BP128BlockDecoder<T>::LoadCurrentPageToBuffer() {
  int p = cur_idx_ / kPageSize;
  if (cur_page_idx_ == p) return;
  LoadPage(p, cur_page_data_);
  cur_page_idx_ = p;
}

template<DataType T>
void BP128BlockDecoder<T>::LoadPage(int page, CppType* __restrict__ dst) {
  typename Bitpacking<CppType>::PageMeta meta;
  meta.reference_val = min_vals_[page];
  meta.num_bits = bit_widths_[page] & 127;
  meta.delta_encoded = bit_widths_[page] & 0x80;
  if (meta.delta_encoded) {
    meta.min_delta = min_deltas_[page];
  }
  const uint8_t* page_data = data_start_ + page_offsets_[page];

  Bitpacking<CppType>::UncompressFullPage(page_data, dst, meta);
}


template<DataType T>
Status BP128BlockDecoder<T>::CopyNextValues(size_t *n, ColumnDataView *dst) {
  DCHECK(parsed_);
  DCHECK_LE(*n, dst->nrows());
  DCHECK_EQ(dst->stride(), sizeof(CppType));
  if (PREDICT_FALSE(*n == 0 || cur_idx_ >= num_elems_)) {
    *n = 0;
    return Status::OK();
  }

  // TODO(todd) sprinkle some __restrict__?
  int to_fetch = std::min<int>(*n, num_elems_ - cur_idx_);
  int fetch_rem = to_fetch;
  uint8_t* page_dst = dst->data();
  while (fetch_rem > 0) {
    int rem_in_page = kPageSize - cur_idx_ % kPageSize;
    int from_this_page = std::min(rem_in_page, fetch_rem);

    if (from_this_page == kPageSize) {
      LoadPage(cur_idx_ / kPageSize, reinterpret_cast<CppType*>(page_dst));
      // we need the whole page, so decode it directly into
      // the dst buffer instead of making an extra copy.
    } else {
      LoadCurrentPageToBuffer();
      memcpy(page_dst, &cur_page_data_[cur_idx_ % kPageSize], from_this_page * sizeof(CppType));
    }
    fetch_rem -= from_this_page;
    page_dst += from_this_page * sizeof(CppType);
    cur_idx_ += from_this_page;
  }
  *n = to_fetch;

  return Status::OK();
}
template<DataType T>
bool BP128BlockDecoder<T>::HasNext() const {
  return cur_idx_ < num_elems_;
}
template<DataType T>
size_t BP128BlockDecoder<T>::Count() const {
  DCHECK(parsed_);
  return num_elems_;
}
template<DataType T>
size_t BP128BlockDecoder<T>::GetCurrentIndex() const {
  return cur_idx_;
}
template<DataType T>
rowid_t BP128BlockDecoder<T>::GetFirstRowId() const {
  DCHECK(parsed_);
  return first_row_idx_;
}

// Explicit instantiations.
template class BP128BlockDecoder<UINT8>;
template class BP128BlockDecoder<UINT16>;
template class BP128BlockDecoder<UINT32>;
template class BP128BlockDecoder<UINT64>;
template class BP128BlockDecoder<INT8>;
template class BP128BlockDecoder<INT16>;
template class BP128BlockDecoder<INT32>;
template class BP128BlockDecoder<INT64>;

template class BP128BlockBuilder<UINT8>;
template class BP128BlockBuilder<UINT16>;
template class BP128BlockBuilder<UINT32>;
template class BP128BlockBuilder<UINT64>;
template class BP128BlockBuilder<INT8>;
template class BP128BlockBuilder<INT16>;
template class BP128BlockBuilder<INT32>;
template class BP128BlockBuilder<INT64>;

} // namespace cfile
} // namespace kudu
