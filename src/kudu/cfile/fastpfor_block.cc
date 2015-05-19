// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <algorithm>

#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/fastpfor_block.h"
#include "kudu/common/columnblock.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/mathlimits.h"

#include "kudu/util/fastpfor-inl.h"

using namespace FastPForLib;

namespace kudu { namespace cfile {

using kudu::coding::AppendInt32GroupFastPFor;
using kudu::coding::DecodeInt32GroupFastPFor;
using kudu::coding::CalcOptimalEncodingCost;
using kudu::coding::PackWithoutMask_SIMD;
using kudu::coding::Unpack_SIMD;

FastPForBlockBuilder::FastPForBlockBuilder(const WriterOptions *options)
 : exception_info_(33),
   size_estimate_(0),
   options_(options) {
  Reset();
}

void FastPForBlockBuilder::Reset() {
  inputs_.clear();
  inputs_.reserve(options_->block_size / sizeof(uint32_t));

  buffer_.clear();
  buffer_.reserve(options_->block_size);
  buffer_.resize(kHeaderSizeBytes);

  metadata_.clear();
  metadata_.reserve(options_->block_size);

  for (size_t i = 0; i < exception_info_.size(); ++i) {
    std::vector<uint32_t> ().swap(exception_info_[i]);
  }
  size_estimate_ = kHeaderSizeBytes;
}

int FastPForBlockBuilder::Add(const uint8_t *vals_void, size_t count) {
  const uint32_t *vals = reinterpret_cast<const uint32_t *>(vals_void);
  size_t added = 0;
  while ((size_estimate_ + kPaddingSizeBytes) <= options_->block_size &&
         added < count) {
    inputs_.push_back(*vals++);
    added++;

    size_t num_inputs = inputs_.size();
    if ((num_inputs % kFastPForBlockSize) == 0) {
      uint32_t *data_ptr = &inputs_[num_inputs - kFastPForBlockSize];

      uint8_t best_b, best_cexcept, max_b;
      size_estimate_ += CalcOptimalEncodingCost(data_ptr,
                                                kFastPForBlockSize,
                                                best_b, best_cexcept, max_b);

      size_t old_size = metadata_.size();
      metadata_.resize(old_size + 2 + ((best_cexcept > 0) ? 1 + best_cexcept : 0));
      uint8_t *ptr = &metadata_[old_size];
      *ptr++ = best_b;
      *ptr++ = best_cexcept;
      if (best_cexcept > 0) {
        *ptr++ = max_b;
        const uint32_t max_val = 1U << best_b;
        for (uint32_t k = 0; k < kFastPForBlockSize; ++k) {
          if (data_ptr[k] >= max_val) {
            (exception_info_[max_b - best_b]).push_back(data_ptr[k] >> best_b);
            *ptr++ = static_cast<uint8_t>(k);
          }
        }
      }

      AppendInt32GroupFastPFor(reinterpret_cast<uint32_t *>(data_ptr),
                               kFastPForBlockSize,
                               &buffer_, best_b);
    }
  }
  return added;
}

uint64_t FastPForBlockBuilder::EstimateEncodedSize() const {
  return size_estimate_ + kPaddingSizeBytes;
}

size_t FastPForBlockBuilder::Count() const {
  return inputs_.size();
}

Status FastPForBlockBuilder::GetFirstKey(void *key) const {
  if (inputs_.empty()) {
    return Status::NotFound("no keys in data block");
  }

  *reinterpret_cast<uint32_t *>(key) = inputs_[0];
  return Status::OK();
}

Slice FastPForBlockBuilder::Finish(rowid_t ordinal_pos) {
  size_t num_elems = inputs_.size();

  size_t num_pending = num_elems % kFastPForBlockSize;
  if (num_pending != 0) {
    uint32_t *data_ptr = &inputs_[num_elems - num_pending];
    size_t old_size = metadata_.size();
    metadata_.resize(old_size + 3 + num_pending);
    uint8_t *ptr = &metadata_[old_size];

    *ptr++ = 0;
    *ptr++ = num_pending;
    *ptr++ = 32;
    for (uint32_t k = 0; k < num_pending; ++k) {
      (exception_info_[32]).push_back(data_ptr[k]);
      *ptr++ = static_cast<uint8_t>(k);
    }
  }

  uint32_t bitmap = 0;
  for (uint32_t k = 2; k <= 32; ++k) {
    if (exception_info_[k].size() != 0) {
      bitmap |= (1U << (k - 1));
    }
  }

  InlineEncodeFixed32(&buffer_[0], buffer_.size());
  InlineEncodeFixed32(&buffer_[4], metadata_.size());
  InlineEncodeFixed32(&buffer_[8], bitmap);
  InlineEncodeFixed32(&buffer_[12], num_elems);
  InlineEncodeFixed32(&buffer_[16], ordinal_pos);

  buffer_.append(metadata_.data(), metadata_.size());
  for (uint32_t k = 2; k <= 32; ++k) {
    if (exception_info_[k].size() > 0)
      PackWithoutMask_SIMD(exception_info_[k], &buffer_, k);
  }

  DCHECK_GE(size_estimate_, buffer_.size());
  return Slice(buffer_.data(), buffer_.size());
}

////////////////////////////////////////////////////////////
// Decoder
////////////////////////////////////////////////////////////

FastPForBlockDecoder::FastPForBlockDecoder(const Slice &slice)
  : data_(slice),
    parsed_(false),
    cur_data_pos_(NULL),
    cur_metadata_pos_(NULL),
    cur_idx_(0),
    exception_info_(33) {
}


Status FastPForBlockDecoder::ParseHeader() {
  CHECK_GE(data_.size(), kHeaderSizeBytes);

#if __BYTE_ORDER != __LITTLE_ENDIAN
#error dont support big endian currently
#endif

  const uint32_t * const header = reinterpret_cast<const uint32_t *>(data_.data());
  data_start_ = reinterpret_cast<const uint32_t *>(header) + (kHeaderSizeBytes >> 2);
  cur_data_pos_ = data_start_;

  metadata_start_ = reinterpret_cast<const uint8_t *>(header) + header[0];
  cur_metadata_pos_ = metadata_start_;

  const uint32_t *exception_ptr = reinterpret_cast<const uint32_t *>(metadata_start_ + header[1]);
  const uint32_t bitmap = header[2];
  for (uint32_t k = 2; k <= 32; ++k) {
    if ((bitmap & (1U << (k - 1))) != 0) {
      exception_ptr = Unpack_SIMD(exception_ptr, exception_info_[k], k);
    }
  }
  num_elems_ = header[3];
  ordinal_pos_base_ = header[4];
  for (uint32_t k = 1; k <= 32; ++k)
    exception_iters[k] = exception_info_[k].begin();

  parsed_ = true;
  SeekToStart();
  return Status::OK();
}

void FastPForBlockDecoder::SeekToPositionInBlock(uint pos) {
  CHECK(parsed_) << "Must call ParseHeader()";
  if (cur_idx_ == pos && cur_data_pos_ != NULL) return;

  // Reset to start
  cur_data_pos_ = data_start_;
  cur_metadata_pos_ = metadata_start_;
  cur_idx_ = 0;
  pending_.clear();

  for (uint32_t k = 1; k <= 32; ++k)
    exception_iters[k] = exception_info_[k].begin();

  pos = std::min(num_elems_, pos);
  while (pos >= kFastPForBlockSize) {
    uint8_t b = *cur_metadata_pos_;
    uint8_t cexcept = *(cur_metadata_pos_ + 1);
    if (cexcept > 0) {
      uint8_t max_bits = *(cur_metadata_pos_ + 2);
      exception_iters[max_bits - b] += cexcept;
    }

    cur_idx_ += kFastPForBlockSize;
    cur_metadata_pos_ += 2 + (cexcept > 0 ? cexcept + 1 : cexcept);
    cur_data_pos_ += ((kFastPForBlockSize * b) >> 5);
    pos -= kFastPForBlockSize;
  }

  if ((num_elems_ - cur_idx_) > 0) {
    pending_.resize(kFastPForBlockSize);
    DoGetNextFastPForBlock(&pending_[0]);
    cur_idx_ += pos;
  }
}

Status FastPForBlockDecoder::SeekAtOrAfterValue(const void *value_void,
                                                bool *exact_match) {
  SeekToPositionInBlock(0);
  uint32_t target = *reinterpret_cast<const uint32_t *>(value_void);

  while ((cur_idx_ + kFastPForBlockSize) <= num_elems_) {
    uint8_t b = *cur_metadata_pos_;
    uint8_t cexcept = *(cur_metadata_pos_ + 1);

    uint32_t first_elem = 0, last_elem = 0;
    if (b > 0) {
      first_elem = cur_data_pos_[0] & ((1U << b) - 1);
      last_elem = cur_data_pos_[((kFastPForBlockSize * b) >> 5) - 1] >> (32 - b);
    }

    if (cexcept > 0) {
      uint8_t max_bits = *(cur_metadata_pos_ + 2);
      std::vector<uint32_t>::const_iterator & iter = exception_iters[max_bits - b];

      if (*(cur_metadata_pos_ + 3) == 0)
        first_elem |= (max_bits - b == 1) ? (1U << b) : (iter[0] << b);

      if (*(cur_metadata_pos_ + cexcept + 2) == (kFastPForBlockSize - 1))
        last_elem |= (max_bits - b == 1) ? (1U << b) : (iter[cexcept - 1] << b);
    }

    if (target < first_elem) {
      *exact_match = false;
      return Status::OK();
    }

    if (target > last_elem) {
      if (cexcept > 0) {
        uint8_t max_bits = *(cur_metadata_pos_ + 2);
        exception_iters[max_bits - b] += cexcept;
      }
      cur_metadata_pos_ += 2 + (cexcept > 0 ? cexcept + 1 : cexcept);
      cur_data_pos_ += ((kFastPForBlockSize * b) >> 5);
      cur_idx_ += kFastPForBlockSize;
    }
    else
      break;
  }

  size_t rem = std::min(num_elems_ - cur_idx_, (size_t)kFastPForBlockSize);
  if (rem > 0) {
    pending_.resize(kFastPForBlockSize);
    DoGetNextFastPForBlock(&pending_[0]);
    for (size_t i = 0; i < rem; i++) {
      if (pending_[i] >= target) {
        *exact_match = pending_[i] == target;
        cur_idx_ += i;
        return Status::OK();
      }
    }
    cur_idx_ += rem;
  }

  *exact_match = false;
  if (cur_idx_ == num_elems_)
    return Status::NotFound("not in block");

  return Status::OK();
}

Status FastPForBlockDecoder::CopyNextValues(size_t *n, ColumnDataView *dst) {
  DCHECK_EQ(dst->type_info()->type(), UINT32);
  DCHECK_EQ(dst->stride(), sizeof(uint32_t));

  return DoGetNextValues(n, reinterpret_cast<uint32_t *>(dst->data()));
}

inline Status FastPForBlockDecoder::DoGetNextValues(size_t *n_param, uint32_t* vals) {
  size_t n = *n_param;
  int start_idx = cur_idx_;
  size_t rem = num_elems_ - cur_idx_;
  assert(rem >= 0);

  n = std::min(rem, n);

  if (pending_.size() > 0) {
    size_t pos = cur_idx_ % kFastPForBlockSize;
    size_t num_in_pending = std::min(n, kFastPForBlockSize - pos);
    memcpy(vals, &pending_[pos], num_in_pending * sizeof(uint32_t));
    vals += num_in_pending;
    cur_idx_ += num_in_pending;
    n -= num_in_pending;
    if ((cur_idx_ % kFastPForBlockSize) == 0)
      pending_.clear();
  }

  while (n >= kFastPForBlockSize) {
    DoGetNextFastPForBlock(vals);
    vals += kFastPForBlockSize;
    cur_idx_ += kFastPForBlockSize;
    n -= kFastPForBlockSize;
  }

  if (n == 0) goto ret;

  pending_.resize(kFastPForBlockSize);
  DoGetNextFastPForBlock(&pending_[0]);
  memcpy(vals, &pending_[0], n * sizeof(uint32_t));
  vals += n;
  cur_idx_ += n;

 ret:
  *n_param = cur_idx_ - start_idx;
  return Status::OK();
}

inline Status FastPForBlockDecoder::DoGetNextFastPForBlock(uint32_t* vals) {
  uint8_t b = *cur_metadata_pos_++;
  uint8_t cexcept = *cur_metadata_pos_++;

  cur_data_pos_ = DecodeInt32GroupFastPFor(cur_data_pos_, kFastPForBlockSize, vals, (uint32_t)b);

  if (cexcept > 0) {
    uint8_t max_bits = *cur_metadata_pos_++;
    if (max_bits - b == 1) {
      for (uint32_t k = 0; k < cexcept; ++k) {
        const uint8_t pos = *cur_metadata_pos_++;
        vals[pos] |= 1U << b;
      }
    } else {
      std::vector<uint32_t>::const_iterator & iter = exception_iters[max_bits - b];
      for (uint32_t k = 0; k < cexcept; ++k) {
        const uint8_t pos = *cur_metadata_pos_++;
        vals[pos] |= (*(iter++)) << b;
      }
    }
  }
  return Status::OK();
}

} // namespace cfile
} // namespace kudu
