// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CFILE_FASTPFOR_BLOCK_H
#define KUDU_CFILE_FASTPFOR_BLOCK_H

#include <stdint.h>
#include <vector>
#include <gtest/gtest_prod.h>

#include "kudu/cfile/block_encodings.h"

namespace kudu {
namespace cfile {

struct WriterOptions;
typedef uint32_t IntType;

using std::vector;

// The encoding layout:
//
//  ------------------------
// | Block Header           |
//  ------------------------
// | Data Area              |
//  ------------------------
// | Metadata Area          |
//  ------------------------
// | Exception Table        |
//  -----------------------
//
class FastPForBlockBuilder : public BlockBuilder {
 public:
  explicit FastPForBlockBuilder(const WriterOptions *options);

  int Add(const uint8_t *vals, size_t count) OVERRIDE;

  Slice Finish(rowid_t ordinal_pos) OVERRIDE;

  void Reset() OVERRIDE;

  uint64_t EstimateEncodedSize() const OVERRIDE;

  size_t Count() const OVERRIDE;

  Status GetFirstKey(void *key) const OVERRIDE;

 private:
  friend class TestEncoding;
  FRIEND_TEST(TestEncoding, TestIntBlockEncoder);

  vector<IntType> inputs_;
  std::vector<std::vector<uint32_t> > exception_info_;

  faststring buffer_;
  faststring metadata_;

  uint64_t size_estimate_;

  const WriterOptions *options_;

  enum {
    kHeaderSizeBytes = 32,
    kFastPForBlockSize = 128,
    kPaddingSizeBytes = kFastPForBlockSize * sizeof(uint32_t) + 3,
  };
};

class FastPForBlockDecoder : public BlockDecoder {
 public:
  explicit FastPForBlockDecoder(const Slice &slice);

  Status ParseHeader() OVERRIDE;
  void SeekToStart() {
    SeekToPositionInBlock(0);
  }

  void SeekToPositionInBlock(uint pos) OVERRIDE;

  Status SeekAtOrAfterValue(const void *value, bool *exact_match) OVERRIDE;

  Status CopyNextValues(size_t *n, ColumnDataView *dst) OVERRIDE;

  size_t GetCurrentIndex() const OVERRIDE {
    DCHECK(parsed_) << "must parse header first";
    return cur_idx_;
  }

  virtual rowid_t GetFirstRowId() const OVERRIDE {
    return ordinal_pos_base_;
  }

  size_t Count() const OVERRIDE {
    return num_elems_;
  }

  bool HasNext() const OVERRIDE {
    return (num_elems_ - cur_idx_) > 0;
  }

 private:
  friend class TestEncoding;

  Status DoGetNextFastPForBlock(uint32_t* vals);
  Status DoGetNextValues(size_t *n, uint32_t* vals);

  Slice data_;
  bool parsed_;

  uint32_t num_elems_;
  rowid_t ordinal_pos_base_;

  const uint32_t *data_start_;
  const uint8_t *metadata_start_;

  const uint32_t *cur_data_pos_;
  const uint8_t *cur_metadata_pos_;
  size_t cur_idx_;

  std::vector<uint32_t> pending_;
  std::vector<std::vector<uint32_t> > exception_info_;
  std::vector<uint32_t>::const_iterator exception_iters[32 + 1];

  enum {
    kHeaderSizeBytes = 32,
    kFastPForBlockSize = 128,
  };

};

} // namespace cfile
} // namespace kudu
#endif
