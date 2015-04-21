// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_BUFFER_CHAIN_H
#define KUDU_UTIL_BUFFER_CHAIN_H

#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"

namespace kudu {

class BufferChain {
 public:
  BufferChain() :
    head_(NULL),
    tail_(NULL),
    size_(0) {
    AddChunk(0);
  }

  ~BufferChain() {
    Chunk* cur = head_;
    while (cur != NULL) {
      Chunk* next = cur->next;
      delete cur;
      cur = next;
    }
  }

  void append(const Slice& s) {
    size_ += s.size();
    if (PREDICT_TRUE(tail_->rem() >= s.size())) {
      if (s.size() <= 4) {
        memcpy_loop(&tail_->data[tail_->used], s.data(), s.size());
      } else {
        memcpy(&tail_->data[tail_->used], s.data(), s.size());
      }
      tail_->used += s.size();
      return;
    }
    AppendSlowPath(s);
  }

  int size() const { return size_; }

  void GetSlices(std::vector<Slice>* slices) const {
    slices->clear();
    for (Chunk* cur = head_;
         cur != NULL;
         cur = cur->next) {
      slices->push_back(Slice(cur->data, cur->used));
    }
  }

 private:
  void memcpy_loop(uint8_t* __restrict__ dst,
                   const uint8_t* __restrict__ src,
                   int len) {
    for (int i = 0; i < len; i++) {
      *dst++ = *src++;
    }
  }

  struct Chunk {
    Chunk* next;
    int used;
    int capacity;
    uint8_t data[0];

    int rem() const {
      return capacity - used;
    }
  };

  void AppendSlowPath(Slice s);
  void AddChunk(int min_size);

  Chunk* head_;
  Chunk* tail_;
  int size_;

  DISALLOW_COPY_AND_ASSIGN(BufferChain);
};

} // namespace kudu
#endif /* KUDU_UTIL_BUFFER_CHAIN_H */
