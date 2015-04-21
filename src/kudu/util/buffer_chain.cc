// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/buffer_chain.h"

#include <algorithm>

#include "kudu/util/slice.h"

namespace kudu {

void BufferChain::AppendSlowPath(Slice s) {
  while (true) {
    int len_fit = std::min<int>(s.size(), tail_->rem());
    memcpy(&tail_->data[tail_->used], s.data(), len_fit);
    tail_->used += len_fit;
    s.remove_prefix(len_fit);
    if (s.empty()) {
      break;
    }
    AddChunk(s.size());
  }
}

void BufferChain::AddChunk(int min_size) {
  int size = std::max(min_size, 16 * 1024);
  uint8_t* buf = new uint8_t[sizeof(Chunk) + size];
  Chunk* c = new (buf) Chunk;
  c->used = 0;
  c->capacity = size;
  c->next = NULL;

  if (tail_ != NULL) {
    tail_->next = c;
  } else {
    head_ = c;
  }
  tail_ = c;
}

} // namespace kudu
