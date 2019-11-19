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

#include <cstddef>
#include <functional>
#include <memory>

#include <boost/optional/optional.hpp>

#include "kudu/common/schema.h"

namespace kudu{

class faststring;
class MemTracker;

namespace fs {
struct IOContext;
}  // namespace fs


namespace cfile{

typedef std::function<void(const void*, faststring*)> ValidxKeyEncoder;

struct WriterOptions {
  // Approximate size of index blocks.
  //
  // Default: 32KB.
  size_t index_block_size;

  // Number of keys between restart points for delta encoding of keys.
  // This parameter can be changed dynamically.  Most clients should
  // leave this parameter alone.
  //
  // This is currently only used by StringPrefixBlockBuilder
  //
  // Default: 16
  int block_restart_interval;

  // Whether the file needs a positional index.
  bool write_posidx;

  // Whether the file needs a value index
  bool write_validx;

  // Whether to optimize index keys by storing shortest separating prefixes
  // instead of entire keys.
  bool optimize_index_keys;

  // Column storage attributes.
  //
  // Default: all default values as specified in the constructor in
  // schema.h
  ColumnStorageAttributes storage_attributes;

  // An optional value index key encoder. If not set, the default encoder
  // encodes the entire value.
  boost::optional<ValidxKeyEncoder> validx_key_encoder;

  WriterOptions();
};

struct ReaderOptions {
  ReaderOptions();

  // The IO context of this reader.
  //
  // Default: nullptr
  const fs::IOContext* io_context = nullptr;

  // The MemTracker that should account for this reader's memory consumption.
  //
  // Default: the root tracker.
  std::shared_ptr<MemTracker> parent_mem_tracker;
};

}
}
