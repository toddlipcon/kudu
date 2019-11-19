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

#include "kudu/common/key_util.h"
#include "kudu/common/key_util-inl.h"

#include <cmath>
#include <cstring>
#include <iterator>
#include <limits>
#include <ostream>
#include <string>
#include <tuple>
#include <type_traits>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"

using std::nextafter;
using std::numeric_limits;
using std::string;
using std::tuple;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace key_util {

namespace {

bool IncrementBoolCell(void* cell_ptr) {
  bool orig;
  memcpy(&orig, cell_ptr, sizeof(bool));
  if (!orig) {
    bool inc = true;
    memcpy(cell_ptr, &inc, sizeof(bool));
    return true;
  } else {
    return false;
  }
}

bool DecrementBoolCell(void* cell_ptr) {
  bool orig;
  memcpy(&orig, cell_ptr, sizeof(bool));
  if (orig) {
    bool dec = false;
    memcpy(cell_ptr, &dec, sizeof(bool));
    return true;
  }
  return false;
}

template<DataType type>
ATTRIBUTE_NO_SANITIZE_INTEGER
bool IncrementIntCell(void* cell_ptr) {
  typedef DataTypeTraits<type> traits;
  typedef typename traits::cpp_type cpp_type;

  cpp_type orig;
  memcpy(&orig, cell_ptr, sizeof(cpp_type));

  cpp_type inc;
  if (std::is_unsigned<cpp_type>::value) {
    inc = orig + 1;
  } else {
    // Signed overflow is undefined in C. So, we'll use a branch here
    // instead of counting on undefined behavior.
    if (orig == MathLimits<cpp_type>::kMax) {
      inc = MathLimits<cpp_type>::kMin;
    } else {
      inc = orig + 1;
    }
  }
  memcpy(cell_ptr, &inc, sizeof(cpp_type));
  return inc > orig;
}

template<DataType type>
ATTRIBUTE_NO_SANITIZE_INTEGER
bool DecrementIntCell(void* cell_ptr) {
  typedef DataTypeTraits<type> traits;
  typedef typename traits::cpp_type cpp_type;

  cpp_type orig;
  memcpy(&orig, cell_ptr, sizeof(cpp_type));

  cpp_type dec;
  if (std::is_unsigned<cpp_type>::value) {
    dec = orig - 1;
  } else {
    // Signed overflow is undefined in C. So, we'll use a branch here
    // instead of counting on undefined behavior.
    if (orig == MathLimits<cpp_type>::kMin) {
      dec = MathLimits<cpp_type>::kMax;
    } else {
      dec = orig - 1;
    }
  }
  if (dec < orig) {
    memcpy(cell_ptr, &dec, sizeof(cpp_type));
    return true;
  }
  return false;
}

template<DataType type>
bool IncrementFloatingPointCell(void* cell_ptr) {
  typedef DataTypeTraits<type> traits;
  typedef typename traits::cpp_type cpp_type;

  cpp_type orig;
  memcpy(&orig, cell_ptr, sizeof(cpp_type));
  cpp_type inc = nextafter(orig, numeric_limits<cpp_type>::infinity());
  memcpy(cell_ptr, &inc, sizeof(cpp_type));
  return inc != orig;
}

template<DataType type>
bool DecrementFloatingPointCell(void* cell_ptr) {
  typedef DataTypeTraits<type> traits;
  typedef typename traits::cpp_type cpp_type;

  cpp_type orig;
  memcpy(&orig, cell_ptr, sizeof(cpp_type));
  cpp_type dec = nextafter(orig, -numeric_limits<cpp_type>::infinity());
  if (dec != orig) {
    memcpy(cell_ptr, &dec, sizeof(cpp_type));
  }
  return false;
}

bool IncrementStringCell(void* cell_ptr, Arena* arena) {
  Slice orig;
  memcpy(&orig, cell_ptr, sizeof(orig));
  uint8_t* new_buf = CHECK_NOTNULL(
      static_cast<uint8_t*>(arena->AllocateBytes(orig.size() + 1)));
  memcpy(new_buf, orig.data(), orig.size());
  new_buf[orig.size()] = '\0';

  Slice inc(new_buf, orig.size() + 1);
  memcpy(cell_ptr, &inc, sizeof(inc));
  return true;
}

bool DecrementStringCell(void* cell_ptr) {
  Slice orig;
  memcpy(&orig, cell_ptr, sizeof(orig));
  if (orig.size() == 0 || orig[orig.size() - 1] != '\0') {
    return false;
  }
  orig.truncate(orig.size() - 1);
  memcpy(cell_ptr, &orig, sizeof(orig));
  return true;
}


} // anonymous namespace

bool IncrementPrimaryKey(ContiguousRow* row, Arena* arena) {
  return IncrementPrimaryKey(row, row->schema()->num_key_columns(), arena);
}

bool IncrementPrimaryKey(ContiguousRow* row, int32_t num_columns, Arena* arena) {
  return IncrementKey(boost::make_counting_iterator(0),
                      boost::make_counting_iterator(num_columns),
                      row,
                      arena);
}

bool IncrementCell(const ColumnSchema& col, void* cell_ptr, Arena* arena) {
  DataType type = col.type_info()->physical_type();
  switch (type) {
    case BOOL:
      return IncrementBoolCell(cell_ptr);
#define HANDLE_TYPE(t) case t: return IncrementIntCell<t>(cell_ptr);
    HANDLE_TYPE(UINT8);
    HANDLE_TYPE(UINT16);
    HANDLE_TYPE(UINT32);
    HANDLE_TYPE(UINT64);
    HANDLE_TYPE(INT8);
    HANDLE_TYPE(INT16);
    HANDLE_TYPE(INT32);
    HANDLE_TYPE(UNIXTIME_MICROS);
    HANDLE_TYPE(INT64);
    HANDLE_TYPE(INT128);
    case FLOAT:
      return IncrementFloatingPointCell<FLOAT>(cell_ptr);
    case DOUBLE:
      return IncrementFloatingPointCell<DOUBLE>(cell_ptr);
    case STRING:
    case BINARY:
      return IncrementStringCell(cell_ptr, arena);
    case UNKNOWN_DATA:
    default: LOG(FATAL) << "Unknown data type: " << type;
  }
  return false; // unreachable
#undef HANDLE_TYPE
}

bool TryDecrementCell(const ColumnSchema &col, void *cell_ptr) {
  DataType type = col.type_info()->physical_type();
  switch (type) {
    case BOOL:
      return DecrementBoolCell(cell_ptr);
#define HANDLE_TYPE(t) case t: return DecrementIntCell<t>(cell_ptr);
    HANDLE_TYPE(UINT8);
    HANDLE_TYPE(UINT16);
    HANDLE_TYPE(UINT32);
    HANDLE_TYPE(UINT64);
    HANDLE_TYPE(INT8);
    HANDLE_TYPE(INT16);
    HANDLE_TYPE(INT32);
    HANDLE_TYPE(UNIXTIME_MICROS);
    HANDLE_TYPE(INT64);
    HANDLE_TYPE(INT128);
    case FLOAT:
      return DecrementFloatingPointCell<FLOAT>(cell_ptr);
    case DOUBLE:
      return DecrementFloatingPointCell<DOUBLE>(cell_ptr);
    case STRING:
    case BINARY:
      return DecrementStringCell(cell_ptr);
    case UNKNOWN_DATA:
    default: LOG(FATAL) << "Unknown data type: " << type;
  }
  return false; // unreachable
#undef HANDLE_TYPE
}


void EncodeKey(const vector<int32_t>& col_idxs, const ContiguousRow& row, string* buffer) {
  for (int i = 0; i < col_idxs.size(); i++) {
    int32_t col_idx = col_idxs[i];
    const auto& encoder = GetKeyEncoder<string>(row.schema()->column(col_idx).type_info());
    encoder.Encode(row.cell_ptr(col_idx), i + 1 == col_idxs.size(), buffer);
  }
}

} // namespace key_util
} // namespace kudu
