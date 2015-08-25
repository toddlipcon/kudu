// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/common/iterator_stats.h"

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {

using std::string;
using strings::Substitute;

IteratorStats::IteratorStats()
    : data_blocks_read_from_disk(0),
      bytes_read_from_disk(0),
      rows_read_from_disk(0) {
}

string IteratorStats::ToString() const {
  return Substitute("data_blocks_read_from_disk=$0 "
                    "bytes_read_from_disk=$1 "
                    "rows_read_from_disk=$2",
                    data_blocks_read_from_disk,
                    bytes_read_from_disk,
                    rows_read_from_disk);
}

void IteratorStats::AddStats(const IteratorStats& other) {
  data_blocks_read_from_disk += other.data_blocks_read_from_disk;
  bytes_read_from_disk += other.bytes_read_from_disk;
  rows_read_from_disk += other.rows_read_from_disk;
}

void IteratorStats::SubtractStats(const IteratorStats& other) {
  data_blocks_read_from_disk -= other.data_blocks_read_from_disk;
  bytes_read_from_disk -= other.bytes_read_from_disk;
  rows_read_from_disk -= other.rows_read_from_disk;
}


} // namespace kudu
