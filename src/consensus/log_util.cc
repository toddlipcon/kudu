// Copyright (c) 2013, Cloudera, inc.

#include "consensus/log_util.h"

#include <algorithm>
#include <boost/foreach.hpp>
#include <iostream>
#include <limits>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "gutil/strings/split.h"
#include "fs/fs_manager.h"
#include "util/coding.h"
#include "util/env_util.h"
#include "util/pb_util.h"

DEFINE_int32(log_segment_size_mb, 64,
             "The default segment size for log roll-overs, in MB");

DEFINE_bool(log_force_fsync_all, false,
            "Whether the Log/WAL should explicitly call fsync() after each write.");

DEFINE_bool(log_preallocate_segments, true,
            "Whether the WAL should preallocate the entire segment before writing to it");

DEFINE_bool(log_async_preallocate_segments, true,
            "Whether the WAL segments preallocation should happen asynchronously");

namespace kudu {
namespace log {

using consensus::OpId;
using env_util::ReadFully;
using std::vector;
using std::tr1::shared_ptr;
using strings::Substitute;
using strings::SubstituteAndAppend;

const char kTmpSuffix[] = ".tmp";

const char kLogSegmentMagicString[] = "kudulogf";

// Header is prefixed with the magic (8 bytes) and the header length (4 bytes).
const size_t kLogSegmentMagicAndHeaderLength = 12;

// Nulls the length of kLogSegmentMagicAndHeaderLength.
// This is used to check the case where we have a nonzero-length empty log file.
const char kLogSegmentNullHeader[] =
           { 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0 };

const size_t kEntryLengthSize = 4;

const int kLogMajorVersion = 1;
const int kLogMinorVersion = 0;

// Maximum log segment header size, in bytes (8 MB).
const uint32_t kLogSegmentMaxHeaderSize = 8 * 1024 * 1024;

LogOptions::LogOptions()
: segment_size_mb(FLAGS_log_segment_size_mb),
  force_fsync_all(FLAGS_log_force_fsync_all),
  preallocate_segments(FLAGS_log_preallocate_segments),
  async_preallocate_segments(FLAGS_log_async_preallocate_segments) {
}

Status ReadableLogSegment::Open(Env* env,
                                const string& path,
                                scoped_refptr<ReadableLogSegment>* segment) {
  VLOG(1) << "Parsing wal segment: " << path;
  uint64_t file_size;
  RETURN_NOT_OK(env->GetFileSize(path, &file_size));
  shared_ptr<RandomAccessFile> readable_file;
  RETURN_NOT_OK(env_util::OpenFileForRandom(env, path, &readable_file));

  segment->reset(new ReadableLogSegment(path, file_size, readable_file));
  RETURN_NOT_OK((*segment)->Init());
  return Status::OK();
}

ReadableLogSegment::ReadableLogSegment(const std::string &path,
                                       uint64_t file_size,
                                       const shared_ptr<RandomAccessFile>& readable_file)
  : path_(path),
    file_size_(file_size),
    readable_file_(readable_file),
    is_initialized_(false) {
}

void ReadableLogSegment::Init(const LogSegmentHeaderPB& header, uint64_t first_entry_offset) {
  DCHECK(!IsInitialized()) << "Can only call Init() once";
  DCHECK(header.IsInitialized()) << "Log segment header must be initialized";
  header_.CopyFrom(header);
  first_entry_offset_ = first_entry_offset;
  is_initialized_ = true;
}

Status ReadableLogSegment::Init() {
  DCHECK(!IsInitialized()) << "Can only call Init() once";

  // Check the size of the file.
  // If it is zero, return Status::Uninitialized().
  uint64_t file_size = 0;
  RETURN_NOT_OK(readable_file_->Size(&file_size));
  if (file_size == 0) {
    return Status::Uninitialized(Substitute("Log segment file $0 is zero-length", path()));
  }

  uint32_t header_size = 0;
  RETURN_NOT_OK(ReadMagicAndHeaderLength(&header_size));
  if (header_size == 0 || header_size > kLogSegmentMaxHeaderSize) {
    return Status::Corruption(Substitute("File is corrupted. "
        "Parsed header size: $0 is zero or bigger than max header size: $1",
        header_size, kLogSegmentMaxHeaderSize));
  }

  uint8_t header_space[header_size];
  Slice header_slice;
  LogSegmentHeaderPB header;

  // Read and parse the log segment header.
  RETURN_NOT_OK(ReadFully(readable_file_.get(), kLogSegmentMagicAndHeaderLength,
                          header_size, &header_slice, header_space));

  RETURN_NOT_OK(pb_util::ParseFromArray(&header,
                                        header_slice.data(),
                                        header_size));

  header_.CopyFrom(header);
  first_entry_offset_ = header_size + kLogSegmentMagicAndHeaderLength;
  is_initialized_ = true;

  return Status::OK();
}

Status ReadableLogSegment::ReadEntries(vector<LogEntryPB*>* entries) {
  vector<int64_t> recent_offsets(4, -1);
  int batches_read = 0;

  uint64_t offset = first_entry_offset();
  VLOG(1) << "Reading segment entries offset: " << offset << " file size: "
          << file_size();
  faststring tmp_buf;
  while (offset < file_size()) {
    const uint64_t this_batch_offset = offset;
    recent_offsets[batches_read++ % recent_offsets.size()] = offset;

    gscoped_ptr<LogEntryBatchPB> current_batch;

    // Read the entry length first, if we get 0 back that just means that
    // the log hasn't been ftruncated().
    uint32_t length;
    Status status = ReadEntryLength(&offset, &length);
    if (status.ok()) {
      if (length == 0) {
        // EOF
        return Status::OK();
      }
      status = ReadEntryBatch(&offset, length, &tmp_buf, &current_batch);
    }

    if (status.ok()) {
      if (VLOG_IS_ON(3)) {
        VLOG(3) << "Read Log entry batch: " << current_batch->DebugString();
      }
      for (size_t i = 0; i < current_batch->entry_size(); ++i) {
        entries->push_back(current_batch->mutable_entry(i));
      }
      current_batch->mutable_entry()->ExtractSubrange(0,
                                                      current_batch->entry_size(),
                                                      NULL);
    } else {
      string err = "Log file corrupted. ";
      SubstituteAndAppend(&err, "Failed trying to read batch #$0 at offset $1. ",
                          batches_read, this_batch_offset);
      err.append("Prior batch offsets:");
      std::sort(recent_offsets.begin(), recent_offsets.end());
      BOOST_FOREACH(int64_t offset, recent_offsets) {
        if (offset >= 0) {
          SubstituteAndAppend(&err, " $0", offset);
        }
      }

      RETURN_NOT_OK_PREPEND(status, err);
    }
  }
  return Status::OK();
}

Status ReadableLogSegment::ReadEntryLength(uint64_t *offset, uint32_t *len) {
  uint8_t scratch[kEntryLengthSize];
  Slice slice;
  RETURN_NOT_OK(ReadFully(readable_file().get(), *offset, kEntryLengthSize,
                          &slice, scratch));
  RETURN_NOT_OK(slice.check_size(kEntryLengthSize));
  *offset += kEntryLengthSize;
  *len = DecodeFixed32(slice.data());
  return Status::OK();
}

Status ReadableLogSegment::ReadEntryBatch(uint64_t *offset,
                                          uint32_t length,
                                          faststring *tmp_buf,
                                          gscoped_ptr<LogEntryBatchPB> *entry_batch) {

  if (length == 0 || length > file_size() - *offset) {
    return Status::Corruption(StringPrintf("Invalid entry length %d.", length));
  }

  tmp_buf->clear();
  tmp_buf->resize(length);
  Slice entry_batch_slice;
  RETURN_NOT_OK(readable_file()->Read(*offset,
                                      length,
                                      &entry_batch_slice,
                                      tmp_buf->data()));

  gscoped_ptr<LogEntryBatchPB> read_entry_batch(new LogEntryBatchPB());
  RETURN_NOT_OK(pb_util::ParseFromArray(read_entry_batch.get(),
                                        entry_batch_slice.data(),
                                        length));
  *offset += length;
  entry_batch->reset(read_entry_batch.release());
  return Status::OK();
}

Status ReadableLogSegment::ReadMagicAndHeaderLength(uint32_t *len) {
  uint8_t scratch[kLogSegmentMagicAndHeaderLength];
  Slice slice;
  RETURN_NOT_OK(ReadFully(readable_file_.get(), 0, kLogSegmentMagicAndHeaderLength,
                          &slice, scratch));
  RETURN_NOT_OK(ParseMagicAndLength(slice, len));
  return Status::OK();
}

Status ReadableLogSegment::ParseMagicAndLength(const Slice &data, uint32_t *parsed_len) {
  RETURN_NOT_OK_PREPEND(data.check_size(kLogSegmentMagicAndHeaderLength),
                        "Log segment file is too small to contain initial magic number");

  if (memcmp(kLogSegmentMagicString, data.data(), strlen(kLogSegmentMagicString)) != 0) {
    // As a special case, we check whether the file was allocated but no header
    // was written. We treat that case as an uninitialized file, much in the
    // same way we treat zero-length files.
    // Note: While the above comparison checks 8 bytes, this one checks the full 12
    // to ensure we have a full 12 bytes of NULL data.
    if (memcmp(kLogSegmentNullHeader, data.data(),
               strlen(kLogSegmentNullHeader)) == 0) {
      // 12 bytes of NULLs, good enough for us to consider this a file that
      // was never written to (but apparently preallocated).
      return Status::Uninitialized(
          Substitute("Log segment file $0 has 12 initial NULL bytes instead of "
                     "magic and header length: $1",
                     path(), data.ToDebugString()));
    }
    // If no magic and not uninitialized, the file is considered corrupt.
    return Status::Corruption(Substitute("Invalid log segment file $0: Bad magic. $1",
                                         path(), data.ToDebugString()));
  }

  *parsed_len = DecodeFixed32(data.data() + strlen(kLogSegmentMagicString));
  return Status::OK();
}


WritableLogSegment::WritableLogSegment(
    const string &path,
    const shared_ptr<WritableFile>& writable_file)
: path_(path),
  writable_file_(writable_file),
  is_header_written_(false) {
}

Status WritableLogSegment::WriteHeader(const LogSegmentHeaderPB& new_header) {
  DCHECK(!IsHeaderWritten()) << "Can only call WriteHeader() once";
  DCHECK(new_header.IsInitialized())
      << "Log segment header must be initialized" << new_header.InitializationErrorString();
  faststring buf;

  // First the magic.
  buf.append(kLogSegmentMagicString);
  // Then Length-prefixed header.
  PutFixed32(&buf, new_header.ByteSize());
  // Then Serialize the PB.
  if (!pb_util::AppendToString(new_header, &buf)) {
    return Status::Corruption("unable to encode header");
  }
  RETURN_NOT_OK(writable_file()->Append(Slice(buf)));

  header_.CopyFrom(new_header);
  first_entry_offset_ = buf.size();
  is_header_written_ = true;

  return Status::OK();
}

int OpIdCompare(const OpId& first, const OpId& second) {
  if (PREDICT_TRUE(first.term() == second.term())) {
    return first.index() < second.index() ? -1 : first.index() == second.index() ? 0 : 1;
  }
  return first.term() < second.term() ? -1 : 1;
}

bool OpIdEquals(const OpId& left, const OpId& right) {
  DCHECK(left.IsInitialized());
  DCHECK(right.IsInitialized());
  return left.term() == right.term() && left.index() == right.index();
}

bool OpIdLessThan(const OpId& left, const OpId& right) {
  DCHECK(left.IsInitialized());
  DCHECK(right.IsInitialized());
  if (left.term() < right.term()) return true;
  if (left.term() > right.term()) return false;
  return left.index() < right.index();
}

bool CopyIfOpIdLessThan(const consensus::OpId& to_compare, consensus::OpId* target) {
  if (to_compare.IsInitialized() &&
      (!target->IsInitialized() || OpIdLessThan(to_compare, *target))) {
    target->CopyFrom(to_compare);
    return true;
  }
  return false;
}

size_t OpIdHashFunctor::operator() (const OpId& id) const {
  return (id.term() + 31) ^ id.index();
}

bool OpIdEqualsFunctor::operator() (const OpId& left, const OpId& right) const {
  return OpIdEquals(left, right);
}

bool OpIdCompareFunctor::operator() (const OpId& left, const OpId& right) const {
  return OpIdLessThan(left, right);
}

bool OpIdBiggerThanFunctor::operator() (const OpId& left, const OpId& right) const {
  if (left.term() > right.term()) return true;
  if (left.term() < right.term()) return false;
  return left.index() > right.index();
}

OpId MinimumOpId() {
  OpId op_id;
  op_id.set_term(0);
  op_id.set_index(0);
  return op_id;
}

OpId MaximumOpId() {
  OpId op_id;
  op_id.set_term(std::numeric_limits<uint64_t>::max());
  op_id.set_index(std::numeric_limits<uint64_t>::max());
  return op_id;
}

// helper hash functor for delta store ids
struct DeltaIdHashFunction {
  size_t operator()(const pair<int64_t, int64_t >& id) const {
    return (id.first + 31) ^ id.second;
  }
};

// helper equals functor for delta store ids
struct DeltaIdEqualsTo {
  bool operator()(const pair<int64_t, int64_t >& left,
                  const pair<int64_t, int64_t >& right) const {
    return left.first == right.first && left.second == right.second;
  }
};

size_t FindStaleSegmentsPrefixSize(const ReadableLogSegmentMap& segment_map,
                                   const consensus::OpId& earliest_needed_opid,
                                   OpIdRange* initial_op_id_range) {
  DCHECK(initial_op_id_range);
  // We iterate in reverse order.
  // Keep the 1st log segment with initial OpId less than or equal to the
  // earliest needed OpId, and delete all the log segments preceding it
  // (preceding meaning in natural order).
  size_t num_stale_segments = 0;
  bool seen_earlier_opid = false;
  BOOST_REVERSE_FOREACH(const ReadableLogSegmentMap::value_type& entry, segment_map) {
    const OpId& initial_op_id_in_segment = entry.first;
    const scoped_refptr<ReadableLogSegment>& segment = entry.second;
    if (OpIdLessThan(initial_op_id_in_segment, earliest_needed_opid) ||
        OpIdEquals(initial_op_id_in_segment, earliest_needed_opid)) {
      if (!seen_earlier_opid) {
        // earliest_needed_opid may be in the middle of this segment, do not
        // delete it (but earlier ones can go).
        seen_earlier_opid = true;
        initial_op_id_range->second = initial_op_id_in_segment;
        initial_op_id_range->first = initial_op_id_in_segment;
      } else {
        // All the earlier logs can go.
        num_stale_segments++;
        initial_op_id_range->first = initial_op_id_in_segment;
      }
    } else {
      CHECK(!seen_earlier_opid)
          << Substitute("Greater OpId found in previous log segment, segments"
                        " out of order! current: %s in %s, earliest needed: %s",
                        segment->header().initial_id().ShortDebugString(),
                        segment->path(),
                        earliest_needed_opid.ShortDebugString());
    }
  }

  return num_stale_segments;
}

bool IsLogFileName(const string& fname) {
  if (HasPrefixString(fname, ".")) {
    // Hidden file or ./..
    VLOG(1) << "Ignoring hidden file: " << fname;
    return false;
  }

  if (HasSuffixString(fname, kTmpSuffix)) {
    LOG(WARNING) << "Ignoring tmp file: " << fname;
    return false;
  }

  vector<string> v = strings::Split(fname, "-");
  if (v.size() != 2 || v[0] != FsManager::kWalFileNamePrefix) {
    VLOG(1) << "Not a log file: " << fname;
    return false;
  }

  return true;
}

}  // namespace log

namespace consensus {

std::ostream& operator<<(std::ostream& os, const consensus::OpId& op_id) {
  os << op_id.ShortDebugString();
  return os;
}

} // namespace consensus
}  // namespace kudu
