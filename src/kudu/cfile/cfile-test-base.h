// Copyright (c) 2013, Cloudera, inc

#ifndef KUDU_CFILE_TEST_BASE_H
#define KUDU_CFILE_TEST_BASE_H

#include <glog/logging.h>
#include <algorithm>
#include <string>

#include "kudu/cfile/cfile-test-base.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/cfile.pb.h"
#include "kudu/common/columnblock.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/status.h"

DEFINE_int32(cfile_test_block_size, 1024,
             "Block size to use for testing cfiles. "
             "Default is low to stress code, but can be set higher for "
             "performance testing");

using kudu::fs::ReadableBlock;
using kudu::fs::WritableBlock;

namespace kudu {
namespace cfile {

// Abstract test data generator.
// You must implement BuildTestValue() to return your test value.
//    Usage example:
//        StringDataGenerator<true> datagen;
//        datagen.Build(10);
//        for (int i = 0; i < datagen.block_entries(); ++i) {
//          bool is_null = BitmpTest(datagen.null_bitmap(), i);
//          Slice& v = datagen[i];
//        }
template<DataType DATA_TYPE, bool HAS_NULLS>
class DataGenerator {
 public:
  static bool has_nulls() {
    return HAS_NULLS;
  }

  static const DataType kDataType;

  typedef typename DataTypeTraits<DATA_TYPE>::cpp_type cpp_type;

  DataGenerator() :
    values_(NULL),
    null_bitmap_(NULL),
    block_entries_(0),
    total_entries_(0)
  {}

  void Reset() {
    block_entries_ = 0;
    total_entries_ = 0;
  }

  void Build(size_t num_entries) {
    Build(total_entries_, num_entries);
    total_entries_ += num_entries;
  }

  // Build "num_entries" using (offset + i) as value
  // You can get the data values and the null bitmap using values() and null_bitmap()
  // both are valid until the class is destructed or until Build() is called again.
  void Build(size_t offset, size_t num_entries) {
    Resize(num_entries);

    for (size_t i = 0; i < num_entries; ++i) {
      if (HAS_NULLS) {
        BitmapChange(null_bitmap_.get(), i, !TestValueShouldBeNull(offset + i));
      }
      values_[i] = BuildTestValue(i, offset + i);
    }
  }

  virtual cpp_type BuildTestValue(size_t block_index, size_t value) = 0;

  bool TestValueShouldBeNull(size_t n) {
    if (!HAS_NULLS) {
      return false;
    }

    // The NULL pattern alternates every 32 rows, cycling between:
    //   32 NULL
    //   32 alternating NULL/NOTNULL
    //   32 NOT-NULL
    //   32 alternating NULL/NOTNULL
    // This is to ensure that we stress the run-length coding for
    // NULL value.
    switch ((n >> 6) & 3) {
      case 0:
        return true;
      case 1:
      case 3:
        return n & 1;
      case 2:
        return false;
      default:
        LOG(FATAL);
    }
  }

  virtual void Resize(size_t num_entries) {
    if (block_entries_ >= num_entries) {
      block_entries_ = num_entries;
      return;
    }

    values_.reset(new cpp_type[num_entries]);
    null_bitmap_.reset(new uint8_t[BitmapSize(num_entries)]);
    block_entries_ = num_entries;
  }

  size_t block_entries() const { return block_entries_; }
  size_t total_entries() const { return total_entries_; }

  const cpp_type *values() const { return values_.get(); }
  const uint8_t *null_bitmap() const { return null_bitmap_.get(); }

  const cpp_type& operator[](size_t index) const {
    return values_[index];
  }

  virtual ~DataGenerator() {}

 private:
  gscoped_array<cpp_type> values_;
  gscoped_array<uint8_t> null_bitmap_;
  size_t block_entries_;
  size_t total_entries_;
};

template<DataType DATA_TYPE, bool HAS_NULLS>
const DataType DataGenerator<DATA_TYPE, HAS_NULLS>::kDataType = DATA_TYPE;


// Data generator for any integral type.
template<DataType DATA_TYPE, bool HAS_NULLS>
class IntDataGenerator : public DataGenerator<DATA_TYPE, HAS_NULLS> {
 public:
  IntDataGenerator() {}
  typedef typename DataTypeTraits<DATA_TYPE>::cpp_type cpp_type;

  cpp_type BuildTestValue(size_t block_index, size_t value) OVERRIDE {
    if (MathLimits<cpp_type>::kIsSigned) {
      return (value * 10) * (value % 2 == 0 ? -1 : 1);
    } else {
      return value * 10;
    }
  }
};

// Handy aliases
template<bool HAS_NULLS>
class UInt32DataGenerator : public IntDataGenerator<UINT32, HAS_NULLS> {};

template<bool HAS_NULLS>
class Int32DataGenerator : public IntDataGenerator<UINT32, HAS_NULLS> {};

template<bool HAS_NULLS>
class StringDataGenerator : public DataGenerator<STRING, HAS_NULLS> {
 public:
  explicit StringDataGenerator(const char* format)
  : format_(format) {
  }

  Slice BuildTestValue(size_t block_index, size_t value) OVERRIDE {
    char *buf = data_buffer_[block_index].data;
    int len = snprintf(buf, kItemBufferSize - 1, format_, value);
    DCHECK_LT(len, kItemBufferSize);
    return Slice(buf, len);
  }

  void Resize(size_t num_entries) OVERRIDE {
    if (num_entries > this->block_entries()) {
      data_buffer_.reset(new Buffer[num_entries]);
    }
    DataGenerator<STRING, HAS_NULLS>::Resize(num_entries);
  }

 private:
  static const int kItemBufferSize = 16;

  struct Buffer {
    char data[kItemBufferSize];
  };

  gscoped_array<Buffer> data_buffer_;
  const char* format_;
};


class CFileTestBase : public KuduTest {
 public:
  void SetUp() OVERRIDE {
    KuduTest::SetUp();

    fs_manager_.reset(new FsManager(env_.get(), test_dir_));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
  }

 protected:
  enum Flags {
    NO_FLAGS = 0,
    WRITE_VALIDX = 1,
    SMALL_BLOCKSIZE = 1 << 1
  };

  template<class DataGeneratorType>
  void WriteTestFile(DataGeneratorType* data_generator,
                     EncodingType encoding,
                     CompressionType compression,
                     int num_entries,
                     uint32_t flags,
                     BlockId* block_id) {
    gscoped_ptr<WritableBlock> sink;
    ASSERT_STATUS_OK(fs_manager_->CreateNewBlock(&sink));
    *block_id = sink->id();
    WriterOptions opts;
    opts.write_posidx = true;

    if (flags & WRITE_VALIDX) {
      opts.write_validx = true;
    }
    if (flags & SMALL_BLOCKSIZE) {
      // Use a smaller block size to exercise multi-level indexing.
      opts.block_size = 1024;
    }

    opts.storage_attributes = ColumnStorageAttributes(encoding, compression);
    CFileWriter w(opts, DataGeneratorType::kDataType,
                  DataGeneratorType::has_nulls(), sink.Pass());

    ASSERT_STATUS_OK(w.Start());

    // Append given number of values to the test tree
    const size_t kBufferSize = 8192;
    size_t i = 0;
    while (i < num_entries) {
      int towrite = std::min(num_entries - i, kBufferSize);

      data_generator->Build(towrite);
      DCHECK_EQ(towrite, data_generator->block_entries());

      if (DataGeneratorType::has_nulls()) {
        ASSERT_STATUS_OK_FAST(w.AppendNullableEntries(data_generator->null_bitmap(),
                                                      data_generator->values(),
                                                      towrite));
      } else {
        ASSERT_STATUS_OK_FAST(w.AppendEntries(data_generator->values(), towrite));
      }
      i += towrite;
    }

    ASSERT_STATUS_OK(w.Finish());
  }

  gscoped_ptr<FsManager> fs_manager_;

};

// Fast unrolled summing of a vector.
// GCC's auto-vectorization doesn't work here, because there isn't
// enough guarantees on alignment and it can't seem to decode the
// constant stride.
template<class Indexable>
uint64_t FastSum(const Indexable &data, size_t n) {
  uint64_t sums[4] = {0, 0, 0, 0};
  int rem = n;
  int i = 0;
  while (rem >= 4) {
    sums[0] += data[i];
    sums[1] += data[i+1];
    sums[2] += data[i+2];
    sums[3] += data[i+3];
    i += 4;
    rem -= 4;
  }
  while (rem > 0) {
    sums[3] += data[i++];
    rem--;
  }
  return sums[0] + sums[1] + sums[2] + sums[3];
}

template<DataType Type>
static void TimeReadFileForDataType(gscoped_ptr<CFileIterator> &iter, int &count) {
  ScopedColumnBlock<Type> cb(8192);

  uint64_t sum = 0;
  while (iter->HasNext()) {
    size_t n = cb.nrows();
    ASSERT_STATUS_OK_FAST(iter->CopyNextValues(&n, &cb));
    sum += FastSum(cb, n);
    count += n;
    cb.arena()->Reset();
  }
  LOG(INFO)<< "Sum: " << sum;
  LOG(INFO)<< "Count: " << count;
}

static void TimeReadFile(FsManager* fs_manager, const BlockId& block_id, size_t *count_ret) {
  Status s;

  gscoped_ptr<fs::ReadableBlock> source;
  ASSERT_STATUS_OK(fs_manager->OpenBlock(block_id, &source));
  gscoped_ptr<CFileReader> reader;
  ASSERT_STATUS_OK(CFileReader::Open(source.Pass(), ReaderOptions(), &reader));

  gscoped_ptr<CFileIterator> iter;
  ASSERT_STATUS_OK(reader->NewIterator(&iter));
  ASSERT_STATUS_OK(iter->SeekToOrdinal(0));

  Arena arena(8192, 8*1024*1024);
  int count = 0;
  switch (reader->data_type()) {
    case UINT32:
    {
      TimeReadFileForDataType<UINT32>(iter, count);
      break;
    }
    case INT32:
    {
      TimeReadFileForDataType<INT32>(iter, count);
      break;
    }
    case STRING:
    {
      ScopedColumnBlock<STRING> cb(100);
      uint64_t sum_lens = 0;
      while (iter->HasNext()) {
        size_t n = cb.nrows();
        ASSERT_STATUS_OK_FAST(iter->CopyNextValues(&n, &cb));
        for (int i = 0; i < n; i++) {
          sum_lens += cb[i].size();
        }
        count += n;
        cb.arena()->Reset();
      }
      LOG(INFO) << "Sum of value lengths: " << sum_lens;
      LOG(INFO) << "Count: " << count;
      break;
    }
    default:
      FAIL() << "Unknown type: " << reader->data_type();
  }
  *count_ret = count;
}

} // namespace cfile
} // namespace kudu

#endif
