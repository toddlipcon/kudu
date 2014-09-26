// Copyright (c) 2012, Cloudera, inc

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <stdlib.h>

#include <boost/assign/list_of.hpp>
#include "kudu/cfile/cfile-test-base.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/index_block.h"
#include "kudu/cfile/index_btree.h"
#include "kudu/common/columnblock.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/stopwatch.h"

namespace kudu {
namespace cfile {

using fs::ReadableBlock;
using fs::WritableBlock;

// Abstract test data generator.
// You must implement BuildTestValue() to return your test value.
//    Usage example:
//        StringDataGenerator datagen;
//        datagen.Build(10);
//        for (int i = 0; i < datagen.block_entries(); ++i) {
//          bool is_null = BitmpTest(datagen.null_bitmap(), i);
//          Slice& v = datagen[i];
//        }
template <class T>
class DataGenerator {
 public:
  DataGenerator() :
    values_(NULL),
    null_bitmap_(NULL),
    block_entries_(0),
    total_entries_(0)
  {}

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
      BitmapChange(null_bitmap_.get(), i, !TestValueShouldBeNull(offset + i));
      values_[i] = BuildTestValue(i, offset + i);
    }
  }

  virtual T BuildTestValue(size_t block_index, size_t value) = 0;

  bool TestValueShouldBeNull(size_t n) {
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

    values_.reset(new T[num_entries]);
    null_bitmap_.reset(new uint8_t[BitmapSize(num_entries)]);
    block_entries_ = num_entries;
  }

  size_t block_entries() const { return block_entries_; }
  size_t total_entries() const { return total_entries_; }

  const T *values() const { return values_.get(); }
  const uint8_t *null_bitmap() const { return null_bitmap_.get(); }

  const T& operator[](size_t index) const {
    return values_[index];
  }

  virtual ~DataGenerator() {}

 private:
  gscoped_array<T> values_;
  gscoped_array<uint8_t> null_bitmap_;
  size_t block_entries_;
  size_t total_entries_;
};

class UInt32DataGenerator : public DataGenerator<uint32_t> {
 public:
  uint32_t BuildTestValue(size_t block_index, size_t value) OVERRIDE {
    return value * 10;
  }
};

class Int32DataGenerator : public DataGenerator<int32_t> {
 public:
  int32_t BuildTestValue(size_t block_index, size_t value) OVERRIDE {
    return (value * 10) *(value % 2 == 0 ? -1 : 1);
  }
};

class StringDataGenerator : public DataGenerator<Slice> {
 public:
  Slice BuildTestValue(size_t block_index, size_t value) OVERRIDE {
    char *buf = data_buffer_[block_index].data;
    int len = snprintf(buf, kItemBufferSize - 1, "ITEM-%zu", value);
    DCHECK_LT(len, kItemBufferSize);
    return Slice(buf, len);
  }

  void Resize(size_t num_entries) OVERRIDE {
    if (num_entries > block_entries()) {
      data_buffer_.reset(new Buffer[num_entries]);
    }
    DataGenerator<Slice>::Resize(num_entries);
  }

 private:
  static const int kItemBufferSize = 16;

  struct Buffer {
    char data[kItemBufferSize];
  };

  gscoped_array<Buffer> data_buffer_;
};

class TestCFile : public CFileTestBase {
 protected:
  template <class DataGeneratorType, DataType data_type>
  void WriteTestFileWithNulls(EncodingType encoding,
                              CompressionType compression,
                              int num_entries,
                              BlockId* block_id) {
    gscoped_ptr<WritableBlock> sink;
    ASSERT_STATUS_OK(fs_manager_->CreateNewBlock(&sink));
    *block_id = sink->id();
    WriterOptions opts;
    opts.write_posidx = true;
    // Use a smaller block size to exercise multi-level indexing.
    opts.block_size = 128;
    opts.storage_attributes = ColumnStorageAttributes(encoding, compression);
    CFileWriter w(opts, data_type, true, sink.Pass());

    ASSERT_STATUS_OK(w.Start());

    DataGeneratorType data_generator;

    // Append given number of values to the test tree
    const size_t kBufferSize = 8192;
    size_t i = 0;
    while (i < num_entries) {
      int towrite = std::min(num_entries - i, kBufferSize);

      data_generator.Build(towrite);
      DCHECK_EQ(towrite, data_generator.block_entries());

      ASSERT_STATUS_OK_FAST(w.AppendNullableEntries(data_generator.null_bitmap(),
                                                    data_generator.values(),
                                                    towrite));

      i += towrite;
    }

    ASSERT_STATUS_OK(w.Finish());
  }

  template <class DataGeneratorType, DataType data_type>
  void WriteTestFileFixedSizeTypes(EncodingType encoding,
                                   CompressionType compression,
                                   int num_entries,
                                   BlockId* block_id) {
    gscoped_ptr<WritableBlock> sink;
    ASSERT_STATUS_OK(fs_manager_->CreateNewBlock(&sink));
    *block_id = sink->id();
    WriterOptions opts;
    opts.write_posidx = true;
    opts.block_size = 128;
    opts.storage_attributes = ColumnStorageAttributes(encoding, compression);
    CFileWriter w(opts, data_type, false, sink.Pass());

    ASSERT_STATUS_OK(w.Start());

    DataGeneratorType data_generator;

    // Append given number of values to the test tree
    const size_t kBufferSize = 8192;
    size_t i = 0;
    while (i < num_entries) {
      int towrite = std::min(num_entries - i, kBufferSize);

      data_generator.Build(towrite);
      DCHECK_EQ(towrite, data_generator.block_entries());

      ASSERT_STATUS_OK_FAST(w.AppendEntries(data_generator.values(), towrite));
      i += towrite;
    }

    ASSERT_STATUS_OK(w.Finish());
  }

  template <class DataGeneratorType, DataType data_type>
  void TestReadWriteFixedSizeTypes(EncodingType encoding) {
    BlockId block_id;
    WriteTestFileFixedSizeTypes<DataGeneratorType, data_type>(encoding,
                                                              NO_COMPRESSION,
                                                              10000,
                                                              &block_id);

    gscoped_ptr<ReadableBlock> block;
    ASSERT_STATUS_OK(fs_manager_->OpenBlock(block_id, &block));
    gscoped_ptr<CFileReader> reader;
    ASSERT_STATUS_OK(CFileReader::Open(block.Pass(), ReaderOptions(), &reader));

    BlockPointer ptr;

    gscoped_ptr<CFileIterator> iter;
    ASSERT_STATUS_OK(reader->NewIterator(&iter));

    ASSERT_STATUS_OK(iter->SeekToOrdinal(5000));
    ASSERT_EQ(5000u, iter->GetCurrentOrdinal());

    // Seek to last key exactly, should succeed
    ASSERT_STATUS_OK(iter->SeekToOrdinal(9999));
    ASSERT_EQ(9999u, iter->GetCurrentOrdinal());

    // Seek to after last key. Should result in not found.
    ASSERT_TRUE(iter->SeekToOrdinal(10000).IsNotFound());

    // Seek to start of file
    ASSERT_STATUS_OK(iter->SeekToOrdinal(0));
    ASSERT_EQ(0u, iter->GetCurrentOrdinal());

    // Fetch all data.
    ScopedColumnBlock<data_type> out(10000);
    size_t n = 10000;
    ASSERT_STATUS_OK(iter->CopyNextValues(&n, &out));
    ASSERT_EQ(10000, n);

    DataGeneratorType data_generator_pre;

    for (int i = 0; i < 10000; i++) {
      if (out[i] != data_generator_pre.BuildTestValue(0,i)) {
        FAIL() << "mismatch at index " << i
               << " expected: " << data_generator_pre.BuildTestValue(0,i)
               << " got: " << out[i];
      }
      out[i] = 0;
    }

    // Fetch all data using small batches of only a few rows.
    // This should catch edge conditions like a batch lining up exactly
    // with the end of a block.
    unsigned int seed = time(NULL);
    LOG(INFO) << "Using random seed: " << seed;
    srand(seed);
    ASSERT_STATUS_OK(iter->SeekToOrdinal(0));
    size_t fetched = 0;
    while (fetched < 10000) {
      ColumnBlock advancing_block(out.type_info(), NULL,
                                  out.data() + (fetched * out.stride()),
                                  out.nrows() - fetched, out.arena());
      ASSERT_TRUE(iter->HasNext());
      size_t batch_size = random() % 5 + 1;
      size_t n = batch_size;
      ASSERT_STATUS_OK(iter->CopyNextValues(&n, &advancing_block));
      ASSERT_LE(n, batch_size);
      fetched += n;
    }
    ASSERT_FALSE(iter->HasNext());

    DataGeneratorType data_generator_post;

    // Re-verify
    for (int i = 0; i < 10000; i++) {
      if (out[i] != data_generator_post.BuildTestValue(0,i)) {
        FAIL() << "mismatch at index " << i
               << " expected: " << data_generator_post.BuildTestValue(0,i)
               << " got: " << out[i];
      }
      out[i] = 0;
    }

    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(10000, n);
  }

  template <class DataGeneratorType, DataType data_type>
  void TimeSeekAndReadFileWithNulls(const BlockId& block_id, size_t num_entries) {
    gscoped_ptr<ReadableBlock> block;
    ASSERT_STATUS_OK(fs_manager_->OpenBlock(block_id, &block));
    gscoped_ptr<CFileReader> reader;
    ASSERT_STATUS_OK(CFileReader::Open(block.Pass(), ReaderOptions(), &reader));
    ASSERT_EQ(data_type, reader->data_type());

    gscoped_ptr<CFileIterator> iter;
    ASSERT_STATUS_OK(reader->NewIterator(&iter) );

    Arena arena(8192, 8*1024*1024);
    ScopedColumnBlock<data_type> cb(10);

    DataGeneratorType data_generator;
    const int kNumLoops = AllowSlowTests() ? num_entries : 10;
    for (int loop = 0; loop < kNumLoops; loop++) {
      // Seek to a random point in the file,
      // or just try each entry as starting point if you're running SlowTests
      int target = AllowSlowTests() ? loop : (random() % (num_entries - 1));
      SCOPED_TRACE(target);
      ASSERT_STATUS_OK(iter->SeekToOrdinal(target));
      ASSERT_TRUE(iter->HasNext());

      // Read and verify several ColumnBlocks from this point in the file.
      int read_offset = target;
      for (int block = 0; block < 3 && iter->HasNext(); block++) {
        SCOPED_TRACE(block);
        size_t n = cb.nrows();
        ASSERT_STATUS_OK_FAST(iter->CopyNextValues(&n, &cb));
        ASSERT_EQ(n, std::min(num_entries - read_offset, cb.nrows()));

        // Verify that the block data is correct.
        data_generator.Build(read_offset, n);
        for (size_t j = 0; j < n; ++j) {
          SCOPED_TRACE(j);
          bool expected_null = data_generator.TestValueShouldBeNull(read_offset + j);
          ASSERT_EQ(expected_null, cb.is_null(j));
          if (!expected_null) {
            ASSERT_EQ(data_generator[j], cb[j]);
          }
        }
        cb.arena()->Reset();
        read_offset += n;
      }
    }
  }

  template <class DataGeneratorType, DataType data_type>
  void TestNullTypes(EncodingType encoding, CompressionType compression) {
    BlockId block_id;
    WriteTestFileWithNulls<DataGeneratorType, data_type>(encoding,
                                                         compression,
                                                         10000,
                                                         &block_id);

    size_t n;
    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(n, 10000);

    TimeSeekAndReadFileWithNulls<DataGeneratorType, data_type>(block_id, n);
  }


  void TestReadWriteRawBlocks(CompressionType compression, int num_entries) {
    // Test Write
    gscoped_ptr<WritableBlock> sink;
    ASSERT_STATUS_OK(fs_manager_->CreateNewBlock(&sink));
    BlockId id = sink->id();
    WriterOptions opts;
    opts.write_posidx = true;
    opts.write_validx = false;
    opts.block_size = FLAGS_cfile_test_block_size;
    opts.storage_attributes = ColumnStorageAttributes(PLAIN_ENCODING, compression);
    CFileWriter w(opts, STRING, false, sink.Pass());
    ASSERT_STATUS_OK(w.Start());
    for (uint32_t i = 0; i < num_entries; i++) {
      vector<Slice> slices;
      slices.push_back(Slice("Head"));
      slices.push_back(Slice("Body"));
      slices.push_back(Slice("Tail"));
      slices.push_back(Slice(reinterpret_cast<uint8_t *>(&i), 4));
      ASSERT_STATUS_OK(w.AppendRawBlock(slices, i, NULL, "raw-data"));
    }
    ASSERT_STATUS_OK(w.Finish());

    // Test Read
    gscoped_ptr<ReadableBlock> source;
    ASSERT_STATUS_OK(fs_manager_->OpenBlock(id, &source));
    gscoped_ptr<CFileReader> reader;
    ASSERT_STATUS_OK(CFileReader::Open(source.Pass(), ReaderOptions(), &reader));

    gscoped_ptr<IndexTreeIterator> iter;
    iter.reset(IndexTreeIterator::Create(reader.get(), UINT32, reader->posidx_root()));
    ASSERT_STATUS_OK(iter->SeekToFirst());

    uint8_t data[16];
    Slice expected_data(data, 16);
    memcpy(data, "HeadBodyTail", 12);

    uint32_t count = 0;
    do {
      BlockCacheHandle dblk_data;
      BlockPointer blk_ptr = iter->GetCurrentBlockPointer();
      ASSERT_STATUS_OK(reader->ReadBlock(blk_ptr, &dblk_data));

      memcpy(data + 12, &count, 4);
      ASSERT_EQ(expected_data, dblk_data.data());

      count++;
    } while (iter->Next().ok());
    ASSERT_EQ(num_entries, count);
  }
};


template<DataType type>
void CopyOne(CFileIterator *it,
             typename TypeTraits<type>::cpp_type *ret,
             Arena *arena) {
  ColumnBlock cb(GetTypeInfo(type), NULL, ret, 1, arena);
  size_t n = 1;
  ASSERT_STATUS_OK(it->CopyNextValues(&n, &cb));
  ASSERT_EQ(1, n);
}

#ifdef NDEBUG
// Only run the 100M entry tests in non-debug mode.
// They take way too long with debugging enabled.

TEST_F(TestCFile, TestWrite100MFileInts) {
  BlockId block_id;
  LOG_TIMING(INFO, "writing 100m ints") {
    LOG(INFO) << "Starting writefile";
    WriteTestFileUInt32(GROUP_VARINT, NO_COMPRESSION, 100000000, &block_id);
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M ints") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(100000000, n);
    LOG(INFO) << "End readfile";
  }
}

TEST_F(TestCFile, TestWrite100MFileNullableInts) {
  BlockId block_id;
  LOG_TIMING(INFO, "writing 100m nullable ints") {
    LOG(INFO) << "Starting writefile";
    WriteTestFileWithNulls<UInt32DataGenerator, UINT32>(
      PLAIN_ENCODING, NO_COMPRESSION, 100000000, &block_id);
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M nullable ints") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(100000000, n);
    LOG(INFO) << "End readfile";
  }
}

TEST_F(TestCFile, TestWrite100MFileStrings) {
  BlockId block_id;
  LOG_TIMING(INFO, "writing 100M strings") {
    LOG(INFO) << "Starting writefile";
    WriteTestFileStrings(PREFIX_ENCODING, NO_COMPRESSION,
                         100000000, "hello %d", &block_id);
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M strings") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(100000000, n);
    LOG(INFO) << "End readfile";
  }
}
#endif

TEST_F(TestCFile, TestFixedSizeReadWriteUInt32) {
  TestReadWriteFixedSizeTypes<UInt32DataGenerator, UINT32>(GROUP_VARINT);
  TestReadWriteFixedSizeTypes<UInt32DataGenerator, UINT32>(PLAIN_ENCODING);
}

TEST_F(TestCFile, TestFixedSizeReadWriteInt32) {
  TestReadWriteFixedSizeTypes<Int32DataGenerator, INT32>(PLAIN_ENCODING);
}

void EncodeStringKey(const Schema &schema, const Slice& key,
                     gscoped_ptr<EncodedKey> *encoded_key) {
  EncodedKeyBuilder kb(&schema);
  kb.AddColumnKey(&key);
  encoded_key->reset(kb.BuildEncodedKey());
}

TEST_F(TestCFile, TestReadWriteStrings) {
  Schema schema(boost::assign::list_of
                (ColumnSchema("key", STRING)),
                1);

  const int nrows = 10000;
  BlockId block_id;
  WriteTestFileStrings(PREFIX_ENCODING, NO_COMPRESSION, nrows,
                       "hello %04d", &block_id);

  gscoped_ptr<ReadableBlock> block;
  ASSERT_STATUS_OK(fs_manager_->OpenBlock(block_id, &block));
  gscoped_ptr<CFileReader> reader;
  ASSERT_STATUS_OK(CFileReader::Open(block.Pass(), ReaderOptions(), &reader));

  rowid_t reader_nrows;
  ASSERT_STATUS_OK(reader->CountRows(&reader_nrows));
  ASSERT_EQ(nrows, reader_nrows);

  BlockPointer ptr;

  gscoped_ptr<CFileIterator> iter;
  ASSERT_STATUS_OK(reader->NewIterator(&iter));

  Arena arena(1024, 1024*1024);

  ASSERT_STATUS_OK(iter->SeekToOrdinal(5000));
  ASSERT_EQ(5000u, iter->GetCurrentOrdinal());
  Slice s;

  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 5000"), s.ToString());

  // Seek to last key exactly, should succeed
  ASSERT_STATUS_OK(iter->SeekToOrdinal(9999));
  ASSERT_EQ(9999u, iter->GetCurrentOrdinal());

  // Seek to after last key. Should result in not found.
  ASSERT_TRUE(iter->SeekToOrdinal(10000).IsNotFound());


  ////////
  // Now try some seeks by the value instead of position
  /////////

  gscoped_ptr<EncodedKey> encoded_key;
  bool exact;
  s = "hello 5000.5";
  EncodeStringKey(schema, s, &encoded_key);
  ASSERT_STATUS_OK(iter->SeekAtOrAfter(*encoded_key, &exact));
  ASSERT_FALSE(exact);
  ASSERT_EQ(5001u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 5001"), s.ToString());

  s = "hello 9000";
  EncodeStringKey(schema, s, &encoded_key);
  ASSERT_STATUS_OK(iter->SeekAtOrAfter(*encoded_key, &exact));
  ASSERT_TRUE(exact);
  ASSERT_EQ(9000u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 9000"), s.ToString());

  // after last entry
  s = "hello 9999x";
  EncodeStringKey(schema, s, &encoded_key);
  EXPECT_TRUE(iter->SeekAtOrAfter(*encoded_key, &exact).IsNotFound());

  // before first entry
  s = "hello";
  EncodeStringKey(schema, s, &encoded_key);
  ASSERT_STATUS_OK(iter->SeekAtOrAfter(*encoded_key, &exact));
  ASSERT_FALSE(exact);
  ASSERT_EQ(0u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 0000"), s.ToString());

  // to last entry
  s = "hello 9999";
  EncodeStringKey(schema, s, &encoded_key);
  ASSERT_STATUS_OK(iter->SeekAtOrAfter(*encoded_key, &exact));
  ASSERT_TRUE(exact);
  ASSERT_EQ(9999u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 9999"), s.ToString());

  // Seek to start of file
  ASSERT_STATUS_OK(iter->SeekToFirst());
  ASSERT_EQ(0u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(string("hello 0000"), s.ToString());

  // Reseek to start and fetch all data.
  ASSERT_STATUS_OK(iter->SeekToFirst());

  ScopedColumnBlock<STRING> cb(10000);
  size_t n = 10000;
  ASSERT_STATUS_OK(iter->CopyNextValues(&n, &cb));
  ASSERT_EQ(10000, n);
}

// Test that metadata entries stored in the cfile are persisted.
TEST_F(TestCFile, TestMetadata) {
  BlockId block_id;

  // Write the file.
  {
    gscoped_ptr<WritableBlock> sink;
    ASSERT_STATUS_OK(fs_manager_->CreateNewBlock(&sink));
    block_id = sink->id();
    WriterOptions opts;
    opts.storage_attributes = ColumnStorageAttributes(GROUP_VARINT);
    CFileWriter w(opts, UINT32, false, sink.Pass());

    w.AddMetadataPair("key_in_header", "header value");
    ASSERT_STATUS_OK(w.Start());

    uint32_t val = 1;
    ASSERT_STATUS_OK(w.AppendEntries(&val, 1));

    w.AddMetadataPair("key_in_footer", "footer value");
    ASSERT_STATUS_OK(w.Finish());
  }

  // Read the file and ensure metadata is present.
  {
    gscoped_ptr<ReadableBlock> source;
    ASSERT_STATUS_OK(fs_manager_->OpenBlock(block_id, &source));
    gscoped_ptr<CFileReader> reader;
    ASSERT_STATUS_OK(CFileReader::Open(source.Pass(), ReaderOptions(), &reader));
    string val;
    ASSERT_TRUE(reader->GetMetadataEntry("key_in_header", &val));
    ASSERT_EQ(val, "header value");
    ASSERT_TRUE(reader->GetMetadataEntry("key_in_footer", &val));
    ASSERT_EQ(val, "footer value");
    ASSERT_FALSE(reader->GetMetadataEntry("not a key", &val));
  }
}

TEST_F(TestCFile, TestDefaultColumnIter) {
  const int kNumItems = 64;
  uint8_t null_bitmap[BitmapSize(kNumItems)];
  uint32_t data[kNumItems];

  // Test Int Default Value
  uint32_t int_value = 15;
  DefaultColumnValueIterator iter(UINT32, &int_value);
  ColumnBlock int_col(GetTypeInfo(UINT32), NULL, data, kNumItems, NULL);
  ASSERT_STATUS_OK(iter.Scan(&int_col));
  for (size_t i = 0; i < int_col.nrows(); ++i) {
    ASSERT_EQ(int_value, *reinterpret_cast<const uint32_t *>(int_col.cell_ptr(i)));
  }

  // Test Int Nullable Default Value
  int_value = 321;
  DefaultColumnValueIterator nullable_iter(UINT32, &int_value);
  ColumnBlock nullable_col(GetTypeInfo(UINT32), null_bitmap, data, kNumItems, NULL);
  ASSERT_STATUS_OK(nullable_iter.Scan(&nullable_col));
  for (size_t i = 0; i < nullable_col.nrows(); ++i) {
    ASSERT_FALSE(nullable_col.is_null(i));
    ASSERT_EQ(int_value, *reinterpret_cast<const uint32_t *>(nullable_col.cell_ptr(i)));
  }

  // Test NULL Default Value
  DefaultColumnValueIterator null_iter(UINT32,  NULL);
  ColumnBlock null_col(GetTypeInfo(UINT32), null_bitmap, data, kNumItems, NULL);
  ASSERT_STATUS_OK(null_iter.Scan(&null_col));
  for (size_t i = 0; i < null_col.nrows(); ++i) {
    ASSERT_TRUE(null_col.is_null(i));
  }

  // Test String Default Value
  Slice str_data[kNumItems];
  Slice str_value("Hello");
  Arena arena(32*1024, 256*1024);
  DefaultColumnValueIterator str_iter(STRING, &str_value);
  ColumnBlock str_col(GetTypeInfo(STRING), NULL, str_data, kNumItems, &arena);
  ASSERT_STATUS_OK(str_iter.Scan(&str_col));
  for (size_t i = 0; i < str_col.nrows(); ++i) {
    ASSERT_EQ(str_value, *reinterpret_cast<const Slice *>(str_col.cell_ptr(i)));
  }
}

TEST_F(TestCFile, TestAppendRaw) {
  TestReadWriteRawBlocks(NO_COMPRESSION, 1000);
  TestReadWriteRawBlocks(SNAPPY, 1000);
  TestReadWriteRawBlocks(LZ4, 1000);
  TestReadWriteRawBlocks(ZLIB, 1000);
}

TEST_F(TestCFile, TestNullInts) {
  TestNullTypes<UInt32DataGenerator, UINT32>(GROUP_VARINT, NO_COMPRESSION);
  TestNullTypes<UInt32DataGenerator, UINT32>(GROUP_VARINT, LZ4);
}

TEST_F(TestCFile, TestNullPrefixStrings) {
  TestNullTypes<StringDataGenerator, STRING>(PLAIN_ENCODING, NO_COMPRESSION);
  TestNullTypes<StringDataGenerator, STRING>(PLAIN_ENCODING, LZ4);
}

TEST_F(TestCFile, TestNullPlainStrings) {
  TestNullTypes<StringDataGenerator, STRING>(PREFIX_ENCODING, NO_COMPRESSION);
  TestNullTypes<StringDataGenerator, STRING>(PREFIX_ENCODING, LZ4);
}


} // namespace cfile
} // namespace kudu
