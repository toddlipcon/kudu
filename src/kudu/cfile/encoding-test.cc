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

#include <sys/types.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/binary_plain_block.h"
#include "kudu/cfile/binary_prefix_block.h"
#include "kudu/cfile/block_encodings.h"
#include "kudu/cfile/block_handle.h"
#include "kudu/cfile/bshuf_block.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/plain_bitmap_block.h"
#include "kudu/cfile/plain_block.h"
#include "kudu/cfile/rle_block.h"
#include "kudu/cfile/type_encodings.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/columnblock-test-util.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/faststring.h"
#include "kudu/util/group_varint-inl.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/int128.h"
#include "kudu/util/int128_util.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace cfile {

class TestEncoding : public KuduTest {
 public:
  TestEncoding()
    : memory_(1024) {
  }

 protected:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    memory_.Reset();
    default_write_options_.storage_attributes.cfile_block_size = 256 * 1024;
  }

  template<DataType type>
  void CopyOne(BlockDecoder *decoder,
               typename TypeTraits<type>::cpp_type *ret) {
    ColumnBlock cb(GetTypeInfo(type), nullptr, ret, 1, &memory_);
    ColumnDataView cdv(&cb);
    size_t n = 1;
    ASSERT_OK(decoder->CopyNextValues(&n, &cdv));
    ASSERT_EQ(1, n);
  }

  unique_ptr<BlockBuilder> CreateBlockBuilderOrDie(DataType type,
                                                   EncodingType encoding) {
    const TypeEncodingInfo* tei;
    CHECK_OK(TypeEncodingInfo::Get(GetTypeInfo(type), encoding, &tei));
    unique_ptr<BlockBuilder> bb;
    CHECK_OK(tei->CreateBlockBuilder(&bb, &default_write_options_));
    return bb;
  }

  static unique_ptr<BlockDecoder> CreateBlockDecoderOrDie(DataType type,
                                                          EncodingType encoding,
                                                          scoped_refptr<BlockHandle> block) {
    const TypeEncodingInfo* tei;
    CHECK_OK(TypeEncodingInfo::Get(GetTypeInfo(type), encoding, &tei));
    unique_ptr<BlockDecoder> bd;
    CHECK_OK(tei->CreateBlockDecoder(&bd, std::move(block), /*parent_cfile_iter=*/nullptr));
    return bd;
  }

  // Insert a given number of strings into the provided BlockBuilder.
  //
  // The strings are generated using the provided 'formatter' function.
  static scoped_refptr<BlockHandle> CreateBinaryBlock(BlockBuilder* sbb,
                                                   int num_items,
                                                   const std::function<string(int)>& formatter) {
    vector<string> to_insert(num_items);
    vector<Slice> slices;
    for (int i = 0; i < num_items; i++) {
      to_insert[i] = formatter(i);
      slices.emplace_back(to_insert[i]);
    }

    int rem = slices.size();
    Slice *ptr = &slices[0];
    while (rem > 0) {
      int added = sbb->Add(reinterpret_cast<const uint8_t *>(ptr),
                           rem);
      CHECK(added > 0);
      rem -= added;
      ptr += added;
    }

    CHECK_EQ(slices.size(), sbb->Count());
    return FinishAndMakeContiguous(sbb, 12345L);
  }

  // Concatenate the given slices and return a BlockHandle with the resulting data.
  static scoped_refptr<BlockHandle> MakeContiguous(const vector<Slice>& slices) {
    // Concat the slices into a local buffer, since the block decoders and
    // tests expect contiguous data.
    faststring buf;
    for (const auto& s : slices) {
      buf.append(s.data(), s.size());
    }

    auto size = buf.size();
    return BlockHandle::WithOwnedData(Slice(buf.release(), size));
  }

  static scoped_refptr<BlockHandle> FinishAndMakeContiguous(BlockBuilder* b, int ord_val) {
    vector<Slice> slices;
    b->Finish(ord_val, &slices);
    return MakeContiguous(slices);
  }

  void TestBinarySeekByValueSmallBlock(EncodingType encoding) {
    auto bb = CreateBlockBuilderOrDie(BINARY, encoding);
    // Insert "hello 0" through "hello 9"
    const int kCount = 10;
    scoped_refptr<BlockHandle> block = CreateBinaryBlock(
        bb.get(), kCount, [](int item) { return StringPrintf("hello %d", item); });

    auto sbd = CreateBlockDecoderOrDie(BINARY, encoding, std::move(block));
    ASSERT_OK(sbd->ParseHeader());

    // Seeking to just after a key should return the
    // next key ('hello 4x' falls between 'hello 4' and 'hello 5')
    Slice q = "hello 4x";
    bool exact;
    ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
    ASSERT_FALSE(exact);

    Slice ret;
    ASSERT_EQ(5U, sbd->GetCurrentIndex());
    CopyOne<STRING>(sbd.get(), &ret);
    ASSERT_EQ(string("hello 5"), ret.ToString());

    sbd->SeekToPositionInBlock(0);

    // Seeking to an exact key should return that key
    q = "hello 4";
    ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
    ASSERT_EQ(4U, sbd->GetCurrentIndex());
    ASSERT_TRUE(exact);
    CopyOne<STRING>(sbd.get(), &ret);
    ASSERT_EQ(string("hello 4"), ret.ToString());

    // Seeking to before the first key should return first key
    q = "hello";
    ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
    ASSERT_EQ(0, sbd->GetCurrentIndex());
    ASSERT_FALSE(exact);
    CopyOne<STRING>(sbd.get(), &ret);
    ASSERT_EQ(string("hello 0"), ret.ToString());

    // Seeking after the last key should return not found
    q = "zzzz";
    ASSERT_TRUE(sbd->SeekAtOrAfterValue(&q, &exact).IsNotFound());

    // Seeking to the last key should succeed
    q = "hello 9";
    ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
    ASSERT_EQ(9U, sbd->GetCurrentIndex());
    ASSERT_TRUE(exact);
    CopyOne<STRING>(sbd.get(), &ret);
    ASSERT_EQ(string("hello 9"), ret.ToString());
  }

  void TestStringSeekByValueLargeBlock(EncodingType encoding) {
    Arena arena(1024); // TODO(todd): move to fixture?
    auto sbb = CreateBlockBuilderOrDie(BINARY, encoding);
    // Insert 'hello 000' through 'hello 999'
    const int kCount = 1000;
    scoped_refptr<BlockHandle> block = CreateBinaryBlock(
        sbb.get(), kCount, [](int item) { return StringPrintf("hello %03d", item); });
    auto sbd = CreateBlockDecoderOrDie(BINARY, encoding, std::move(block));
    ASSERT_OK(sbd->ParseHeader());

    // Seeking to just after a key should return the
    // next key ('hello 444x' falls between 'hello 444' and 'hello 445')
    Slice q = "hello 444x";
    bool exact;
    ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
    ASSERT_FALSE(exact);

    Slice ret;
    ASSERT_EQ(445U, sbd->GetCurrentIndex());
    CopyOne<STRING>(sbd.get(), &ret);
    ASSERT_EQ(string("hello 445"), ret.ToString());

    sbd->SeekToPositionInBlock(0);

    // Seeking to an exact key should return that key
    q = "hello 004";
    ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
    EXPECT_TRUE(exact);
    EXPECT_EQ(4U, sbd->GetCurrentIndex());
    CopyOne<STRING>(sbd.get(), &ret);
    ASSERT_EQ(string("hello 004"), ret.ToString());

    // Seeking to before the first key should return first key
    q = "hello";
    ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
    EXPECT_FALSE(exact);
    EXPECT_EQ(0, sbd->GetCurrentIndex());
    CopyOne<STRING>(sbd.get(), &ret);
    ASSERT_EQ(string("hello 000"), ret.ToString());

    // Seeking after the last key should return not found
    q = "zzzz";
    ASSERT_TRUE(sbd->SeekAtOrAfterValue(&q, &exact).IsNotFound());

    // Seeking to the last key should succeed
    q = "hello 999";
    ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
    EXPECT_TRUE(exact);
    EXPECT_EQ(999U, sbd->GetCurrentIndex());
    CopyOne<STRING>(sbd.get(), &ret);
    ASSERT_EQ(string("hello 999"), ret.ToString());

    // Randomized seek
    char target[20];
    char before_target[20];
    for (int i = 0; i < 1000; i++) {
      int ord = random() % kCount;
      int len = snprintf(target, sizeof(target), "hello %03d", ord);
      q = Slice(target, len);

      ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
      EXPECT_TRUE(exact);
      EXPECT_EQ(ord, sbd->GetCurrentIndex());
      CopyOne<STRING>(sbd.get(), &ret);
      ASSERT_EQ(string(target), ret.ToString());

      // Seek before this key
      len = snprintf(before_target, sizeof(target), "hello %03d.before", ord-1);
      q = Slice(before_target, len);
      ASSERT_OK(sbd->SeekAtOrAfterValue(&q, &exact));
      EXPECT_FALSE(exact);
      EXPECT_EQ(ord, sbd->GetCurrentIndex());
      CopyOne<STRING>(sbd.get(), &ret);
      ASSERT_EQ(string(target), ret.ToString());
    }
  }

  void TestBinaryBlockRoundTrip(EncodingType encoding) {
    auto sbb = CreateBlockBuilderOrDie(BINARY, encoding);

    auto seed = SeedRandom();
    Random r(seed);

    // For each row, generate random data based on this run's seed.
    // Using random data helps the ability to trigger bugs like KUDU-2085.
    const auto& GenTestString = [seed](int i) {
      Random local_rng(seed + i);
      int len = local_rng.Uniform(8);
      return RandomString(len, &local_rng);
    };

    // Use a random number of elements (but at least 1).
    //
    // This is necessary to trigger bugs like KUDU-2085 that only occur in specific cases
    // such as when the number of elements is a multiple of the 'restart interval' in
    // prefix-encoded blocks.
    const uint kCount = r.Uniform(1000) + 1;
    scoped_refptr<BlockHandle> block = CreateBinaryBlock(sbb.get(), kCount, GenTestString);

    LOG(INFO) << "Block: " << HexDump(block->data());

    // The encoded data should take at least 1 byte per entry.
    ASSERT_GT(block->data().size(), kCount);

    // Check first/last keys
    Slice key;
    ASSERT_OK(sbb->GetFirstKey(&key));
    ASSERT_EQ(GenTestString(0), key);
    ASSERT_OK(sbb->GetLastKey(&key));
    ASSERT_EQ(GenTestString(kCount - 1), key);

    auto sbd = CreateBlockDecoderOrDie(BINARY, encoding, std::move(block));
    ASSERT_OK(sbd->ParseHeader());
    ASSERT_EQ(kCount, sbd->Count());
    ASSERT_EQ(12345U, sbd->GetFirstRowId());
    ASSERT_TRUE(sbd->HasNext());

    // Iterate one by one through data, verifying that it matches
    // what we put in.
    for (uint i = 0; i < kCount; i++) {
      ASSERT_EQ(i, sbd->GetCurrentIndex());
      ASSERT_TRUE(sbd->HasNext()) << "Failed on iter " << i;
      Slice s;
      CopyOne<STRING>(sbd.get(), &s);
      string expected = GenTestString(i);
      ASSERT_EQ(expected, s.ToString()) << "failed at iter " << i;
    }
    ASSERT_FALSE(sbd->HasNext());

    // Now iterate backwards using positional seeking
    for (int i = static_cast<int>(kCount - 1); i >= 0; i--) {
      sbd->SeekToPositionInBlock(i);
      ASSERT_EQ(i, sbd->GetCurrentIndex());
    }

    // Test the special case of seeking to the end of the block.
    sbd->SeekToPositionInBlock(kCount);
    ASSERT_EQ(kCount, sbd->GetCurrentIndex());
    ASSERT_FALSE(sbd->HasNext());

    // Try to request a bunch of data in one go
    ScopedColumnBlock<STRING> cb(kCount + 10);
    ColumnDataView cdv(&cb);
    sbd->SeekToPositionInBlock(0);
    size_t n = kCount + 10;
    ASSERT_OK(sbd->CopyNextValues(&n, &cdv));
    ASSERT_EQ(kCount, n);
    ASSERT_FALSE(sbd->HasNext());

    for (uint i = 0; i < kCount; i++) {
      string expected = GenTestString(i);
      ASSERT_EQ(expected, cb[i].ToString());
    }
  }

  template<DataType IntType>
  void DoSeekTest(EncodingType encoding, int num_ints, int num_queries, bool verify) {
    // TODO(unknown) : handle and verify seeking inside a run for testing RLE
    typedef typename TypeTraits<IntType>::cpp_type CppType;

    const CppType kBase = std::is_signed<CppType>::value ? -6 : 6;

    CppType data[num_ints];
    // CppType may be uint8, int16 etc. and hence clang-tidy warning about loop variable smaller
    // than num_ints data-type int. But callers supply correct values within range.
    for (CppType i = 0; i < num_ints; i++) { // NOLINT(bugprone-too-small-loop-variable)
      data[i] = kBase + i * 2;
    }
    const CppType max_seek_target = data[num_ints - 1] + 1;

    auto ibb = CreateBlockBuilderOrDie(IntType, encoding);
    CHECK_EQ(num_ints, ibb->Add(reinterpret_cast<uint8_t *>(&data[0]),
                               num_ints));
    scoped_refptr<BlockHandle> block = FinishAndMakeContiguous(ibb.get(), 0);
    LOG(INFO) << "Created " << TypeTraits<IntType>::name() << " block with " << num_ints
              << " ints"
              << " (" << block->data().size() << " bytes)";
    auto ibd = CreateBlockDecoderOrDie(IntType, encoding, std::move(block));
    ASSERT_OK(ibd->ParseHeader());

    // Benchmark seeking
    LOG_TIMING(INFO, strings::Substitute("Seeking in $0 block", TypeTraits<IntType>::name())) {
      for (int i = 0; i < num_queries; i++) {
        bool exact = false;
        // Seek to a random value which falls between data[0] and max_seek_target
        CppType target = kBase + random() % (max_seek_target - kBase);
        Status s = ibd->SeekAtOrAfterValue(&target, &exact);
        if (verify) {
          SCOPED_TRACE(target);
          if (s.IsNotFound()) {
            // If we didn't find a match 'at or after' the given value, then the
            // only case that could happen is if we seeked to max_seek_target,
            // which is larger than the largest int in the block.
            ASSERT_EQ(max_seek_target, target);
            continue;
          }
          ASSERT_OK_FAST(s);

          CppType got;
          CopyOne<IntType>(ibd.get(), &got);

          if (target < kBase) {
            ASSERT_EQ(kBase, got);
            ASSERT_FALSE(exact);
          } else if (target % 2 == 0) {
            // Was inserted
            ASSERT_EQ(target, got);
            ASSERT_TRUE(exact);
          } else {
            ASSERT_EQ(target + 1, got);
            ASSERT_FALSE(exact);
          }
        }
      }
    }
  }

  void TestEmptyBlockEncodeDecode(DataType type, EncodingType encoding) {
    auto bb = CreateBlockBuilderOrDie(type, encoding);
    scoped_refptr<BlockHandle> block = FinishAndMakeContiguous(bb.get(), 0);
    ASSERT_GT(block->data().size(), 0);
    LOG(INFO) << "Encoded size for 0 items: " << block->data().size();

    auto bd = CreateBlockDecoderOrDie(type, encoding, std::move(block));
    ASSERT_OK(bd->ParseHeader());
    ASSERT_EQ(0, bd->Count());
    ASSERT_FALSE(bd->HasNext());
  }

  template <DataType Type>
  void TestEncodeDecodeTemplateBlockEncoder(const typename TypeTraits<Type>::cpp_type* src,
                                            uint32_t size,
                                            EncodingType encoding) {
    typedef typename TypeTraits<Type>::cpp_type CppType;
    const uint32_t kOrdinalPosBase = 12345;

    auto bb = CreateBlockBuilderOrDie(Type, encoding);
    bb->Add(reinterpret_cast<const uint8_t *>(src), size);
    scoped_refptr<BlockHandle> block = FinishAndMakeContiguous(bb.get(), kOrdinalPosBase);

    LOG(INFO) << "Encoded size for " << size << " elems: " << block->data().size();

    auto bd = CreateBlockDecoderOrDie(Type, encoding, std::move(block));
    ASSERT_OK(bd->ParseHeader());
    ASSERT_EQ(kOrdinalPosBase, bd->GetFirstRowId());
    ASSERT_EQ(0, bd->GetCurrentIndex());

    vector<CppType> decoded;
    decoded.resize(size);

    ColumnBlock dst_block(GetTypeInfo(Type), nullptr, &decoded[0], size, &memory_);
    ColumnDataView view(&dst_block);
    int dec_count = 0;
    while (bd->HasNext()) {
      ASSERT_EQ((int32_t )(dec_count), bd->GetCurrentIndex());

      size_t to_decode = (random() % 30) + 1;
      size_t n = to_decode > view.nrows() ? view.nrows() : to_decode;
      ASSERT_OK_FAST(bd->CopyNextValues(&n, &view));
      ASSERT_GE(to_decode, n);
      view.Advance(n);
      dec_count += n;
    }

    ASSERT_EQ(0, view.nrows())<< "Should have no space left in the buffer after "
                              << "decoding all rows";

    for (uint i = 0; i < size; i++) {
      if (src[i] != decoded[i]) {
        FAIL()<< "Fail at index " << i <<
              " inserted=" << src[i] << " got=" << decoded[i];
      }
    }

    // Test Seek within block by ordinal
    for (int i = 0; i < 100; i++) {
      int seek_off = random() % decoded.size();
      bd->SeekToPositionInBlock(seek_off);

      EXPECT_EQ((int32_t )(seek_off), bd->GetCurrentIndex());
      CppType ret;
      CopyOne<Type>(bd.get(), &ret);
      EXPECT_EQ(decoded[seek_off], ret);
    }
  }

  // Test truncation of blocks
  template<class DecoderType>
  void TestBinaryBlockTruncation(EncodingType encoding) {
    auto sbb = CreateBlockBuilderOrDie(BINARY, encoding);
    const int kCount = 10;
    size_t sbsize;

    scoped_refptr<BlockHandle> block = CreateBinaryBlock(
        sbb.get(), kCount, [](int item) { return StringPrintf("hello %d", item); });
    CHECK(block);
    do {
      sbsize = block->data().size();

      LOG(INFO) << "Block: " << HexDump(block->data());

      auto sbd = CreateBlockDecoderOrDie(BINARY, encoding, block);
      Status st = sbd->ParseHeader();

      if (sbsize < DecoderType::kMinHeaderSize) {
        ASSERT_TRUE(st.IsCorruption());
        ASSERT_STR_CONTAINS(st.ToString(), "not enough bytes for header");
      } else if (sbsize < coding::DecodeGroupVarInt32_GetGroupSize(block->data().data())) {
        ASSERT_TRUE(st.IsCorruption());
        ASSERT_STR_CONTAINS(st.ToString(), "less than length");
      }
      if (sbsize > 0) {
        block = block->SubrangeBlock(0, sbsize - 1);
        CHECK(block);
      }
    } while (sbsize > 0);
  }

  // Test encoding and decoding of integer datatypes
  template <DataType IntType>
  void TestIntBlockRoundTrip(EncodingType encoding) {
    typedef typename DataTypeTraits<IntType>::cpp_type CppType;

    LOG(INFO) << "Testing with IntType = " << DataTypeTraits<IntType>::name();

    const uint32_t kOrdinalPosBase = 12345;

    srand(123);

    vector<CppType> to_insert;
    Random rd(SeedRandom());
    for (int i = 0; i < 10003; i++) {
      int64_t val = rd.Next64() % std::numeric_limits<CppType>::max();

      // For signed types, randomly use both negative and positive values.
      if (std::numeric_limits<CppType>::is_signed && (random() % 2) == 1) {
        val *= -1;
      }
      to_insert.push_back(val);

      // Occasionally insert a run of 40 identical values to exercise
      // RLE code paths.
      if (random() % 100 == 1) {
        for (int run = 0; run < 40; run++) {
          to_insert.push_back(val);
        }
      }
    }
    auto ibb = CreateBlockBuilderOrDie(IntType, encoding);
    ibb->Add(reinterpret_cast<const uint8_t *>(&to_insert[0]),
             to_insert.size());
    scoped_refptr<BlockHandle> block = FinishAndMakeContiguous(ibb.get(), kOrdinalPosBase);

    // Check GetFirstKey() and GetLastKey().
    CppType key;
    ASSERT_OK(ibb->GetFirstKey(&key));
    ASSERT_EQ(to_insert.front(), key);
    ASSERT_OK(ibb->GetLastKey(&key));
    ASSERT_EQ(to_insert.back(), key);

    auto ibd = CreateBlockDecoderOrDie(IntType, encoding, std::move(block));
    ASSERT_OK(ibd->ParseHeader());

    ASSERT_EQ(kOrdinalPosBase, ibd->GetFirstRowId());

    vector<CppType> decoded;
    decoded.resize(to_insert.size());

    ColumnBlock dst_block(GetTypeInfo(IntType), nullptr,
                          &decoded[0],
                          to_insert.size(),
                          &memory_);
    int dec_count = 0;
    while (ibd->HasNext()) {
      ASSERT_EQ((uint32_t)(dec_count), ibd->GetCurrentIndex());

      size_t to_decode = std::min(to_insert.size() - dec_count,
                                  static_cast<size_t>((random() % 30) + 1));
      size_t n = to_decode;
      ColumnDataView dst_data(&dst_block, dec_count);
      DCHECK_EQ((unsigned char *)(&decoded[dec_count]), dst_data.data());
      ASSERT_OK_FAST(ibd->CopyNextValues(&n, &dst_data));
      ASSERT_GE(to_decode, n);
      dec_count += n;
    }

    ASSERT_EQ(dec_count, dst_block.nrows())
                  << "Should have decoded all rows to fill the buffer";

    for (uint i = 0; i < to_insert.size(); i++) {
      if (to_insert[i] != decoded[i]) {
        FAIL() << "Fail at index " << i <<
               " inserted=" << to_insert[i] << " got=" << decoded[i];
      }
    }

    // Test Seek within block by ordinal
    for (int i = 0; i < 100; i++) {
      int seek_off = random() % decoded.size();
      ibd->SeekToPositionInBlock(seek_off);

      EXPECT_EQ((uint32_t)(seek_off), ibd->GetCurrentIndex());
      CppType ret;
      CopyOne<IntType>(ibd.get(), &ret);
      EXPECT_EQ(decoded[seek_off], ret);
    }

    // Test Seek forward within block.
    ibd->SeekToPositionInBlock(0);
    int skip_step = 7;
    EXPECT_EQ((uint32_t) 0, ibd->GetCurrentIndex());
    for (uint32_t i = 0; i < decoded.size()/skip_step; i++) {
      // Skip just before the end of the step.
      int skip = skip_step-1;
      ibd->SeekForward(&skip);
      EXPECT_EQ((uint32_t) i*skip_step+skip, ibd->GetCurrentIndex());
      CppType ret;
      // CopyOne will move the decoder forward by one.
      CopyOne<IntType>(ibd.get(), &ret);
      EXPECT_EQ(decoded[i*skip_step + skip], ret);
    }
  }

  // Test encoding and decoding BOOL datatypes
  void TestBoolBlockRoundTrip(EncodingType encoding) {
    const uint32_t kOrdinalPosBase = 12345;

    srand(123);

    vector<uint8_t> to_insert;
    for (int i = 0; i < 10003; ) {
      int run_size = static_cast<int>(random() % 100);
      bool val = random() % 2;
      for (int j = 0; j < run_size; j++) {
        to_insert.push_back(val);
      }
      i += run_size;
    }

    auto bb = CreateBlockBuilderOrDie(BOOL, encoding);
    bb->Add(reinterpret_cast<const uint8_t *>(&to_insert[0]),
            to_insert.size());
    scoped_refptr<BlockHandle> block = FinishAndMakeContiguous(bb.get(), kOrdinalPosBase);

    auto bd = CreateBlockDecoderOrDie(BOOL, encoding, std::move(block));
    ASSERT_OK(bd->ParseHeader());

    ASSERT_EQ(kOrdinalPosBase, bd->GetFirstRowId());

    vector<uint8_t> decoded;
    decoded.resize(to_insert.size());

    ColumnBlock dst_block(GetTypeInfo(BOOL), nullptr,
                          &decoded[0],
                          to_insert.size(),
                          &memory_);

    int dec_count = 0;
    while (bd->HasNext()) {
      ASSERT_EQ((uint32_t)(dec_count), bd->GetCurrentIndex());

      size_t to_decode = std::min(to_insert.size() - dec_count,
                                  static_cast<size_t>((random() % 30) + 1));
      size_t n = to_decode;
      ColumnDataView dst_data(&dst_block, dec_count);
      DCHECK_EQ((unsigned char *)(&decoded[dec_count]), dst_data.data());
      ASSERT_OK_FAST(bd->CopyNextValues(&n, &dst_data));
      ASSERT_GE(to_decode, n);
      dec_count += n;
    }

    ASSERT_EQ(dec_count, dst_block.nrows())
                  << "Should have decoded all rows to fill the buffer";

    for (uint i = 0; i < to_insert.size(); i++) {
      if (to_insert[i] != decoded[i]) {
        FAIL() << "Fail at index " << i <<
               " inserted=" << to_insert[i] << " got=" << decoded[i];
      }
    }

    // Test Seek within block by ordinal
    for (int i = 0; i < 100; i++) {
      int seek_off = static_cast<int>(random() % decoded.size());
      bd->SeekToPositionInBlock(seek_off);

      EXPECT_EQ((uint32_t) (seek_off), bd->GetCurrentIndex());
      bool ret;
      CopyOne<BOOL>(bd.get(), &ret);
      EXPECT_EQ(static_cast<bool>(decoded[seek_off]), ret);
    }
  }

  RowBlockMemory memory_;
  WriterOptions default_write_options_;
};

TEST_F(TestEncoding, TestPlainBlockEncoder) {
  const uint32_t kSize = 10000;

  unique_ptr<int32_t[]> ints(new int32_t[kSize]);
  for (int i = 0; i < kSize; i++) {
    ints.get()[i] = random();
  }

  TestEncodeDecodeTemplateBlockEncoder<INT32>(ints.get(), kSize, PLAIN_ENCODING);
}

// Test for bitshuffle block, for INT32, INT64, INT128, FLOAT, DOUBLE
TEST_F(TestEncoding, TestBShufInt32BlockEncoder) {
  using limits = std::numeric_limits<int32_t>;
  Random rng(SeedRandom());
  auto sequences = {
      CreateRandomIntegersInRange<int32_t>(10000, 1000, 2000, &rng),
      CreateRandomIntegersInRange<int32_t>(10000, 0, limits::max(), &rng),
      CreateRandomIntegersInRange<int32_t>(10000, limits::min(), limits::max(), &rng)
  };
  for (const auto& ints : sequences) {
    TestEncodeDecodeTemplateBlockEncoder<INT32>(ints.data(), ints.size(), BIT_SHUFFLE);
  }
}

TEST_F(TestEncoding, TestBShufInt64BlockEncoder) {
  using limits = std::numeric_limits<int64_t>;
  Random rng(SeedRandom());
  auto sequences = {
      CreateRandomIntegersInRange<int64_t>(10000, 1000, 2000, &rng),
      CreateRandomIntegersInRange<int64_t>(10000, 0, limits::max(), &rng),
      CreateRandomIntegersInRange<int64_t>(10000, limits::min(), limits::max(), &rng)
  };
  for (const auto& ints : sequences) {
    TestEncodeDecodeTemplateBlockEncoder<INT64>(ints.data(), ints.size(), BIT_SHUFFLE);
  }
}

TEST_F(TestEncoding, TestBShufInt128BlockEncoder) {
  Random rng(SeedRandom());
  // As per the note in int128.h, numeric_limits<> on int128_t can give incorrect results.
  // Hence using predefined min, max constants.
  auto sequences = {
      CreateRandomIntegersInRange<int128_t>(10000, 1000, 2000, &rng),
      CreateRandomIntegersInRange<int128_t>(10000, 0, INT128_MAX, &rng),
      CreateRandomIntegersInRange<int128_t>(10000, INT128_MIN, INT128_MAX, &rng)
  };
  for (const auto& ints : sequences) {
    TestEncodeDecodeTemplateBlockEncoder<INT128>(ints.data(), ints.size(), BIT_SHUFFLE);
  }
}

TEST_F(TestEncoding, TestBShufFloatBlockEncoder) {
  const int kSize = 10000;

  unique_ptr<float[]> floats(new float[kSize]);
  for (int i = 0; i < kSize; i++) {
    floats.get()[i] = static_cast<float>(random()) +
                      static_cast<float>(random())/INT_MAX;
  }

  TestEncodeDecodeTemplateBlockEncoder<FLOAT>(floats.get(), kSize, BIT_SHUFFLE);
}

TEST_F(TestEncoding, TestBShufDoubleBlockEncoder) {
  const int kSize = 10000;

  unique_ptr<double[]> doubles(new double[kSize]);
  for (int i = 0; i < kSize; i++) {
    doubles.get()[i] = static_cast<double>(random()) +
                       static_cast<double>(random())/INT_MAX;
  }

  TestEncodeDecodeTemplateBlockEncoder<DOUBLE>(doubles.get(), kSize, BIT_SHUFFLE);
}

TEST_F(TestEncoding, TestRleIntBlockEncoder) {
  auto ibb = CreateBlockBuilderOrDie(UINT32, RLE);
  Random rand(SeedRandom());
  auto ints = CreateRandomIntegersInRange<uint32_t>(10000, 0, std::numeric_limits<uint32_t>::max(),
                                                    &rand);
  ibb->Add(reinterpret_cast<const uint8_t *>(ints.data()), 10000);

  scoped_refptr<BlockHandle> block = FinishAndMakeContiguous(ibb.get(), 12345);
  LOG(INFO) << "RLE Encoded size for 10k ints: " << block->data().size();

  ibb->Reset();
  ints.resize(100);
  for (int i = 0; i < 100; i++) {
    ints[i] = 0;
  }
  ibb->Add(reinterpret_cast<const uint8_t *>(ints.data()), 100);
  block = FinishAndMakeContiguous(ibb.get(), 12345);
  ASSERT_EQ(14UL, block->data().size());
}

TEST_F(TestEncoding, TestPlainBitMapRoundTrip) {
  TestBoolBlockRoundTrip(PLAIN_ENCODING);
}

TEST_F(TestEncoding, TestRleBitMapRoundTrip) {
  TestBoolBlockRoundTrip(RLE);
}

// Test seeking to a value in a small block.
// Regression test for a bug seen in development where this would
// infinite loop when there are no 'restarts' in a given block.
TEST_F(TestEncoding, TestBinaryPrefixBlockBuilderSeekByValueSmallBlock) {
  TestBinarySeekByValueSmallBlock(PREFIX_ENCODING);
}

TEST_F(TestEncoding, TestBinaryPlainBlockBuilderSeekByValueSmallBlock) {
  TestBinarySeekByValueSmallBlock(PLAIN_ENCODING);
}

// Test seeking to a value in a large block which contains
// many 'restarts'
TEST_F(TestEncoding, TestBinaryPrefixBlockBuilderSeekByValueLargeBlock) {
  TestStringSeekByValueLargeBlock(PREFIX_ENCODING);
}

TEST_F(TestEncoding, TestBinaryPlainBlockBuilderSeekByValueLargeBlock) {
  TestStringSeekByValueLargeBlock(PLAIN_ENCODING);
}

// Test round-trip encode/decode of a binary block.
TEST_F(TestEncoding, TestBinaryPrefixBlockBuilderRoundTrip) {
  TestBinaryBlockRoundTrip(PREFIX_ENCODING);
}

TEST_F(TestEncoding, TestBinaryPlainBlockBuilderRoundTrip) {
  TestBinaryBlockRoundTrip(PLAIN_ENCODING);
}

// Test empty block encode/decode
TEST_F(TestEncoding, TestBinaryPlainEmptyBlockEncodeDecode) {
  TestEmptyBlockEncodeDecode(BINARY, PLAIN_ENCODING);
}

TEST_F(TestEncoding, TestBinaryPrefixEmptyBlockEncodeDecode) {
  TestEmptyBlockEncodeDecode(BINARY, PREFIX_ENCODING);
}

// Test encode/decode of a binary block with various-sized truncations.
TEST_F(TestEncoding, TestBinaryPlainBlockBuilderTruncation) {
  TestBinaryBlockTruncation<BinaryPlainBlockDecoder>(PLAIN_ENCODING);
}

TEST_F(TestEncoding, TestBinaryPrefixBlockBuilderTruncation) {
  TestBinaryBlockTruncation<BinaryPrefixBlockDecoder>(PREFIX_ENCODING);
}

class IntEncodingTest : public TestEncoding, public ::testing::WithParamInterface<EncodingType> {
 public:
  template <DataType IntType>
  void DoIntSeekTest(int num_ints, int num_queries, bool verify) {
    DoSeekTest<IntType>(GetParam(), num_ints, num_queries, verify);
  }

  template <DataType IntType>
  void DoIntSeekTestTinyBlock() {
    for (int block_size = 1; block_size < 16; block_size++) {
      DoIntSeekTest<IntType>(block_size, 1000, true);
    }
  }

  template <DataType IntType>
  void DoIntRoundTripTest() {
    TestIntBlockRoundTrip<IntType>(GetParam());
  }
};
INSTANTIATE_TEST_CASE_P(Encodings, IntEncodingTest,
                        ::testing::Values(RLE, PLAIN_ENCODING, BIT_SHUFFLE));

TEST_P(IntEncodingTest, TestSeekAllTypes) {
  this->template DoIntSeekTest<UINT8>(100, 1000, true);
  this->template DoIntSeekTest<INT8>(100, 1000, true);
  this->template DoIntSeekTest<UINT16>(10000, 1000, true);
  this->template DoIntSeekTest<INT16>(10000, 1000, true);
  this->template DoIntSeekTest<UINT32>(10000, 1000, true);
  this->template DoIntSeekTest<INT32>(10000, 1000, true);
  this->template DoIntSeekTest<UINT64>(10000, 1000, true);
  this->template DoIntSeekTest<INT64>(10000, 1000, true);
  // TODO: Uncomment when adding 128 bit support to RLE (KUDU-2284)
  // this->template DoIntSeekTest<INT128>();
}

TEST_P(IntEncodingTest, IntSeekTestTinyBlockAllTypes) {
  this->template DoIntSeekTestTinyBlock<UINT8>();
  this->template DoIntSeekTestTinyBlock<INT8>();
  this->template DoIntSeekTestTinyBlock<UINT16>();
  this->template DoIntSeekTestTinyBlock<INT16>();
  this->template DoIntSeekTestTinyBlock<UINT32>();
  this->template DoIntSeekTestTinyBlock<INT32>();
  this->template DoIntSeekTestTinyBlock<UINT64>();
  this->template DoIntSeekTestTinyBlock<INT64>();
  // TODO: Uncomment when adding 128 bit support to RLE (KUDU-2284)
  // this->template DoIntSeekTestTinyBlock<INT128>();
}

TEST_P(IntEncodingTest, TestRoundTrip) {
  this->template DoIntRoundTripTest<UINT8>();
  this->template DoIntRoundTripTest<INT8>();
  this->template DoIntRoundTripTest<UINT16>();
  this->template DoIntRoundTripTest<INT16>();
  this->template DoIntRoundTripTest<UINT32>();
  this->template DoIntRoundTripTest<INT32>();
  this->template DoIntRoundTripTest<UINT64>();
  this->template DoIntRoundTripTest<INT64>();
  // TODO: Uncomment when adding 128 bit support to RLE (KUDU-2284)
  // this->template DoIntRoundTripTest<INT128>();
}

#ifdef NDEBUG
TEST_P(IntEncodingTest, IntSeekBenchmark) {
  this->template DoIntSeekTest<INT32>(32768, 10000, false);
}
#endif

} // namespace cfile
} // namespace kudu
