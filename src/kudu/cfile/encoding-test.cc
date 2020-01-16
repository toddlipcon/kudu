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
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/binary_plain_block.h"
#include "kudu/cfile/binary_prefix_block.h"
#include "kudu/cfile/block_encodings.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/type_encodings.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
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

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace cfile {

class TestEncoding : public KuduTest {
 public:
  TestEncoding()
    : arena_(1024) {
  }

 protected:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    arena_.Reset();
    default_write_options_.storage_attributes.cfile_block_size = 256 * 1024;
  }

  template<DataType type>
  void CopyOne(BlockDecoder *decoder,
               typename TypeTraits<type>::cpp_type *ret) {
    ColumnBlock cb(GetTypeInfo(type), nullptr, ret, 1, &arena_);
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

  static unique_ptr<BlockDecoder> CreateBlockDecoderOrDie(
      DataType type, EncodingType encoding, Slice s) {
    const TypeEncodingInfo* tei;
    CHECK_OK(TypeEncodingInfo::Get(GetTypeInfo(type), encoding, &tei));
    unique_ptr<BlockDecoder> bd;
    CHECK_OK(tei->CreateBlockDecoder(&bd, s, /*iter=*/nullptr));
    return bd;
  }

  // Insert a given number of strings into the provided BlockBuilder.
  //
  // The strings are generated using the provided 'formatter' function.
  Slice CreateBinaryBlock(BlockBuilder *sbb,
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

  // Concatenate the given slices into 'contiguous_buf_' and return a new slice.
  Slice MakeContiguous(const vector<Slice>& slices) {
    if (slices.size() == 1) {
      return slices[0];
    }
    // Concat the slices into a local buffer, since the block decoders and
    // tests expect contiguous data.
    contiguous_buf_.clear();
    for (const auto& s : slices) {
      contiguous_buf_.append(s.data(), s.size());
    }

    return Slice(contiguous_buf_);
  }

  Slice FinishAndMakeContiguous(BlockBuilder* b, int ord_val) {
    vector<Slice> slices;
    b->Finish(ord_val, &slices);
    return MakeContiguous(slices);
  }

  void TestBinarySeekByValueSmallBlock(EncodingType encoding) {
    auto bb = CreateBlockBuilderOrDie(BINARY, encoding);
    // Insert "hello 0" through "hello 9"
    const int kCount = 10;
    Slice s = CreateBinaryBlock(
        bb.get(), kCount, std::bind(StringPrintf, "hello %d", std::placeholders::_1));

    auto sbd = CreateBlockDecoderOrDie(BINARY, encoding, s);
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
    Slice s = CreateBinaryBlock(
        sbb.get(), kCount, std::bind(StringPrintf, "hello %03d", std::placeholders::_1));
    auto sbd = CreateBlockDecoderOrDie(BINARY, encoding, s);;
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
    const int kCount = r.Uniform(1000) + 1;
    Slice s = CreateBinaryBlock(sbb.get(), kCount, GenTestString);

    LOG(INFO) << "Block: " << HexDump(s);

    // The encoded data should take at least 1 byte per entry.
    ASSERT_GT(s.size(), kCount);

    // Check first/last keys
    Slice key;
    ASSERT_OK(sbb->GetFirstKey(&key));
    ASSERT_EQ(GenTestString(0), key);
    ASSERT_OK(sbb->GetLastKey(&key));
    ASSERT_EQ(GenTestString(kCount - 1), key);

    auto sbd = CreateBlockDecoderOrDie(BINARY, encoding, s);
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
    for (int i = kCount - 1; i >= 0; i--) {
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

    const CppType kBase =
        std::numeric_limits<CppType>::is_signed ? -6 : 6;

    CppType data[num_ints];
    for (CppType i = 0; i < num_ints; i++) {
      data[i] = kBase + i * 2;
    }
    const CppType max_seek_target = data[num_ints - 1] + 1;

    auto ibb = CreateBlockBuilderOrDie(IntType, encoding);
    CHECK_EQ(num_ints, ibb->Add(reinterpret_cast<uint8_t *>(&data[0]),
                               num_ints));
    Slice s = FinishAndMakeContiguous(ibb.get(), 0);
    LOG(INFO) << "Created " << TypeTraits<IntType>::name() << " block with " << num_ints << " ints"
              << " (" << s.size() << " bytes)";
    auto ibd = CreateBlockDecoderOrDie(IntType, encoding, s);
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
    Slice s = FinishAndMakeContiguous(bb.get(), 0);
    ASSERT_GT(s.size(), 0);
    LOG(INFO) << "Encoded size for 0 items: " << s.size();

    auto bd = CreateBlockDecoderOrDie(type, encoding, s);
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
    Slice s = FinishAndMakeContiguous(bb.get(), kOrdinalPosBase);

    LOG(INFO)<< "Encoded size for " << size << " elems: " << s.size();

    auto bd = CreateBlockDecoderOrDie(Type, encoding, s);
    ASSERT_OK(bd->ParseHeader());
    ASSERT_EQ(kOrdinalPosBase, bd->GetFirstRowId());
    ASSERT_EQ(0, bd->GetCurrentIndex());

    vector<CppType> decoded;
    decoded.resize(size);

    ColumnBlock dst_block(GetTypeInfo(Type), nullptr, &decoded[0], size, &arena_);
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

    Slice s = CreateBinaryBlock(
        sbb.get(), kCount, std::bind(StringPrintf, "hello %d", std::placeholders::_1));
    do {
      sbsize = s.size();

      LOG(INFO) << "Block: " << HexDump(s);

      auto sbd = CreateBlockDecoderOrDie(BINARY, encoding, s);
      Status st = sbd->ParseHeader();

      if (sbsize < DecoderType::kMinHeaderSize) {
        ASSERT_TRUE(st.IsCorruption());
        ASSERT_STR_CONTAINS(st.ToString(), "not enough bytes for header");
      } else if (sbsize < coding::DecodeGroupVarInt32_GetGroupSize(s.data())) {
        ASSERT_TRUE(st.IsCorruption());
        ASSERT_STR_CONTAINS(st.ToString(), "less than length");
      }
      if (sbsize > 0) {
        s.truncate(sbsize - 1);
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
    for (int i = 0; i < 2; i++) {
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
    Slice s = FinishAndMakeContiguous(ibb.get(), kOrdinalPosBase);

    // Check GetFirstKey() and GetLastKey().
    CppType key;
    ASSERT_OK(ibb->GetFirstKey(&key));
    ASSERT_EQ(to_insert.front(), key);
    ASSERT_OK(ibb->GetLastKey(&key));
    ASSERT_EQ(to_insert.back(), key);

    auto ibd = CreateBlockDecoderOrDie(IntType, encoding, s);
    ASSERT_OK(ibd->ParseHeader());

    ASSERT_EQ(kOrdinalPosBase, ibd->GetFirstRowId());

    vector<CppType> decoded;
    decoded.resize(to_insert.size());

    ColumnBlock dst_block(GetTypeInfo(IntType), nullptr,
                          &decoded[0],
                          to_insert.size(),
                          &arena_);
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

    ASSERT_THAT(decoded, testing::ElementsAreArray(to_insert));

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
      int run_size = random() % 100;
      bool val = random() % 2;
      for (int j = 0; j < run_size; j++) {
        to_insert.push_back(val);
      }
      i += run_size;
    }

    auto bb = CreateBlockBuilderOrDie(BOOL, encoding);
    bb->Add(reinterpret_cast<const uint8_t *>(&to_insert[0]),
            to_insert.size());
    Slice s = FinishAndMakeContiguous(bb.get(), kOrdinalPosBase);

    auto bd = CreateBlockDecoderOrDie(BOOL, encoding, s);
    ASSERT_OK(bd->ParseHeader());

    ASSERT_EQ(kOrdinalPosBase, bd->GetFirstRowId());

    vector<uint8_t> decoded;
    decoded.resize(to_insert.size());

    ColumnBlock dst_block(GetTypeInfo(BOOL), nullptr,
                          &decoded[0],
                          to_insert.size(),
                          &arena_);

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

      EXPECT_EQ((uint32_t)(seek_off), bd->GetCurrentIndex());
      bool ret;
      CopyOne<BOOL>(bd.get(), &ret);
      EXPECT_EQ(static_cast<bool>(decoded[seek_off]), ret);
    }
  }

  Arena arena_;
  faststring contiguous_buf_;
  WriterOptions default_write_options_;
};

TEST_F(TestEncoding, TestPlainBlockEncoder) {
  const uint32_t kSize = 10000;

  gscoped_ptr<int32_t[]> ints(new int32_t[kSize]);
  for (int i = 0; i < kSize; i++) {
    ints.get()[i] = random();
  }

  TestEncodeDecodeTemplateBlockEncoder<INT32>(ints.get(), kSize, PLAIN_ENCODING);
}

template<typename INT>
static vector<INT> RandInts(int count, INT min_val, INT max_val) {
  Random rng(SeedRandom());
  vector<INT> ints(count);
  for (auto& v : ints) {
    v = min_val + rng.Uniform64(max_val - min_val);
  }
  return ints;
}

// Test for bitshuffle/bp128 blocks, for INT32, INT64, INT128, FLOAT, DOUBLE
TEST_F(TestEncoding, TestInt32BlockEncoders) {
  using limits = std::numeric_limits<int32_t>;
  auto sequences = {
    RandInts<int32_t>(10000, 1000, 2000),
    RandInts<int32_t>(10000, 0, limits::max()),
    RandInts<int32_t>(10000, limits::min(), limits::max())
  };
  for (auto encoding : {BIT_SHUFFLE, BP128}) {
    for (const auto& ints : sequences) {
      TestEncodeDecodeTemplateBlockEncoder<INT32>(ints.data(), ints.size(), BIT_SHUFFLE);
    }
  }
}

TEST_F(TestEncoding, TestInt64BlockEncoders) {
  using limits = std::numeric_limits<int64_t>;
  auto sequences = {
    RandInts<int64_t>(10000, 1000, 2000),
    RandInts<int64_t>(10000, 0, limits::max()),
    RandInts<int64_t>(10000, limits::min(), limits::max())
  };
  for (auto encoding : {BIT_SHUFFLE, BP128}) {
    for (const auto& ints : sequences) {
      TestEncodeDecodeTemplateBlockEncoder<INT64>(ints.data(), ints.size(), BIT_SHUFFLE);
    }
  }
}

TEST_F(TestEncoding, TestInt128BlockEncoders) {
  using limits = std::numeric_limits<int128_t>;
  auto sequences = {
    RandInts<int128_t>(10000, 1000, 2000),
    // RNG doesn't support full 128-bit range, so just generate some low and high numbers.
    RandInts<int128_t>(10000, limits::max() - 10000, limits::max()),
    RandInts<int128_t>(10000, limits::min(), limits::min() + 10000)
  };
  for (auto encoding : {BIT_SHUFFLE, BP128}) {
    for (const auto& ints : sequences) {
      TestEncodeDecodeTemplateBlockEncoder<INT128>(ints.data(), ints.size(), BIT_SHUFFLE);
    }
  }
}

TEST_F(TestEncoding, TestBShufFloatBlockEncoder) {
  const uint32_t kSize = 10000;

  vector<float> floats(kSize);
  for (auto& f : floats) {
    f = static_cast<float>(random()) + static_cast<float>(random())/INT_MAX;
  }

  TestEncodeDecodeTemplateBlockEncoder<FLOAT>(floats.data(), kSize, BIT_SHUFFLE);
}

TEST_F(TestEncoding, TestBShufDoubleBlockEncoder) {
  const uint32_t kSize = 10000;

  vector<double> doubles(kSize);
  for (auto& f : doubles) {
    f = static_cast<double>(random()) + static_cast<double>(random())/INT_MAX;
  }

  TestEncodeDecodeTemplateBlockEncoder<DOUBLE>(doubles.data(), kSize, BIT_SHUFFLE);
}

TEST_F(TestEncoding, TestRleIntBlockEncoder) {
  auto ibb = CreateBlockBuilderOrDie(UINT32, RLE);
  auto ints = RandInts<uint32_t>(10000, 0, std::numeric_limits<uint32_t>::max());
  ibb->Add(reinterpret_cast<const uint8_t *>(ints.data()), 10000);

  Slice s = FinishAndMakeContiguous(ibb.get(), 12345);
  LOG(INFO) << "RLE Encoded size for 10k ints: " << s.size();

  ibb->Reset();
  ints.resize(100);
  for (int i = 0; i < 100; i++) {
    ints[i] = 0;
  }
  ibb->Add(reinterpret_cast<const uint8_t *>(ints.data()), 100);
  s = FinishAndMakeContiguous(ibb.get(), 12345);
  ASSERT_EQ(14UL, s.size());
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
                        ::testing::Values(RLE, PLAIN_ENCODING, BIT_SHUFFLE, BP128));

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
