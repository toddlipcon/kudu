// Copyright (c) 2014, Cloudera, inc.

#include "kudu/cfile/cfile-test-base.h"

namespace kudu {
namespace cfile {

// A little C++ magic -- we can parameterize gtests by type,
// but not by static value. So, we make a little wrapper class
// which holds a DataType.
template<DataType type>
struct TypeContainer {
  static const DataType kType;
};
template<DataType type>
const DataType TypeContainer<type>::kType = type;


typedef ::testing::Types<
  TypeContainer<UINT8>,
  TypeContainer<INT8>,
  TypeContainer<UINT16>,
  TypeContainer<INT16>,
  TypeContainer<UINT32>,
  TypeContainer<INT32>,
  TypeContainer<UINT64>,
  TypeContainer<INT64>
  > IntDataTypes;


template<class TypeContainer>
class CFileBenchmark : public CFileTestBase {
};
TYPED_TEST_CASE(CFileBenchmark, IntDataTypes);

TYPED_TEST(CFileBenchmark, Benchmark) {
  static int kNumRows = 100000;
  BlockId block_id;
  LOG_TIMING(INFO, "writing 100m ints") {
    LOG(INFO) << "Starting writefile";
    IntDataGenerator<TypeParam::kType, false> generator;
    ASSERT_NO_FATAL_FAILURE(
      this->WriteTestFile(&generator, GROUP_VARINT, NO_COMPRESSION, kNumRows,
                          CFileTestBase::NO_FLAGS, &block_id));
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M ints") {
    LOG(INFO) << "Starting readfile";
    size_t n = 0;
    ASSERT_NO_FATAL_FAILURE(
      TimeReadFile(this->fs_manager_.get(), block_id, &n));
    ASSERT_EQ(kNumRows, n);
    LOG(INFO) << "End readfile";
  }

}


} // namespace cfile
} // namespace kudu
