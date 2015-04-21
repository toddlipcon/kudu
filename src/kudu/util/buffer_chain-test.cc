// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <vector>

#include "kudu/util/buffer_chain.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/test_util.h"

using std::vector;

namespace kudu {

class BufferChainTest : public KuduTest {

 protected:

  // Check that the BufferChain implementation yields the same results
  // as the faststring implementation.
  void Check() {
    int expected_size = check_buffer_.size();
    ASSERT_EQ(expected_size, chain_.size());

    // Verify that the size of the returned slices matches the size()
    // value.
    vector<Slice> slices;
    chain_.GetSlices(&slices);

    faststring returned;
    BOOST_FOREACH(const Slice& s, slices) {
      returned.append(s.data(), s.size());
    }
    ASSERT_EQ(expected_size, returned.size());
    ASSERT_EQ(0, Slice(returned).compare(check_buffer_));
  }

  void AppendAndCheck(const Slice& s) {
    chain_.append(s);
    check_buffer_.append(s.data(), s.size());
    Check();
  }


  BufferChain chain_;

  // Regular faststring to compare against. We append everything to both
  // 'chain_' and 'check_buffer_' and then verify that the results are
  // equivalent.
  faststring check_buffer_;
};


TEST_F(BufferChainTest, TestSimple) {
  AppendAndCheck("12345");
  AppendAndCheck("6789");
}

TEST_F(BufferChainTest, TestRandom) {
  SeedRandom();
  Random r(rand());

  // Append 1000 random strings with lengths uniformly distributed between
  // 0 and 1024 bytes. This ensures that we test multiple buffers inside
  // the BufferChain.
  for (int i = 0; i < 1000; i++) {
    char buf[1024];
    int len = r.Uniform(arraysize(buf));
    RandomString(buf, len, &r);
    AppendAndCheck(Slice(buf, len));
  }
}

} // namespace kudu
