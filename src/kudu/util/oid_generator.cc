// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/oid_generator.h"

#include <boost/thread/locks.hpp>
#include <string>

#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"

namespace kudu {

ObjectIdGenerator::ObjectIdGenerator()
  : r_(GetRandomSeed32()) {
}

string ObjectIdGenerator::Next() {
  boost::lock_guard<LockType> l(oid_lock_);

  //boost::uuids::uuid oid = oid_generator_();
  //const uint8_t *uuid = oid.data;
  uint8_t uuid[16];
  RandomString(uuid, arraysize(uuid), &r_);
  return StringPrintf("%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
               uuid[0], uuid[1], uuid[2], uuid[3], uuid[4], uuid[5], uuid[6], uuid[7],
               uuid[8], uuid[9], uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15]);
}

} // namespace kudu
