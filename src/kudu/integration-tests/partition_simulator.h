// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_INTEGRATION_TESTS_PARTITION_SIMULATOR_H
#define KUDU_INTEGRATION_TESTS_PARTITION_SIMULATOR_H

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class ExternalTabletServer;

class PartitionSimulator {
 public:
  PartitionSimulator();
  ~PartitionSimulator();

  Status DropPackets(ExternalTabletServer* ts);
  Status RestorePackets(ExternalTabletServer* ts);

 private:
  DISALLOW_COPY_AND_ASSIGN(PartitionSimulator);
};

} // namespace kudu
#endif /* KUDU_INTEGRATION_TESTS_PARTITION_SIMULATOR_H */
