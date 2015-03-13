// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/integration-tests/partition_simulator.h"

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

using strings::Substitute;

namespace kudu {

namespace {

Status Call(const string& cmdline) {
  int rc = Subprocess::Call(cmdline);
  if (rc != 0) {
    return Status::IOError("Unable to execute command", cmdline, rc);
  }
  return Status::OK();
}

} // anonymous namespace

PartitionSimulator::PartitionSimulator() {
  WARN_NOT_OK(Call("/usr/bin/sudo -n iptables -N kudu-drop"),
              "Unable to create kudu-drop chain");
  WARN_NOT_OK(Call("/usr/bin/sudo -n iptables -A kudu-drop -j DROP"),
              "Unable to set up kudu-drop chain");
}

PartitionSimulator::~PartitionSimulator() {
  ignore_result(Call("/usr/bin/sudo -n iptables -F kudu-drop"));
  ignore_result(Call("/usr/bin/sudo -n iptables -X kudu-drop"));
}

Status PartitionSimulator::DropPackets(ExternalTabletServer* ts) {
  string addr = ts->bound_rpc_hostport().host();

  RETURN_NOT_OK(Call(Substitute("/usr/bin/sudo -n iptables -A INPUT -s $0 ! -d 127.0.0.1 -j kudu-drop", addr)));
  RETURN_NOT_OK(Call(Substitute("/usr/bin/sudo -n iptables -A OUTPUT -d $0 ! -s 127.0.0.1 -j kudu-drop", addr)));
  return Status::OK();
}

Status PartitionSimulator::RestorePackets(ExternalTabletServer* ts) {
  string addr = ts->bound_rpc_hostport().host();


  RETURN_NOT_OK(Call(Substitute("/usr/bin/sudo -n iptables -D INPUT -s $0 ! -d 127.0.0.1 -j kudu-drop", addr)));
  RETURN_NOT_OK(Call(Substitute("/usr/bin/sudo -n iptables -D OUTPUT -d $0 ! -s 127.0.0.1 -j kudu-drop", addr)));
  return Status::OK();
}

} // namespace kudu
