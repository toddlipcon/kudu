// Copyright (C) 2020 Cloudera, inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.


#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/tsdb/influx_wire_protocol.h"

namespace kudu {
namespace tsdb {

class InfluxWireProtocolTest : public KuduTest {
 public:

};

const char* kTestBatch = R"(
cpu,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch=x86,team=SF,service=19,service_version=1,service_environment=test usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38i 1451606400000000000
cpu,hostname=host_1,region=us-west-1,datacenter=us-west-1a,rack=41,os=Ubuntu15.10,arch=x64,team=NYC,service=9,service_version=1,service_environment=staging usage_user=84i,usage_system=11i,usage_idle=53i,usage_nice=87i,usage_iowait=29i,usage_irq=20i,usage_softirq=54i,usage_steal=77i,usage_guest=53i,usage_guest_nice=74i 1451606400000000000
cpu,hostname=host_2,region=sa-east-1,datacenter=sa-east-1a,rack=89,os=Ubuntu16.04LTS,arch=x86,team=LON,service=13,service_version=0,service_environment=staging usage_user=29i,usage_system=48i,usage_idle=5i,usage_nice=63i,usage_iowait=17i,usage_irq=52i,usage_softirq=60i,usage_steal=49i,usage_guest=93i,usage_guest_nice=1i 1451606400000000000
cpu,hostname=host_3,region=us-west-2,datacenter=us-west-2b,rack=12,os=Ubuntu15.10,arch=x64,team=CHI,service=18,service_version=1,service_environment=production usage_user=8i,usage_system=21i,usage_idle=89i,usage_nice=78i,usage_iowait=30i,usage_irq=81i,usage_softirq=33i,usage_steal=24i,usage_guest=24i,usage_guest_nice=82i 1451606400000000000
cpu,hostname=host_4,region=sa-east-1,datacenter=sa-east-1c,rack=74,os=Ubuntu16.10,arch=x86,team=SF,service=7,service_version=0,service_environment=staging usage_user=2i,usage_system=26i,usage_idle=64i,usage_nice=6i,usage_iowait=38i,usage_irq=20i,usage_softirq=71i,usage_steal=19i,usage_guest=40i,usage_guest_nice=54i 1451606400000000000
cpu,hostname=host_5,region=us-west-1,datacenter=us-west-1b,rack=18,os=Ubuntu16.10,arch=x64,team=CHI,service=14,service_version=0,service_environment=staging usage_user=76i,usage_system=40i,usage_idle=63i,usage_nice=7i,usage_iowait=81i,usage_irq=20i,usage_softirq=29i,usage_steal=55i,usage_guest=20i,usage_guest_nice=15i 1451606400000000000
cpu,hostname=host_6,region=ap-southeast-1,datacenter=ap-southeast-1b,rack=49,os=Ubuntu16.10,arch=x86,team=CHI,service=7,service_version=0,service_environment=staging usage_user=44i,usage_system=70i,usage_idle=20i,usage_nice=67i,usage_iowait=65i,usage_irq=11i,usage_softirq=7i,usage_steal=92i,usage_guest=0i,usage_guest_nice=31i 1451606400000000000
cpu,hostname=host_7,region=eu-west-1,datacenter=eu-west-1c,rack=44,os=Ubuntu16.10,arch=x64,team=LON,service=7,service_version=1,service_environment=test usage_user=92i,usage_system=35i,usage_idle=99i,usage_nice=9i,usage_iowait=31i,usage_irq=1i,usage_softirq=2i,usage_steal=24i,usage_guest=96i,usage_guest_nice=69i 1451606400000000000
cpu,hostname=host_8,region=eu-west-1,datacenter=eu-west-1a,rack=17,os=Ubuntu16.04LTS,arch=x64,team=LON,service=2,service_version=0,service_environment=test usage_user=21i,usage_system=77i,usage_idle=90i,usage_nice=83i,usage_iowait=41i,usage_irq=84i,usage_softirq=26i,usage_steal=60i,usage_guest=43i,usage_guest_nice=36i 1451606400000000000
cpu,hostname=host_9,region=ap-southeast-2,datacenter=ap-southeast-2a,rack=0,os=Ubuntu16.04LTS,arch=x86,team=CHI,service=18,service_version=0,service_environment=production usage_user=90i,usage_system=0i,usage_idle=81i,usage_nice=28i,usage_iowait=25i,usage_irq=44i,usage_softirq=8i,usage_steal=89i,usage_guest=11i,usage_guest_nice=76i 1451606400000000000
cpu,hostname=host_10,region=sa-east-1,datacenter=sa-east-1a,rack=95,os=Ubuntu16.10,arch=x64,team=LON,service=8,service_version=0,service_environment=staging usage_user=76i,usage_system=85i,usage_idle=24i,usage_nice=0i,usage_iowait=44i,usage_irq=88i,usage_softirq=90i,usage_steal=40i,usage_guest=72i,usage_guest_nice=63i 1451606400000000000
cpu,hostname=host_11,region=us-west-2,datacenter=us-west-2b,rack=66,os=Ubuntu15.10,arch=x64,team=NYC,service=6,service_version=1,service_environment=production usage_user=80i,usage_system=11i,usage_idle=50i,usage_nice=72i,usage_iowait=52i,usage_irq=18i,usage_softirq=68i,usage_steal=88i,usage_guest=54i,usage_guest_nice=50i 1451606400000000000
cpu,hostname=host_12,region=eu-central-1,datacenter=eu-central-1a,rack=79,os=Ubuntu16.10,arch=x86,team=CHI,service=6,service_version=1,service_environment=production usage_user=64i,usage_system=81i,usage_idle=55i,usage_nice=54i,usage_iowait=89i,usage_irq=81i,usage_softirq=69i,usage_steal=33i,usage_guest=53i,usage_guest_nice=25i 1451606400000000000
cpu,hostname=host_13,region=eu-west-1,datacenter=eu-west-1a,rack=79,os=Ubuntu16.10,arch=x64,team=CHI,service=19,service_version=1,service_environment=staging usage_user=48i,usage_system=0i,usage_idle=64i,usage_nice=91i,usage_iowait=13i,usage_irq=88i,usage_softirq=79i,usage_steal=41i,usage_guest=48i,usage_guest_nice=4i 1451606400000000000
cpu,hostname=host_14,region=us-west-1,datacenter=us-west-1b,rack=8,os=Ubuntu15.10,arch=x64,team=LON,service=3,service_version=0,service_environment=production usage_user=81i,usage_system=47i,usage_idle=98i,usage_nice=33i,usage_iowait=59i,usage_irq=71i,usage_softirq=81i,usage_steal=5i,usage_guest=97i,usage_guest_nice=47i 1451606400000000000
)";

// Test that two subsequent time reads are monotonically increasing.
TEST_F(InfluxWireProtocolTest, BenchmarkParse) {

  InfluxBatch b;
  for (int i = 0; i < 10000; i++) {
    CHECK_OK(b.Parse(kTestBatch));
  }
}

TEST_F(InfluxWireProtocolTest, TestParse) {

  InfluxBatch b;
  CHECK_OK(b.Parse(kTestBatch));
  ASSERT_GT(b.measurements.size(), 1);
  auto& m = b.measurements[0];
  // cpu,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch=x86,team=SF,service=19,service_version=1,service_environment=test usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38i 1451606400000000000
  ASSERT_EQ(m.metric_name, "cpu");
  ASSERT_EQ(m.timestamp_us, 1451606400000000);
  ASSERT_EQ(m.tags.size(), 10);
  ASSERT_EQ(m.tags[0].first, "hostname");
  ASSERT_EQ(m.tags[0].second, "host_0");
  ASSERT_EQ(m.tags[1].first, "region");
  ASSERT_EQ(m.tags[1].second, "eu-central-1");
  ASSERT_EQ(m.fields[0].first, "usage_user");
  ASSERT_EQ(boost::get<int64_t>(m.fields[0].second), 58);
  ASSERT_EQ(m.fields[9].first, "usage_guest_nice");
  ASSERT_EQ(boost::get<int64_t>(m.fields[9].second), 38);
}

}  // namespace tsdb
}  // namespace kudu

