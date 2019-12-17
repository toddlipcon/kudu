#include <stdlib.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tsdb/series_id.h"
#include "kudu/tsdb/metrics_store.h"
#include "kudu/tsdb/series_store.h"
#include "kudu/tsdb/ql/analysis.h"
#include "kudu/tsdb/ql/expr.h"
#include "kudu/tsdb/ql/influxql.h"
#include "kudu/tsdb/ql/planner.h"
#include "kudu/tsdb/ql/qcontext.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/test_macros.h"
#include <gtest/gtest.h>

using kudu::client::KuduColumnSchema;
using std::string;
using std::pair;
using std::vector;
using std::unordered_map;
using std::unordered_set;
using strings::Substitute;

namespace kudu {
namespace tsdb {
namespace influxql {

class MockSeriesStore : public SeriesStore {
 public:
  void AddSeries(string measurement_name,
                 unordered_map<string, string> tags,
                 int32_t id) {
    mock_series_.emplace_back(std::move(measurement_name),
                              std::move(tags),
                              id);
  }

  // Find all series which have the given tag key/value pair.
  Status FindMatchingSeries(StringPiece measurement_name,
                            StringPiece tag_key,
                            StringPiece tag_value,
                            std::vector<int32_t>* ids) override {
    string tag_key_s = tag_key.ToString();

    for (const auto& s : mock_series_) {
      if (s.measurement_name == measurement_name &&
          FindWithDefault(s.tags, tag_key_s, "") == tag_value) {
        ids->push_back(s.id);
      }
    }

    return Status::OK();
  }


  virtual Status FindAllSeries(StringPiece measurement_name,
                               const std::vector<string>& fetch_tags,
                               std::vector<SeriesIdWithTags>* results) override {
    for (const auto& s : mock_series_) {
      if (s.measurement_name == measurement_name) {
        SeriesIdWithTags::TagMap tags;
        for (const auto& t : fetch_tags) {
          tags.emplace(t, FindWithDefault(s.tags, t, ""));
        }
        results->emplace_back(s.id, std::move(tags));
      }
    }
    return Status::OK();
  }

 private:
  struct MockSeries {
    using TagMap = unordered_map<string, string>;

    MockSeries(string measurement_name, TagMap tags, int32_t id)
        : measurement_name(std::move(measurement_name)),
          tags(std::move(tags)),
          id(id) {
    }

    string measurement_name;
    unordered_map<string, string> tags;
    int32_t id;
  };
  vector<MockSeries> mock_series_;
};

class MockColumnSource : public MetricsColumnSource {
 public:
  Status GetColumnsForMeasurement(
      StringPiece measurement,
      std::map<std::string, KuduColumnSchema::DataType>* cols) override {
    *cols = {{"usage_user", KuduColumnSchema::DataType::INT64},
             {"usage_guest", KuduColumnSchema::DataType::INT64},
             {"usage_system", KuduColumnSchema::DataType::INT64}};
    return Status::OK();
  }
};

class InfluxQLTest : public KuduTest {
 public:
  enum MockSeriesIds : int32_t {
    kHost1 = 1,
    kHost2,
    kHost3
  };

  InfluxQLTest()
      : ctx_(&mock_series_store_, &mock_column_source_),
        parser_(&ctx_),
        analyzer_(&ctx_) {
    mock_series_store_.AddSeries(
        "cpu",
        {{"hostname", "host1"},
         {"rack", "rack1"}},
        kHost1);
    mock_series_store_.AddSeries(
        "cpu",
        {{"hostname", "host2"},
         {"rack", "rack1"}},
        kHost2);
    mock_series_store_.AddSeries(
        "cpu",
        {{"hostname", "host3"},
         {"rack", "rack2"}},
        kHost3);
  }

 protected:
  Status ParseAndAnalyze(const string& query, AnalyzedSelectStmt** asel) {
    SelectStmt* sel;
    RETURN_NOT_OK(parser_.ParseSelectStatement(query, &sel));
    return analyzer_.AnalyzeSelectStmt(sel, asel);
  }

  MockSeriesStore mock_series_store_;
  MockColumnSource mock_column_source_;
  QContext ctx_;
  Parser parser_;
  Analyzer analyzer_;
};

TEST_F(InfluxQLTest, TestParser) {
  SelectStmt* sel;
  ASSERT_OK(parser_.ParseSelectStatement("select foo + bar - baz from cpu where time < 100 and time > 10;", &sel));
  ASSERT_OK(parser_.ParseSelectStatement(
      "select max(usage_user),max(usage_system) from cpu "
      "where (hostname = 'host_9') and "
      "time >= '2016-01-01T02:16:22Z' and "
      "time <= '2016-01-01T10:16:22Z' and "
      "usage_user > 90.0 "
      "group by time(1m);", &sel));
  ASSERT_EQ("((hostname = 'host_9') AND "
            "(time >= '2016-01-01T02:16:22Z') AND "
            "(time <= '2016-01-01T10:16:22Z') AND "
            "(usage_user > 90))",
            sel->where_->ToQL());

}

TEST_F(InfluxQLTest, TestAnalysis) {
  SelectStmt* sel;
  ASSERT_OK(parser_.ParseSelectStatement("select foo + bar - baz from cpu where "
                                         "time < 100 and time > 10 "
                                         "and (hostname = 'host1' or hostname = 'host2') "
                                         "and usage_user > 123;", &sel));
  AnalyzedSelectStmt asel(sel);
  ASSERT_OK(analyzer_.AnalyzeFromClause(&asel));
  ASSERT_OK(analyzer_.AnalyzeWhereClause(&asel));
  std::string s;
  sel->where_->AppendToString("", &s);
  LOG(INFO) << s;

  ASSERT_OK(analyzer_.PlacePredicates(&asel));
  ASSERT_EQ(1, asel.predicates->tag.size());
  ASSERT_EQ("((hostname = 'host1') OR (hostname = 'host2'))", asel.predicates->tag[0]->ToQL());

  ASSERT_EQ(2, asel.predicates->time.size());
  EXPECT_EQ("(time < 100)", asel.predicates->time[0]->ToQL());
  EXPECT_EQ("(time > 10)", asel.predicates->time[1]->ToQL());

  ASSERT_EQ(1, asel.predicates->metric.size());
  EXPECT_EQ("(usage_user > 123)", asel.predicates->metric[0]->ToQL());

  ASSERT_OK(analyzer_.AnalyzeTimeRange(&asel));
  ASSERT_EQ(11, *asel.time_range->min_us);
  ASSERT_EQ(99, *asel.time_range->max_us);
}


TEST_F(InfluxQLTest, TestAnalyzeSelectList) {
  const vector<std::tuple<string,string,vector<StringPiece>>> kCases = {
    {"select max(usage_user) from cpu",
     "OK",
     {"usage_user"}},

    {"select max(usage_user + usage_guest) from cpu",
     "OK",
     {"usage_user", "usage_guest"}},

    {"select max(usage_user) + max(usage_guest) from cpu",
     "OK",
     {"usage_user", "usage_guest"}},

    {"select max(usage_user + 1) from cpu",
     "OK",
     {"usage_user"}},

    {"select max(usage_user + 1) + 1 from cpu",
     "OK",
    {"usage_user"}},

    {"select max(usage_user, 1) from cpu",
     "Invalid argument: expected 1 argument: max",
     {}},

    {"select max(max(usage_user)) from cpu",
     "Invalid argument: aggregate function calls cannot be nested: max(max(usage_user))",
     {}},

    {"select max(1 + max(usage_user)) from cpu",
     "Invalid argument: aggregate function calls cannot be nested: max((1 + max(usage_user)))",
     {}},

    {"select time(2) from cpu",
     "Invalid argument: time() function may only be used in a GROUP BY clause",
     {}},

    {"select 1 from cpu where max(usage) > 99",
     "Invalid argument: aggregate function may only be in the SELECT list: max(usage)",
     {}},

    // two aggregates are OK.
    {"select max(usage_user), max(usage_guest) from cpu",
     "OK",
     {"usage_user", "usage_guest"}},

    // aggregate and non-aggregate are bad.
    // TODO(todd) need to implement influx "selector".
    {"select max(usage_user), usage_guest from cpu",
     "Invalid argument: field reference to usage_guest is not a grouped dimension",
     {}},

    // SELECT(*) expansion support
    {"select * from cpu",
     "OK",
     {"usage_guest", "usage_user", "usage_system"}},

    {"select *, usage_user from cpu",
     "OK",
     {"usage_guest", "usage_user", "usage_system"}},

    // TODO: select usage_user from cpu group by time(1m);
    // should give an error that group by must have an aggregate

  };

  for (const auto& test_case : kCases) {
    const auto& query = std::get<0>(test_case);
    const auto& expected_status = std::get<1>(test_case);
    const auto& expected_fields = std::get<2>(test_case);
    SCOPED_TRACE(query);

    AnalyzedSelectStmt* asel;
    Status s = ParseAndAnalyze(query, &asel);
    EXPECT_EQ(s.ToString(), expected_status);
    if (s.ok()) {
      EXPECT_THAT(*asel->selected_fields, testing::UnorderedElementsAreArray(expected_fields));
    }
  }
}

TEST_F(InfluxQLTest, TestParseTime) {
  int64_t us;
  ASSERT_TRUE(Analyzer::ParseRFC3339("2016-01-01T21:09:04Z", &us));
  ASSERT_EQ(1451682544000000L, us);
}

TEST_F(InfluxQLTest, TestAnalysisFailure) {
  AnalyzedSelectStmt* sel;
  Status s = ParseAndAnalyze("select foo from cpu where time > 10 or hostname = 'abc';", &sel);
  EXPECT_EQ(s.ToString(),
             "Invalid argument: cannot evaluate expression across metric values and tags: ((time > 10) OR (hostname = 'abc'))");

  s = ParseAndAnalyze("select foo from cpu where time(3m) > 10;", &sel);
  EXPECT_EQ(s.ToString(), "Invalid argument: time() function may only be used in a GROUP BY clause");
}

TEST_F(InfluxQLTest, TestAnalyzeDimensions) {
  AnalyzedSelectStmt* sel;
  ASSERT_OK(ParseAndAnalyze("select hostname from cpu group by time(10m), hostname;", &sel));
  ASSERT_EQ(1, sel->dimensions->tag_keys.size());
  EXPECT_EQ("hostname", sel->dimensions->tag_keys[0]);
  EXPECT_EQ(10L * 60 * 1000000, sel->dimensions->time_granularity_us.get());

  // TODO(todd) negative case coverage
}

TEST_F(InfluxQLTest, TestPlanSeriesSelectors) {
  const vector<std::tuple<string, string, string, string>> kCases = {
    {"select foo from cpu where hostname = 'host1'",
     "OK",
     "SingleTagSeriesSelector(cpu, hostname, [host1])",
     "{id=1, tags={}}"},

    // Same but with a grouping dimension
    {"select foo from cpu where hostname = 'host1' group by hostname",
     "OK",
     "SingleTagSeriesSelector(cpu, hostname, [host1])",
     "{id=1, tags={hostname=host1}}"},

    {"select foo from cpu where (hostname = 'host1' or hostname = 'host2')"
     " group by hostname",
     "OK",
     "SingleTagSeriesSelector(cpu, hostname, [host1,host2])",
     "{id=1, tags={hostname=host1}},{id=2, tags={hostname=host2}}"},

    // tag values should be made unique
    {"select foo from cpu where (hostname = 'host1' or hostname = 'host1')",
     "OK",
     "SingleTagSeriesSelector(cpu, hostname, [host1])",
     "{id=1, tags={}}"},


    {"select foo from cpu where hostname = 'host1' or rack = 'host2'",
     "unable to process disjunction across multiple tags",
     "",
     ""},

    {"select foo from cpu where hostname = 'host1' and hostname = 'host2'",
     ".*unable to process tag conjunction.*",
     "",
     ""},

    // Fetch all series with no tags.
    {"select foo from cpu",
     "OK",
     "AllSeriesSelector(cpu)",
     "{id=1, tags={}},{id=2, tags={}},{id=3, tags={}}"},

    // Fetch all series with tags.
    {"select foo from cpu group by hostname, rack",
     "OK",
     "AllSeriesSelector(cpu)",
     ("{id=1, tags={hostname=host1,rack=rack1}},"
      "{id=2, tags={hostname=host2,rack=rack1}},"
      "{id=3, tags={hostname=host3,rack=rack2}}")},
  };

  for (const auto& testcase : kCases) {
    const auto& query = std::get<0>(testcase);
    const auto& expected_status = std::get<1>(testcase);
    const auto& expected_sels = std::get<2>(testcase);
    const auto& expected_series = std::get<3>(testcase);
    SCOPED_TRACE(query);

    AnalyzedSelectStmt* sel;
    ASSERT_OK(ParseAndAnalyze(query, &sel));
    ASSERT_EQ(0, sel->predicates->metric.size());
    ASSERT_EQ(0, sel->predicates->time.size());

    Planner planner(&ctx_);
    SeriesSelector* series_sel = nullptr;
    Status s = planner.PlanSeriesSelector(sel, &series_sel);
    ASSERT_STR_MATCHES(s.ToString(), expected_status);
    if (s.ok()) {
      ASSERT_NE(series_sel, nullptr);
      ASSERT_EQ(expected_sels, series_sel->ToString());
      vector<SeriesIdWithTags> series;
      ASSERT_OK(series_sel->Execute(&ctx_, &series));
      EXPECT_EQ(expected_series, JoinMapped(series, std::mem_fn(&SeriesIdWithTags::ToString), ","));
    }
  }
}

TEST_F(InfluxQLTest, TestPlanAgg) {
  const vector<std::tuple<string, string>> kTestCases = {
    {"select max(usage_user), max(usage_guest) from cpu group by time(10m);",
     "OK"},

    {"select usage_user, usage_guest from cpu group by time(10m);",
     "OK"}
  };

  for (const auto& test_case : kTestCases) {
    const auto& query = std::get<0>(test_case);
    const auto& expected_status = std::get<1>(test_case);

    AnalyzedSelectStmt* sel;
    TSBlockConsumerFactory factory;
    Planner planner(&ctx_);
    ASSERT_OK(ParseAndAnalyze(query, &sel));
    Status s = planner.PlanSelectExpressions(sel, &factory);
    ASSERT_STR_MATCHES(s.ToString(), expected_status);
  }
}

} // namespace influxql
} // namespace tsdb
} // namespace kudu
