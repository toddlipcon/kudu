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

#include <unordered_set>
#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <iostream>

#include <libdivide.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/stl_logging.h>

#include "kudu/common/types.h"
#include "kudu/client/client.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/webserver.h"
#include "kudu/tsdb/influx_wire_protocol.h"
#include "kudu/tsdb/metrics_store.h"
#include "kudu/tsdb/ql/analysis.h"
#include "kudu/tsdb/ql/expr.h"
#include "kudu/tsdb/ql/planner.h"
#include "kudu/tsdb/ql/influxql.h"
#include "kudu/tsdb/ql/qcontext.h"
#include "kudu/tsdb/series_id.h"
#include "kudu/tsdb/series_store.h"
#include "kudu/util/flags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/init.h"
#include "kudu/util/promise.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/version_info.h"

using std::pair;
using std::unique_ptr;
using std::string;
using std::shared_ptr;
using std::map;
using std::vector;

using kudu::client::KuduClient;
using kudu::client::KuduError;
using kudu::client::KuduSession;
using kudu::client::KuduClientBuilder;

DECLARE_int32(webserver_port);

DEFINE_int32(parallel_query_threads, 1,
             "how many threads to use for executing queries");

namespace kudu {
namespace tsdb {

using influxql::TSBlock;

class BucketAggregator {
 public:
  BucketAggregator(int64_t start_time,
                   int64_t end_time,
                   int64_t granularity)
      : granularity_(granularity),
        divider_(granularity),
        base_bucket_(start_time / granularity),
        end_bucket_(end_time / granularity) {


    bucket_times_.resize(num_buckets());
    for (int i = 0; i < num_buckets(); i++) {
      bucket_times_[i] = (i + base_bucket_) * granularity_;
    }
  }

  template<class T>
  void MergeAgg(const std::vector<int64_t>& src_times,
                const std::vector<T>& src_vals,
                std::vector<T>* dst_vals) {
    dst_vals->resize(num_buckets());
    DoMergeAgg(src_times.data(), src_vals.data(), dst_vals->data(), src_times.size());
  }

  const vector<int64_t>& bucket_times() const {
    return bucket_times_;
  }
  
 private:
  template<class T>
  void DoMergeAgg(const int64_t* __restrict__ src_times,
                  const T* __restrict__ src_vals,
                  T* __restrict__ dst_vals,
                  int n) {
#pragma unroll(4)
    for (int i = 0; i < n; i++) {
      int64_t bucket = src_times[i] / divider_ - base_bucket_;
      dst_vals[bucket] = std::max(dst_vals[bucket], src_vals[i]);
    }
  }


  
  int num_buckets() const {
    return end_bucket_ - base_bucket_ + 1;
  }

  const int64_t granularity_;
  const libdivide::divider<int64_t> divider_;

  const int64_t base_bucket_;
  const int64_t end_bucket_;

  vector<int64_t> bucket_times_;
};

class Server {
 public:
  Server()
      : rng_(123) {
    for (int i = 0; i < 4000; i++) {
      all_hosts_.emplace_back(strings::Substitute("host_$0", i));
    }
  }
  
  Status Start() {
    RETURN_NOT_OK(ThreadPoolBuilder("tsdb-query")
                  .set_max_threads(FLAGS_parallel_query_threads)
                  .Build(&pool_));

    RETURN_NOT_OK(KuduClientBuilder()
                  .master_server_addrs({"localhost"}) // TODO  flag
                  .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
                  .Build(&client_));

    series_store_.reset(new SeriesStoreImpl(client_));
    RETURN_NOT_OK(series_store_->Init());

    metrics_store_.reset(new MetricsStore(client_));
    RETURN_NOT_OK(metrics_store_->Init());

    WebserverOptions opts;
    opts.port = FLAGS_webserver_port;
    opts.enable_doc_root = false;
    webserver_.reset(new Webserver(opts));

    bool is_styled = false;
    bool is_on_nav_bar = false;
    webserver_->RegisterPrerenderedPathHandler(
        "/write", "",
        [this](const Webserver::WebRequest& req,
               Webserver::PrerenderedWebResponse* resp) {
          this->HandleInfluxPut(req, resp);
        },
        is_styled, is_on_nav_bar);
    webserver_->RegisterPrerenderedPathHandler(
        "/query", "",
        [this](const Webserver::WebRequest& req,
               Webserver::PrerenderedWebResponse* resp) {
          this->HandleInfluxQuery(req, resp);
        },
        is_styled, is_on_nav_bar);

    RETURN_NOT_OK(webserver_->Start());
    return Status::OK();
  }

 private:

  void HandleInfluxQuery(const Webserver::WebRequest& req,
                       Webserver::PrerenderedWebResponse* resp) {
    Status s = DoHandleInfluxQuery(req, resp);
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
      resp->status_code = HttpStatusCode::InternalServerError;
      resp->output << s.ToString();
      return;
    }
  }

  std::pair<int64_t, int64_t> get_time_range(MonoDelta range_width) {
    //Python: (dateutil.parser.parse("2016-01-01T00:00:00") - datetime.datetime(1970,1,1)).total_seconds()*1000000
    const int64_t kMinTimeInData = 1451606400000000LL;
    const int64_t kDataDurationUs = 1000000L * 60 * 60 * 24 * 3; // 3 days

    int64_t range_width_us = range_width.ToMicroseconds();
    CHECK_LT(range_width_us, kDataDurationUs);

    int64_t start_ts = kMinTimeInData + rng_.Uniform64(kDataDurationUs - range_width_us);
    int64_t end_ts = start_ts + range_width_us;
    return {start_ts, end_ts};
  }

  vector<StringPiece> get_cpu_metrics(int num_metrics) {
    vector<StringPiece> metrics = {
      "usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait",
      "usage_irq", "usage_softirq", "usage_steal", "usage_guest", "usage_guest_nice"};
    CHECK_LE(num_metrics, metrics.size());
    metrics.resize(num_metrics);
    return metrics;
  }

  vector<string> get_hosts(int num_hosts) {
    vector<string> hosts;
    ReservoirSample(all_hosts_, num_hosts, /*avoid=*/std::unordered_set<string>{}, &rng_, &hosts);
    return hosts;
  }

  void SubmitFuncToQueryPool(const std::function<Status()>& f, Promise<Status>* promise) {
    if (FLAGS_parallel_query_threads == 1) {
      promise->Set(f());
      return;
    }
    Status submit_status = pool_->SubmitFunc(
        [=]() {
          promise->Set(f());
        });
    if (!submit_status.ok()) {
      promise->Set(submit_status);
    }
  }

  Status ParallelMap(int num_tasks, const std::function<Status(int i)>& f) {
    CHECK_GE(num_tasks, 0);
    unique_ptr<Promise<Status>[]> promises(new Promise<Status>[num_tasks]);
    for (int i = 0; i < num_tasks; i++) {
      SubmitFuncToQueryPool([&, i]() { return f(i); }, &promises[i]);
    }
    for (int i = 0; i < num_tasks; i++) {
      promises[i].Get();
    }
    for (int i = 0; i < num_tasks; i++) {
      RETURN_NOT_OK(promises[i].Get());
    }
    return Status::OK();
  }

  void WriteResponseSeries(const vector<StringPiece>& metrics,
                           vector<int64_t> timestamps,
                           const vector<pair<StringPiece,StringPiece>>& tags,
                           vector<vector<int64_t>> results,
                           JsonWriter* jw) {
    TSBlock block;
    block.times = timestamps;
    for (int i = 0; i < metrics.size(); i++) {
      block.AddColumn(metrics[i].ToString(), std::move(results[i]));
    }
    WriteResponseSeries("cpu", tags, {std::move(block)}, TimestampFormat::RFC3339, jw);
  }
  
  void WriteResponseSeries(const string& measurement_name,
                           const vector<pair<StringPiece,StringPiece>>& tags,
                           const vector<TSBlock>& blocks,
                           TimestampFormat ts_format,
                           JsonWriter* jw) {
    if (blocks.empty()) return;

    jw->StartObject(); // first series
    jw->String("name");
    jw->String(measurement_name);
    if (!tags.empty()) {
      jw->String("tags");
      jw->StartObject();
      for (const auto& tag : tags) {
        jw->String(tag.first.data(), tag.first.size());
        jw->String(tag.second.data(), tag.second.size());
      }
      jw->EndObject();
    }
    jw->String("columns");
    jw->StartArray();
    jw->String("time");
    for (const auto& m : blocks[0].column_names) {
      jw->String(m);
    }
    jw->EndArray(); // columns
    jw->String("values");
    jw->StartArray();
    string ts_str;
    for (const auto& block : blocks) {
      for (int i = 0; i < block.times.size(); i++) {
        jw->StartArray();

        int64_t ts = block.times[i];
        switch (ts_format) {
          case TimestampFormat::RFC3339:
            ts_str.clear();
            DataTypeTraits<UNIXTIME_MICROS>::AppendDebugString(ts, false, &ts_str);
            jw->String(ts_str);
            break;
          case TimestampFormat::NS:
            jw->Int64(ts * 1000LL);
            break;
          case TimestampFormat::US:
            jw->Int64(ts);
            break;
          case TimestampFormat::MS:
            jw->Int64(ts / 1000L);
            break;
        }

        for (const auto& col : block.columns) {
          if (auto* v = boost::get<vector<int64_t>>(&col)) {
            jw->Int64((*v)[i]);
          } else if (auto* v = boost::get<vector<double>>(&col)) {
            jw->Double((*v)[i]);
          } else {
            LOG(FATAL);
          }
        }
        jw->EndArray();
      }
    }
    jw->EndArray(); // values
    jw->EndObject(); // first series
  }
 

  Status DoSingleGroupBy(int num_metrics,
                         int num_hosts,
                         int num_hours,
                         Webserver::PrerenderedWebResponse* resp) {

    vector<string> hosts = get_hosts(num_hosts);
    vector<StringPiece> metrics = get_cpu_metrics(num_metrics);
    int64_t start_ts, end_ts;
    std::tie(start_ts, end_ts) = get_time_range(MonoDelta::FromHours(num_hours));

    // TODO(todd) handle double aggs, etc.
    BucketAggregator agg(start_ts, end_ts, 60*1000000L);;
    vector<vector<int64_t>> agg_results(num_metrics);
    Mutex agg_lock;
    RETURN_NOT_OK(ParallelMap(
        hosts.size(),
        [&](int i) {
          const auto& host = hosts[i];
          vector<int32_t> ids;
          RETURN_NOT_OK(series_store_->FindMatchingSeries("cpu", "hostname", host, &ids));

          for (int32_t id : ids) {
            std::vector<int64_t> times;
            std::vector<InfluxVec> vals;
            RETURN_NOT_OK(metrics_store_->Read("cpu", id, start_ts, end_ts,
                                               metrics, {},
                                               &times, &vals));
            CHECK_EQ(vals.size(), agg_results.size());
            {
              MutexLock l(agg_lock);
              for (int col = 0; col < vals.size(); col++) {
                agg.MergeAgg(times, boost::get<vector<int64_t>>(vals[col]), &agg_results[col]);
              }
            }
          }
          return Status::OK();
        }));

    WriteResponse(resp, [&](JsonWriter* jw) {
                          WriteResponseSeries(metrics, agg.bucket_times(), {},
                                              std::move(agg_results), jw);
                  });
    return Status::OK();
  }

  void WriteResponse(Webserver::PrerenderedWebResponse* resp,
                     const std::function<void(JsonWriter*)> series_writer) {
    JsonWriter jw(&resp->output, JsonWriter::COMPACT);
    jw.StartObject();
    jw.String("results");
    jw.StartArray();
    jw.StartObject(); // first result
    jw.String("statement_id");
    jw.Int(0);
    jw.String("series");
    jw.StartArray();
    series_writer(&jw);
    jw.EndArray(); // series
    jw.EndObject(); // first result
    jw.EndArray(); // results
    jw.EndObject(); // root
    resp->output << "\n";
  }

  // Aggregate on across both time and host, giving the average of <N> CPU metric per host per hour for 12 hours
  Status DoDoubleGroupBy(int num_metrics,
                         Webserver::PrerenderedWebResponse* resp) {
    int64_t start_ts, end_ts;
    std::tie(start_ts, end_ts) = get_time_range(MonoDelta::FromHours(12));
    vector<StringPiece> metrics = get_cpu_metrics(num_metrics);

    // Fetch all series for the cpu measurement and hostname tag.
    std::vector<std::pair<std::string, int32_t>> hosts_with_ids;
    RETURN_NOT_OK(series_store_->FindSeriesWithTag("cpu", "hostname", &hosts_with_ids));
    std::sort(hosts_with_ids.begin(), hosts_with_ids.end());

    // Verify we only got one measurement per host -- for the benchmark purposes this should
    // be valid, though a general purpose impl would probably do cross-series grouping here.
    for (int i = 1; i < hosts_with_ids.size(); i++) {
      if (hosts_with_ids[i].first == hosts_with_ids[i - 1].first) {
        return Status::RuntimeError("more than one series for hostname", hosts_with_ids[i].first);
      }
    }

    // Set up buckets.
    BucketAggregator agg(start_ts, end_ts, 60*60*1000000L);;

    vector<vector<vector<int64_t>>> results_by_host;
    for (int i = 0; i < hosts_with_ids.size(); i++) {
      results_by_host.emplace_back(num_metrics);
    }

    RETURN_NOT_OK(ParallelMap(
        hosts_with_ids.size(),
        [&](int i) {
          int32_t series_id = hosts_with_ids[i].second;
          vector<vector<int64_t>>* agg_results = &results_by_host[i];
          std::vector<int64_t> times;
          std::vector<InfluxVec> vals;
          RETURN_NOT_OK(metrics_store_->Read("cpu", series_id, start_ts, end_ts, metrics, {},
                                             &times, &vals));
          CHECK_EQ(vals.size(), agg_results->size());
          for (int col = 0; col < vals.size(); col++) {
            agg.MergeAgg(times, boost::get<vector<int64_t>>(vals[col]), &(*agg_results)[col]);
          }
          return Status::OK();
        }));

    WriteResponse(
        resp, [&](JsonWriter* jw) {
                for (int i = 0; i < hosts_with_ids.size(); i++) {
                  const auto& hostname = hosts_with_ids[i].first;
                  auto& agg_results = results_by_host[i];
                  WriteResponseSeries(metrics,
                                      agg.bucket_times(),
                                      {{"hostname", hostname}},
                                      std::move(agg_results),
                                      jw);
                }
              });
    
    return Status::OK();
  }

  Status DoHighCpu(bool is_all, Webserver::PrerenderedWebResponse* resp) {
    if (is_all) {
      return Status::NotSupported("TODO not implemented");
    }
    
    vector<StringPiece> metrics = get_cpu_metrics(10);
    int64_t start_ts, end_ts;
    // The docs for tsbs say "all the points" but the implementation seems to do 12 hours.
    std::tie(start_ts, end_ts) = get_time_range(MonoDelta::FromHours(12));

    vector<int32_t> ids;
    string host = get_hosts(1)[0];
    RETURN_NOT_OK(series_store_->FindMatchingSeries("cpu", "hostname", host, &ids));
    if (ids.size() != 1) {
      return Status::NotFound("expected one series");
    }
    int32_t series_id = ids[0];
    std::vector<int64_t> times;
    std::vector<InfluxVec> vals;
    std::vector<influxql::Predicate> preds = {
      {"usage_user", ">", 90L}
    };
    RETURN_NOT_OK(metrics_store_->Read("cpu", series_id, start_ts, end_ts,
                                       metrics,
                                       preds,
                                       &times, &vals));

    return Status::OK();
  }

  Status DoInfluxQL(const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
    const auto& query = FindOrDie(req.parsed_args, "q");
    TimestampFormat timestamp_format;
    RETURN_NOT_OK(ParseTimestampFormat(FindWithDefault(req.parsed_args, "epoch", "rfc3339"), &timestamp_format));

    influxql::QContext ctx(series_store_.get(), metrics_store_.get());
    influxql::Parser p(&ctx);
    influxql::SelectStmt* sel;
    LOG(INFO) << "q: " << query;
    RETURN_NOT_OK(p.ParseSelectStatement(query, &sel));

    influxql::Analyzer analyzer(&ctx);
    influxql::AnalyzedSelectStmt *asel = nullptr;
    RETURN_NOT_OK(analyzer.AnalyzeSelectStmt(sel, &asel));;

    influxql::Planner planner(&ctx);
    influxql::SeriesSelector* series_sel = nullptr;
    RETURN_NOT_OK_PREPEND(planner.PlanSeriesSelector(asel, &series_sel),
                          "couldn't plan selection of time series");

    vector<SeriesIdWithTags> series;
    RETURN_NOT_OK_PREPEND(series_sel->Execute(&ctx, &series),
                          "couldn't fetch relevant series");
    VLOG(1) << JoinMapped(series, std::mem_fn(&SeriesIdWithTags::ToString), ",");

    VLOG(1) << "Dimensions: " << asel->dimensions->ToString();

    // Plan the actual "select" clause.
    influxql::TSBlockConsumerFactory agg_factory;
    RETURN_NOT_OK(planner.PlanSelectExpressions(asel, &agg_factory));

    vector<influxql::Predicate> metric_predicates;
    RETURN_NOT_OK(planner.PlanPredicates(asel, &metric_predicates));

    // Group the series by the grouping tag dimensions.
    struct GroupTask {
      vector<SeriesId> series_ids;
      vector<TSBlock> results;
    };
    map<vector<StringPiece>, GroupTask> grouped_series;
    for (const auto& series_with_tags : series) {
      vector<StringPiece> tag_values;
      for (const auto& grouping_tag : asel->dimensions->tag_keys) {
        tag_values.emplace_back(FindWithDefault(series_with_tags.tags, grouping_tag, ""));
      }
      grouped_series[tag_values].series_ids.emplace_back(series_with_tags.series_id);
    }

    VLOG(1) << "Result will have " << grouped_series.size() << " series";

    // For each group, read the data and perform the aggregate.
    int64_t count = 0;
    TSBlock input_block;
    for (auto& p : grouped_series) {
      const auto& series_ids = p.second.series_ids;

      influxql::BlockBuffer output;
      unique_ptr<influxql::TSBlockConsumer> agg;
      RETURN_NOT_OK(agg_factory(&output, &agg));

      for (auto series_id : series_ids) {
        const auto& fields = *asel->selected_fields;
        input_block.Clear();
        std::vector<InfluxVec> vals;
        // TODO(todd) read directly into agg, block-at-a-time
        RETURN_NOT_OK(metrics_store_->Read(
            asel->stmt->from_.measurement,
            series_id,
            asel->time_range->min_us.value_or(0),
            asel->time_range->max_us.value_or(asel->now_us),
            fields,
            metric_predicates,
            &input_block.times, &vals));

        for (int i = 0; i < asel->selected_fields->size(); i++) {
          input_block.AddColumn(fields[i].ToString(), std::move(vals[i]));
        }
        count += input_block.times.size();
        RETURN_NOT_OK(agg->Consume(&input_block));
        // TODO(todd) need to handle pass-through of multiple blocks
      }

      RETURN_NOT_OK(agg->Finish());
      p.second.results = output.TakeResults();
    }
    VLOG(1) << "scanned " << count << " rows";

    // Output the results.
    WriteResponse(
        resp, [&](JsonWriter* jw) {
                vector<pair<StringPiece, StringPiece>> tag_kvs;
                for (const auto& p : grouped_series) {
                  const auto& tags = p.first;
                  const auto& task = p.second;

                  tag_kvs.clear();
                  for (int i = 0; i < tags.size(); i++) {
                    tag_kvs.emplace_back(asel->dimensions->tag_keys[i], tags[i]);
                  }

                  WriteResponseSeries(asel->stmt->from_.measurement, tag_kvs, task.results, timestamp_format, jw);
                }
              });

    return Status::OK();
  }
  
  Status DoHandleInfluxQuery(const Webserver::WebRequest& req,
                       Webserver::PrerenderedWebResponse* resp) {
    const std::string& query = FindWithDefault(req.parsed_args, "q", "");
    if (query == "show databases") {
      resp->output << R"##({"results":[{"statement_id":0,"series":[{"name":"database","columns":["name"],"values":[["_internal"],["benchmark_db"],["benchmark"]]}]}]})##";
      return Status::OK();
    }

    if (query == "single-groupby-1-1-1") {
      RETURN_NOT_OK(DoSingleGroupBy(1, 1, 1, resp));
    } else if (query == "single-groupby-1-1-12") {
      RETURN_NOT_OK(DoSingleGroupBy(1, 1, 12, resp));
    } else if (query == "single-groupby-1-8-1") {
      RETURN_NOT_OK(DoSingleGroupBy(1, 8, 1, resp));
    } else if (query == "single-groupby-5-1-1") {
      RETURN_NOT_OK(DoSingleGroupBy(5, 1, 1, resp));
    } else if (query == "single-groupby-5-1-12") {
      RETURN_NOT_OK(DoSingleGroupBy(5, 1, 12, resp));
    } else if (query == "single-groupby-5-8-1") {
      RETURN_NOT_OK(DoSingleGroupBy(5, 8, 1, resp));
    } else if (query == "double-groupby-1") {
      RETURN_NOT_OK(DoDoubleGroupBy(1, resp));
    } else if (query == "double-groupby-5") {
      RETURN_NOT_OK(DoDoubleGroupBy(5, resp));
    } else if (query == "double-groupby-all") {
      RETURN_NOT_OK(DoDoubleGroupBy(10, resp));
    } else if (query == "high-cpu-1") {
      RETURN_NOT_OK(DoHighCpu(false, resp));
    } else if (query == "high-cpu-all") {
      RETURN_NOT_OK(DoHighCpu(true, resp));
    } else {
      RETURN_NOT_OK(DoInfluxQL(req, resp));
    }

    /*
    const const char* const kPattern =
        r"^SELECT max\((.+?)\) from (\w+) where hostname="(.+)" and time 
    */
    return Status::OK();
  }

  void HandleInfluxPut(const Webserver::WebRequest& req,
                       Webserver::PrerenderedWebResponse* resp) {
    Status s = DoHandleInfluxPut(req);
    if (!s.ok()) {
      resp->status_code = HttpStatusCode::InternalServerError;
      resp->output << s.ToString();
      return;
    }
    resp->status_code = HttpStatusCode::NoContent;
  }

  Status DoHandleInfluxPut(const Webserver::WebRequest& req) {
    InfluxBatch parsed;
    RETURN_NOT_OK_PREPEND(parsed.Parse(req.post_data), "failed to parse");

    auto session = client_->NewSession();
    CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    for (const auto& measurement : parsed.measurements) {
      SeriesId series_id;
      RETURN_NOT_OK_PREPEND(series_store_->FindOrCreateSeries(
          measurement.metric_name, measurement.tags, &series_id),
                            "could not fetch series id");
      RETURN_NOT_OK_PREPEND(metrics_store_->Write(series_id, measurement, session.get()),
                            "could not write metrics data");
    }
    Status s = session->Flush();
    if (!s.ok()) {
      vector<KuduError*> errors;
      bool overflow;
      session->GetPendingErrors(&errors, &overflow);
      for (auto* e : errors) {
        s = s.CloneAndPrepend(e->status().ToString() + "\n");
        delete e;
      }
    }
    RETURN_NOT_OK_PREPEND(s, "could not flush data");
    return Status::OK();
  }

  client::sp::shared_ptr<KuduClient> client_;
  unique_ptr<SeriesStoreImpl> series_store_;
  unique_ptr<MetricsStore> metrics_store_;
  unique_ptr<Webserver> webserver_;
  Random rng_;

  vector<string> all_hosts_;

  unique_ptr<ThreadPool> pool_;
};

static int Main(int argc, char** argv) {
  RETURN_MAIN_NOT_OK(kudu::InitKudu(), "InitKudu() failed", 1);

  FLAGS_webserver_port = 4242;

  GFlagsMap default_flags = GetFlagsMap();
  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }
  std::string nondefault_flags = GetNonDefaultFlags(default_flags);
  InitGoogleLoggingSafe(argv[0]);

  LOG(INFO) << "TSDBD non-default flags:\n"
            << nondefault_flags << '\n'
            << "TSDBD server version:\n"
            << VersionInfo::GetAllVersionInfo();

  Server server;
  CHECK_OK(server.Start());
  while (true) {
    SleepFor(MonoDelta::FromSeconds(60));
  }
  return 0;
}

} // namespace tsdb
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::tsdb::Main(argc, argv);
}
