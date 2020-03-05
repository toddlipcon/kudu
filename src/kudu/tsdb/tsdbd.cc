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
#include "kudu/server/default_path_handlers.h"
#include "kudu/server/tracing_path_handlers.h"
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
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/init.h"
#include "kudu/util/promise.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
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

METRIC_DEFINE_histogram(server, influxql_query_duration,
                        "InfluxQL Query Duration",
                        kudu::MetricUnit::kMicroseconds,
                        "Histogram of the duration of InfluxQL queries",
                        kudu::MetricLevel::kInfo,
                        120000000LU, 2);

DEFINE_int32(tsdbd_num_metrics_store_clients, 4, "number of unique client instances");

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
      : metric_registry_(new MetricRegistry()),
        metric_entity_(METRIC_ENTITY_server.Instantiate(
            metric_registry_.get(), "tsdbd")),
        query_duration_histo_(METRIC_influxql_query_duration.Instantiate(metric_entity_)) {
  }


  MetricsStore* GetMetricsStore() {
    return metrics_store_shards_[next_metric_store_shard_++ % metrics_store_shards_.size()].get();
  }
  
  Status Start() {
    RETURN_NOT_OK(ThreadPoolBuilder("tsdb-query")
                  .set_max_threads(FLAGS_parallel_query_threads)
                  .Build(&pool_));

    auto BuildClient = [&](client::sp::shared_ptr<KuduClient>* client) -> Status {
                         return KuduClientBuilder()
                             .master_server_addrs({"localhost"}) // TODO  flag
                             .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
                             .Build(client);
                       };
    RETURN_NOT_OK(BuildClient(&client_));
    series_store_.reset(new SeriesStoreImpl(client_));
    RETURN_NOT_OK(series_store_->Init());

    for (int i = 0; i < FLAGS_tsdbd_num_metrics_store_clients; i++) {
      client::sp::shared_ptr<KuduClient> ms_client;
      RETURN_NOT_OK(BuildClient(&ms_client));
      metrics_store_shards_.emplace_back(new MetricsStore(std::move(ms_client)));
      RETURN_NOT_OK(metrics_store_shards_.back()->Init());
    }

    WebserverOptions opts;
    opts.port = FLAGS_webserver_port;
    opts.enable_doc_root = true;
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

    RegisterMetricsJsonHandler(webserver_.get(), metric_registry_.get());
    AddDefaultPathHandlers(webserver_.get());
    server::TracingPathHandlers::RegisterHandlers(webserver_.get());
    RETURN_NOT_OK(webserver_->Start());
    return Status::OK();
  }

 private:

  void HandleInfluxQuery(const Webserver::WebRequest& req,
                       Webserver::PrerenderedWebResponse* resp) {
    TRACE_EVENT0("tsdb", "HandleInfluxQuery");
    ScopedLatencyMetric slm(query_duration_histo_.get());
    Status s = DoHandleInfluxQuery(req, resp);
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
      resp->status_code = HttpStatusCode::InternalServerError;
      resp->output << s.ToString();
      return;
    }
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

  void WriteResponseSeries(const string& measurement_name,
                           const vector<pair<StringPiece,StringPiece>>& tags,
                           const vector<scoped_refptr<const TSBlock>>& blocks,
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
    for (const auto& m : blocks[0]->column_names) {
      jw->String(m);
    }
    jw->EndArray(); // columns
    jw->String("values");
    jw->StartArray();
    string ts_str;
    for (const auto& block : blocks) {
      for (int i = 0; i < block->times.size(); i++) {
        jw->StartArray();

        int64_t ts = block->times[i];
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

        for (const auto& col : block->columns) {
          if (col.null_at_index(i)) {
            jw->Null();
          } else if (auto* v = col.data_as<int64_t>()) {
            jw->Int64((*v)[i]);
          } else if (auto* v = col.data_as<double>()) {
            double d_val = (*v)[i];
            if (std::isnan(d_val)) {
              jw->Null();
            } else {
              jw->Double((*v)[i]);
            }
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

  Status DoInfluxQL(const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
    const auto& query = FindOrDie(req.parsed_args, "q");
    TimestampFormat timestamp_format;
    RETURN_NOT_OK(ParseTimestampFormat(FindWithDefault(req.parsed_args, "epoch", "rfc3339"), &timestamp_format));
    auto* ms = GetMetricsStore();
    influxql::QContext ctx(series_store_.get(), ms);
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
      vector<scoped_refptr<const TSBlock>> results;
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
    vector<GroupTask*> tasks;
    for (auto& p : grouped_series) {
      tasks.push_back(&p.second);
    }
    ParallelMap(
        grouped_series.size(),
        [&](int i) -> Status {
          GroupTask* task = tasks[i];
          const auto& series_ids = task->series_ids;

          influxql::BlockBuffer output;
          unique_ptr<influxql::TSBlockConsumer> agg;
          RETURN_NOT_OK(agg_factory(&output, &agg));

          for (auto series_id : series_ids) {
            const auto& fields = *asel->selected_fields;
            std::vector<InfluxVec> vals;
            RETURN_NOT_OK(ms->Read(
                asel->stmt->from_.measurement,
                series_id,
                asel->time_range->min_us.value_or(0),
                asel->time_range->max_us.value_or(asel->now_us),
                fields,
                metric_predicates,
                &ctx, agg.get()));
          }

          RETURN_NOT_OK(agg->Finish());
          task->results = output.TakeResults();
          return Status::OK();
        });
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

    RETURN_NOT_OK(DoInfluxQL(req, resp));

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

    auto* ms = GetMetricsStore();
    auto session = ms->client()->NewSession();
    CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    for (const auto& measurement : parsed.measurements) {
      SeriesId series_id;
      RETURN_NOT_OK_PREPEND(series_store_->FindOrCreateSeries(
          measurement.metric_name, measurement.tags, &series_id),
                            "could not fetch series id");
      RETURN_NOT_OK_PREPEND(ms->Write(series_id, measurement, session.get()),
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
  std::atomic<uint32_t> next_metric_store_shard_ { 0 };
  vector<unique_ptr<MetricsStore>> metrics_store_shards_;
  unique_ptr<Webserver> webserver_;

  unique_ptr<ThreadPool> pool_;

  std::unique_ptr<MetricRegistry> metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  scoped_refptr<Histogram> query_duration_histo_;
};

static int Main(int argc, char** argv) {
  RETURN_MAIN_NOT_OK(kudu::InitKudu(), "InitKudu() failed", 1);

  FLAGS_webserver_port = 4242;

  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }
  std::string nondefault_flags = GetNonDefaultFlags();
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
