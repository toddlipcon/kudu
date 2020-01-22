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

#include "kudu/tsdb/metrics_store.h"

#include <unordered_set>

#include "kudu/client/client.h"
#include "kudu/client/write_op.h"
#include "kudu/client/value.h"
#include "kudu/gutil/bits.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/tsdb/influx_wire_protocol.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/rw_mutex.h"


using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSession;
using kudu::client::KuduUpsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;

using std::unordered_set;
using std::string;
using std::unique_ptr;
using std::vector;
using std::pair;

namespace kudu {
namespace tsdb {

using influxql::Predicate;

namespace {
template<class T>
void ResizeForAppend(vector<T>* v, int n) {
  int needed = v->size() + n;
  if (v->capacity() < needed) {
    v->reserve(1 << Bits::Log2Ceiling(needed));
  }
  v->resize(needed);
}

} // anonymous namespace

MetricsStore::~MetricsStore() = default;

Status MetricsStore::Init() {
  return Status::OK();
}

Status MetricsStore::Write(SeriesId series_id, const InfluxMeasurement& measurement, KuduSession* session) {

  client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK_PREPEND(FindOrCreateTableAndEnsureSchema(measurement, &table),
                        "could not get table for metric");

  KuduUpsert* ins = table->NewUpsert();
  auto* row = ins->mutable_row();
  CHECK_OK(row->SetInt32("series_id", series_id));
  CHECK_OK(row->SetUnixTimeMicros("timestamp", measurement.timestamp_us));
  for (const auto& field : measurement.fields) {
    const auto& name = field.first;
    Slice name_slice(name.data(), name.size());
    const auto& val = field.second;
    switch (val.which()) {
      case kValTypeDouble:
        CHECK_OK(row->SetDouble(name_slice, boost::get<double>(val)));
        break;
      case kValTypeInt64:
        CHECK_OK(row->SetInt64(name_slice, boost::get<int64_t>(val)));
        break;
      default:
        LOG(FATAL) << "bad type index";
    }
  }
  return session->Apply(ins);

}

Status MetricsStore::FindTable(StringPiece metric_name,
                               client::sp::shared_ptr<KuduTable>* table) {
  string table_name = kTablePrefix + metric_name.as_string();
  {
    shared_lock<RWMutex> l(table_lock_);
    *table = FindWithDefault(tables_, table_name, {});
  }
  if (!*table) {
    RETURN_NOT_OK(client_->OpenTable(table_name, table));
  }
  {
    std::lock_guard<RWMutex> l(table_lock_);
    tables_.emplace(table_name, *table);
  }
  return Status::OK();
}

Status MetricsStore::FindOrCreateTableAndEnsureSchema(const InfluxMeasurement& measurement,
                                                      client::sp::shared_ptr<KuduTable>* table) {
  string table_name = kTablePrefix + measurement.metric_name.as_string();
  {
    shared_lock<RWMutex> l(table_lock_);
    *table = FindWithDefault(tables_, table_name, {});
  }
  if (!*table) {
    Status s = client_->OpenTable(table_name, table);
    if (s.IsNotFound()) {
      RETURN_NOT_OK_PREPEND(CreateTable(measurement), "could not create table");
    }
    s = client_->OpenTable(table_name, table);
    RETURN_NOT_OK(s);

    {
      std::lock_guard<RWMutex> l(table_lock_);
      tables_.emplace(table_name, *table);
    }
  }
  // TODO handle alter

  return Status::OK();
}

Status MetricsStore::CreateTable(const InfluxMeasurement& measurement) {
  string table_name = kTablePrefix + measurement.metric_name.as_string(); // TODO copy paste
  KuduSchemaBuilder b;
  b.AddColumn("series_id")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("timestamp")->Type(KuduColumnSchema::TIMESTAMP)->NotNull()
      ->Encoding(client::KuduColumnStorageAttributes::BP128);

  unordered_set<string> added_fields;
  for (const auto& field : measurement.fields) {
    const auto& name = field.first;
    if (!InsertIfNotPresent(&added_fields, name.as_string())) {
      LOG(WARNING) << "measurement has multiple values for field " << name << " (skipping all but first)";
      continue;
    }

    const auto& val = field.second;
    switch (val.which()) {
      case kValTypeDouble:
        b.AddColumn(name.as_string())->Type(KuduColumnSchema::DOUBLE);
        break;
      case kValTypeInt64:
        b.AddColumn(name.as_string())->Type(KuduColumnSchema::INT64)
            ->Encoding(client::KuduColumnStorageAttributes::BP128);
        break;
      default:
        LOG(FATAL) << "bad index";
    }
  }
  b.SetPrimaryKey({"series_id", "timestamp"});
  KuduSchema schema;
  RETURN_NOT_OK_PREPEND(b.Build(&schema), "could not create measurement schema");
  LOG(INFO) << "schema: " << schema.ToString();

  while (true) {
    // TODO(todd) range partition?
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    Status s = table_creator->table_name(table_name)
        .schema(&schema)
        .add_hash_partitions({"series_id"}, 4)
        .Create();
    if (s.IsAlreadyPresent() || s.ok()) {
      return Status::OK();
    }
    if (s.IsServiceUnavailable() && MatchPattern(s.ToString(), "*new table name * is already reserved")) {
      // TODO(todd) need to wait in the case that it's "already reserved" -- we annoyingly
      // get an error that the table doesn't exist if we try to Open it in this state.
      // See KUDU-3022.
      SleepFor(MonoDelta::FromMilliseconds(50));
      continue;
    }

    RETURN_NOT_OK_PREPEND(s, "could not create table");
  }
  return Status::OK();
}


Status MetricsStore::Read(StringPiece metric_name,
                          SeriesId series_id, int64_t start_time, int64_t end_time,
                          const std::vector<StringPiece>& project,
                          const std::vector<Predicate>& preds,
                          std::vector<int64_t>* result_times,
                          std::vector<InfluxVec>* result_vals) {
  client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(FindTable(metric_name, &table));

  KuduScanner scanner(table.get());

  KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(
     table->NewComparisonPredicate(
      "series_id", KuduPredicate::EQUAL, KuduValue::FromInt(series_id))));
  KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(
     table->NewComparisonPredicate(
      "timestamp", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(start_time))));
  KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(
     table->NewComparisonPredicate(
      "timestamp", KuduPredicate::LESS, KuduValue::FromInt(end_time))));

  for (const auto& p : preds) {
      KuduPredicate::ComparisonOp op;
      if (p.op == ">") {
        op = KuduPredicate::GREATER;
      } else {
        return Status::InvalidArgument("bad predicate", p.op);
      }
      KuduColumnSchema col_schema;
      if (!table->schema().HasColumn(p.field, &col_schema)) {
        return Status::InvalidArgument("predicate on missing field", p.field);
      }

      KuduValue* pval;
      if (auto* v = boost::get<int64_t>(&p.val)) {
        switch (col_schema.type()) {
          case KuduColumnSchema::DataType::INT64:
            pval = KuduValue::FromInt(*v);
            break;
          case KuduColumnSchema::DataType::DOUBLE:
            pval = KuduValue::FromDouble(static_cast<double>(*v));
            break;
          default:
            // TODO(todd) better error message
            return Status::NotSupported("mismatched type for predicate on field", p.field);
        }
      } else if (auto* v = boost::get<string>(&p.val)) {
        pval = KuduValue::CopyString(*v);
      } else if (auto* v = boost::get<double>(&p.val)) {
        switch (col_schema.type()) {
          case KuduColumnSchema::DataType::INT64:
            // TODO(todd): truncating cast is probably wrong here --
            // eg:
            //  "< 1.5" should turn into "<= 1" or "< 2".
            //  ">= 1.1" should turn into "> 1" or ">= 2"
            pval = KuduValue::FromInt(static_cast<int64_t>(*v));
            break;
          case KuduColumnSchema::DataType::DOUBLE:
            pval = KuduValue::FromDouble(*v);
            break;
          default:
            // TODO(todd) better error message
            return Status::NotSupported("mismatched type for predicate on field", p.field);
        }
      } else {
        return Status::NotSupported("unknown value for predicate on field", p.field);
      }

      KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(
          table->NewComparisonPredicate(
              p.field, op, std::move(pval))));
  }

  vector<string> proj;
  proj.emplace_back("timestamp");

  result_times->clear();

  const int n_sel = project.size();
  result_vals->clear();
  result_vals->resize(n_sel);

  struct ColDesc {
    KuduColumnSchema::DataType type;
    int proj_idx;
    int data_offset;
    bool nullable;
    InfluxVec* vec;
  };
  vector<ColDesc> int64_cols;
  vector<ColDesc> double_cols;
  int offset = 0;
  int ts_offset = 0;
  bool has_nullables = false;
  offset += sizeof(int64_t);
  int i = 0;
  for (const auto& sel_col_name : project) {
    string sel_col_name_str = sel_col_name.as_string();
    KuduColumnSchema cs;
    if (!table->schema().HasColumn(sel_col_name_str, &cs)) {
      return Status::InvalidArgument("column not found", sel_col_name);
    }
    ColDesc desc;
    desc.type = cs.type();
    desc.proj_idx = i + 1;
    desc.data_offset = offset;
    desc.nullable = cs.is_nullable();
    desc.vec = &(*result_vals)[i];

    switch (cs.type()) {
      case KuduColumnSchema::INT64:
        (*result_vals)[i] = InfluxVec::WithNoNulls<int64_t>({});
        int64_cols.emplace_back(std::move(desc));
        offset += sizeof(int64_t);
        break;
      case KuduColumnSchema::DOUBLE:
        (*result_vals)[i] = InfluxVec::WithNoNulls<double>({});
        double_cols.emplace_back(std::move(desc));
        offset += sizeof(double);
        break;
      default:
        LOG(FATAL) << "bad column type for " << cs.name();
        break;
    }
    has_nullables |= desc.nullable;
    proj.emplace_back(std::move(sel_col_name_str));
    i++;
  }

  int bitmap_offset = offset;
  offset += has_nullables ? BitmapSize(proj.size()) : 0;
  int row_stride = offset;

  KUDU_RETURN_NOT_OK(scanner.SetProjectedColumnNames(proj));
  // TODO(todd): currently the aggregation doesn't rely on order.
  // If we switch that, we need to turn on FaultTolerant.
  // KUDU_RETURN_NOT_OK(scanner.SetFaultTolerant());
  KUDU_RETURN_NOT_OK(scanner.Open());


  KuduScanBatch batch;
  int dst_idx = 0;
  while (scanner.HasMoreRows()) {
    KUDU_RETURN_NOT_OK(scanner.NextBatch(&batch));
    int n = batch.NumRows();

    if (batch.direct_data().size() != n * row_stride) {
      return Status::RuntimeError("unexpected batch data size");
    }
    const uint8_t* row_base = batch.direct_data().data();

    for (auto& p : int64_cols) {
      auto* v = p.vec->data_as<int64_t>();
      ResizeForAppend(v, n);
      ResizeForAppend(&p.vec->nulls, n);
    }
    for (auto& p : double_cols) {
      auto* v = p.vec->data_as<double>();
      ResizeForAppend(v, n);
      ResizeForAppend(&p.vec->nulls, n);
    }
    ResizeForAppend(result_times, n);

    for (int i = 0; i < n; i++) {
      // TODO(todd) check NULLs.
      for (const auto& p : int64_cols) {
        if (p.nullable && BitmapTest(row_base + bitmap_offset, p.proj_idx)) {
          p.vec->set(dst_idx, nullptr);
        } else {
          int64_t val = UnalignedLoad<int64_t>(row_base + p.data_offset);
          p.vec->set(dst_idx, val);
        }
      }
      for (const auto& p : double_cols) {
        if (p.nullable && BitmapTest(row_base + bitmap_offset, p.proj_idx)) {
          p.vec->set(dst_idx, nullptr);
        } else {
          double val = UnalignedLoad<double>(row_base + p.data_offset);
          p.vec->set(dst_idx, val);
        }
      }

      int64_t ts = UnalignedLoad<int64_t>(row_base + ts_offset);
      (*result_times)[dst_idx] = ts;
      dst_idx++;
      row_base += row_stride;
    }
  }
  return Status::OK();
}

Status MetricsStore::GetColumnsForMeasurement(
    StringPiece measurement,
    std::map<std::string, KuduColumnSchema::DataType>* cols) {
  client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(FindTable(measurement, &table));
  cols->clear();
  for (int i = 0; i < table->schema().num_columns(); i++) {
    const auto& col = table->schema().Column(i);
    const auto& name = col.name();
    if (name != "measurement" && name != "timestamp" && name != "series_id") {
      cols->emplace(name, col.type());
    }
  }
  return Status::OK();
}


} // namespace tsdb
} // namespace kudu
