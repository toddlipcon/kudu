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

#include "kudu/client/getter-internal.h"

#include <boost/bind.hpp>
#include <string>
#include <vector>

#include "kudu/client/client-internal.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/row_result.h"
#include "kudu/client/table-internal.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/hexdump.h"

using std::set;
using std::string;
using std::vector;

namespace kudu {

using rpc::RpcController;
using tserver::ColumnRangePredicatePB;
using tserver::NewScanRequestPB;
using tserver::ScanResponsePB;

namespace client {

using internal::RemoteTabletServer;

KuduGetter::Data::Data(KuduTable* table)
  : table_(DCHECK_NOTNULL(table)),
    selection_(KuduClient::LEADER_ONLY),
    timeout_(MonoDelta::FromMilliseconds(kGetTimeoutMillis)) {
}

KuduGetter::Data::~Data() {
}

Status KuduGetter::Data::SetProjectedColumnNames(const vector<string>& col_names) {
  gscoped_ptr<KuduSchema> s(new KuduSchema());
  s->schema_ = new Schema();
  RETURN_NOT_OK(table_->schema().schema_->CreateProjectionByNames(col_names, s->schema_));
  RETURN_NOT_OK(SchemaToColumnPBs(*s->schema_, request_.mutable_projected_columns(),
                                  SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES | SCHEMA_PB_WITHOUT_IDS));
  projection_.reset(s.release());
  return Status::OK();
}

Status KuduGetter::Data::SetProjectedColumnIndexes(const vector<int>& col_indexes) {
  gscoped_ptr<KuduSchema> s(new KuduSchema());
  s->schema_ = new Schema();
  RETURN_NOT_OK(table_->schema().schema_->CreateProjectionByIndexes(col_indexes, s->schema_));
  RETURN_NOT_OK(SchemaToColumnPBs(*s->schema_, request_.mutable_projected_columns(),
                                  SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES | SCHEMA_PB_WITHOUT_IDS));
  projection_.reset(s.release());
  return Status::OK();
}

const KuduSchema & KuduGetter::Data::GetProjection() {
  if (projection_) {
    return *projection_;
  } else {
    return table_->schema();
  }
}

Status KuduGetter::Data::Get(const KuduPartialRow& key, KuduRowResult* row) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(timeout_);
  string* key_str = request_.mutable_key();
  RETURN_NOT_OK(key.EncodeRowKey(key_str));
  string partition_key;
  RETURN_NOT_OK(table_->partition_schema().EncodeKey(key, &partition_key));
  // If column PBs is empty and default projection is used, this means column PBs is
  // not initialized yet, so initialize it.
  // Just check request_.projected_columns is empty is not enough, cause
  // user can provide an empty project to just check row exists.
  if (request_.projected_columns_size() == 0 && !projection_) {
    RETURN_NOT_OK(SchemaToColumnPBs(*table_->schema().schema_, request_.mutable_projected_columns(),
                                    SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES | SCHEMA_PB_WITHOUT_IDS));
  }
  scoped_refptr<internal::RemoteTablet> remote;
  vector<RemoteTabletServer*> candidates;
  set<string> blacklist;
  for (int attempt = 1;; attempt++) {
    Synchronizer sync;
    table_->client()->data_->meta_cache_->LookupTabletByKey(table_,
                                                            partition_key,
                                                            deadline,
                                                            &remote,
                                                            sync.AsStatusCallback());
    RETURN_NOT_OK(sync.Wait());
    request_.set_tablet_id(remote->tablet_id());

    RemoteTabletServer* ts;
    Status lookup_status = table_->client()->data_->GetTabletServer(
        table_->client(),
        remote,
        selection_,
        blacklist,
        &candidates,
        &ts);
    // If we get ServiceUnavailable, this indicates that the tablet doesn't
    // currently have any known leader. We should sleep and retry, since
    // it's likely that the tablet is undergoing a leader election and will
    // soon have one.
    if (lookup_status.IsServiceUnavailable() &&
        MonoTime::Now(MonoTime::FINE).ComesBefore(deadline)) {
      int sleep_ms = attempt * 100;
      VLOG(1) << "Tablet " << remote->tablet_id() << " current unavailable: "
              << lookup_status.ToString() << ". Sleeping for " << sleep_ms << "ms "
              << "and retrying...";
      SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
      continue;
    }
    RETURN_NOT_OK(lookup_status);

    // Recalculate the deadlines.
    MonoTime rpc_deadline = MonoTime::Now(MonoTime::FINE);
    rpc_deadline.AddDelta(table_->client()->default_rpc_timeout());
    MonoTime actual_deadline = MonoTime::Earliest(rpc_deadline, deadline);
    controller_.Reset();
    controller_.set_deadline(actual_deadline);

    CHECK(ts->proxy());
    std::shared_ptr<tserver::TabletServerServiceProxy> proxy = ts->proxy();
    response_.Clear();
    RETURN_NOT_OK(proxy->Get(request_, &response_, &controller_));
    if (PREDICT_FALSE(response_.has_error())) {
      return StatusFromPB(response_.error().status());
    }
    RETURN_NOT_OK(ExtractRow(row));
    break;
  }
  return Status::OK();
}

Status KuduGetter::Data::ExtractRow(KuduRowResult* row) {
  RowwiseRowBlockPB* rowblock_pb = response_.mutable_data();
  int n_rows = rowblock_pb->num_rows();
  if (PREDICT_FALSE(n_rows == 0)) {
    return Status::NotFound("Not found");
  }

  Slice direct, indirect;

  if (PREDICT_FALSE(!rowblock_pb->has_rows_sidecar())) {
    return Status::Corruption("Server sent invalid response: no row data");
  } else {
    Status s = controller_.GetSidecar(rowblock_pb->rows_sidecar(), &direct);
    if (!s.ok()) {
      return Status::Corruption("Server sent invalid response: row data "
                                "sidecar index corrupt", s.ToString());
    }
  }

  if (rowblock_pb->has_indirect_data_sidecar()) {
    Status s = controller_.GetSidecar(rowblock_pb->indirect_data_sidecar(),
                                      &indirect);
    if (!s.ok()) {
      return Status::Corruption("Server sent invalid response: indirect data "
                                "sidecar index corrupt", s.ToString());
    }
    // TODO: Optimize this, too expensive for single row.
    RETURN_NOT_OK(RewriteRowBlockPointers(
        *GetProjection().schema_, *rowblock_pb, indirect, &direct));
  }

  *row = KuduRowResult(GetProjection().schema_, direct.data());
  return Status::OK();
}

} // namespace client
} // namespace kudu
