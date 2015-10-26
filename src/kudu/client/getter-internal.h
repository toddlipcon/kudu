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
#ifndef KUDU_CLIENT_GETTER_INTERNAL_H
#define KUDU_CLIENT_GETTER_INTERNAL_H

#include <set>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/common/encoded_key.h"
#include "kudu/gutil/macros.h"
#include "kudu/tserver/tserver_service.proxy.h"

namespace kudu {

namespace client {

class KuduGetter::Data {
 public:
  explicit Data(KuduTable* table);
  ~Data();

  Status SetProjectedColumnNames(const std::vector<std::string>& col_names);

  Status SetProjectedColumnIndexes(const std::vector<int>& col_indexes);

  Status Get(const KuduPartialRow& key, KuduRowResult* row);

  Status ExtractRow(KuduRowResult* row);

  const KuduSchema & GetProjection();

  tserver::GetRequestPB request_;

  tserver::GetResponsePB response_;

  // RPC controller.
  rpc::RpcController controller_;

  // The table we're scanning.
  KuduTable* table_;

  // The projection schema used for get or null if table schema is used(default).
  gscoped_ptr<KuduSchema> projection_;

  KuduClient::ReplicaSelection selection_;

  // Timeout for scanner RPCs.
  MonoDelta timeout_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
