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

#include "kudu/tsdb/sequence_generator.h"

#include <memory>
#include <string>

#include "kudu/client/client.h"
#include "kudu/client/write_op.h"
#include "kudu/client/value.h"
#include "kudu/util/status.h"

#include <glog/logging.h>

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduScanner;
using kudu::client::KuduScanBatch;
using kudu::client::KuduSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduValue;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduInsert;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;

using std::string;
using std::unique_ptr;
 
namespace kudu {
namespace tsdb {


SequenceGenerator::~SequenceGenerator() = default;


Status SequenceGenerator::Init() {
  Status s = client_->OpenTable(kTableName, &table_);
  if (s.IsNotFound()) {
    RETURN_NOT_OK_PREPEND(CreateTable(), "could not create sequence table");
    s = client_->OpenTable(kTableName, &table_);
  }
  RETURN_NOT_OK_PREPEND(s, "could not open sequence table");
  return Status::OK();
}

Status SequenceGenerator::CreateTable() {
  KuduSchemaBuilder b;
  b.AddColumn("sequence_name")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("neg_reserved")->Type(KuduColumnSchema::INT64)->NotNull();
  b.SetPrimaryKey({"sequence_name", "neg_reserved"});
  KuduSchema schema;
  RETURN_NOT_OK_PREPEND(b.Build(&schema), "could not create series schema");

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  RETURN_NOT_OK_PREPEND(
      table_creator->table_name(kTableName)
      .schema(&schema)
      .set_range_partition_columns({})
      .Create(),
      "could not create table");
  return Status::OK();
}

Status SequenceGenerator::ReserveChunk() {
  while (true) {
    int64_t prev_reserved;
    RETURN_NOT_OK_PREPEND(GetMaxReservedFromTable(&prev_reserved),
                          "could not get previous reservation");

    int64_t my_reservation_end = prev_reserved + 100;
    auto session = client_->NewSession();
    CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(60000);
    KuduInsert* insert = table_->NewInsert();
    CHECK_OK(insert->mutable_row()->SetStringCopy("sequence_name", name_));
    CHECK_OK(insert->mutable_row()->SetInt64("neg_reserved", -my_reservation_end));
    CHECK_OK(session->Apply(insert));
    Status s = session->Flush();
    if (s.IsAlreadyPresent()) {
      continue;
    }
    RETURN_NOT_OK_PREPEND(s, "could not insert sequence reservation");
    VLOG(1) << "reserved ids [" << prev_reserved << "," << my_reservation_end << ") "
            << "for sequence " << name_;
    next_ = prev_reserved;
    reserved_until_ = my_reservation_end;
    return Status::OK();
  }
}

Status SequenceGenerator::GetMaxReservedFromTable(int64_t* max_reserved) {
  KuduScanner scanner(table_.get());
  RETURN_NOT_OK(scanner.AddConjunctPredicate(table_->NewComparisonPredicate(
      "sequence_name", KuduPredicate::EQUAL, KuduValue::CopyString(name_))));
  RETURN_NOT_OK(scanner.SetLimit(1));
  RETURN_NOT_OK(scanner.SetFaultTolerant());
  RETURN_NOT_OK_PREPEND(scanner.Open(), "could not open scanner");

  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK_PREPEND(scanner.NextBatch(&batch), "could not fetch scan batch");
    for (KuduScanBatch::RowPtr row : batch) {
      int64_t neg_reserved;
      RETURN_NOT_OK_PREPEND(row->GetInt64("neg_reserved", &neg_reserved), "could not get neg_reserved");
      *max_reserved = -neg_reserved;
      return Status::OK();
    }
  }
  *max_reserved = 0;
  return Status::OK();
  
}


} // namespace tsdb
} // namespace kudu
