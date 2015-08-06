// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/table_alterer-internal.h"
#include "kudu/client/schema-internal.h"

#include "kudu/common/wire_protocol.h"
#include "kudu/master/master.pb.h"

#include <boost/foreach.hpp>
#include <string>

using std::string;

namespace kudu {
namespace client {

using master::AlterTableRequestPB;
using master::AlterTableRequestPB_AlterColumn;

KuduTableAlterer_Data::KuduTableAlterer_Data(
    KuduClient* client, const string& name)
  : client_(client),
    table_name_(name),
    wait_(true) {
}

KuduTableAlterer_Data::~KuduTableAlterer_Data() {
  BOOST_FOREACH(Step& s, steps_) {
    delete s.spec;
  }
}

Status KuduTableAlterer_Data::ToRequest(AlterTableRequestPB* req) {
  if (!status_.ok()) {
    return status_;
  }

  if (!rename_to_.is_initialized() &&
      steps_.empty()) {
    return Status::InvalidArgument("No alter steps provided");
  }

  req->Clear();
  req->mutable_table()->set_table_name(table_name_);
  if (rename_to_.is_initialized()) {
    req->set_new_table_name(rename_to_.get());
  }

  BOOST_FOREACH(const Step& s, steps_) {
    AlterTableRequestPB::Step* pb_step = req->add_alter_schema_steps();
    pb_step->set_type(s.step_type);

    switch (s.step_type) {
      case AlterTableRequestPB::ADD_COLUMN:
      {
        KuduColumnSchema col;
        RETURN_NOT_OK(s.spec->ToColumnSchema(&col));
        ColumnSchemaToPB(*col.col_,
                         pb_step->mutable_add_column()->mutable_schema());
        break;
      }
      case AlterTableRequestPB::DROP_COLUMN:
      {
        pb_step->mutable_drop_column()->set_name(s.spec->data_->name);
        break;
      }
      case AlterTableRequestPB::ALTER_COLUMN:
        if (s.spec->data_->has_type ||
            s.spec->data_->has_encoding ||
            s.spec->data_->has_compression ||
            s.spec->data_->has_nullable ||
            s.spec->data_->primary_key ||
            s.spec->data_->has_default ||
            s.spec->data_->default_val ||
            s.spec->data_->remove_default) {
          return Status::NotSupported("cannot support AlterColumn of this type",
                                      s.spec->data_->name);
        }
        // We only support rename column
        if (!s.spec->data_->has_rename_to) {
          return Status::InvalidArgument("no alter operation specified",
                                         s.spec->data_->name);
        }
        pb_step->mutable_rename_column()->set_old_name(s.spec->data_->name);
        pb_step->mutable_rename_column()->set_new_name(s.spec->data_->rename_to);
        pb_step->set_type(AlterTableRequestPB::RENAME_COLUMN);
        break;
      default:
        LOG(FATAL) << "unknown step type " << s.step_type;
    }
  }

  return Status::OK();
}

} // namespace client
} // namespace kudu
