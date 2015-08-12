// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/table_alterer-internal.h"
#include "kudu/client/schema-internal.h"

#include "kudu/common/wire_protocol.h"

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

Status KuduTableAlterer_Data::SpecToAlterPB(
    const KuduColumnSpec& spec,
    AlterTableRequestPB_AlterColumn* pb) {
  const KuduColumnSpec::Data& d = *spec.data_;

  pb->set_name(d.name);
  if (d.has_type) {
    return Status::NotSupported("altering column types is unsupported");
  }
  if (d.has_encoding) {
    pb->set_new_encoding(ToInternalEncodingType(d.encoding));
  }
  if (d.has_compression) {
    pb->set_new_compression(ToInternalCompressionType(d.compression));
  }
  if (d.has_nullable) {
    return Status::NotSupported("altering column nullability is unsupported");
  }
  if (d.primary_key) {
    return Status::NotSupported("cannot alter primary key");
  }
  if (d.has_default) {
    LOG(FATAL);
    //pb->set_new_default(...);
  }
  if (d.remove_default) {
    pb->set_remove_default(true);
  }

  if (d.has_rename_to) {
    pb->set_rename_to(d.rename_to);
  }


  return Status::OK();
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
        RETURN_NOT_OK(SpecToAlterPB(*s.spec, pb_step->mutable_alter_column()));
        break;
      default:
        LOG(FATAL) << "unknown step type " << s.step_type;
    }
  }
  // TODO: construct steps

  return Status::OK();
}

} // namespace client
} // namespace kudu
