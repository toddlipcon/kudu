// Copyright (c) 2013, Cloudera, inc.

#include <assert.h>
#include <string.h>

#include <string>

#include <glog/logging.h>

#include "common/wire_protocol.h"
#include "client/client.h"
#include "util/status.h"
#include "libkudu/libkudu.h"
#include "tablet/tablet.h"

using namespace kudu;
using kudu::client::KuduClient;
using kudu::client::KuduClientOptions;
using kudu::client::KuduScanner;
using kudu::client::KuduTable;
using kudu::tserver::ColumnRangePredicatePB;
using std::string;
using std::tr1::shared_ptr;
using std::vector;

#define C_RETURN_NOT_OK(s) do { \
    Status _s = (s); \
    if (PREDICT_FALSE(!_s.ok())) return SaveError(errptr, _s);  \
  } while (0);

extern "C" {

// Exported types
struct kudu_schema_t { Schema rep; };
struct kudu_client_t { shared_ptr<KuduClient> rep; };
struct kudu_table_t { shared_ptr<KuduTable> rep; };
struct kudu_scanner_t { KuduScanner *rep; };
struct kudu_rowblock_t { RowBlock *rep; };
struct kudu_rowbuilder_t { RowBuilder *rep; };
struct kudu_schema_builder_t {
  kudu_schema_builder_t() :
    num_key_cols(-1)
  {}

  vector<ColumnSchema> cols;
  int num_key_cols;
};
struct kudu_range_predicate_t { ColumnRangePredicatePB pb; };


static kudu_err_t SaveError(char** errptr, const Status& s) {
  assert(errptr != NULL);
  if (s.ok()) {
    return KUDU_OK;
  } else if (*errptr == NULL) {
    *errptr = strdup(s.ToString().c_str());
  } else {
    free(*errptr);
    *errptr = strdup(s.ToString().c_str());
  }

  if (s.IsNotFound()) { return KUDU_NOTFOUND; }
  if (s.IsCorruption()) { return KUDU_CORRUPTION; }
  if (s.IsIOError()) { return KUDU_IO_ERROR; }
  if (s.IsInvalidArgument()) { return KUDU_INVALID_ARGUMENT; }
  if (s.IsAlreadyPresent()) { return KUDU_ALREADY_PRESENT; }
  if (s.IsRuntimeError()) { return KUDU_RUNTIME_ERROR; }
  if (s.IsNetworkError()) { return KUDU_NETWORK_ERROR; }
  return KUDU_UNKNOWN_ERROR;
}


kudu_schema_builder_t *kudu_new_schema_builder() {
  return new kudu_schema_builder_t();
}

void kudu_schema_builder_add(kudu_schema_builder_t *builder,
                             const char *col_name, kudu_type_t type,
                             int nullable) {
  DataType dt;
  switch (type) {
    case KUDU_UINT8:  dt = kudu::UINT8; break;
    case KUDU_INT8:   dt = kudu::INT8; break;
    case KUDU_UINT16: dt = kudu::UINT16; break;
    case KUDU_INT16:  dt = kudu::INT16; break;
    case KUDU_UINT32: dt = kudu::UINT32; break;
    case KUDU_INT32:  dt = kudu::INT32; break;
    case KUDU_UINT64: dt = kudu::UINT64; break;
    case KUDU_INT64:  dt = kudu::INT64; break;
    case KUDU_STRING: dt = kudu::STRING; break;
    default:
      LOG(FATAL) << "Unknown type: " << type;
      break;
  }
  builder->cols.push_back(ColumnSchema(col_name, dt, nullable));
}

void kudu_schema_builder_set_num_key_columns(kudu_schema_builder_t *builder,
                                             int num_cols) {
  builder->num_key_cols = num_cols;
}


kudu_err_t kudu_schema_builder_build(kudu_schema_builder_t *builder,
                                     kudu_schema_t** schema,
                                     char** errptr) {
  // Release the builder even if we fail.
  gscoped_ptr<kudu_schema_builder_t> scoped_builder(builder);
  gscoped_ptr<kudu_schema_t> ret(new kudu_schema_t);
  C_RETURN_NOT_OK(ret->rep.Reset(builder->cols, builder->num_key_cols));
  *schema = ret.release();
  return KUDU_OK;
}

void kudu_schema_builder_free(kudu_schema_builder_t* builder) {
  delete builder;
}

size_t kudu_schema_get_column_offset(kudu_schema_t *schema, int col_idx) {
  return schema->rep.column_offset(col_idx);
}

void kudu_schema_free(kudu_schema_t *schema) {
  delete schema;
}

kudu_rowbuilder_t *kudu_new_rowbuilder(const kudu_schema_t *schema) {
  kudu_rowbuilder_t *ret = new kudu_rowbuilder_t;
  ret->rep = new RowBuilder(schema->rep);
  return ret;
}

void kudu_rowbuilder_reset(kudu_rowbuilder_t *rb) {
  rb->rep->Reset();
}

void kudu_rowbuilder_add_string(kudu_rowbuilder_t *rb, const kudu_slice_t *slice) {
  rb->rep->AddString(*reinterpret_cast<const Slice *>(slice));
}

void kudu_rowbuilder_add_uint32(kudu_rowbuilder_t *rb, uint32_t val) {
  rb->rep->AddUint32(val);
}

void kudu_rowbuilder_free(kudu_rowbuilder_t *rb) {
  delete rb->rep;
  delete rb;
}


kudu_rowblock_t *kudu_new_rowblock(kudu_schema_t *schema,
                                   int nrows) {
  kudu_rowblock_t *ret = new kudu_rowblock_t;
  Arena *arena = new Arena(1024, 4*1024*1024);
  ret->rep = new RowBlock(schema->rep, nrows, arena);
  return ret;
}

void kudu_rowblock_free(kudu_rowblock_t *block) {
  Arena *arena = block->rep->arena();
  delete block->rep;
  delete arena;
  delete block;
}

int kudu_rowblock_nrows(kudu_rowblock_t *block) {
  return block->rep->nrows();
}

void kudu_rowblock_get_column(kudu_rowblock_t *block, int col,
                              void **col_data,
                              uint8_t **null_bitmap) {
  ColumnBlock cblock(block->rep->column_block(col));
  *col_data = cblock.data();
  *null_bitmap = cblock.null_bitmap();
}

uint8_t *kudu_rowblock_get_selection_vector(kudu_rowblock_t *block) {
  return block->rep->selection_vector()->mutable_bitmap();
}

//////////////////////////////
// Client
//////////////////////////////

kudu_err_t kudu_client_open(kudu_client_opts_t *opts, kudu_client_t **client_ret,
                            char **errptr) {
  KuduClientOptions cpp_opts;
  // TODO: add C APIs to set the address
  cpp_opts.tablet_server_addr = "localhost";
  shared_ptr<KuduClient> client;
  C_RETURN_NOT_OK(KuduClient::Create(cpp_opts, &client));

  *client_ret = new kudu_client_t;
  (*client_ret)->rep.swap(client);
  return KUDU_OK;
}


void kudu_client_free(kudu_client_t* client) {
  delete client;
}

kudu_err_t kudu_client_open_table(kudu_client_t* client, const char* table_name,
                                  kudu_table_t** table_ret, char** errptr) {
  shared_ptr<KuduTable> t;
  C_RETURN_NOT_OK(client->rep->OpenTable(string(table_name), &t));

  *table_ret = new kudu_table_t;
  (*table_ret)->rep = t;
  return KUDU_OK;
}


// Table
void kudu_table_free(kudu_table_t* table) {
  delete table;
}

kudu_err_t kudu_table_insert(kudu_table_t *table, kudu_rowbuilder_t *row,
                             char **errptr) {
  return KUDU_NOT_SUPPORTED;
}

kudu_err_t kudu_table_flush(kudu_table_t *tablet, char **errptr) {
  return KUDU_NOT_SUPPORTED;
}

//////////////////////////////
// Predicates
//////////////////////////////

kudu_err_t kudu_create_range_predicate(kudu_schema_t* schema, int col_idx,
                                       kudu_range_predicate_t** pred,
                                       char** errptr) {
  const ColumnSchema& col = schema->rep.column(col_idx);

  gscoped_ptr<kudu_range_predicate_t> ret(new kudu_range_predicate_t);
  ColumnSchemaToPB(col, ret->pb.mutable_column());
  *pred = ret.release();
  return KUDU_OK;
}

static Status CheckAndCopyPrimitive(DataType type, const void* val, size_t val_size, string* ret) {
  if (type == STRING) {
    // STRING types are just written directly into the protobuf.
    ret->assign(reinterpret_cast<const char*>(val), val_size);
    return Status::OK();
  }

  const TypeInfo& type_info = GetTypeInfo(type);

  if (val_size != type_info.size()) {
    return Status::InvalidArgument(
      StringPrintf("Wrong size %zd for type %s, expected %zd",
                   val_size, type_info.name().c_str(), type_info.size()));
  }
  ret->assign(reinterpret_cast<const char*>(val), val_size);
  return Status::OK();
}

kudu_err_t kudu_range_predicate_set(kudu_range_predicate_t* pred, kudu_predicate_field_t field,
                                    const void* val, size_t val_size, char** errptr) {
  string copied_val;

  switch (field) {
    case KUDU_LOWER_BOUND_EXCLUSIVE:
    case KUDU_UPPER_BOUND_EXCLUSIVE:
      C_RETURN_NOT_OK(Status::NotSupported("can't change exclusivity of range predicates"));
      break;
    case KUDU_UPPER_BOUND:
      C_RETURN_NOT_OK(CheckAndCopyPrimitive(pred->pb.column().type(), val, val_size, &copied_val));
      pred->pb.mutable_upper_bound()->swap(copied_val);
      break;
    case KUDU_LOWER_BOUND:
      C_RETURN_NOT_OK(CheckAndCopyPrimitive(pred->pb.column().type(), val, val_size, &copied_val));
      pred->pb.mutable_lower_bound()->swap(copied_val);
      break;
  }
  return KUDU_OK;
}

void kudu_range_predicate_free(kudu_range_predicate_t* pred) {
  delete pred;
}


// Scanner
kudu_err_t kudu_table_create_scanner(kudu_table_t* table, kudu_scanner_t** scanner_ret,
                                     char** errptr) {
  *scanner_ret = new kudu_scanner_t;
  (*scanner_ret)->rep = new KuduScanner(table->rep.get());
  return KUDU_OK;
}

void kudu_scanner_free(kudu_scanner_t* scanner) {
  delete scanner->rep;
  delete scanner;
}

// Set the projection to scan. This makes a copy of the provided schema object.
kudu_err_t kudu_scanner_set_projection(kudu_scanner_t* scanner, kudu_schema_t* projection,
                                       char** errptr) {
  C_RETURN_NOT_OK(scanner->rep->SetProjection(projection->rep));
  return KUDU_OK;
}

kudu_err_t kudu_scanner_add_range_predicate(kudu_scanner_t* scanner, kudu_range_predicate_t* pred,
                                            char** errptr) {
  C_RETURN_NOT_OK(scanner->rep->AddConjunctPredicate(pred->pb));
  delete pred;
  return KUDU_OK;
}

kudu_err_t kudu_scanner_open(kudu_scanner_t* scanner, char** errptr) {
  C_RETURN_NOT_OK(scanner->rep->Open());
  return KUDU_OK;
}

int kudu_scanner_has_more_rows(kudu_scanner_t* scanner) {
  return scanner->rep->HasMoreRows() ? 1 : 0;
}

kudu_err_t kudu_scanner_next_batch(kudu_scanner_t* scanner,
                                   kudu_rowptr_t** rows, size_t* nrows,
                                   char** errptr) {
  std::vector<const uint8_t*> vec;
  C_RETURN_NOT_OK(scanner->rep->NextBatch(&vec));

  if (*rows == NULL || *nrows < vec.size()) {
      // Not enough space in user-provided buffer - realloc it
      *rows = reinterpret_cast<kudu_rowptr_t*>(realloc(*rows, sizeof(kudu_rowptr_t) * vec.size()));
  }
  *nrows = vec.size();
  memcpy(*rows, &vec[0], sizeof(kudu_rowptr_t) * vec.size());
  return KUDU_OK;
}

extern int kudu_debug_row(kudu_schema_t* schema, kudu_rowptr_t row,
                           char* buf, size_t buflen) {
  ConstContiguousRow crow(schema->rep, row);
  string ret = schema->rep.DebugRow(crow);
  return snprintf(buf, buflen, "%s", ret.c_str());
}


} // extern "C"
