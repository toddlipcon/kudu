// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_LIBKUDU_LIBKUDU_H
#define KUDU_LIBKUDU_LIBKUDU_H

// C bindings for Kudu client.
//
// Most of the APIs in this file return an error code and also take a
// char** errptr argument. In the case that they return an error,
// *errptr is set to a newly allocated string describing the error.
// *errptr must then be freed by the caller to avoid a leak.
//
// TODO: document these APIs.

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kudu_schema_builder_t kudu_schema_builder_t;
typedef struct kudu_schema_t kudu_schema_t;
typedef struct kudu_client_t kudu_client_t;
typedef struct kudu_client_opts_t kudu_client_opts_t;
typedef struct kudu_table_t kudu_table_t;
typedef struct kudu_scanner_t kudu_scanner_t;
typedef struct kudu_rowblock_t kudu_rowblock_t;
typedef struct kudu_rowbuilder_t kudu_rowbuilder_t;
typedef struct kudu_range_predicate_t kudu_range_predicate_t;

typedef struct kudu_slice {
  uint8_t *data;
  size_t size;
} kudu_slice_t;

typedef enum {
  KUDU_OK = 0,
  KUDU_NOTFOUND = 1,
  KUDU_CORRUPTION = 2,
  KUDU_NOT_SUPPORTED = 3,
  KUDU_INVALID_ARGUMENT = 4,
  KUDU_IO_ERROR = 5,
  KUDU_ALREADY_PRESENT = 6,
  KUDU_RUNTIME_ERROR = 7,
  KUDU_NETWORK_ERROR = 8,
  KUDU_UNKNOWN_ERROR = 9
} kudu_err_t;

typedef enum {
  KUDU_UINT8 = 0,
  KUDU_INT8 = 1,
  KUDU_UINT16 = 2,
  KUDU_INT16 = 3,
  KUDU_UINT32 = 4,
  KUDU_INT32 = 5,
  KUDU_UINT64 = 6,
  KUDU_INT64 = 7,
  KUDU_STRING = 8
} kudu_type_t;

typedef enum {
  KUDU_LOWER_BOUND,
  KUDU_UPPER_BOUND,
  KUDU_LOWER_BOUND_EXCLUSIVE,
  KUDU_UPPER_BOUND_EXCLUSIVE
} kudu_predicate_field_t;

// Schema builder
extern kudu_schema_builder_t *kudu_new_schema_builder();
extern void kudu_schema_builder_add(kudu_schema_builder_t *builder,
                                    const char *col_name, kudu_type_t type,
                                    int nullable);
extern void kudu_schema_builder_set_num_key_columns(kudu_schema_builder_t *builder,
                                                    int num_cols);
extern void kudu_free_schema_builder(kudu_schema_builder_t* builder);

// Build the schema object. This has the side effect of freeing the builder object,
// even if the build fails.
extern kudu_err_t kudu_schema_builder_build(kudu_schema_builder_t *builder,
                                            kudu_schema_t** schema,
                                            char** errptr);

// Schema
extern size_t kudu_schema_get_column_offset(kudu_schema_t *schema, int col_idx);
extern void kudu_schema_free(kudu_schema_t *schema);

// Row builder
extern kudu_rowbuilder_t *kudu_new_rowbuilder(const kudu_schema_t *schema);
extern void kudu_rowbuilder_reset(kudu_rowbuilder_t *rb);
extern void kudu_rowbuilder_add_string(kudu_rowbuilder_t *rb, const kudu_slice_t *slice);
extern void kudu_rowbuilder_add_uint32(kudu_rowbuilder_t *rb, uint32_t val);
extern void kudu_rowbuilder_free(kudu_rowbuilder_t *rb);

// Row block
extern kudu_rowblock_t *kudu_new_rowblock(kudu_schema_t *schema,
                                          int nrows);
extern void kudu_rowblock_free(kudu_rowblock_t *block);
extern int kudu_rowblock_nrows(kudu_rowblock_t *block);
extern void kudu_rowblock_get_column(kudu_rowblock_t *block, int col,
                                     void **col_data,
                                     uint8_t **null_bitmap);
extern uint8_t *kudu_rowblock_get_selection_vector(kudu_rowblock_t *block);

// Client
extern kudu_err_t kudu_client_open(kudu_client_opts_t *opts, kudu_client_t **client_ret,
                                   char **errptr);
extern void kudu_client_free(kudu_client_t* client);
extern kudu_err_t kudu_client_open_table(kudu_client_t* client, const char* table_name,
                                         kudu_table_t** table_ret, char** errptr);

// Table
extern void kudu_table_free(kudu_table_t* table);
extern kudu_err_t kudu_table_insert(kudu_table_t *table, kudu_rowbuilder_t *row,
                                     char **errptr);
extern kudu_err_t kudu_table_flush(kudu_table_t *tablet, char **errptr);


// Scanner
extern kudu_err_t kudu_table_create_scanner(kudu_table_t* table, kudu_scanner_t** scanner_ret,
                                            char** errptr);
extern void kudu_scanner_free(kudu_scanner_t* scanner);

// Set the projection to scan. This makes a copy of the provided schema object.
extern kudu_err_t kudu_scanner_set_projection(kudu_scanner_t* scanner, kudu_schema_t* projection,
                                              char** errptr);

extern kudu_err_t kudu_create_range_predicate(kudu_schema_t* schema, int col_idx,
                                              kudu_range_predicate_t** pred,
                                              char** errptr);

extern kudu_err_t kudu_range_predicate_set(kudu_range_predicate_t* pred, kudu_predicate_field_t field,
                                           const void* val, size_t val_size, char** errptr);

extern void kudu_range_predicate_free(kudu_range_predicate_t* pred);

// Add the given range predicate to the scanner.
// The ownership of the predicate object is taken over by the scanner object - there is no need
// to free it.
extern kudu_err_t kudu_scanner_add_range_predicate(kudu_scanner_t* scanner, kudu_range_predicate_t* pred,
                                                   char** errptr);
extern kudu_err_t kudu_scanner_open(kudu_scanner_t* scanner, char** errptr);
extern int kudu_scanner_has_more_rows(kudu_scanner_t* scanner);


typedef const uint8_t* kudu_rowptr_t;
extern kudu_err_t kudu_scanner_next_batch(kudu_scanner_t* scanner,
                                          kudu_rowptr_t** rows, size_t* nrows,
                                          char** errptr);


// Row accessors
extern int kudu_debug_row(kudu_schema_t* schema, kudu_rowptr_t row,
                          char* buf, size_t buflen);
// TODO: add other accessors for row contents, or document what format a kudu_rowptr_t
// points to.


#ifdef __cplusplus
} // extern "C"
#endif

#endif
