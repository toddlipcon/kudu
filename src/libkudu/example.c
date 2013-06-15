// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
//
// Compile this with something like:
//   gcc --std=c99 -o example example.c  -lkudu -L.
// and run with:
//   LD_LIBRARY_PATH=. ./test
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libkudu.h"

int main() {
  char* err = NULL;
  kudu_err_t rc;

  kudu_schema_builder_t *sb = kudu_new_schema_builder();
  kudu_schema_builder_add(sb, "tweet_id", KUDU_UINT64, 0);
  kudu_schema_builder_add(sb, "text", KUDU_STRING, 0);
  kudu_schema_builder_add(sb, "source", KUDU_STRING, 0);
  kudu_schema_builder_add(sb, "created_at", KUDU_STRING, 0);
  kudu_schema_builder_add(sb, "user_id", KUDU_UINT64, 0);
  kudu_schema_builder_add(sb, "user_name", KUDU_STRING, 0);
  kudu_schema_builder_add(sb, "user_description", KUDU_STRING, 0);
  kudu_schema_builder_add(sb, "user_location", KUDU_STRING, 0);
  kudu_schema_builder_add(sb, "user_followers_count", KUDU_UINT32, 0);
  kudu_schema_builder_add(sb, "user_friends_count", KUDU_UINT32, 0);
  kudu_schema_builder_add(sb, "user_image_url", KUDU_STRING, 0);
  kudu_schema_builder_set_num_key_columns(sb, 1);

  kudu_schema_t* schema;
  if ((rc = kudu_schema_builder_build(sb, &schema, &err)) != KUDU_OK) {
    fprintf(stderr, "error building schema: %s\n", err);
    exit(1);
  }

  kudu_client_t* client;
  if ((rc = kudu_client_open(NULL, &client, &err)) != KUDU_OK) {
    fprintf(stderr, "error opening client: %s\n", err);
    exit(1);
  }

  kudu_table_t* table;
  if ((rc = kudu_client_open_table(client, "twitter", &table, &err)) != KUDU_OK) {
    fprintf(stderr, "error opening table: %s\n", err);
    exit(1);
  }

/*
  // TODO: re-implement Insert()

  kudu_rowbuilder_t *rb = kudu_new_rowbuilder(schema);
  for (int i = 0; i < 1000; i++) {
    kudu_rowbuilder_reset(rb);
    kudu_rowbuilder_add_uint32(rb, i);
    kudu_rowbuilder_add_uint32(rb, i * 123);
    kudu_rowbuilder_add_uint32(rb, i * 456);
    kudu_err_t rc = kudu_tablet_insert(tablet, rb, &err);
    if (rc != KUDU_OK && rc != KUDU_ALREADY_PRESENT) {
      fprintf(stderr, "error inserting: %s\n", err);
      exit(1);
    }
  }
  kudu_rowbuilder_free(rb);

  if (kudu_tablet_flush(tablet, &err) != KUDU_OK) {
    fprintf(stderr, "error inserting: %s\n", err);
    exit(1);
  }
*/

  kudu_scanner_t *scanner;
  if ((rc = kudu_table_create_scanner(table, &scanner, &err)) != KUDU_OK) {
    fprintf(stderr, "error creating scanner: %s\n", err);
    exit(1);
  }

  if ((rc = kudu_scanner_set_projection(scanner, schema, &err)) != KUDU_OK) {
    fprintf(stderr, "error settin projection: %s\n", err);
    exit(1);
  }

  const int kUsernameColumn = 5;
  kudu_range_predicate_t* pred;
  if ((rc = kudu_create_range_predicate(schema, kUsernameColumn, &pred, &err)) != KUDU_OK) {
    fprintf(stderr, "error creating predicate: %s\n", err);
    exit(1);
  }

  const char* lower_bound = "f";
  const char* upper_bound = "l";
  if ((rc = kudu_range_predicate_set(pred, KUDU_LOWER_BOUND, lower_bound, strlen(lower_bound), &err)) != KUDU_OK ||
      (rc = kudu_range_predicate_set(pred, KUDU_UPPER_BOUND, upper_bound, strlen(upper_bound), &err)) != KUDU_OK) {
    fprintf(stderr, "error setting up predicate range: %s\n", err);
    exit(1);
  }

  if ((rc = kudu_scanner_add_range_predicate(scanner, pred, &err)) != KUDU_OK) {
    fprintf(stderr, "error adding range predicate: %s\n", err);
    exit(1);
  }

  if ((rc = kudu_scanner_open(scanner, &err)) != KUDU_OK) {
    fprintf(stderr, "error opening scanner: %s\n", err);
    exit(1);
  }


  kudu_rowptr_t* rows = NULL;
  size_t nrows = 0;

  while (kudu_scanner_has_more_rows(scanner)) {
    if (kudu_scanner_next_batch(scanner, &rows, &nrows, &err)) {
      fprintf(stderr, "error advancing iter: %s\n", err);
      exit(1);
    }

    char buf[10000];
    for (int i = 0; i < nrows; i++) {
      kudu_debug_row(schema, rows[i], buf, sizeof(buf));

      printf("%s\n", buf);
    }
/*
    uint32_t *cdata[2];
    uint8_t *null_bits[2];
    kudu_rowblock_get_column(block, 0, (void **)&cdata[0], &null_bits[0]);
    kudu_rowblock_get_column(block, 1, (void **)&cdata[1], &null_bits[1]);
    int nrows = kudu_rowblock_nrows(block);
    for (int i = 0; i < nrows; i++) {
      printf("%d\t%d\n", cdata[0][i], cdata[1][i]);
    }
*/
  }

  free(rows);

  kudu_scanner_free(scanner);
  kudu_client_free(client);
  kudu_schema_free(schema);
}

