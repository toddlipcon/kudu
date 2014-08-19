// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_TEST_BASE_H
#define KUDU_TABLET_TABLET_TEST_BASE_H

#include <boost/assign/list_of.hpp>
#include <boost/thread/thread.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>
#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_graph.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/gutil/strings/numbers.h"

using std::tr1::unordered_set;
using strings::Substitute;

namespace kudu {
namespace tablet {

// The base class takes as a template argument a "setup" class
// which can customize the schema for the tests. This way we can
// get coverage on various schemas without duplicating test code.
struct StringKeyTestSetup {
  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
                  (ColumnSchema("key", STRING))
                  (ColumnSchema("key_idx", UINT32))
                  (ColumnSchema("val", UINT32)),
                  1);
  }

  void BuildRowKey(KuduPartialRow *row, uint64_t key_idx) {
    // This is called from multiple threads, so can't move this buffer
    // to be a class member. However, it's likely to get inlined anyway
    // and loop-hosted.
    char buf[256];
    FormatKey(buf, sizeof(buf), key_idx);
    CHECK_OK(row->SetStringCopy(0, Slice(buf)));
  }

  // builds a row key from an existing row for updates
  void BuildRowKeyFromExistingRow(KuduPartialRow *row, const RowBlockRow& src_row) {
    CHECK_OK(row->SetStringCopy(0, *reinterpret_cast<const Slice*>(src_row.cell_ptr(0))));
  }

  void BuildRow(KuduPartialRow *row, uint64_t key_idx, uint32_t val = 0) {
    BuildRowKey(row, key_idx);
    CHECK_OK(row->SetUInt32(1, key_idx));
    CHECK_OK(row->SetUInt32(2, val));
  }

  static void FormatKey(char *buf, size_t buf_size, uint64_t key_idx) {
    snprintf(buf, buf_size, "hello %ld", key_idx);
  }

  string FormatDebugRow(uint64_t key_idx, uint32_t val, bool updated) {
    char buf[256];
    FormatKey(buf, sizeof(buf), key_idx);

    return Substitute(
      "(string key=$0, uint32 key_idx=$1, uint32 val=$2)",
      buf, key_idx, val);
  }

  // Slices can be arbitrarily large
  // but in practice tests won't overflow a uint64_t
  uint64_t GetMaxRows() const {
    return std::numeric_limits<uint64_t>::max() - 1;
  }
};

// Setup for testing composite keys
struct CompositeKeyTestSetup {
  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
                  (ColumnSchema("key1", STRING))
                  (ColumnSchema("key2", UINT32))
                  (ColumnSchema("key_idx", UINT32))
                  (ColumnSchema("val", UINT32)),
                  2);
  }

  // builds a row key from an existing row for updates
  void BuildRowKeyFromExistingRow(KuduPartialRow *row, const RowBlockRow& src_row) {
    row->SetStringCopy(0, *reinterpret_cast<const Slice*>(src_row.cell_ptr(0)));
    row->SetUInt32(1, *reinterpret_cast<const uint32_t*>(src_row.cell_ptr(1)));
  }

  static void FormatKey(char *buf, size_t buf_size, uint64_t key_idx) {
    snprintf(buf, buf_size, "hello %ld", key_idx);
  }

  string FormatDebugRow(uint64_t key_idx, uint32_t val, bool updated) {
    char buf[256];
    FormatKey(buf, sizeof(buf), key_idx);
    return Substitute(
      "(string key1=$0, uint32 key2=$1, uint32 val=$2, uint32 val=$3)",
      buf, key_idx, key_idx, val);
  }

  // Slices can be arbitrarily large
  // but in practice tests won't overflow a uint64_t
  uint64_t GetMaxRows() const {
    return std::numeric_limits<uint64_t>::max() - 1;
  }
};

// Setup for testing integer keys
template<DataType Type>
struct IntKeyTestSetup {
  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
                  (ColumnSchema("key", Type))
                  (ColumnSchema("key_idx", UINT32))
                  (ColumnSchema("val", UINT32)), 1);;
  }

  void BuildRowKey(KuduPartialRow *row, int64_t i) {
    CHECK(false) << "Unsupported type";
  }

  // builds a row key from an existing row for updates
  template<class RowType>
  void BuildRowKeyFromExistingRow(KuduPartialRow *dst_row, const RowType& row) {
    CHECK(false) << "Unsupported type";
  }

  void BuildRow(KuduPartialRow *row, uint64_t key_idx,
                uint32_t val = 0) {
    BuildRowKey(row, key_idx);
    CHECK_OK(row->SetUInt32(1, key_idx));
    CHECK_OK(row->SetUInt32(2, val));
  }

  string FormatDebugRow(int64_t key_idx, uint32_t val, bool updated) {
    CHECK(false) << "Unsupported type";
    return "";
  }

  uint64_t GetMaxRows() const {
    return std::numeric_limits<typename DataTypeTraits<Type>::cpp_type>::max() - 1;
  }
};

template<>
void IntKeyTestSetup<UINT8>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetUInt8(0, (uint8_t) i));
}

template<>
void IntKeyTestSetup<INT8>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetInt8(0, (int8_t) i * (i % 2 == 0 ? -1 : 1)));
}

template<>
void IntKeyTestSetup<UINT16>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetUInt16(0, (uint16_t) i));
}

template<>
void IntKeyTestSetup<INT16>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetInt16(0, (int16_t) i * (i % 2 == 0 ? -1 : 1)));
}

template<>
void IntKeyTestSetup<UINT32>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetUInt32(0, (uint32_t) i));
}

template<>
void IntKeyTestSetup<INT32>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetInt32(0, (int32_t) i * (i % 2 == 0 ? -1 : 1)));
}

template<>
void IntKeyTestSetup<UINT64>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetUInt64(0, (uint64_t) i));
}

template<>
void IntKeyTestSetup<INT64>::BuildRowKey(KuduPartialRow *row, int64_t i) {
  CHECK_OK(row->SetInt64(0, (int64_t) i * (i % 2 == 0 ? -1 : 1)));
}

template<> template<class RowType>
void IntKeyTestSetup<UINT8>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                        const RowType& src_row) {
  CHECK_OK(row->SetUInt8(0, *reinterpret_cast<const uint8_t*>(src_row.cell_ptr(0))));
}

template<> template<class RowType>
void IntKeyTestSetup<INT8>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                       const RowType& src_row) {
  CHECK_OK(row->SetInt8(0, *reinterpret_cast<const int8_t*>(src_row.cell_ptr(0))));
}

template<> template<class RowType>
void IntKeyTestSetup<UINT16>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                         const RowType& src_row) {
  CHECK_OK(row->SetUInt16(0, *reinterpret_cast<const uint16_t*>(src_row.cell_ptr(0))));
}

template<> template<class RowType>
void IntKeyTestSetup<INT16>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                        const RowType& src_row) {
  CHECK_OK(row->SetInt16(0, *reinterpret_cast<const int16_t*>(src_row.cell_ptr(0))));
}

template<> template<class RowType>
void IntKeyTestSetup<UINT32>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                         const RowType& src_row) {
  CHECK_OK(row->SetUInt32(0, *reinterpret_cast<const uint32_t*>(src_row.cell_ptr(0))));
}

template<> template<class RowType>
void IntKeyTestSetup<INT32>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                        const RowType& src_row) {
  CHECK_OK(row->SetInt32(0, *reinterpret_cast<const int32_t*>(src_row.cell_ptr(0))));
}

template<> template<class RowType>
void IntKeyTestSetup<UINT64>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                         const RowType& src_row) {
  CHECK_OK(row->SetUInt64(0, *reinterpret_cast<const uint64_t*>(src_row.cell_ptr(0))));
}

template<> template<class RowType>
void IntKeyTestSetup<INT64>::BuildRowKeyFromExistingRow(KuduPartialRow *row,
                                                        const RowType& src_row) {
  CHECK_OK(row->SetInt64(0, *reinterpret_cast<const int64_t*>(src_row.cell_ptr(0))));
}

template<>
string IntKeyTestSetup<UINT8>::FormatDebugRow(int64_t key_idx, uint32_t val, bool updated) {
  return Substitute(
    "(uint8 key=$0, uint32 key_idx=$1, uint32 val=$2)",
    key_idx, key_idx, val);
}

template<>
string IntKeyTestSetup<INT8>::FormatDebugRow(int64_t key_idx, uint32_t val, bool updated) {
  return Substitute(
    "(int8 key=$0, uint32 key_idx=$1, uint32 val=$2)",
    (key_idx % 2 == 0) ? -key_idx : key_idx, key_idx, val);
}

template<>
string IntKeyTestSetup<UINT16>::FormatDebugRow(int64_t key_idx, uint32_t val, bool updated) {
  return Substitute(
    "(uint16 key=$0, uint32 key_idx=$1, uint32 val=$2)",
    key_idx, key_idx, val);
}

template<>
string IntKeyTestSetup<INT16>::FormatDebugRow(int64_t key_idx, uint32_t val, bool updated) {
  return Substitute(
    "(int16 key=$0, uint32 key_idx=$1, uint32 val=$2)",
    (key_idx % 2 == 0) ? -key_idx : key_idx, key_idx, val);
}

template<>
string IntKeyTestSetup<UINT32>::FormatDebugRow(int64_t key_idx, uint32_t val, bool updated) {
  return Substitute(
    "(uint32 key=$0, uint32 key_idx=$1, uint32 val=$2)",
    key_idx, key_idx, val);
}

template<>
string IntKeyTestSetup<INT32>::FormatDebugRow(int64_t key_idx, uint32_t val, bool updated) {
  return Substitute(
    "(int32 key=$0, uint32 key_idx=$1, uint32 val=$2)",
    (key_idx % 2 == 0) ? -key_idx : key_idx, key_idx, val);
}

template<>
string IntKeyTestSetup<UINT64>::FormatDebugRow(int64_t key_idx, uint32_t val, bool updated) {
  return Substitute(
    "(uint64 key=$0, uint32 key_idx=$1, uint32 val=$2)",
    key_idx, key_idx, val);
}

template<>
string IntKeyTestSetup<INT64>::FormatDebugRow(int64_t key_idx, uint32_t val, bool updated) {
  return Substitute(
    "(int64 key=$0, uint32 key_idx=$1, uint32 val=$2)",
    (key_idx % 2 == 0) ? -key_idx : key_idx, key_idx, val);
}

// Setup for testing nullable columns
struct NullableValueTestSetup {
  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
                  (ColumnSchema("key", UINT32))
                  (ColumnSchema("key_idx", UINT32))
                  (ColumnSchema("val", UINT32, true)), 1);
  }

  void BuildRowKey(KuduPartialRow *row, uint64_t i) {
    CHECK_OK(row->SetUInt32(0, (uint32_t)i));
  }

  // builds a row key from an existing row for updates
  template<class RowType>
  void BuildRowKeyFromExistingRow(KuduPartialRow *row, const RowType& src_row) {
    CHECK_OK(row->SetUInt32(0, *reinterpret_cast<const uint32_t*>(src_row.cell_ptr(0))));
  }

  void BuildRow(KuduPartialRow *row, uint64_t key_idx, uint32_t val = 0) {
    BuildRowKey(row, key_idx);
    CHECK_OK(row->SetUInt32(1, key_idx));
    if (ShouldInsertAsNull(key_idx)) {
      row->SetNull(2);
    } else {
      CHECK_OK(row->SetUInt32(2, val));
    }
  }

  string FormatDebugRow(uint64_t key_idx, uint64_t val, bool updated) {
    if (!updated && ShouldInsertAsNull(key_idx)) {
      return Substitute(
      "(uint32 key=$0, uint32 key_idx=$1, uint32 val=NULL)",
        (uint32_t)key_idx, key_idx);
    }

    return Substitute(
      "(uint32 key=$0, uint32 key_idx=$1, uint32 val=$2)",
      (uint32_t)key_idx, key_idx, val);
  }

  static bool ShouldInsertAsNull(uint64_t key_idx) {
    return (key_idx & 2) != 0;
  }

  uint64_t GetMaxRows() const {
    return std::numeric_limits<uint32_t>::max() - 1;
  }
};

// Use this with TYPED_TEST_CASE from gtest
typedef ::testing::Types<
                         StringKeyTestSetup,
                         IntKeyTestSetup<UINT8>,
                         IntKeyTestSetup<INT8>,
                         IntKeyTestSetup<UINT16>,
                         IntKeyTestSetup<INT16>,
                         IntKeyTestSetup<UINT32>,
                         IntKeyTestSetup<INT32>,
                         IntKeyTestSetup<UINT64>,
                         IntKeyTestSetup<INT64>,
                         NullableValueTestSetup
                         > TabletTestHelperTypes;

template<class TESTSETUP>
class TabletTestBase : public KuduTabletTest {
 public:
  TabletTestBase() :
    KuduTabletTest(TESTSETUP::CreateSchema()),
    setup_(),
    max_rows_(setup_.GetMaxRows()),
    arena_(1024, 4*1024*1024)
  {}

  // Inserts "count" rows.
  void InsertTestRows(uint64_t first_row,
                      uint64_t count,
                      uint32_t val,
                      TimeSeries *ts = NULL) {

    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);

    uint64_t inserted_since_last_report = 0;
    for (uint64_t i = first_row; i < first_row + count; i++) {
      setup_.BuildRow(&row, i, val);
      CHECK_OK(writer.Insert(row));

      if ((inserted_since_last_report++ > 100) && ts) {
        ts->AddValue(static_cast<double>(inserted_since_last_report));
        inserted_since_last_report = 0;
      }
    }

    if (ts) {
      ts->AddValue(static_cast<double>(inserted_since_last_report));
    }
  }

  // Inserts a single test row within a transaction.
  Status InsertTestRow(LocalTabletWriter* writer,
                       uint64_t key_idx,
                       uint32_t val) {
    KuduPartialRow row(&client_schema_);
    setup_.BuildRow(&row, key_idx, val);
    return writer->Insert(row);
  }

  Status UpdateTestRow(LocalTabletWriter* writer,
                       uint64_t key_idx,
                       uint32_t new_val) {
    KuduPartialRow row(&client_schema_);
    setup_.BuildRowKey(&row, key_idx);

    // select the col to update (the third if there is only one key
    // or the fourth if there are two col keys).
    int col_idx = schema_.num_key_columns() == 1 ? 2 : 3;
    CHECK_OK(row.SetUInt32(col_idx, new_val));
    return writer->Update(row);
  }

  Status UpdateTestRowToNull(LocalTabletWriter* writer,
                             uint64_t key_idx) {
    KuduPartialRow row(&client_schema_);
    setup_.BuildRowKey(&row, key_idx);

    // select the col to update (the third if there is only one key
    // or the fourth if there are two col keys).
    int col_idx = schema_.num_key_columns() == 1 ? 2 : 3;
    CHECK_OK(row.SetNull(col_idx));
    return writer->Update(row);
  }

  Status DeleteTestRow(LocalTabletWriter* writer, uint64_t key_idx) {
    KuduPartialRow row(&client_schema_);
    setup_.BuildRowKey(&row, key_idx);
    return writer->Delete(row);
  }

  template <class RowType>
  void VerifyRow(const RowType& row, uint64_t key_idx, uint32_t val) {
    ASSERT_EQ(setup_.FormatDebugRow(key_idx, val, false), schema_.DebugRow(row));
  }

  void VerifyTestRows(uint64_t first_row, uint64_t expected_count) {
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet()->NewRowIterator(client_schema_, &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));
    int batch_size = std::max(
      (size_t)1, std::min((size_t)(expected_count / 10),
                          4*1024*1024 / schema_.byte_size()));
    Arena arena(32*1024, 256*1024);
    RowBlock block(schema_, batch_size, &arena);

    if (expected_count > INT_MAX) {
      LOG(INFO) << "Not checking rows for duplicates -- duplicates expected since "
                << "there were more than " << INT_MAX << " rows inserted.";
      return;
    }

    // Keep a bitmap of which rows have been seen from the requested
    // range.
    std::vector<bool> seen_rows;
    seen_rows.resize(expected_count);

    while (iter->HasNext()) {
      ASSERT_STATUS_OK_FAST(iter->NextBlock(&block));

      RowBlockRow rb_row = block.row(0);
      if (VLOG_IS_ON(2)) {
        VLOG(2) << "Fetched batch of " << block.nrows() << "\n"
            << "First row: " << schema_.DebugRow(rb_row);
      }

      for (int i = 0; i < block.nrows(); i++) {
        rb_row.Reset(&block, i);
        uint32_t key_idx = *schema_.ExtractColumnFromRow<UINT32>(rb_row, 1);
        if (key_idx >= first_row && key_idx < first_row + expected_count) {
          size_t rel_idx = key_idx - first_row;
          if (seen_rows[rel_idx]) {
            FAIL() << "Saw row " << key_idx << " twice!\n"
                   << "Row: " << schema_.DebugRow(rb_row);
          }
          seen_rows[rel_idx] = true;
        }
      }
    }

    // Verify that all the rows were seen.
    for (int i = 0; i < expected_count; i++) {
      ASSERT_EQ(true, seen_rows[i]) << "Never saw row: " << (i + first_row);
    }
    LOG(INFO) << "Successfully verified " << expected_count << "rows";
  }

  // Iterate through the full table, stringifying the resulting rows
  // into the given vector. This is only useful in tests which insert
  // a very small number of rows.
  Status IterateToStringList(vector<string> *out) {
    gscoped_ptr<RowwiseIterator> iter;
    RETURN_NOT_OK(this->tablet()->NewRowIterator(this->client_schema_, &iter));
    RETURN_NOT_OK(iter->Init(NULL));
    return kudu::tablet::IterateToStringList(iter.get(), out);
  }

  // Return the number of rows in the tablet.
  uint64_t TabletCount() const {
    uint64_t count;
    CHECK_OK(tablet()->CountRows(&count));
    return count;
  }

  // because some types are small we need to
  // make sure that we don't overflow the type on inserts
  // or else we get errors because the key already exists
  uint64_t ClampRowCount(uint64_t proposal) const {
    uint64_t num_rows = min(max_rows_, proposal);
    if (num_rows < proposal) {
      LOG(WARNING) << "Clamping max rows to " << num_rows << " to prevent overflow";
    }
    return num_rows;
  }

  TESTSETUP setup_;

  const uint64_t max_rows_;

  Arena arena_;
};


} // namespace tablet
} // namespace kudu

#endif
