// Copyright (c) 2012, Cloudera, inc.
//
// A DiskRowSet is a horizontal slice of a Kudu tablet.
// Each DiskRowSet contains data for a a disjoint set of keys.
// See src/tablet/README for a detailed description.

#ifndef KUDU_TABLET_DISKROWSET_H_
#define KUDU_TABLET_DISKROWSET_H_

#include <boost/thread/mutex.hpp>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "common/row.h"
#include "common/row_changelist.h"
#include "common/schema.h"
#include "gutil/macros.h"
#include "tablet/delta_key.h"
#include "server/metadata.h"
#include "tablet/rowset.h"
#include "util/bloom_filter.h"
#include "util/locks.h"

namespace kudu {

class RowBlock;

namespace cfile {
class BloomFileWriter;
class Writer;
}

namespace log {
class OpIdAnchorRegistry;
}

namespace tablet {

class CFileSet;
class DeltaFileWriter;
class DeltaStats;
class DeltaTracker;
class MultiColumnWriter;
class Mutation;
class OperationResultPB;

class DiskRowSetWriter {
 public:
  // TODO: document ownership of rowset_metadata
  DiskRowSetWriter(metadata::RowSetMetadata *rowset_metadata,
                   const BloomFilterSizing &bloom_sizing);

  ~DiskRowSetWriter();

  Status Open();

  // The block is written to all column writers as well as the bloom filter,
  // if configured.
  // Rows must be appended in ascending order.
  Status AppendBlock(const RowBlock &block);

  Status Finish();

  rowid_t written_count() const {
    CHECK(finished_);
    return written_count_;
  }

  // Return the total number of bytes written so far to this DiskRowSet.
  // Additional bytes may be written by "Finish()", but this should provide
  // a reasonable estimate for the total data size.
  size_t written_size() const;

  const Schema& schema() const { return rowset_metadata_->schema(); }

 private:
  DISALLOW_COPY_AND_ASSIGN(DiskRowSetWriter);

  Status InitBloomFileWriter();

  // Initializes the index writer required for compound keys
  // this index is written to a new file instead of embedded in the col_* files
  Status InitAdHocIndexWriter();

  // Return the cfile::Writer responsible for writing the key index.
  // (the ad-hoc writer for composite keys, otherwise the key column writer)
  cfile::Writer *key_index_writer();

  metadata::RowSetMetadata *rowset_metadata_;
  BloomFilterSizing bloom_sizing_;

  bool finished_;
  rowid_t written_count_;
  gscoped_ptr<MultiColumnWriter> col_writer_;
  gscoped_ptr<cfile::BloomFileWriter> bloom_writer_;
  gscoped_ptr<cfile::Writer> ad_hoc_index_writer_;

  // The last encoded key written.
  faststring last_encoded_key_;
};


// Wrapper around DiskRowSetWriter which "rolls" to a new DiskRowSet after
// a certain amount of data has been written. Each output rowset is suffixed
// with ".N" where N starts at 0 and increases as new rowsets are generated.
class RollingDiskRowSetWriter {
 public:
  // Create a new rolling writer. The given 'tablet_metadata' must stay valid
  // for the lifetime of this writer, and is used to construct the new rowsets
  // that this RollingDiskRowSetWriter creates.
  RollingDiskRowSetWriter(metadata::TabletMetadata* tablet_metadata,
                          const Schema &schema,
                          const BloomFilterSizing &bloom_sizing,
                          size_t target_rowset_size);
  ~RollingDiskRowSetWriter();

  Status Open();

  // The block is written to all column writers as well as the bloom filter,
  // if configured.
  // Rows must be appended in ascending order.
  Status AppendBlock(const RowBlock &block);

  // Appends a sequence of REDO deltas for the same row to the current
  // redo delta file. 'row_idx_in_next_block' is the positional index after
  // the last written block. The 'row_idx_in_drs' out parameter will be set
  // with the row index from the start of the DiskRowSet currently being written.
  Status AppendRedoDeltas(rowid_t row_idx_in_next_block,
                          Mutation* redo_deltas,
                          rowid_t* row_idx_in_drs);

  // Appends a sequence of UNDO deltas for the same row to the current
  // undo delta file. 'row_idx_in_next_block' is the positional index after
  // the last written block. The 'row_idx_in_drs' out parameter will be set
  // with the row index from the start of the DiskRowSet currently being written.
  Status AppendUndoDeltas(rowid_t row_idx_in_next_block,
                          Mutation* undo_deltas,
                          rowid_t* row_idx_in_drs);

  Status Finish();

  int64_t written_count() const { return written_count_; }

  const Schema &schema() const { return schema_; }

  // Return the set of rowset paths that were written by this writer.
  // This must only be called after Finish() returns an OK result.
  void GetWrittenRowSetMetadata(metadata::RowSetMetadataVector* metas) const;

  uint64_t written_size() const { return written_size_; }

 private:
  Status RollWriter();
  Status FinishCurrentWriter();

  template<DeltaType Type>
  Status AppendDeltas(rowid_t row_idx_in_block,
                      Mutation* delta_head,
                      rowid_t* row_idx,
                      DeltaFileWriter* writer,
                      DeltaStats* delta_stats);

  enum State {
    kInitialized,
    kStarted,
    kFinished
  };
  State state_;

  metadata::TabletMetadata* tablet_metadata_;
  const Schema schema_;
  shared_ptr<metadata::RowSetMetadata> cur_drs_metadata_;
  const BloomFilterSizing bloom_sizing_;
  const size_t target_rowset_size_;

  gscoped_ptr<DiskRowSetWriter> cur_writer_;

  // A delta writer to store the undos for each DRS
  gscoped_ptr<DeltaFileWriter> cur_undo_writer_;
  gscoped_ptr<DeltaStats> cur_undo_delta_stats;
  // a delta writer to store the redos for each DRS
  gscoped_ptr<DeltaFileWriter> cur_redo_writer_;
  gscoped_ptr<DeltaStats> cur_redo_delta_stats;
  BlockId cur_undo_ds_block_id_;
  BlockId cur_redo_ds_block_id_;

  uint64_t row_idx_in_cur_drs_;

  // The index for the next output.
  int output_index_;

  // RowSetMetadata objects for diskrowsets which have been successfully
  // written out.
  metadata::RowSetMetadataVector written_drs_metas_;

  int64_t written_count_;
  uint64_t written_size_;

  DISALLOW_COPY_AND_ASSIGN(RollingDiskRowSetWriter);
};

////////////////////////////////////////////////////////////
// DiskRowSet
////////////////////////////////////////////////////////////

class MajorDeltaCompaction;
class RowSetColumnUpdater;

class DiskRowSet : public RowSet {
 public:
  static const char *kMinKeyMetaEntryName;
  static const char *kMaxKeyMetaEntryName;

  // Open a rowset from disk.
  // If successful, sets *rowset to the newly open rowset
  static Status Open(const shared_ptr<metadata::RowSetMetadata>& rowset_metadata,
                     log::OpIdAnchorRegistry* opid_anchor_registry,
                     shared_ptr<DiskRowSet> *rowset);

  ////////////////////////////////////////////////////////////
  // "Management" functions
  ////////////////////////////////////////////////////////////

  // Flush all accumulated delta data to disk.
  Status FlushDeltas() OVERRIDE;

  // Perform delta store minor compaction.
  // This compacts the delta files down to a single one.
  // If there is already only a single delta file, this does nothing.
  Status MinorCompactDeltaStores() OVERRIDE;

  ////////////////////////////////////////////////////////////
  // RowSet implementation
  ////////////////////////////////////////////////////////////

  ////////////////////
  // Updates
  ////////////////////

  // Update the given row.
  // 'key' should be the key portion of the row -- i.e a contiguous
  // encoding of the key columns.
  Status MutateRow(Timestamp timestamp,
                   const RowSetKeyProbe &probe,
                   const RowChangeList &update,
                   const consensus::OpId& op_id,
                   ProbeStats* stats,
                   OperationResultPB* result) OVERRIDE;

  Status CheckRowPresent(const RowSetKeyProbe &probe,
                         bool *present,
                         ProbeStats* stats) const OVERRIDE;

  ////////////////////
  // Read functions.
  ////////////////////
  RowwiseIterator *NewRowIterator(const Schema *projection,
                                  const MvccSnapshot &snap) const OVERRIDE;

  virtual CompactionInput *NewCompactionInput(const Schema* projection,
                                              const MvccSnapshot &snap) const OVERRIDE;

  // Count the number of rows in this rowset.
  Status CountRows(rowid_t *count) const OVERRIDE;

  // See RowSet::GetBounds(...)
  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const OVERRIDE;

  // Estimate the number of bytes on-disk
  uint64_t EstimateOnDiskSize() const OVERRIDE;

  size_t DeltaMemStoreSize() const OVERRIDE;

  size_t CountDeltaStores() const OVERRIDE;

  Status MajorCompactDeltaStores(const metadata::ColumnIndexes& col_indexes);

  Status AlterSchema(const Schema& schema) OVERRIDE;

  boost::mutex *compact_flush_lock() OVERRIDE {
    return &compact_flush_lock_;
  }

  DeltaTracker *delta_tracker() {
    return DCHECK_NOTNULL(delta_tracker_.get());
  }

  shared_ptr<metadata::RowSetMetadata> metadata() OVERRIDE {
    return rowset_metadata_;
  }

  const Schema& schema() const OVERRIDE {
    return rowset_metadata_->schema();
  }

  std::string ToString() const OVERRIDE {
    return rowset_metadata_->ToString();
  }

  virtual Status DebugDump(std::vector<std::string> *out = NULL) OVERRIDE;

 private:
  FRIEND_TEST(TestRowSet, TestRowSetUpdate);
  FRIEND_TEST(TestRowSet, TestDMSFlush);
  FRIEND_TEST(TestCompaction, TestOneToOne);

  friend class CompactionInput;
  friend class Tablet;

  DiskRowSet(const shared_ptr<metadata::RowSetMetadata>& rowset_metadata,
             log::OpIdAnchorRegistry* opid_anchor_registry);

  Status Open();

  // Create a new major delta compaction object to compact the specified columns.
  MajorDeltaCompaction* NewMajorDeltaCompaction(
    const metadata::ColumnIndexes& col_indexes) const;

  shared_ptr<metadata::RowSetMetadata> rowset_metadata_;

  bool open_;

  log::OpIdAnchorRegistry* opid_anchor_registry_;

  // Base data for this rowset.
  mutable percpu_rwlock component_lock_;
  shared_ptr<CFileSet> base_data_;
  gscoped_ptr<DeltaTracker> delta_tracker_;

  // Lock governing this rowset's inclusion in a compact/flush. If locked,
  // no other compactor will attempt to include this rowset.
  boost::mutex compact_flush_lock_;

  DISALLOW_COPY_AND_ASSIGN(DiskRowSet);
};

} // namespace tablet
} // namespace kudu

#endif // KUDU_TABLET_DISKROWSET_H_
