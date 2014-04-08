// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_ROWSET_H
#define KUDU_TABLET_ROWSET_H

#include <string>
#include <vector>

#include "cfile/cfile_util.h"
#include "common/iterator.h"
#include "common/rowid.h"
#include "common/schema.h"
#include "gutil/macros.h"
#include "tablet/mvcc.h"
#include "util/bloom_filter.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {

class RowChangeList;

namespace consensus {
class OpId;
}

namespace metadata {
class RowSetMetadata;
}

namespace tablet {

class CompactionInput;
class MvccSnapshot;
class RowSetKeyProbe;
class OperationResultPB;
struct ProbeStats;

class RowSet {
 public:
  // Check if a given row key is present in this rowset.
  // Sets *present and returns Status::OK, unless an error
  // occurs.
  //
  // If the row was once present in this rowset, but no longer present
  // due to a DELETE, then this should set *present = false, as if
  // it were never there.
  virtual Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present,
                                 ProbeStats* stats) const = 0;

  // Update/delete a row in this rowset.
  // The 'update_schema' is the client schema used to encode the 'update' RowChangeList.
  //
  // If the row does not exist in this rowset, returns
  // Status::NotFound().
  virtual Status MutateRow(Timestamp timestamp,
                           const RowSetKeyProbe &probe,
                           const RowChangeList &update,
                           const consensus::OpId& op_id,
                           ProbeStats* stats,
                           OperationResultPB* result) = 0;

  // Return a new RowIterator for this rowset, with the given projection.
  // The projection schema must remain valid for the lifetime of the iterator.
  // The iterator will return rows/updates which were committed as of the time of
  // 'snap'.
  // The returned iterator is not Initted.
  virtual RowwiseIterator *NewRowIterator(const Schema *projection,
                                          const MvccSnapshot &snap) const = 0;

  // Create the input to be used for a compaction.
  // The provided 'projection' is for the compaction output. Each row
  // will be projected into this Schema.
  virtual CompactionInput *NewCompactionInput(const Schema* projection,
                                              const MvccSnapshot &snap) const = 0;

  // Count the number of rows in this rowset.
  virtual Status CountRows(rowid_t *count) const = 0;

  // Return the bounds for this RowSet. 'min_encoded_key' and 'max_encoded_key'
  // are set to the first and last encoded keys for this RowSet. The storage
  // for these slices is part of the RowSet and only guaranteed to stay valid
  // until the RowSet is destroyed.
  //
  // In the case that the rowset is still mutable (eg MemRowSet), this may
  // return Status::NotImplemented.
  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const = 0;

  // Return a displayable string for this rowset.
  virtual string ToString() const = 0;

  // Dump the full contents of this rowset, for debugging.
  // This is very verbose so only useful within unit tests.
  virtual Status DebugDump(vector<string> *lines = NULL) = 0;

  // Estimate the number of bytes on-disk
  virtual uint64_t EstimateOnDiskSize() const = 0;

  // Return the lock used for including this DiskRowSet in a compaction.
  // This prevents multiple compactions and flushes from trying to include
  // the same rowset.
  virtual boost::mutex *compact_flush_lock() = 0;

  // Return the schema for data in this rowset.
  virtual const Schema &schema() const = 0;

  // Alter RowSet Schema
  virtual Status AlterSchema(const Schema& schema) = 0;

  // Returns the metadata associated with this rowset.
  virtual shared_ptr<metadata::RowSetMetadata> metadata() = 0;

  // Get the size of the delta's MemStore
  virtual size_t DeltaMemStoreSize() const = 0;

  // Return the number of separate delta stores in the rowset,
  // not including the DeltaMemStore.
  virtual size_t CountDeltaStores() const = 0;

  // Flush the DMS if there's one
  virtual Status FlushDeltas() = 0;

  // Compact delta stores if more than one.
  virtual Status MinorCompactDeltaStores() = 0;


  virtual ~RowSet() {}

  // Return true if this RowSet is available for compaction, based on
  // the current state of the compact_flush_lock. This should only be
  // used under the Tablet's compaction selection lock, or else the
  // lock status may change at any point.
  virtual bool IsAvailableForCompaction() {
    // Try to obtain the lock. If we don't succeed, it means the rowset
    // was already locked for compaction by some other compactor thread,
    // or it is a RowSet type which can't be used as a compaction input.
    //
    // We can be sure that our check here will remain true until after
    // the compaction selection has finished because only one thread
    // makes compaction selection at a time on a given Tablet due to
    // Tablet::compact_select_lock_.
    boost::mutex::scoped_try_lock try_lock(*compact_flush_lock());
    return try_lock.owns_lock();
  }

};

// Used often enough, may as well typedef it.
typedef vector<shared_ptr<RowSet> > RowSetVector;
// Structure which caches an encoded and hashed key, suitable
// for probing against rowsets.
class RowSetKeyProbe {
 public:
  // row_key: a reference to the key portion of a row in memory
  // to probe for.
  //
  // NOTE: row_key is not copied and must be valid for the lifetime
  // of this object.
  explicit RowSetKeyProbe(const ConstContiguousRow& row_key)
      : row_key_(row_key) {
    cfile::EncodeKey(row_key, &encoded_key_);
    bloom_probe_ = BloomKeyProbe(encoded_key_slice());
  }

  // RowSetKeyProbes are usually allocated on the stack, which means that we
  // must copy it if we require it later (e.g. Table::Mutate()).
  //
  // Still, the ConstContiguousRow row_key_ remains a reference to the data
  // underlying the original RowsetKeyProbe and is not copied.
  explicit RowSetKeyProbe(const RowSetKeyProbe& probe)
  : row_key_(probe.row_key_) {
    cfile::EncodeKey(row_key_, &encoded_key_);
    bloom_probe_ = BloomKeyProbe(encoded_key_slice());
  }

  const ConstContiguousRow& row_key() const { return row_key_; }

  // Pointer to the key which has been encoded to be contiguous
  // and lexicographically comparable
  const Slice &encoded_key_slice() const { return encoded_key_->encoded_key(); }

  // Return the cached structure used to query bloom filters.
  const BloomKeyProbe &bloom_probe() const { return bloom_probe_; }

  // The schema containing the key.
  const Schema &schema() const { return row_key_.schema(); }

  const EncodedKey &encoded_key() const {
    return *encoded_key_;
  }

 private:
  const ConstContiguousRow& row_key_;
  gscoped_ptr<EncodedKey> encoded_key_;
  BloomKeyProbe bloom_probe_;
};

// Statistics collected during row operations, counting how many times
// various structures had to be consulted to perform the operation.
//
// These eventually propagate into tablet-scoped metrics, and when we
// have RPC tracing capability, we could also stringify them into the
// trace to understand why an RPC may have been slow.
struct ProbeStats {
  ProbeStats()
    : blooms_consulted(0),
      keys_consulted(0),
      deltas_consulted(0),
      mrs_consulted(0) {
  }

  // Incremented for each bloom filter consulted.
  int blooms_consulted;

  // Incremented for each key cfile consulted.
  int keys_consulted;

  // Incremented for each delta file consulted.
  int deltas_consulted;

  // Incremented for each MemRowSet consulted.
  int mrs_consulted;
};

// RowSet which is used during the middle of a flush or compaction.
// It consists of a set of one or more input rowsets, and a single
// output rowset. All mutations are duplicated to the appropriate input
// rowset as well as the output rowset. All reads are directed to the
// union of the input rowsets.
//
// See compaction.txt for a little more detail on how this is used.
class DuplicatingRowSet : public RowSet {
 public:
  DuplicatingRowSet(const RowSetVector &old_rowsets,
                    const RowSetVector &new_rowsets);

  virtual Status MutateRow(Timestamp timestamp,
                           const RowSetKeyProbe &probe,
                           const RowChangeList &update,
                           const consensus::OpId& op_id,
                           ProbeStats* stats,
                           OperationResultPB* result) OVERRIDE;

  Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present,
                         ProbeStats* stats) const;

  virtual RowwiseIterator *NewRowIterator(const Schema *projection,
                                          const MvccSnapshot &snap) const OVERRIDE;

  virtual CompactionInput *NewCompactionInput(const Schema* projection,
                                              const MvccSnapshot &snap) const OVERRIDE;

  Status CountRows(rowid_t *count) const;

  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const;

  uint64_t EstimateOnDiskSize() const;

  Status AlterSchema(const Schema& schema);

  string ToString() const;

  virtual Status DebugDump(vector<string> *lines = NULL);

  shared_ptr<metadata::RowSetMetadata> metadata();

  // A flush-in-progress rowset should never be selected for compaction.
  boost::mutex *compact_flush_lock() {
    LOG(FATAL) << "Cannot be compacted";
    return NULL;
  }

  virtual bool IsAvailableForCompaction() OVERRIDE {
    return false;
  }

  ~DuplicatingRowSet();

  const Schema &schema() const {
    return schema_;
  }

  size_t DeltaMemStoreSize() const { return 0; }

  size_t CountDeltaStores() const { return 0; }

  Status FlushDeltas() { return Status::OK(); }

  Status MinorCompactDeltaStores() { return Status::OK(); }

 private:
  friend class Tablet;

  DISALLOW_COPY_AND_ASSIGN(DuplicatingRowSet);

  RowSetVector old_rowsets_;
  RowSetVector new_rowsets_;

  const Schema &schema_;
  const Schema key_schema_;
};


} // namespace tablet
} // namespace kudu

#endif
