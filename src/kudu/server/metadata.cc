// Copyright (c) 2013, Cloudera, inc.

#include "kudu/server/metadata.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <algorithm>
#include <utility>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/trace.h"
#include "kudu/server/metadata.pb.h"

namespace kudu {
namespace metadata {

using base::subtle::Barrier_AtomicIncrement;
using strings::Substitute;

const int64 kNoDurableMemStore = -1;

// ============================================================================
//  Tablet Metadata
// ============================================================================

Status TabletMetadata::CreateNew(FsManager* fs_manager,
                                 const TabletMasterBlockPB& master_block,
                                 const string& table_name,
                                 const Schema& schema,
                                 const QuorumPB& quorum,
                                 const string& start_key, const string& end_key,
                                 scoped_refptr<TabletMetadata>* metadata) {
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager,
                                                       master_block,
                                                       table_name,
                                                       schema,
                                                       quorum,
                                                       start_key,
                                                       end_key));
  RETURN_NOT_OK(ret->Flush());
  metadata->swap(ret);
  // TODO: should we verify that neither of the blocks referenced in the master block
  // exist?
  return Status::OK();
}

Status TabletMetadata::Load(FsManager* fs_manager,
                            const TabletMasterBlockPB& master_block,
                            scoped_refptr<TabletMetadata>* metadata) {
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager, master_block));
  RETURN_NOT_OK(ret->LoadFromDisk());
  metadata->swap(ret);
  return Status::OK();
}

Status TabletMetadata::LoadOrCreate(FsManager* fs_manager,
                                    const TabletMasterBlockPB& master_block,
                                    const string& table_name,
                                    const Schema& schema,
                                    const QuorumPB& quorum,
                                    const string& start_key, const string& end_key,
                                    scoped_refptr<TabletMetadata>* metadata) {
  Status s = Load(fs_manager, master_block, metadata);
  if (s.ok()) {
    if (!(*metadata)->schema().Equals(schema)) {
      return Status::Corruption(Substitute("Schema on disk ($0) does not "
        "match expected schema ($1)", (*metadata)->schema().ToString(),
        schema.ToString()));
    }
    return Status::OK();
  } else if (s.IsNotFound()) {
    return CreateNew(fs_manager, master_block, table_name, schema,
                     quorum, start_key, end_key, metadata);
  } else {
    return s;
  }
}

Status TabletMetadata::OpenMasterBlock(Env* env,
                                       const string& master_block_path,
                                       const string& expected_tablet_id,
                                       TabletMasterBlockPB* master_block) {
  RETURN_NOT_OK(pb_util::ReadPBFromPath(env, master_block_path, master_block));
  if (expected_tablet_id != master_block->tablet_id()) {
    LOG_AND_RETURN(ERROR, Status::Corruption(
        strings::Substitute("Corrupt master block $0: PB has wrong tablet ID",
                            master_block_path),
        master_block->ShortDebugString()));
  }
  return Status::OK();
}

Status TabletMetadata::PersistMasterBlock(FsManager* fs_manager,
                                          const TabletMasterBlockPB& pb) {
  string path = fs_manager->GetMasterBlockPath(pb.tablet_id());
  return pb_util::WritePBToPath(fs_manager->env(), path, pb);
}


TabletMetadata::TabletMetadata(FsManager *fs_manager,
                               const TabletMasterBlockPB& master_block,
                               const string& table_name,
                               const Schema& schema,
                               const QuorumPB& quorum,
                               const string& start_key,
                               const string& end_key)
  : state_(kNotWrittenYet),
    start_key_(start_key), end_key_(end_key),
    fs_manager_(fs_manager),
    master_block_(master_block),
    sblk_sequence_(0),
    next_rowset_idx_(0),
    last_durable_mrs_id_(kNoDurableMemStore),
    schema_(schema),
    schema_version_(0),
    table_name_(table_name),
    quorum_(quorum),
    num_flush_pins_(0),
    needs_flush_(false) {
  CHECK(schema_.has_column_ids());
}

TabletMetadata::~TabletMetadata() {
}

TabletMetadata::TabletMetadata(FsManager *fs_manager, const TabletMasterBlockPB& master_block)
  : state_(kNotLoadedYet),
    fs_manager_(fs_manager),
    master_block_(master_block),
    next_rowset_idx_(0),
    num_flush_pins_(0),
    needs_flush_(false) {
}

Status TabletMetadata::LoadFromDisk() {
  CHECK_EQ(state_, kNotLoadedYet);
  TabletSuperBlockPB superblock;
  RETURN_NOT_OK(ReadSuperBlock(&superblock));
  VLOG(1) << "Loaded tablet superblock " << superblock.DebugString();

  // Verify that the tablet id matches with the one in the protobuf
  if (superblock.oid() != master_block_.tablet_id()) {
    return Status::Corruption("Expected id=" + master_block_.tablet_id() +
                              " found " + superblock.oid(),
                              superblock.DebugString());
  }

  sblk_sequence_ = superblock.sequence() + 1;
  start_key_ = superblock.start_key();
  end_key_ = superblock.end_key();
  last_durable_mrs_id_ = superblock.last_durable_mrs_id();

  table_name_ = superblock.table_name();
  schema_version_ = superblock.schema_version();
  RETURN_NOT_OK_PREPEND(SchemaFromPB(superblock.schema(), &schema_),
                        "Failed to parse Schema from superblock " +
                        superblock.ShortDebugString());
  DCHECK(schema_.has_column_ids());

  quorum_ = superblock.quorum();

  BOOST_FOREACH(const RowSetDataPB& rowset_pb, superblock.rowsets()) {
    gscoped_ptr<RowSetMetadata> rowset_meta;
    RETURN_NOT_OK(RowSetMetadata::Load(this, rowset_pb, &rowset_meta));
    next_rowset_idx_ = std::max(next_rowset_idx_, rowset_meta->id() + 1);
    rowsets_.push_back(shared_ptr<RowSetMetadata>(rowset_meta.release()));
  }

  state_ = kInitialized;
  return Status::OK();
}

Status TabletMetadata::ReadSuperBlock(TabletSuperBlockPB *pb) {
  CHECK_EQ(state_, kNotLoadedYet);
  TabletSuperBlockPB pb2;
  Status sa, sb;

  // Try to read the block_a if exists
  sa = fs_manager_->ReadMetadataBlock(BlockId(master_block_.block_a()), pb);

  // Try to read the block_b if exists
  sb = fs_manager_->ReadMetadataBlock(BlockId(master_block_.block_b()), &pb2);

  // Both super-blocks are valid, pick the latest
  if (sa.ok() && sb.ok()) {
    if (pb->sequence() < pb2.sequence()) {
      *pb = pb2;
    }
    return Status::OK();
  }

  // block-a is valid, block-b is not (may not exists or be corrupted)
  if (sa.ok() && !sb.ok()) {
    return Status::OK();
  }

  // block-b is valid, block-a is not (may not exists or be corrupted)
  if (!sa.ok() && sb.ok()) {
    *pb = pb2;
    return Status::OK();
  }

  // No super-block found
  if (sa.IsNotFound() && sb.IsNotFound()) {
    return Status::NotFound("Tablet '" + master_block_.tablet_id() + "' SuperBlock not found",
                            master_block_.DebugString());
  }

  // Both super-blocks are corrupted
  if (sa.IsCorruption() && sb.IsCorruption()) {
    return Status::NotFound("Tablet '" + master_block_.tablet_id() + "' SuperBlocks are corrupted",
                            master_block_.DebugString());
  }

  return sa;
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add,
                                      shared_ptr<TabletSuperBlockPB> *super_block) {
  boost::lock_guard<LockType> l(lock_);
  return UpdateAndFlushUnlocked(to_remove, to_add, last_durable_mrs_id_, super_block);
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add,
                                      int64_t last_durable_mrs_id,
                                      shared_ptr<TabletSuperBlockPB> *super_block) {
  boost::lock_guard<LockType> l(lock_);
  return UpdateAndFlushUnlocked(to_remove, to_add, last_durable_mrs_id, super_block);
}

void TabletMetadata::PinFlush() {
  boost::lock_guard<LockType> l(lock_);
  CHECK_GE(num_flush_pins_, 0);
  num_flush_pins_++;
  VLOG(1) << "Number of flush pins: " << num_flush_pins_;
}

Status TabletMetadata::UnPinFlush() {
  boost::lock_guard<LockType> l(lock_);
  CHECK_GT(num_flush_pins_, 0);
  num_flush_pins_--;
  if (needs_flush_) {
    RETURN_NOT_OK(Flush());
  }
  return Status::OK();
}

Status TabletMetadata::Flush() {
  boost::lock_guard<LockType> l(lock_);
  CHECK_GE(num_flush_pins_, 0);
  if (num_flush_pins_ > 0) {
    needs_flush_ = true;
    LOG(INFO) << "Not flushing: waiting for " << num_flush_pins_ << " pins to be released.";
    return Status::OK();
  }
  needs_flush_ = false;
  return UpdateAndFlushUnlocked(RowSetMetadataIds(), RowSetMetadataVector(),
                                last_durable_mrs_id_, NULL);
}

Status TabletMetadata::UpdateAndFlushUnlocked(
    const RowSetMetadataIds& to_remove,
    const RowSetMetadataVector& to_add,
    int64_t last_durable_mrs_id,
    shared_ptr<TabletSuperBlockPB> *super_block) {
  CHECK_NE(state_, kNotLoadedYet);
  DCHECK_GE(last_durable_mrs_id, last_durable_mrs_id_);
  last_durable_mrs_id_ = last_durable_mrs_id;

  RowSetMetadataVector new_rowsets = rowsets_;
  RowSetMetadataVector::iterator it = new_rowsets.begin();
  while (it != new_rowsets.end()) {
    if (ContainsKey(to_remove, (*it)->id())) {
      it = new_rowsets.erase(it);
    } else {
      it++;
    }
  }

  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, to_add) {
    new_rowsets.push_back(meta);
  }

  shared_ptr<TabletSuperBlockPB> pb(new TabletSuperBlockPB());
  RETURN_NOT_OK(ToSuperBlockUnlocked(&pb, new_rowsets));

  RETURN_NOT_OK(ReplaceSuperBlockUnlocked(*pb));
  rowsets_ = new_rowsets;
  if (super_block != NULL) {
    super_block->swap(pb);
  }
  TRACE("Metadata flushed");
  return Status::OK();
}

Status TabletMetadata::ReplaceSuperBlock(const TabletSuperBlockPB &pb) {
  boost::lock_guard<LockType> l(lock_);
  return ReplaceSuperBlockUnlocked(pb);
}

Status TabletMetadata::ReplaceSuperBlockUnlocked(const TabletSuperBlockPB &pb) {
  // Flush
  BlockId a_blk(master_block_.block_a());
  BlockId b_blk(master_block_.block_b());
  if (sblk_sequence_ & 1) {
    TRACE("Writing metadata block");
    RETURN_NOT_OK(fs_manager_->WriteMetadataBlock(a_blk, pb));
    TRACE("Deleting old metadata block");
    Status s = fs_manager_->DeleteBlock(b_blk);
    if (!s.ok() && !s.IsNotFound()) {
      WARN_NOT_OK(s, "Unable to delete old metadata block " + b_blk.ToString()
                  + " for tablet " + oid());
    }
  } else {
    TRACE("Writing metadata block");
    RETURN_NOT_OK(fs_manager_->WriteMetadataBlock(b_blk, pb));
    TRACE("Deleting old metadata block");
    Status s = fs_manager_->DeleteBlock(a_blk);
    if (!s.ok() && !s.IsNotFound()) {
      WARN_NOT_OK(s, "Unable to delete old metadata block " + a_blk.ToString()
                  + " for tablet " + oid());
    }
  }

  sblk_sequence_++;
  return Status::OK();
}

Status TabletMetadata::ToSuperBlock(shared_ptr<TabletSuperBlockPB> *super_block) const {
  // acquire the lock so that rowsets_ doesn't get changed until we're finished.
  boost::lock_guard<LockType> l(lock_);
  return ToSuperBlockUnlocked(super_block, rowsets_);
}

Status TabletMetadata::ToSuperBlockUnlocked(shared_ptr<TabletSuperBlockPB> *super_block,
                                            const RowSetMetadataVector& rowsets) const {

  // Convert to protobuf
  gscoped_ptr<TabletSuperBlockPB> pb(new TabletSuperBlockPB());
  pb->set_sequence(sblk_sequence_);
  pb->set_oid(oid());
  pb->set_start_key(start_key_);
  pb->set_end_key(end_key_);
  pb->set_last_durable_mrs_id(last_durable_mrs_id_);
  pb->set_schema_version(schema_version_);
  pb->set_table_name(table_name_);

  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, rowsets) {
    meta->ToProtobuf(pb->add_rowsets());
  }

  DCHECK(schema_.has_column_ids());
  RETURN_NOT_OK_PREPEND(SchemaToPB(schema_, pb->mutable_schema()),
                        "Couldn't serialize schema into superblock");

  pb->mutable_quorum()->CopyFrom(quorum_);

  super_block->reset(pb.release());
  return Status::OK();
}

Status TabletMetadata::CreateRowSet(shared_ptr<RowSetMetadata> *rowset,
                                    const Schema& schema) {
  AtomicWord rowset_idx = Barrier_AtomicIncrement(&next_rowset_idx_, 1) - 1;
  gscoped_ptr<RowSetMetadata> scoped_rsm;
  RETURN_NOT_OK(RowSetMetadata::CreateNew(this, rowset_idx, schema, &scoped_rsm));
  rowset->reset(DCHECK_NOTNULL(scoped_rsm.release()));
  return Status::OK();
}

const RowSetMetadata *TabletMetadata::GetRowSetForTests(int64_t id) const {
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& rowset_meta, rowsets_) {
    if (rowset_meta->id() == id) {
      return rowset_meta.get();
    }
  }
  return NULL;
}

RowSetMetadata *TabletMetadata::GetRowSetForTests(int64_t id) {
  boost::lock_guard<LockType> l(lock_);
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& rowset_meta, rowsets_) {
    if (rowset_meta->id() == id) {
      return rowset_meta.get();
    }
  }
  return NULL;
}

void TabletMetadata::SetSchema(const Schema& schema, uint32_t version) {
  DCHECK(schema.has_column_ids());
  boost::lock_guard<LockType> l(lock_);
  schema_ = schema;
  schema_version_ = version;
}

void TabletMetadata::SetTableName(const string& table_name) {
  boost::lock_guard<LockType> l(lock_);
  table_name_ = table_name;
}

const string& TabletMetadata::table_name() const {
  boost::lock_guard<LockType> l(lock_);
  DCHECK_NE(state_, kNotLoadedYet);
  return table_name_;
}

uint32_t TabletMetadata::schema_version() const {
  boost::lock_guard<LockType> l(lock_);
  DCHECK_NE(state_, kNotLoadedYet);
  return schema_version_;
}

Schema TabletMetadata::schema() const {
  boost::lock_guard<LockType> l(lock_);
  return schema_;
}

void TabletMetadata::SetQuorum(const QuorumPB& quorum) {
  boost::lock_guard<LockType> l(lock_);
  quorum_ = quorum;
}

QuorumPB TabletMetadata::Quorum() const {
  boost::lock_guard<LockType> l(lock_);
  return quorum_;
}

// ============================================================================
//  RowSet Metadata
// ============================================================================
Status RowSetMetadata::Load(TabletMetadata* tablet_metadata,
                            const RowSetDataPB& pb,
                            gscoped_ptr<RowSetMetadata>* metadata) {
  gscoped_ptr<RowSetMetadata> ret(new RowSetMetadata(tablet_metadata));
  RETURN_NOT_OK(ret->InitFromPB(pb));
  metadata->reset(ret.release());
  return Status::OK();
}

Status RowSetMetadata::CreateNew(TabletMetadata* tablet_metadata,
                                 int64_t id,
                                 const Schema& schema,
                                 gscoped_ptr<RowSetMetadata>* metadata) {
  metadata->reset(new RowSetMetadata(tablet_metadata, id, schema));
  return Status::OK();
}

Status RowSetMetadata::InitFromPB(const RowSetDataPB& pb) {
  CHECK(!initted_);

  id_ = pb.id();

  // Load Bloom File
  if (pb.has_bloom_block()) {
    bloom_block_ = BlockId::FromPB(pb.bloom_block());
  }

  // Load AdHoc Index File
  if (pb.has_adhoc_index_block()) {
    adhoc_index_block_ = BlockId::FromPB(pb.adhoc_index_block());
  }

  // Load Column Files
  int key_columns = 0;
  std::vector<size_t> cols_ids;
  std::vector<ColumnSchema> cols;
  BOOST_FOREACH(const ColumnDataPB& col_pb, pb.columns()) {
    column_blocks_.push_back(BlockId::FromPB(col_pb.block()));
    cols.push_back(ColumnSchemaFromPB(col_pb.schema()));
    cols_ids.push_back(col_pb.schema().id());
    key_columns += !!col_pb.schema().is_key();
  }
  RETURN_NOT_OK(schema_.Reset(cols, cols_ids, key_columns));

  // Load redo delta files
  BOOST_FOREACH(const DeltaDataPB& redo_delta_pb, pb.redo_deltas()) {
    redo_delta_blocks_.push_back(BlockId::FromPB(redo_delta_pb.block()));
  }

  last_durable_redo_dms_id_ = pb.last_durable_dms_id();

  // Load undo delta files
  BOOST_FOREACH(const DeltaDataPB& undo_delta_pb, pb.undo_deltas()) {
    undo_delta_blocks_.push_back(BlockId::FromPB(undo_delta_pb.block()));
  }

  initted_ = true;
  return Status::OK();
}

void RowSetMetadata::ToProtobuf(RowSetDataPB *pb) {
  pb->set_id(id_);

  // Write Column Files
  size_t idx = 0;
  BOOST_FOREACH(const BlockId& block_id, column_blocks_) {
    ColumnDataPB *col_data = pb->add_columns();
    ColumnSchemaPB *col_schema = col_data->mutable_schema();
    block_id.CopyToPB(col_data->mutable_block());
    ColumnSchemaToPB(schema_.column(idx), col_schema);
    col_schema->set_id(schema_.column_id(idx));
    col_schema->set_is_key(idx < schema_.num_key_columns());
    idx++;
  }

  // Write Delta Files
  {
    boost::lock_guard<LockType> l(deltas_lock_);
    pb->set_last_durable_dms_id(last_durable_redo_dms_id_);

    BOOST_FOREACH(const BlockId& redo_delta_block, redo_delta_blocks_) {
      DeltaDataPB *redo_delta_pb = pb->add_redo_deltas();
      redo_delta_block.CopyToPB(redo_delta_pb->mutable_block());
    }

    BOOST_FOREACH(const BlockId& undo_delta_block, undo_delta_blocks_) {
      DeltaDataPB *undo_delta_pb = pb->add_undo_deltas();
      undo_delta_block.CopyToPB(undo_delta_pb->mutable_block());
    }
  }

  // Write Bloom File
  if (!bloom_block_.IsNull()) {
    bloom_block_.CopyToPB(pb->mutable_bloom_block());
  }

  // Write AdHoc Index
  if (!adhoc_index_block_.IsNull()) {
    adhoc_index_block_.CopyToPB(pb->mutable_adhoc_index_block());
  }
}

const string RowSetMetadata::ToString() const {
  return "RowSet(" + boost::lexical_cast<string>(id_) + ")";
}

void RowSetMetadata::SetColumnDataBlocks(const std::vector<BlockId>& blocks) {
  CHECK_EQ(blocks.size(), schema_.num_columns());
  boost::lock_guard<LockType> l(deltas_lock_);
  column_blocks_ = blocks;
}

Status RowSetMetadata::CommitRedoDeltaDataBlock(int64_t dms_id,
                                                const BlockId& block_id) {
  boost::lock_guard<LockType> l(deltas_lock_);
  last_durable_redo_dms_id_ = dms_id;
  redo_delta_blocks_.push_back(block_id);
  return Status::OK();
}

Status RowSetMetadata::CommitUndoDeltaDataBlock(const BlockId& block_id) {
  boost::lock_guard<LockType> l(deltas_lock_);
  undo_delta_blocks_.push_back(block_id);
  return Status::OK();
}

Status RowSetMetadata::CommitUpdate(const RowSetMetadataUpdate& update) {
  boost::lock_guard<LockType> l(deltas_lock_);

  BOOST_FOREACH(const RowSetMetadataUpdate::ReplaceDeltaBlocks rep, update.replace_redo_blocks_) {
    CHECK(!rep.to_remove.empty());

    vector<BlockId>::iterator start_it =
      std::find(redo_delta_blocks_.begin(), redo_delta_blocks_.end(), rep.to_remove[0]);

    vector<BlockId>::iterator end_it = start_it;
    BOOST_FOREACH(const BlockId& b, rep.to_remove) {
      if (end_it == redo_delta_blocks_.end() || *end_it != b) {
        return Status::InvalidArgument(
          Substitute("Cannot find subsequence <$0> in <$1>",
                     BlockId::JoinStrings(rep.to_remove),
                     BlockId::JoinStrings(redo_delta_blocks_)));
      }
      ++end_it;
    }

    redo_delta_blocks_.erase(start_it, end_it);
    redo_delta_blocks_.insert(start_it, rep.to_add.begin(), rep.to_add.end());
  }

  // Add new redo blocks
  BOOST_FOREACH(const BlockId& b, update.new_redo_blocks_) {
    redo_delta_blocks_.push_back(b);
  }

  typedef std::pair<int, BlockId> IntBlockPair;
  BOOST_FOREACH(const IntBlockPair& e, update.cols_to_replace_) {
    CHECK_LT(e.first, column_blocks_.size());
    column_blocks_[e.first] = e.second;
  }
  return Status::OK();
}

RowSetMetadataUpdate::RowSetMetadataUpdate() {
}
RowSetMetadataUpdate::~RowSetMetadataUpdate() {
}
RowSetMetadataUpdate& RowSetMetadataUpdate::ReplaceColumnBlock(
    int col_idx, const BlockId& block_id) {
  InsertOrDie(&cols_to_replace_, col_idx, block_id);
  return *this;
}
RowSetMetadataUpdate& RowSetMetadataUpdate::ReplaceRedoDeltaBlocks(
    const std::vector<BlockId>& to_remove,
    const std::vector<BlockId>& to_add) {

  ReplaceDeltaBlocks rdb = { to_remove, to_add };
  replace_redo_blocks_.push_back(rdb);
  return *this;
}



} // namespace metadata
} // namespace kudu
