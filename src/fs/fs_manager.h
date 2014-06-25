// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_FS_FS_MANAGER_H
#define KUDU_FS_FS_MANAGER_H

#include <iosfwd>
#include <tr1/memory>
#include <string>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "util/env.h"
#include "util/oid_generator.h"
#include "util/path_util.h"

namespace google {
namespace protobuf {
class MessageLite;
} // namespace protobuf
} // namespace google

namespace kudu {

class BlockId;

class InstanceMetadataPB;

// FsManager provides helpers to read data and metadata files,
// and it's responsible for abstracting the file-system layout.
//
// The user should not be aware of where files are placed,
// but instead should interact with the storage in terms of "open the block xyz"
// or "write a new schema metadata file for table kwz".
//
// The FsManager should never be accessed directly, but instead the "Metadata"
// wrappers like "TableMetadata" or "TabletMetadata" should be used.
// Those wrappers are also responsible for writing every transaction like
// "add this new data file" to the related WALs.
// The TabletMetadata is also responsible for keeping track of the tablet files;
// each new file added/removed is added to the WAL and then flushed
// to the "tablet/data-files" meta file.
//
// The current layout is:
//    <kudu.root.dir>/data/
//    <kudu.root.dir>/data/<prefix-0>/<prefix-2>/<prefix-4>/<name>
class FsManager {
 public:
  static const char *kWalFileNamePrefix;
  static const char *kWalsRecoveryDirSuffix;

  FsManager(Env *env, const string& root_path);

  ~FsManager();

  // Initialize and load the basic filesystem metadata.
  // If the file system has not been initialized, returns NotFound.
  // In that case, CreateInitialFileSystemLayout may be used to initialize
  // the on-disk structures.
  Status Open();

  // Create the initial filesystem layout.
  // This has no effect if the layout is already initialized.
  Status CreateInitialFileSystemLayout();
  void DumpFileSystemTree(std::ostream& out);

  // Return the UUID persisted in the local filesystem. If Open()
  // has not been called, this will crash.
  const std::string& uuid() const;

  // ==========================================================================
  //  Data read/write interfaces
  // ==========================================================================

  Status CreateNewBlock(std::tr1::shared_ptr<WritableFile> *writer,
                        BlockId *block_id);
  Status OpenBlock(const BlockId& block_id,
                   std::tr1::shared_ptr<RandomAccessFile> *reader);

  Status DeleteBlock(const BlockId& block) {
    return env_->DeleteFile(GetBlockPath(block));
  }

  bool BlockExists(const BlockId& block) const {
    return env_->FileExists(GetBlockPath(block));
  }

  // ==========================================================================
  //  Metadata read/write interfaces
  // ==========================================================================

  Status WriteMetadataBlock(const BlockId& block_id,
                            const google::protobuf::MessageLite& msg);
  Status ReadMetadataBlock(const BlockId& block_id,
                           google::protobuf::MessageLite *msg);

  // ==========================================================================
  //  on-disk path
  // ==========================================================================
  const string& GetRootDir() const {
    return root_path_;
  }

  string GetDataRootDir() const {
    return JoinPathSegments(GetRootDir(), kDataDirName);
  }

  std::string GetBlockPath(const BlockId& block_id) const;

  std::string GetWalsRootDir() const {
    return JoinPathSegments(root_path_, kWalDirName);
  }

  std::string GetTabletWalDir(const std::string& tablet_id) const {
    return JoinPathSegments(GetWalsRootDir(), tablet_id);
  }

  std::string GetTabletWalRecoveryDir(const std::string& tablet_id) const;

  std::string GetWalSegmentFileName(const std::string& tablet_id,
                                    uint64_t sequence_number) const;

  // Return the directory where tablet master blocks should be stored.
  std::string GetMasterBlockDir() const;

  // Return the path for a specific tablet's master block.
  std::string GetMasterBlockPath(const std::string& tablet_id) const;

  // Return the path where InstanceMetadataPB is stored.
  std::string GetInstanceMetadataPath() const;

  // Generate a new block ID.
  BlockId GenerateBlockId();

  Env *env() { return env_; }

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  bool Exists(const std::string& path) const {
    return env_->FileExists(path);
  }

  Status ListDir(const std::string& path, std::vector<std::string> *objects) const {
    return env_->GetChildren(path, objects);
  }

  Status CreateDirIfMissing(const std::string& path) {
    Status s = env_->CreateDir(path);
    return s.IsAlreadyPresent() ? Status::OK() : s;
  }

 private:
  // Creates the parent directory hierarchy to contain the given block id.
  Status CreateBlockDir(const BlockId& block_id);

  // Create a new InstanceMetadataPB and save it to the filesystem.
  // Does not mutate the current state of the fsmanager.
  Status CreateAndWriteInstanceMetadata();

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  void DumpFileSystemTree(std::ostream& out,
                          const std::string& prefix,
                          const std::string& path,
                          const std::vector<std::string>& objects);

  static const char *kDataDirName;
  static const char *kMasterBlockDirName;
  static const char *kWalDirName;
  static const char *kCorruptedSuffix;
  static const char *kInstanceMetadataFileName;

  Env *env_;
  std::string root_path_;

  ObjectIdGenerator oid_generator_;

  gscoped_ptr<InstanceMetadataPB> metadata_;

  DISALLOW_COPY_AND_ASSIGN(FsManager);
};

} // namespace kudu

#endif
