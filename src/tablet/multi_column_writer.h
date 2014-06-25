// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_TABLET_MULTI_COLUMN_WRITER_H
#define KUDU_TABLET_MULTI_COLUMN_WRITER_H

#include <glog/logging.h>
#include <vector>

#include "fs/fs_manager.h"
#include "gutil/macros.h"

namespace kudu {

class RowBlock;
class Schema;

namespace cfile {
class Writer;
} // namespace cfile

namespace tablet {

// Wrapper which writes several columns in parallel corresponding to some
// Schema.
class MultiColumnWriter {
 public:
  MultiColumnWriter(FsManager* fs,
                    const Schema* schema);

  virtual ~MultiColumnWriter();

  // Open and start writing the columns.
  Status Open();

  // Append the given block to the output columns.
  //
  // Note that the selection vector here is ignored.
  Status AppendBlock(const RowBlock& block);

  // Close the in-progress files.
  //
  // The file's blocks may be retrieved using FlushedBlocks().
  Status Finish();

  // Return the number of bytes written so far.
  size_t written_size() const;

  cfile::Writer* writer_for_col_idx(int i) {
    DCHECK_LT(i, cfile_writers_.size());
    return cfile_writers_[i];
  }

  // Return the block IDs of the written columns.
  //
  // REQUIRES: Finish() already called.
  std::vector<BlockId> FlushedBlocks() const;

 private:
  FsManager* const fs_;
  const Schema* const schema_;

  bool finished_;

  std::vector<cfile::Writer *> cfile_writers_;
  std::vector<BlockId> block_ids_;

  DISALLOW_COPY_AND_ASSIGN(MultiColumnWriter);
};

} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_MULTI_COLUMN_WRITER_H */
