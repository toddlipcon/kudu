// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/rpc/rpc_sidecar.h"

#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/buffer_chain.h"
#include "kudu/util/faststring.h"

using std::vector;

namespace kudu {
namespace rpc {

RpcSidecar::~RpcSidecar() {}

// ------------------------------------------------------------
// Buffer Chain sidecar
// ------------------------------------------------------------

class FaststringSidecar : public RpcSidecar {
 public:
  // Generates a sidecar with the parameter faststring as its data.
  explicit FaststringSidecar(gscoped_ptr<faststring> data) : data_(std::move(data)) {}

  // Returns a Slice representation of the sidecar's data.
  void AddSlices(vector<Slice>* slices) const OVERRIDE {
    slices->push_back(Slice(*data_));
  }

  int NumSlices() const OVERRIDE {
    return 1;
  }

  int TotalSize() const OVERRIDE {
    return data_->size();
  }

 private:
  const gscoped_ptr<faststring> data_;

  DISALLOW_COPY_AND_ASSIGN(FaststringSidecar);
};

gscoped_ptr<RpcSidecar> RpcSidecar::FromFaststring(gscoped_ptr<faststring> data) {
  return gscoped_ptr<RpcSidecar>(new FaststringSidecar(std::move(data)));
}

// ------------------------------------------------------------
// Buffer Chain sidecar
// ------------------------------------------------------------

class BufferChainSidecar : public RpcSidecar {
 public:
  // Generates a sidecar with the parameter faststring as its data.
  explicit BufferChainSidecar(gscoped_ptr<BufferChain> data) : data_(data.Pass()) {
    data_->GetSlices(&slices_);
  }

  // Returns a Slice representation of the sidecar's data.
  void AddSlices(vector<Slice>* slices) const OVERRIDE {
    slices->insert(slices->end(), slices_.begin(), slices_.end());
  }

  int NumSlices() const OVERRIDE {
    return slices_.size();
  }

  int TotalSize() const OVERRIDE {
    return data_->size();
  }

 private:
  const gscoped_ptr<BufferChain> data_;
  vector<Slice> slices_;

  DISALLOW_COPY_AND_ASSIGN(BufferChainSidecar);
};

gscoped_ptr<RpcSidecar> RpcSidecar::FromBufferChain(gscoped_ptr<BufferChain> data) {
  return gscoped_ptr<RpcSidecar>(new BufferChainSidecar(data.Pass()));
}

} // namespace rpc
} // namespace kudu
