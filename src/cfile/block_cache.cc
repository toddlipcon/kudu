// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>

#include "cfile/block_cache.h"
#include "gutil/port.h"
#include "gutil/singleton.h"
#include "util/cache.h"
#include "util/slice.h"


DEFINE_int64(block_cache_capacity_mb, 512, "block cache capacity in MB");
DEFINE_string(block_cache_default_compression_codec, "",
              "Default block cache compression codec.");

namespace kudu {
namespace cfile {

const CacheEntryCallbacks BlockCache::block_cache_callbacks_(
  &BlockCache::ValueDeleter, &BlockCache::ValueCompressor, &BlockCache::ValueDecompressor);

static CompressionType GetDefaultCompressionCodec() {
  if (FLAGS_block_cache_default_compression_codec.size() == 0)
    return LZ4;
  return(GetCompressionCodecType(FLAGS_block_cache_default_compression_codec));
}

struct CacheKey {
  CacheKey(BlockCache::FileId file_id, uint64_t offset) :
    file_id_(file_id),
    offset_(offset)
  {}

  const Slice slice() const {
    return Slice(reinterpret_cast<const uint8_t *>(this), sizeof(*this));
  }

  BlockCache::FileId file_id_;
  uint64_t offset_;
} PACKED;

struct CacheValue {
  size_t uncompressed_len;
  size_t compressed_len;
  const uint8_t *data;

  CacheValue(const Slice& value) {
    uncompressed_len = value.size();
    compressed_len = 0;
    data = value.data();
  }
};

BlockCache::BlockCache() :
  cache_(CHECK_NOTNULL(NewFreqCache(FLAGS_block_cache_capacity_mb * 1024 * 1024)))
{}

BlockCache::BlockCache(size_t capacity) :
  cache_(CHECK_NOTNULL(NewLRUCache(capacity)))
{}

BlockCache *BlockCache::GetSingleton() {
  return Singleton<BlockCache>::get();
}

BlockCache::FileId BlockCache::GenerateFileId() {
  return cache_->NewId();
}

bool BlockCache::Lookup(FileId file_id, uint64_t offset, BlockCacheHandle *handle) {
  CacheKey key(file_id, offset);
  Cache::Handle *h = cache_->Lookup(key.slice());
  if (h != NULL) {
    handle->SetHandle(cache_.get(), h);
  }
  return h != NULL;
}

void BlockCache::Insert(FileId file_id, uint64_t offset, const Slice &block_data,
                        BlockCacheHandle *inserted) {
  CacheKey key(file_id, offset);

  // Allocate a copy of the value Slice (not the referred-to-data!)
  // for insertion in the cache.
  CacheValue *value = new CacheValue(block_data);

  Cache::Handle *h = cache_->Insert(key.slice(), value, value->uncompressed_len,
                                    &block_cache_callbacks_);
  inserted->SetHandle(cache_.get(), h);
}

void BlockCache::ValueDeleter(const Slice &key, void *cached_value) {
  CacheValue *value = reinterpret_cast<CacheValue *>(cached_value);
  delete[] value->data;
  delete value;
}

bool BlockCache::ValueCompressor(const Slice &key, void **cached_value, size_t *charge) {
  CacheValue *value = reinterpret_cast<CacheValue *>(*cached_value);
  //if (value->uncompressed_len <= 128)
  //  return false;

  CompressionType compressionType = GetDefaultCompressionCodec();
  if (compressionType == NO_COMPRESSION)
    return true;

  shared_ptr<CompressionCodec> compression_codec;
  Status s = GetCompressionCodec(compressionType, &compression_codec);
  if (!s.ok()) return false;

  size_t compressed_len = value->compressed_len;
  if (compressed_len == 0) {
    compressed_len = compression_codec->MaxCompressedLength(value->uncompressed_len);
  }

  gscoped_array<uint8_t> compressed(new uint8_t[compressed_len]);
  s = compression_codec->Compress(Slice(value->data, value->uncompressed_len),
                                  compressed.get(), &compressed_len);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to decompress " << s.ToString();
    return false;
  }

  // free the uncompressed data
  delete[] value->data;

  // Assign the compressed data
  value->compressed_len = compressed_len;
  value->data = compressed.release();
  return true;
}

bool BlockCache::ValueDecompressor(const Slice &key, void **cached_value, size_t *charge) {
  CacheValue *value = reinterpret_cast<CacheValue *>(*cached_value);

  CompressionType compressionType = GetDefaultCompressionCodec();
  if (compressionType == NO_COMPRESSION)
    return true;

  shared_ptr<CompressionCodec> compression_codec;
  Status s = GetCompressionCodec(compressionType, &compression_codec);
  if (!s.ok()) return false;

  gscoped_array<uint8_t> uncompressed(new uint8_t[value->uncompressed_len]);
  s = compression_codec->Uncompress(Slice(value->data, value->compressed_len),
                                    uncompressed.get(), value->uncompressed_len);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to decompress " << s.ToString();
    return false;
  }

  // free the compressed data
  delete[] value->data;

  // Assign the uncompressed data
  value->data = uncompressed.release();
  return true;
}

const Slice BlockCacheHandle::data() const {
  const CacheValue *value = reinterpret_cast<const CacheValue *>(cache_->Value(handle_));
  return Slice(value->data, value->uncompressed_len);
}

} // namespace cfile
} // namespace kudu
