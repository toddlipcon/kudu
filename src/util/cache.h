// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// This is taken from LevelDB and evolved to fit the kudu codebase.
//
// TODO: this is pretty lock-heavy. Would be good to sub out something
// a little more concurrent.

#ifndef KUDU_UTIL_CACHE_H_
#define KUDU_UTIL_CACHE_H_

#include <boost/noncopyable.hpp>
#include <stdint.h>

#include "util/slice.h"

namespace kudu {

class Cache;

// Callback to free the specified value
typedef void (*CacheEntryValueDeleter) (const Slice& key, void *value);

// Callback to notify the user that the "key" is hot.
// The user should return true, if the "value" should be replaced
// with the "new_value" and the "new_charge".
// The cache will call the value deleter once no one else is referencing it.
typedef bool (*CacheEntryValuePromoteHot) (const Slice& key,
                                           const void *value,
                                           size_t charge,
                                           void **new_value,
                                           size_t *new_charge);

struct CacheEntryCallbacks {
  // called when the element is removed from the cache.
  // The user is responsible to delete the "value".
  CacheEntryValueDeleter deleter;

  // called when the element becames hot. (used by the freq cache)
  CacheEntryValuePromoteHot promoteHot;
};

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
extern Cache* NewLRUCache(size_t capacity);

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a frequency eviction policy.
extern Cache *NewFreqCache(size_t capacity);

class Cache : boost::noncopyable {
 public:
  Cache() { }

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  struct Handle { };

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         const CacheEntryCallbacks *callbacks) = 0;

  // If the cache has no mapping for "key", returns NULL.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

 private:
  // No copying allowed
  Cache(const Cache&);
  void operator=(const Cache&);
};

}  // namespace kudu

#endif
