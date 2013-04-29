// Copyright (c) 2013, Cloudera, inc.
//
// Some portions copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <boost/intrusive/list.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <gutil/hash/city.h>

#include <stdlib.h>

#include "gutil/atomic_refcount.h"
#include "util/cache.h"
#include "util/pthread_spinlock.h"
#include "util/locks.h"

namespace kudu {

Cache::~Cache() {
}

namespace {

typedef PThreadSpinLock MutexType;

enum CacheEntryStates {
  // The entry is new. Not added to any queue
  CACHE_ENTRY_IS_NEW,
  // The entry was removed from the hashtable and cache queue
  CACHE_ENTRY_IS_EVICTED,
  // The entry is replaced by a newer entry/value
  CACHE_ENTRY_IS_REPLACED,

  // The entry is in the lru queue
  CACHE_ENTRY_IS_IN_LRU_QUEUE,

  // The entry is in the active queue of the freq cache
  CACHE_ENTRY_IS_IN_FREQ_ACTIVE,
  // The entry is in the inactive queue of the freq cache
  CACHE_ENTRY_IS_IN_FREQ_INACTIVE,
};

struct CacheHandle {
  // number of users that are referencing this object + the cache
  volatile Atomic32 refs;

  // number of times this item was requested
  volatile base::subtle::Atomic64 freq;

  // entry state, used by the cache policy or debug
  volatile CacheEntryStates state;
  // value compression state
  volatile bool is_compressed;
  // entry "hot" value state
  volatile bool is_hot;

  // Pointer to the next entry in the same hash table bucket
  CacheHandle *ht_next;

  // queue hook (LRU, ...)
  boost::intrusive::list_member_hook<> queue_hook;

  // Hash of the key, used to lookup the hash table bucket
  uint32_t key_hash;

  // space taken in the cache
  size_t charge;

  // key_data size
  size_t key_size;

  // Entry lock, used to compress/uncompress the value
  MutexType lock;

  // Value associated with this entry
  void *value;

  // entry events callbacks (deleter, promoteHot)
  const CacheEntryCallbacks *callbacks;

  // key of the entry, allocated as part of the CacheHandle
  Slice key() const {
    const uint8_t *mem = reinterpret_cast<const uint8_t *>(this) + sizeof(CacheHandle);
    return Slice(mem, key_size);
  }

  // returns true if the entry is evicted or replaced
  bool is_deletable() const {
    return this->state == CACHE_ENTRY_IS_EVICTED || this->state == CACHE_ENTRY_IS_REPLACED;
  }

  void set_state(CacheEntryStates new_state) {
    if (!this->is_deletable()) {
      this->state = new_state;
    }
  }
};

typedef boost::intrusive::list<CacheHandle,
    boost::intrusive::constant_time_size<true>,
    boost::intrusive::member_hook<CacheHandle, boost::intrusive::list_member_hook<>,
      &CacheHandle::queue_hook> > CacheQueue;

/* ===========================================================================
 *  Hash Table
 *  Chained hashtable with a read-write lock used to block everyone on table resize.
 *  Each node has a rwlock taken during each operation.
 */
class HandleTable {
 private:
  struct Bucket {
    rw_spinlock lock;
    // First entry chained from this bucket, or NULL if the bucket is empty.
    CacheHandle *chain_head;
    Bucket() : chain_head(NULL) {}
  };

 public:
  HandleTable();
  ~HandleTable();

  CacheHandle *Insert(CacheHandle *entry);
  CacheHandle *Lookup(const Slice& key, uint32_t hash);
  CacheHandle *Remove(const Slice& key, uint32_t hash);
  bool Remove(CacheHandle *entry);

 private:
  Bucket *FindBucket (uint32_t hash) {
    return &(buckets_[hash & mask_]);
  }

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  CacheHandle **FindPointer (Bucket *bucket, const Slice& key, uint32_t hash) {
    CacheHandle **node = &(bucket->chain_head);
    while (*node && ((*node)->key_hash != hash || key != (*node)->key())) {
      node = &((*node)->ht_next);
    }
    return node;
  }

  void Resize();

 private:
  percpu_rwlock lock_;    // table rwlock used as write on resize
  uint64_t mask_;         // size - 1 used to lookup the bucket (hash & mask_)
  uint64_t size_;         // number of bucket in the table
  gscoped_array<Bucket> buckets_;              // table buckets
  volatile base::subtle::Atomic64 item_count_; // number of items in the table
};

HandleTable::HandleTable()
  : mask_(0), size_(0), item_count_(0)
{
  Resize();
}

HandleTable::~HandleTable() {
}

CacheHandle *HandleTable::Insert(CacheHandle *entry) {
  CacheHandle *old;

  {
    boost::lock_guard<rw_spinlock> table_rdlock(lock_.get_lock());
    Bucket *bucket = FindBucket(entry->key_hash);
    {
      boost::unique_lock<rw_spinlock> bucket_wrlock(bucket->lock);
      CacheHandle **node = FindPointer(bucket, entry->key(), entry->key_hash);
      old = *node;
      *node = entry;
      if (old != NULL) {
        DCHECK(!old->is_deletable());
        entry->ht_next = old->ht_next;
        entry->set_state(old->state);
        old->set_state(CACHE_ENTRY_IS_REPLACED);
      } else {
        entry->ht_next = NULL;
        entry->set_state(CACHE_ENTRY_IS_NEW);
      }
    }
  }

  if (old == NULL && base::subtle::NoBarrier_AtomicIncrement(&item_count_, 1) > size_) {
    boost::unique_lock<percpu_rwlock> table_wrlock(lock_, boost::try_to_lock);
    // if we can't take the lock, means that someone else is resizing
    if (table_wrlock.owns_lock()) {
      Resize();
    }
  }

  return old;
}

CacheHandle *HandleTable::Lookup(const Slice& key, uint32_t hash) {
  boost::lock_guard<rw_spinlock> table_rdlock(lock_.get_lock());
  Bucket *bucket = FindBucket(hash);
  {
    boost::shared_lock<rw_spinlock> bucket_rdlock(bucket->lock);
    CacheHandle *entry = *FindPointer(bucket, key, hash);
    if (entry != NULL) {
      DCHECK(!entry->is_deletable());
      base::RefCountInc(&(entry->refs));
      return entry;
    }
  }
  return NULL;
}

CacheHandle *HandleTable::Remove(const Slice& key, uint32_t hash) {
  CacheHandle *entry;

  {
    boost::lock_guard<rw_spinlock> table_rdlock(lock_.get_lock());
    Bucket *bucket = FindBucket(hash);
    {
      boost::unique_lock<rw_spinlock> bucket_wrlock(bucket->lock);
      CacheHandle **node = FindPointer(bucket, key, hash);
      if ((entry = *node) != NULL) {
        DCHECK(!entry->is_deletable());
        *node = entry->ht_next;
        base::RefCountInc(&(entry->refs));
      }
    }
  }

  if (entry != NULL)
    base::subtle::NoBarrier_AtomicIncrement(&item_count_, -1);
  return entry;
}

bool HandleTable::Remove(CacheHandle *entry) {
  bool found = false;

  {
    boost::lock_guard<rw_spinlock> table_rdlock(lock_.get_lock());
    Bucket *bucket = FindBucket(entry->key_hash);
    {
      boost::unique_lock<rw_spinlock> bucket_wrlock(bucket->lock);
      for (CacheHandle **node = &(bucket->chain_head); *node != NULL; node = &((*node)->ht_next)) {
        if (*node == entry) {
          *node = entry->ht_next;
          entry->set_state(CACHE_ENTRY_IS_EVICTED);
          found = true;
          break;
        }
      }
    }
  }

  if (found)
    base::subtle::NoBarrier_AtomicIncrement(&item_count_, -1);
  return found;
}

void HandleTable::Resize() {
  // Calculate a new table size
  size_t new_size = 4;
  while (new_size < item_count_) {
    new_size <<= 1;
  }

  if (size_ >= new_size)
    return;

  // Allocate a new bucket list
  gscoped_array<Bucket> new_buckets(new Bucket[new_size]);
  size_t new_mask = new_size - 1;

  // Copy entries
  for (size_t i = 0; i < size_; ++i) {
    CacheHandle *p = buckets_[i].chain_head;
    while (p != NULL) {
      CacheHandle *next = p->ht_next;

      // Insert Entry
      Bucket *bucket = &(new_buckets[p->key_hash & new_mask]);
      p->ht_next = bucket->chain_head;
      bucket->chain_head = p;

      p = next;
    }
  }

  // Swap the bucket
  mask_ = new_mask;
  size_ = new_size;
  buckets_.swap(new_buckets);
}


/* ===========================================================================
 *  Cache
 */
class CachePolicy;
class AbstractCache {
 public:
  AbstractCache(CachePolicy *policy);
  virtual ~AbstractCache();

  Cache::Handle *Insert (const Slice& key, uint32_t hash, void *value, size_t charge,
                         const CacheEntryCallbacks *callbacks, bool is_hot=false);
  Cache::Handle *Lookup (const Slice& key, uint32_t hash);
  void Erase (const Slice& key, uint32_t hash);

  void Release (Cache::Handle *handle) {
    EntryUnref(reinterpret_cast<CacheHandle *>(handle));
  }

  void SetCapacity (uint64_t capacity) {
    capacity_ = capacity;
  }

  bool IsOverCapacity (void) const {
    return space_used_ > capacity_;
  }

  uint64_t capacity (void) const {
    return capacity_;
  }

  uint64_t space_used (void) const {
    return space_used_;
  }

 private:
  CacheHandle *CreateNewHandle(const Slice& key, uint32_t hash, void *value, size_t charge,
                               const CacheEntryCallbacks *callbacks, bool is_hot);
  CacheHandle *PromoteHotEntry(CacheHandle *entry);
  void EntryUnref(CacheHandle *entry);
  void EntryReclaim(CacheHandle *entry);
  void EntryCompress(CacheHandle *entry);
  bool EntryUncompress(CacheHandle *entry);

 private:
  // Number of lookups with the element found
  volatile base::subtle::Atomic64 hit_count_;
  // Number of lookups with the element not found
  volatile base::subtle::Atomic64 miss_count_;
  // Space used, accumulated from entry->charge
  volatile base::subtle::Atomic64 space_used_;

  // Total Space available in the cache
  uint64_t capacity_;
  // Cache entries hashtable
  HandleTable table_;
  // Pluggable Cache Policy (LRU, Freq, ...)
  CachePolicy *policy_;

  friend class CachePolicy;
};

class CachePolicy {
 public:
  virtual ~CachePolicy() {}

  virtual void Insert (AbstractCache *cache, CacheHandle *entry, CacheHandle *old) = 0;
  virtual CacheHandle *Update(AbstractCache *cache, CacheHandle *entry) = 0;
  virtual void Remove(AbstractCache *cache, CacheHandle *entry) = 0;
  virtual void Reclaim(AbstractCache *cache) = 0;
  virtual void Erase (AbstractCache *cache) = 0;

 protected:
  CacheHandle *PromoteHotEntry(AbstractCache *cache, CacheHandle *entry) {
    return cache->PromoteHotEntry(entry);
  }

  void EntryUnref(AbstractCache *cache, CacheHandle *entry) {
    cache->EntryUnref(entry);
  }

  size_t EntryReclaim(AbstractCache *cache, CacheQueue *queue, size_t toReclaim = -1) {
    uint64_t space_used = cache->space_used();
    size_t reclaimed = 0;
    while (reclaimed != toReclaim && space_used > cache->capacity() && !queue->empty()) {
      CacheHandle *entry = EntryPopFromQueue(queue);
      space_used -= entry->charge;
      cache->EntryReclaim(entry);
      reclaimed++;
    }
    return reclaimed;
  }

  void EraseQueue(AbstractCache *cache, CacheQueue *queue) {
    while (!queue->empty()) {
      CacheHandle *entry = EntryPopFromQueue(queue);
      DCHECK_EQ(entry->refs, 1);   // Error if caller has an unreleased handle
      cache->EntryReclaim(entry);  // this looks wrong, but it's not...
    }
  }

  void EntryAddToQueueHead (CacheQueue *queue, CacheHandle *entry) {
    DCHECK(!entry->queue_hook.is_linked());
    queue->push_front(*entry);  // don't worry you're inserting a pointer...
  }

  void EntryMoveToQueueHead (CacheQueue *queue, CacheHandle *entry) {
    EntryRemoveFromQueue(queue, entry);
    EntryAddToQueueHead(queue, entry);
  }

  CacheHandle *EntryPopFromQueue (CacheQueue *queue) {
    CacheHandle *entry = &queue->back();  // this looks wrong, but it's not...
    queue->pop_back();
    DCHECK(!entry->queue_hook.is_linked());
    return entry;
  }

  void EntryRemoveFromQueue (CacheQueue *queue, CacheHandle *entry) {
    if (entry->queue_hook.is_linked()) {
      queue->erase(queue->iterator_to(*entry));
      DCHECK(!entry->queue_hook.is_linked());
    }
  }
};

/* ===========================================================================
 *  Abstract Cache helpers.
 */
AbstractCache::AbstractCache(CachePolicy *policy)
  : hit_count_(0), miss_count_(0), space_used_(0), capacity_(0), policy_(policy)
{
}

AbstractCache::~AbstractCache() {
  policy_->Erase(this);
  delete policy_;
}

Cache::Handle *AbstractCache::Insert(const Slice& key, uint32_t hash, void *value, size_t charge,
                                     const CacheEntryCallbacks *callbacks, bool is_hot) {
  CacheHandle *entry = CreateNewHandle(key, hash, value, charge, callbacks, is_hot);

  CacheHandle *old = table_.Insert(entry);
  policy_->Insert(this, entry, old);
  if (old != NULL) {
    if (charge > old->charge && IsOverCapacity())
      policy_->Reclaim(this);
    EntryUnref(old);
  } else if (IsOverCapacity()) {
    policy_->Reclaim(this);
  }
  EntryUnref(entry);
  return reinterpret_cast<Cache::Handle *>(entry);
}

Cache::Handle *AbstractCache::Lookup(const Slice& key, uint32_t hash) {
  CacheHandle *entry;
  if ((entry = table_.Lookup(key, hash)) != NULL) {
    entry = policy_->Update(this, entry);
    base::subtle::NoBarrier_AtomicIncrement(&hit_count_, 1);
    if (!entry->is_hot && !EntryUncompress(entry)) {
      // Something went wrong during uncompression...
      // ask the user to fetch the block again.
      EntryUnref(entry);
      return NULL;
    }
  } else {
    base::subtle::NoBarrier_AtomicIncrement(&miss_count_, 1);
  }
  return reinterpret_cast<Cache::Handle *>(entry);
}

void AbstractCache::Erase(const Slice& key, uint32_t hash) {
  CacheHandle *entry;
  if ((entry = table_.Remove(key, hash)) != NULL) {
    policy_->Remove(this, entry);
    EntryUnref(entry);
  }
}

CacheHandle *AbstractCache::CreateNewHandle(const Slice& key, uint32_t hash, void *value,
                          size_t charge, const CacheEntryCallbacks *callbacks, bool is_hot)
{
  uint8_t *mem = static_cast<uint8_t *>(malloc(sizeof(CacheHandle) + key.size()));
  CacheHandle *entry = new (mem) CacheHandle();
  memcpy(mem + sizeof(CacheHandle), key.data(), key.size());

  // One ref from the cache, one ref for the user returned pointer and one ref for the lookup func
  entry->refs = 3;
  entry->state = CACHE_ENTRY_IS_NEW;
  entry->is_compressed = false;
  entry->is_hot = is_hot;
  entry->freq = 0;

  entry->ht_next = NULL;
  DCHECK(!entry->queue_hook.is_linked());

  entry->key_hash = hash;
  entry->charge = charge;
  entry->key_size = key.size();

  entry->value = value;
  entry->callbacks = callbacks;

  base::subtle::NoBarrier_AtomicIncrement(&space_used_, charge);
  return(entry);
}

CacheHandle *AbstractCache::PromoteHotEntry(CacheHandle *entry) {
  const CacheEntryCallbacks *callbacks = entry->callbacks;
  if (callbacks->promoteHot == NULL) {
    if (EntryUncompress(entry)) {
      entry->is_hot = true;
    }
    return entry;
  }

  void *new_value = NULL;
  size_t new_charge = entry->charge;
  if (callbacks->promoteHot(entry->key(), entry->value, entry->charge, &new_value, &new_charge)) {
    // the user has returned a new value as replacement for the old one.
    CacheHandle *newEntry = reinterpret_cast<CacheHandle *>(Insert(entry->key(), entry->key_hash,
        new_value, new_charge, callbacks, true));
    // the old value will be deleted on zero-references.
    EntryUnref(entry);
    return newEntry;
  }
  return entry;
}

void AbstractCache::EntryUnref(CacheHandle *entry) {
  DCHECK_GT(entry->refs, 0);

  if (!entry->is_hot && base::subtle::Acquire_Load(&(entry->refs)) == 2) {
    EntryCompress(entry);
  }

  if (base::RefCountDec(&(entry->refs)) == false) {
    DCHECK(!entry->queue_hook.is_linked());
    base::subtle::NoBarrier_AtomicIncrement(&space_used_, -(entry->charge));
    entry->callbacks->deleter(entry->key(), entry->value);
    free(entry);
  }
}

void AbstractCache::EntryReclaim(CacheHandle *entry) {
  if (table_.Remove(entry)) {
    EntryUnref(entry);
  }
}

void AbstractCache::EntryCompress(CacheHandle *entry) {
  if (entry->callbacks->compressor != NULL) {
    boost::lock_guard<MutexType> lock(entry->lock);
    if (entry->is_compressed || entry->is_deletable() ||
        base::subtle::Acquire_Load(&(entry->refs)) > 2)
    {
      return;
    }

    size_t charge = entry->charge;
    void *new_value = entry->value;
    if (entry->callbacks->compressor(entry->key(), &new_value, &charge)) {
      entry->is_compressed = true;
      entry->value = new_value;
    }
  }
}

bool AbstractCache::EntryUncompress(CacheHandle *entry) {
  if (entry->callbacks->decompressor != NULL) {
    boost::lock_guard<MutexType> entry_lock(entry->lock);
    if (!entry->is_compressed)
      return true;

    size_t charge = entry->charge;
    void *new_value = entry->value;
    Status s = entry->callbacks->decompressor(entry->key(), &new_value, &charge);
    if (!s.ok()) {
      LOG(ERROR) << "Unable to decompress " << entry->key().ToString()
                 << " cached value: " << s.ToString();
      return false;
    }

    entry->is_compressed = false;
    entry->value = new_value;
  }
  return true;
}

/* ===========================================================================
 *  LRU Cache
 */
class LRUCachePolicy : public CachePolicy {
 public:
  void Erase (AbstractCache *cache) {
    EraseQueue(cache, &lru_);
  }

  void Insert (AbstractCache *cache, CacheHandle *entry, CacheHandle *old) {
    boost::lock_guard<MutexType> lock(mutex_);
    if (old != NULL)
      EntryRemoveFromQueue(&lru_, old);
    if (entry->state == CACHE_ENTRY_IS_NEW) {
      // Insert to the head of LRU
      entry->set_state(CACHE_ENTRY_IS_IN_LRU_QUEUE);
      EntryMoveToQueueHead(&lru_, entry);
    } else if (!entry->is_deletable()) {
      // Move to the head of LRU
      EntryMoveToQueueHead(&lru_, entry);
    }
  }

  CacheHandle *Update(AbstractCache *cache, CacheHandle *entry) {
    boost::lock_guard<MutexType> lock(mutex_);
    if (!entry->is_deletable())
      EntryMoveToQueueHead(&lru_, entry);
    return entry;
  }

  void Remove(AbstractCache *cache, CacheHandle *entry) {
    {
      boost::lock_guard<MutexType> lock(mutex_);
      EntryRemoveFromQueue(&lru_, entry);
    }
    EntryUnref(cache, entry);
  }

  void Reclaim(AbstractCache *cache) {
    boost::lock_guard<MutexType> lock(mutex_);
    EntryReclaim(cache, &lru_);
  }

 private:
  MutexType mutex_;
  CacheQueue lru_;
};

class LRUCache : public AbstractCache {
 public:
  LRUCache() : AbstractCache(new LRUCachePolicy) {};
};

/* ===========================================================================
 *  Freq Cache. It mantains two queues "inactive" and "active".
 *  Every insertion goes to the "inactive" LRU queue.
 *  Once an item is requested it goes in the active queue and the subsequent
 *  requests increments the entry frequency.
 *  If the cache is full, entries are reclaimed from inactive queue.
 *  If the inactive queue is empty, some entries are moved from the end of the
 *  active queue to the head of the inactive one.
 */
class FreqCachePolicy : public CachePolicy {
 public:
  void Erase (AbstractCache *cache) {
    EraseQueue(cache, &inactive_);
    EraseQueue(cache, &active_);
  }

  void Insert (AbstractCache *cache, CacheHandle *entry, CacheHandle *old) {
    boost::lock_guard<MutexType> lock(mutex_);
    if (old != NULL)
      EntryRemoveFromQueue(old);

    if (entry->state == CACHE_ENTRY_IS_NEW) {
      // Insert to the inactive queue
      entry->set_state(CACHE_ENTRY_IS_IN_FREQ_INACTIVE);
      EntryMoveToQueueHead(&inactive_, entry);
    } else if (!entry->is_deletable()) {
      if (entry->freq < kActiveFreq) {
        // Move to head of the inactive queue
        entry->set_state(CACHE_ENTRY_IS_IN_FREQ_INACTIVE);
        EntryMoveToQueueHead(&inactive_, entry);
      } else {
        // Move to head of the active queue
        entry->set_state(CACHE_ENTRY_IS_IN_FREQ_ACTIVE);
        EntryMoveToQueueHead(&active_, entry);
      }
    }
  }

  CacheHandle *Update(AbstractCache *cache, CacheHandle *entry) {
    uint64_t freq = base::subtle::NoBarrier_AtomicIncrement(&(entry->freq), 1);
    if (freq == kActiveFreq) {
      // Promote to Active queue
      boost::lock_guard<MutexType> lock(mutex_);
      if (!entry->is_deletable()) {
        entry->set_state(CACHE_ENTRY_IS_IN_FREQ_ACTIVE);
        EntryMoveToQueueHead(&active_, entry);
      }
    } else if (freq == kHotFreq) {
      // Mark as Hot block (e.g. tell user to decompress it)
      entry = PromoteHotEntry(cache, entry);
    }
    return entry;
  }

  void Remove(AbstractCache *cache, CacheHandle *entry) {
    {
      boost::lock_guard<MutexType> lock(mutex_);
      EntryRemoveFromQueue(entry);
    }
    EntryUnref(cache, entry);
  }

  void Reclaim(AbstractCache *cache) {
    boost::lock_guard<MutexType> lock(mutex_);
    for (int priority = 12; cache->IsOverCapacity(); priority--) {
      // Move some active pages to the inactive queue head
      size_t active_to_move = (active_.size() >> priority) + 1;
      while (active_to_move-- && !active_.empty()) {
        CacheHandle *entry = EntryPopFromQueue(&active_);
        EntryAddToQueueHead(&inactive_, entry);
        Release_Store(&(entry->freq), kActiveFreq - 1);
      }

      // Reclaim entries from the inactive queue
      size_t inactive_to_reclaim = (inactive_.size() >> priority) + 1;
      EntryReclaim(cache, &inactive_, inactive_to_reclaim);
    }
  }

 private:
  void EntryRemoveFromQueue(CacheHandle *entry) {
    if (entry->freq < kActiveFreq)
      CachePolicy::EntryRemoveFromQueue(&inactive_, entry);
    else
      CachePolicy::EntryRemoveFromQueue(&active_, entry);
  }

 private:
  MutexType mutex_;
  CacheQueue active_;
  CacheQueue inactive_;

  static const unsigned int kHotFreq = 3;
  static const unsigned int kActiveFreq = 1;
};

class FreqCache : public AbstractCache {
 public:
  FreqCache() : AbstractCache(new FreqCachePolicy) {};
};

/* ===========================================================================
 *  Sharded Cache
 */
static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

template <class TCache>
class ShardedCache : public Cache {
 public:
    explicit ShardedCache(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedCache() { }

  virtual Handle *Insert(const Slice& key, void* value, size_t charge,
                         const CacheEntryCallbacks *callbacks) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, callbacks);
  }

  virtual Handle *Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }

  virtual void Release(Handle *handle) {
    CacheHandle *h = reinterpret_cast<CacheHandle *>(handle);
    shard_[Shard(h->key_hash)].Release(handle);
  }

  virtual void* Value(Handle* handle) {
    return reinterpret_cast<CacheHandle *>(handle)->value;
  }

  virtual bool IsHot(Handle* handle) {
    return reinterpret_cast<CacheHandle *>(handle)->is_hot;
  }

  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }

  virtual uint64_t NewId() {
    boost::lock_guard<MutexType> l(id_mutex_);
    return ++(last_id_);
  }

 private:
  static inline uint32_t HashSlice(const Slice& s) {
    return util_hash::CityHash64(
      reinterpret_cast<const char *>(s.data()), s.size());
  }

  static uint32_t Shard(uint32_t hash) {
    return kNumShards > 0 ? hash >> (32 - kNumShardBits) : 0;
  }

 private:
  TCache shard_[kNumShards];

  MutexType id_mutex_;
  uint64_t last_id_;
};

}  // end anonymous namespace

Cache *NewLRUCache(size_t capacity) {
  return new ShardedCache<LRUCache>(capacity);
}

Cache *NewFreqCache(size_t capacity) {
  return new ShardedCache<FreqCache>(capacity);
}

}  // namespace kudu
