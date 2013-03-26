// Copyright (c) 2013, Cloudera, inc.
//
// Some portions copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <gutil/hash/city.h>

#include <stdlib.h>

#include "gutil/atomic_refcount.h"
#include "util/cache.h"
#include "util/pthread_spinlock.h"
#include "util/percpu_rwlock.h"
namespace kudu {

Cache::~Cache() {
}

namespace {

typedef PThreadSpinLock MutexType;

/* ============================================================================
 *  Read-Write lock. 32bit uint that contains the number of readers.
 *  When someone wants to write, tries to set the 32bit, and waits until
 *  the readers have finished. Readers are spinning while the write flag is set.
 */
#if 0
  #define CompareAndSwap          __sync_val_compare_and_swap
  #define AtomicIncrement         __sync_add_and_fetch
  #define RefCountInc(x)          AtomicIncrement(x, 1)
  #define RefCountDec(x)          AtomicIncrement(x, -1)
#else
  #define CompareAndSwap          base::subtle::Acquire_CompareAndSwap
  #define AtomicIncrement         base::subtle::Barrier_AtomicIncrement
  #define RefCountInc(x)          base::RefCountInc(x)
  #define RefCountDec(x)          base::RefCountDec(x)
#endif

class RwLock {
  public:
    RwLock() : state_(0) {}
    ~RwLock() {}

    void lock_shared() {
      Atomic32 new_state;
      Atomic32 expected;
      int loop_count = 0;
      while (true) {
        expected = state_ & 0x7fffffff;   // I expect no write lock
        new_state = expected + 1;         // Add me as reader
        if (CompareAndSwap(&state_, expected, new_state) == expected)
          break;
        // Either was already locked by someone else, or CAS failed.
        boost::detail::yield(loop_count++);
      }
    }

    void unlock_shared() {
      Atomic32 new_state;
      Atomic32 expected;
      int loop_count = 0;
      while (true) {
        expected = state_;          // I expect a write lock and other readers
        new_state = expected - 1;   // Drop me as reader
        if (CompareAndSwap(&state_, expected, new_state) == expected)
          break;
        // Either was already locked by someone else, or CAS failed.
        boost::detail::yield(loop_count++);
      }
    }

    bool try_lock() {
      Atomic32 expected = state_ & 0x7fffffff;    // I expect some 0+ readers
      Atomic32 new_state = (1 << 31) | expected;  // I want to lock the other writers
      if (CompareAndSwap(&state_, expected, new_state) != expected)
        return false;

      // Wait pending reads
      int loop_count = 0;
      while ((state_ & 0x7fffffff) > 0)
        boost::detail::yield(loop_count++);

      return true;
    }

    void lock() {
      int loop_count = 0;
      while (!try_lock())
        boost::detail::yield(loop_count++);
    }

    void unlock() {
      // I expect to be the only writer
      DCHECK_EQ(state_, 1 << 31);
      // reset: no writers/no readers
      state_ = 0;
    }

  private:
     volatile Atomic32 state_;
};

enum CacheEntryStates {
  CACHE_ENTRY_IS_NEW,
  CACHE_ENTRY_IS_EVICTED,
  CACHE_ENTRY_IS_REPLACED,
  /* LRU */
  CACHE_ENTRY_IS_IN_LRU_QUEUE,
  /* Freq */
  CACHE_ENTRY_IS_IN_FREQ_ACTIVE,
  CACHE_ENTRY_IS_IN_FREQ_INACTIVE,
};

struct CacheHandle {
  volatile Atomic32 refs;     // number of users that are referencing this object + the cache
  CacheEntryStates state;     // entry state, used by the cache policy or debug

  volatile base::subtle::Atomic64 freq; // number of times this item was requested
  uint64_t time;              // time of the last hit

  CacheHandle *ht_next;       // Pointer to the next entry in the same hash table bucket
  CacheHandle *queue_next;    // Pointer to the next entry in the queue (LRU, ...)
  CacheHandle *queue_prev;    // Pointer to the previous entry in the queue (LRU, ...)

  uint32_t key_hash;          // Hash of the key, used to lookup the hash table bucket
  uint64_t charge;            // space taken in the cache
  size_t key_size;            // key_data size
  void *value;                // Value associated with this entry

  // called when the element is removed from the cache.
  // The user is responsible to delete the "value".
  void (*deleter)(const Slice&, void* value);

  // called when the element becames hot. (used by the freq cache)
  // The user is responsible to replace/delete the "value" and return the new one.
  void *(*hot)(const Slice&, void* value);

  uint8_t key_data[1];        // key of the entry, allocated as part of the CacheHandle

  Slice key() const {
    return Slice(key_data, key_size);
  }

  bool is_deletable() const {
    return state == CACHE_ENTRY_IS_EVICTED || state == CACHE_ENTRY_IS_REPLACED;
  }
};

static uint64_t GetCurrentTime(void) {
  struct timeval now;
  gettimeofday(&now, NULL);
  return(now.tv_sec * 1000000ull + now.tv_usec);
}

/* ===========================================================================
 *  Hash Table
 *  Chained hashtable with a read-write lock used to block everyone on table resize.
 *  Each node has a rwlock taken during each operation.
 */
#if 1
#define BucketLock    RwLock
#else
class BucketLock {
public:
  BucketLock() {}
  ~BucketLock() {}
  void ReadLock() { mutex_.lock(); }
  void ReadUnlock() { mutex_.unlock(); }
  void WriteLock() { mutex_.lock(); }
  void WriteUnlock() { mutex_.unlock(); }
private:
  MutexType mutex_;
};
#endif

#if 1
  #define TableRwLock   RwLock
#else
class TableRwLock {
  public:
    TableRwLock() {}
    ~TableRwLock() {}

    void lock_shared() { lock_.get_lock().lock(); }
    void unlock_shared() { lock_.get_lock().unlock(); }
    bool try_lock() { return lock_.try_lock(); }
    void lock() { lock_.lock(); }
    void unlock() { lock_.unlock(); }

  private:
    percpu_rwlock lock_;
};
#endif

class HandleTable {
  private:
    struct Bucket {
      BucketLock lock;
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

    CacheHandle **FindHandle (Bucket *bucket, const Slice& key, uint32_t hash) {
      CacheHandle **node = &(bucket->chain_head);
      while (*node && ((*node)->key_hash != hash || key != (*node)->key())) {
        node = &((*node)->ht_next);
      }
      return node;
    }

    void Resize();

  private:
    TableRwLock lock_;      // table rwlock used as write on resize
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
    boost::shared_lock<TableRwLock> table_rdlock(lock_);
    Bucket *bucket = FindBucket(entry->key_hash);
    {
      boost::unique_lock<BucketLock> bucket_wrlock(bucket->lock);
      CacheHandle **node = FindHandle(bucket, entry->key(), entry->key_hash);
      old = *node;
      *node = entry;
      if (old != NULL) {
        DCHECK(!old->is_deletable());
        entry->ht_next = old->ht_next;
        entry->state = old->state;
        old->state = CACHE_ENTRY_IS_REPLACED;
      } else {
        entry->ht_next = NULL;
        entry->state = CACHE_ENTRY_IS_NEW;
      }
    }
  }

  if (old == NULL && AtomicIncrement(&item_count_, 1) > size_) {
    // if we can't take the lock, means that someone else is resizing
    if (lock_.try_lock()) {
      Resize();
      lock_.unlock();
    }
  }

  return old;
}

CacheHandle *HandleTable::Lookup(const Slice& key, uint32_t hash) {
  boost::shared_lock<TableRwLock> table_rdlock(lock_);
  Bucket *bucket = FindBucket(hash);
  {
    boost::shared_lock<BucketLock> bucket_rdlock(bucket->lock);
    CacheHandle *entry = *FindHandle(bucket, key, hash);
    if (entry != NULL) {
      DCHECK(!entry->is_deletable());
      RefCountInc(&(entry->refs));
      return entry;
    }
  }
  return NULL;
}

CacheHandle *HandleTable::Remove(const Slice& key, uint32_t hash) {
  CacheHandle *entry;

  {
    boost::shared_lock<TableRwLock> table_rdlock(lock_);
    Bucket *bucket = FindBucket(hash);
    {
      boost::unique_lock<BucketLock> bucket_wrlock(bucket->lock);
      CacheHandle **node = FindHandle(bucket, key, hash);
      if ((entry = *node) != NULL) {
        DCHECK(!entry->is_deletable());
        *node = entry->ht_next;
        RefCountInc(&(entry->refs));
      }
    }
  }

  if (entry != NULL)
    AtomicIncrement(&item_count_, -1);
  return entry;
}

bool HandleTable::Remove(CacheHandle *entry) {
  bool found = false;

  {
    boost::shared_lock<TableRwLock> table_rdlock(lock_);
    Bucket *bucket = FindBucket(entry->key_hash);
    {
      boost::unique_lock<BucketLock> bucket_wrlock(bucket->lock);
      for (CacheHandle **node = &(bucket->chain_head); *node != NULL; node = &((*node)->ht_next)) {
        if (*node == entry) {
          *node = entry->ht_next;
          entry->state = CACHE_ENTRY_IS_EVICTED;
          found = true;
          break;
        }
      }
    }
  }

  if (found)
    AtomicIncrement(&item_count_, -1);
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
      CacheHandle *queue_next = p->ht_next;

      // Insert Entry
      Bucket *bucket = &(new_buckets[p->key_hash & new_mask]);
      p->ht_next = bucket->chain_head;
      bucket->chain_head = p;

      p = queue_next;
    }
  }

  // Swap the bucket
  mask_ = new_mask;
  size_ = new_size;
  buckets_.swap(new_buckets);
}

/* ===========================================================================
 *  List
 */
#define __list_init(list)                                       \
  do {                                                          \
    (list)->queue_next = list;                                  \
    (list)->queue_prev = list;                                  \
  } while (0)

#define __list_add(inew, iqueue_prev, iqueue_next)              \
  do {                                                          \
    (inew)->queue_next = iqueue_next;                           \
    (inew)->queue_prev = iqueue_prev;                           \
    (iqueue_next)->queue_prev = inew;                           \
    (iqueue_prev)->queue_next = inew;                           \
  } while (0)

#define __list_del(iqueue_prev, iqueue_next)                    \
  do {                                                          \
    (iqueue_next)->queue_prev = iqueue_prev;                    \
    (iqueue_prev)->queue_next = iqueue_next;                    \
  } while (0)

#define list_add(inew, head)                                    \
  __list_add(inew, head, (head)->queue_next)

#define list_del(entry)                                         \
  do {                                                          \
    __list_del((entry)->queue_prev, (entry)->queue_next);       \
    __list_init(entry);                                         \
  } while (0);

#define list_move(list, head)                                   \
  do {                                                          \
    __list_del((list)->queue_prev, (list)->queue_next);         \
    list_add(list, head);                                       \
  } while (0);

#define list_for_each(pos, head)                                \
  for (pos = (head)->queue_next; pos != (head); pos = (pos)->queue_next)

#define list_for_each_safe(pos, n, head)                        \
  for (pos = (head)->queue_next, n = (pos)->queue_next;         \
       pos != (head); pos = n, n = (pos)->queue_next)

/* ===========================================================================
 *  Cache
 */
class CachePolicy;
class AbstractCache {
  public:
    AbstractCache(CachePolicy *policy);
    virtual ~AbstractCache();

    Cache::Handle *Insert (const Slice& key, uint32_t hash, void *value, size_t charge,
                           void (*deleter)(const Slice& key, void* value),
                           void *(*hot)(const Slice& key, void* value));
    Cache::Handle *Lookup (const Slice& key, uint32_t hash);
    void Erase (const Slice& key, uint32_t hash);

    void Release (Cache::Handle *handle) {
      EntryUnref(reinterpret_cast<CacheHandle *>(handle));
    }

    void SetCapacity (uint64_t capacity) {
      capacity_ = capacity;
    }

    uint64_t capacity (void) const {
      return capacity_;
    }

    uint64_t space_used (void) const {
      return space_used_;
    }

  private:
    CacheHandle *CreateNewHandle(const Slice& key, uint32_t hash, void *value, size_t charge,
                                 void (*deleter)(const Slice& key, void* value),
                                 void *(*hot)(const Slice& key, void* value));
    void EntryIsHot(CacheHandle *entry);
    void EntryUnref(CacheHandle *entry);
    void EntryReclaim(CacheHandle *entry);

  private:
     volatile base::subtle::Atomic64 hit_count_;    // Number of lookups with the element found
     volatile base::subtle::Atomic64 miss_count_;   // Number of lookups with the element not found
     volatile base::subtle::Atomic64 space_used_;   // Space used, accumulated from entry->charge

    uint64_t capacity_;       // Total Space available in the cache
    HandleTable table_;       // Hash Table
    CachePolicy *policy_;     // Pluggable Cache Policy (LRU, Freq, ...)
    friend class CachePolicy;
};

class CachePolicy {
  public:
    virtual ~CachePolicy() {}

    virtual void Insert (AbstractCache *cache, CacheHandle *entry, CacheHandle *old) = 0;
    virtual void Update(AbstractCache *cache, CacheHandle *entry) = 0;
    virtual void Remove(AbstractCache *cache, CacheHandle *entry) = 0;
    virtual void Reclaim(AbstractCache *cache) = 0;
    virtual void Erase (AbstractCache *cache) = 0;

  protected:
    void EntryIsHot(AbstractCache *cache, CacheHandle *entry) {
      cache->EntryIsHot(entry);
    }

    void EntryUnref(AbstractCache *cache, CacheHandle *entry) {
      cache->EntryUnref(entry);
    }

    void EntryReclaim(AbstractCache *cache, CacheHandle *entry) {
      cache->EntryReclaim(entry);
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
                                     void (*deleter)(const Slice& key, void* value),
                                     void *(*hot)(const Slice& key, void* value)) {
  CacheHandle *entry = CreateNewHandle(key, hash, value, charge, deleter, hot);
  CacheHandle *old = table_.Insert(entry);
  policy_->Insert(this, entry, old);
  if (old != NULL) {
    if (charge > old->charge)
      policy_->Reclaim(this);
    EntryUnref(old);
  } else {
    policy_->Reclaim(this);
  }
  EntryUnref(entry);
  return reinterpret_cast<Cache::Handle *>(entry);
}

Cache::Handle *AbstractCache::Lookup(const Slice& key, uint32_t hash) {
  CacheHandle *entry;
  if ((entry = table_.Lookup(key, hash)) != NULL) {
    entry->time = GetCurrentTime();
    policy_->Update(this, entry);
    AtomicIncrement(&hit_count_, 1);
  } else {
    AtomicIncrement(&miss_count_, 1);
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
    size_t charge, void (*deleter)(const Slice& key, void* value),
    void *(*hot)(const Slice& key, void* value)) {
  CacheHandle *entry = reinterpret_cast<CacheHandle*>(malloc(sizeof(CacheHandle)-1 + key.size()));

  entry->value = value;
  entry->deleter = deleter;
  entry->hot = hot;

  // One from the cache, one for the user returned pointer and one for the lookup func
  entry->refs = 3;
  entry->state = CACHE_ENTRY_IS_NEW;

  entry->freq = 0;
  entry->time = GetCurrentTime();

  entry->ht_next = NULL;
  entry->queue_next = entry;
  entry->queue_prev = entry;

  entry->key_hash = hash;
  entry->charge = charge;
  entry->key_size = key.size();

  memcpy(entry->key_data, key.data(), key.size());

  AtomicIncrement(&space_used_, charge);
  return(entry);
}

void AbstractCache::EntryIsHot(CacheHandle *entry) {
  if (entry->hot == NULL)
    return;

  void *new_value = entry->hot(entry->key(), entry->value);
  if (new_value != NULL) {
    // the user has returned a new value as replacement for the old one.
    // the user is repsponsible for calling the deleter on the value.
    entry->value = new_value;
  }
}

void AbstractCache::EntryUnref(CacheHandle *entry) {
  DCHECK_GT(entry->refs, 0);
  if (RefCountDec(&(entry->refs)) == 0) {
    AtomicIncrement(&space_used_, -(entry->charge));
    entry->deleter(entry->key(), entry->value);
    free(entry);
  }
}

void AbstractCache::EntryReclaim(CacheHandle *entry) {
  if (table_.Remove(entry)) {
    list_del(entry);
    EntryUnref(entry);
  }
}

/* ===========================================================================
 *  LRU Cache
 */
class LRUCachePolicy : public CachePolicy {
  public:
    LRUCachePolicy() {
      __list_init(&lru_);
    }

    void Erase (AbstractCache *cache) {
      CacheHandle *p, *n;
      list_for_each_safe(p, n, &lru_) {
        DCHECK_EQ(p->refs, 1);  // Error if caller has an unreleased handle
        EntryUnref(cache, p);
      }
    }

    void Insert (AbstractCache *cache, CacheHandle *entry, CacheHandle *old) {
      boost::lock_guard<MutexType> lock(mutex_);
      if (old != NULL)
        list_del(old);
      if (entry->state == CACHE_ENTRY_IS_NEW) {
        /* Insert to the head of LRU */
        list_add(entry, &lru_);
        entry->state = CACHE_ENTRY_IS_IN_LRU_QUEUE;
      } else if (entry->state != CACHE_ENTRY_IS_REPLACED) {
        /* Move to the head of LRU */
        list_move(entry, &lru_);
      }
    }

    void Update(AbstractCache *cache, CacheHandle *entry) {
      boost::lock_guard<MutexType> lock(mutex_);
      if (entry->state != CACHE_ENTRY_IS_REPLACED)
        list_move(entry, &lru_);
    }

    void Remove(AbstractCache *cache, CacheHandle *entry) {
      {
        boost::lock_guard<MutexType> lock(mutex_);
        list_del(entry);
      }
      EntryUnref(cache, entry);
    }

    void Reclaim(AbstractCache *cache) {
      boost::lock_guard<MutexType> lock(mutex_);
      CacheHandle *tail = lru_.queue_prev;
      int64_t space_used = cache->space_used();
      while (space_used > cache->capacity() && tail != &lru_) {
        CacheHandle *evicted = tail;
        tail = tail->queue_prev;
        space_used -= evicted->charge;
        EntryReclaim(cache, evicted);
      }
    }

  private:
    MutexType mutex_;
    CacheHandle lru_;
};

class LRUCache : public AbstractCache {
  public:
    LRUCache() : AbstractCache(new LRUCachePolicy) {};
};

/* ===========================================================================
 *  Freq Cache
 */
static inline double CalcTimeFreq(const CacheHandle *entry) {
  return (entry->freq * 0.6) + (entry->time * 0.4);
}

static bool CacheHandleFreqComparer (CacheHandle *a, CacheHandle *b) {
  return CalcTimeFreq(a) < CalcTimeFreq(b);
}

class FreqCachePolicy : public CachePolicy {
  public:
    FreqCachePolicy() {
      __list_init(&active_);
      __list_init(&inactive_);
    }

    void Erase (AbstractCache *cache) {
      CacheHandle *p, *n;
      list_for_each_safe(p, n, &inactive_) {
        DCHECK_EQ(p->refs, 1);  // Error if caller has an unreleased handle
        EntryUnref(cache, p);
      }
      list_for_each_safe(p, n, &active_) {
        DCHECK_EQ(p->refs, 1);  // Error if caller has an unreleased handle
        EntryUnref(cache, p);
      }
    }

    void Insert (AbstractCache *cache, CacheHandle *entry, CacheHandle *old) {
      boost::lock_guard<MutexType> lock(mutex_);
      if (old != NULL)
        list_del(old);
      if (entry->state == CACHE_ENTRY_IS_NEW) {
        /* Insert to the inactive queue */
        entry->state = CACHE_ENTRY_IS_IN_FREQ_INACTIVE;
        list_add(entry, &inactive_);
      } else if (entry->state != CACHE_ENTRY_IS_REPLACED) {
        /* Move to head of the inactive queue */
        entry->state = CACHE_ENTRY_IS_IN_FREQ_INACTIVE;
        list_move(entry, &inactive_);
      }
    }

    void Update(AbstractCache *cache, CacheHandle *entry) {
      if (AtomicIncrement(&(entry->freq), 1) == 1) {
        {
          // Promote to Active queue
          boost::lock_guard<MutexType> lock(mutex_);
          if (entry->state != CACHE_ENTRY_IS_REPLACED) {
            entry->state = CACHE_ENTRY_IS_IN_FREQ_ACTIVE;
            list_move(entry, &active_);
          }
        }
        EntryIsHot(cache, entry);
      }
    }

    void Remove(AbstractCache *cache, CacheHandle *entry) {
      {
        boost::lock_guard<MutexType> lock(mutex_);
        list_del(entry);
      }
      EntryUnref(cache, entry);
    }

    void Reclaim(AbstractCache *cache) {
      boost::lock_guard<MutexType> lock(mutex_);

      uint64_t space_used = cache->space_used();
      CacheHandle *tail = inactive_.queue_prev;
      while (space_used > cache->capacity() && tail != &inactive_) {
        CacheHandle *evicted = tail;
        tail = tail->queue_prev;
        space_used -= evicted->charge;
        EntryReclaim(cache, evicted);
      }
      if (space_used > cache->capacity()) {
        SortActiveQueue();
        tail = active_.queue_prev;
        while (space_used > cache->capacity() && tail != &active_) {
          CacheHandle *evicted = tail;
          tail = tail->queue_prev;
          space_used -= evicted->charge;
          EntryReclaim(cache, evicted);
        }
      }
    }

  private:
    void SortActiveQueue (void) {
      std::vector<CacheHandle *> v;

      CacheHandle *p;
      list_for_each(p, &active_) {
        v.push_back(p);
      }

      // TODO: This will take a while...
      // Sort the list by time & freqency
      std::sort(v.begin(), v.end(), CacheHandleFreqComparer);

      // Rebuild the sorted list
      __list_init(&active_);
      for (std::vector<CacheHandle *>::iterator it = v.begin(); it != v.end(); ++it) {
        list_add(*it, &active_);
      }
    }

  private:
    MutexType mutex_;
    CacheHandle active_;
    CacheHandle inactive_;
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
                         void (*deleter)(const Slice& key, void* value),
                         void *(*hot)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter, hot);
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
    return hash >> (32 - kNumShardBits);
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
