// Copyright (c) 2013, Cloudera, inc.
//
// Some portions copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <gutil/hash/city.h>

#include <stdlib.h>

#include "util/cache.h"
#include "util/pthread_spinlock.h"

namespace kudu {

Cache::~Cache() {
}

namespace {

typedef PThreadSpinLock MutexType;

#define AtomicIncrement         __sync_add_and_fetch
#define CompareAndSwap          __sync_val_compare_and_swap

#define RefCountInc(x)          __sync_add_and_fetch(x, 1)
#define RefCountDec(x)          __sync_sub_and_fetch(x, 1)

typedef uint32_t Atomic64;
typedef uint32_t Atomic32;

/* ============================================================================
 *  Read-Write lock. 32bit uint that contains the number of readers.
 *  When someone wants to write, tries to set the 32bit, and waits until
 *  the readers have finished. Readers are spinning while the write flag is set.
 */
class RwLock {
  public:
    RwLock() : state_(0) {}
    ~RwLock() {}

    void ReadLock() {
      Atomic32 new_state;
      Atomic32 expected;
      do {
        expected = state_ & 0x7fffffff;   // I expect no write lock
        new_state = expected + 1;         // Add me as reader
      } while (CompareAndSwap(&state_, expected, new_state) != expected);
    }

    void ReadUnlock() {
      Atomic32 new_state;
      Atomic32 expected;
      do {
        expected = state_;          // I expect a write lock and other readers
        new_state = expected - 1;   // Drop me as reader
      } while (CompareAndSwap(&state_, expected, new_state) != expected);
    }

    bool WriteTryLock() {
      Atomic32 expected = state_ & 0x7fffffff;    // I expect some 0+ readers
      Atomic32 new_state = (1 << 31) | expected;  // I want to lock the other writers
      if (CompareAndSwap(&state_, expected, new_state) != expected)
        return false;

      // Wait pending reads
      while ((state_ & 0x7fffffff) > 0) /* cpu_relax(); */;

      return true;
    }

    void WriteLock() {
      while (!WriteTryLock());
    }

    void WriteUnlock() {
      Atomic32 new_state;
      Atomic32 expected;
      do {
        expected = 1 << 31;  // I expect to be the only writer
        new_state = 0;       // reset: no writers/no readers
      } while (CompareAndSwap(&state_, expected, new_state) != expected);
    }

  private:
    Atomic32 state_;
};

enum entry_states {
  CACHE_ENTRY_IS_NEW,
  CACHE_ENTRY_IS_EVICTED,
  CACHE_ENTRY_IS_REPLACED,
  /* LRU */
  CACHE_ENTRY_IS_IN_LRU_QUEUE,
  /* 2Q */
  CACHE_ENTRY_IS_IN_2Q_AM,
  CACHE_ENTRY_IS_IN_2Q_A1IN,
  CACHE_ENTRY_IS_IN_2Q_A1OUT,
  /* Freq */
  CACHE_ENTRY_IS_IN_FREQ_ACTIVE,
  CACHE_ENTRY_IS_IN_FREQ_INACTIVE,
};

struct CacheHandle {
  Atomic64 freq;
  Atomic32 refs;
  uint32_t state;

  CacheHandle *hash;
  CacheHandle *next;
  CacheHandle *prev;

  uint32_t key_hash;
  uint32_t charge;
  size_t key_size;
  void *value;
  void (*deleter)(const Slice&, void* value);
  uint8_t key_data[1];

  Slice key() const {
    return Slice(key_data, key_size);
  }

  bool isDeletable() const {
    return state == CACHE_ENTRY_IS_EVICTED || state == CACHE_ENTRY_IS_REPLACED;
  }
};

/* ===========================================================================
 *  Hash Table
 *  Chained hashtable with a read-write lock used to block everyone on table resize.
 *  Each node has a rwlock taken during each operation.
 */
#if 1
class BucketLock : public RwLock { };
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

class HandleTable {
  private:
    struct Bucket {
      BucketLock lock;
      CacheHandle *entry;
      Bucket() : entry(NULL) {}
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
      Bucket *bucket = &(list_[hash & mask_]);
      //__builtin_prefetch(bucket);
      return bucket;
    }

    CacheHandle **FindHandle (Bucket *bucket, const Slice& key, uint32_t hash) {
      CacheHandle **node = &(bucket->entry);
      while (*node && ((*node)->key_hash != hash || key != (*node)->key())) {
        node = &((*node)->hash);
      }
      return node;
    }

    void Resize();

  private:
    RwLock lock_;
    size_t mask_;
    size_t used_;
    size_t size_;
    Bucket *list_;
};

HandleTable::HandleTable()
  : mask_(0), used_(0), size_(0), list_(NULL)
{
  Resize();
}

HandleTable::~HandleTable() {
  delete[] list_;
}

CacheHandle *HandleTable::Insert(CacheHandle *entry) {
  lock_.ReadLock();
  Bucket *bucket = FindBucket(entry->key_hash);
  bucket->lock.WriteLock();
  CacheHandle **node = FindHandle(bucket, entry->key(), entry->key_hash);
  CacheHandle *old = *node;
  *node = entry;
  if (old != NULL) {
    DCHECK(!old->isDeletable());
    entry->hash = old->hash;
    entry->freq = old->freq;
    entry->state = old->state;
    old->state = CACHE_ENTRY_IS_REPLACED;
  } else {
    entry->hash = NULL;
    entry->state = CACHE_ENTRY_IS_NEW;
  }
  bucket->lock.WriteUnlock();
  lock_.ReadUnlock();

  if (old == NULL && AtomicIncrement(&used_, 1) > size_) {
    // if we can't take the lock, means that someone else is resizing
    if (lock_.WriteTryLock()) {
      Resize();
      lock_.WriteUnlock();
    }
  }

  return old;
}

CacheHandle *HandleTable::Lookup(const Slice& key, uint32_t hash) {
  CacheHandle *entry;
  lock_.ReadLock();
  Bucket *bucket = FindBucket(hash);
  bucket->lock.ReadLock();
  if ((entry = *FindHandle(bucket, key, hash)) != NULL) {
    DCHECK(!entry->isDeletable());
    RefCountInc(&(entry->refs));
  }
  bucket->lock.ReadUnlock();
  lock_.ReadUnlock();
  return entry;
}

CacheHandle *HandleTable::Remove(const Slice& key, uint32_t hash) {
  CacheHandle **node;
  CacheHandle *entry;
  lock_.ReadLock();
  Bucket *bucket = FindBucket(hash);
  bucket->lock.WriteLock();
  node = FindHandle(bucket, key, hash);
  if ((entry = *node) != NULL) {
    DCHECK(!entry->isDeletable());
    *node = entry->hash;
    RefCountInc(&(entry->refs));
  }
  bucket->lock.WriteUnlock();
  lock_.ReadUnlock();
  if (entry != NULL)
    AtomicIncrement(&used_, -1);
  return entry;
}

bool HandleTable::Remove(CacheHandle *entry) {
  CacheHandle **node;
  bool found = false;
  lock_.ReadLock();
  Bucket *bucket = FindBucket(entry->key_hash);
  bucket->lock.WriteLock();
  for (node = &(bucket->entry); *node != NULL; node = &((*node)->hash)) {
    if (*node == entry) {
      *node = entry->hash;
      entry->state = CACHE_ENTRY_IS_EVICTED;
      found = true;
      break;
    }
  }
  bucket->lock.WriteUnlock();
  lock_.ReadUnlock();
  if (found)
    AtomicIncrement(&used_, -1);
  return found;
}

void HandleTable::Resize() {
  // Calculate a new table size
  size_t new_size = 4;
  while (new_size < used_) {
    new_size <<= 1;
  }

  if (size_ >= new_size)
    return;

  // Allocate a new bucket list
  Bucket *new_list = new Bucket[new_size];
  size_t new_mask = new_size - 1;
  if (list_ != NULL) {
    // Copy entries
    for (size_t i = 0; i < size_; ++i) {
      CacheHandle *p = list_[i].entry;
      while (p != NULL) {
        CacheHandle *next = p->hash;

        // Insert Entry
        Bucket *bucket = &(new_list[p->key_hash & new_mask]);
        p->hash = bucket->entry;
        bucket->entry = p;

        p = next;
      }
    }
    // Delete the old bucket
    delete[] list_;
  }

  // Swap the bucket
  mask_ = new_mask;
  size_ = new_size;
  list_ = new_list;
}

/* ===========================================================================
 *  List
 */
#define __list_init(list)                       \
  do {                                          \
    (list)->next = list;                        \
    (list)->prev = list;                        \
  } while (0)

#define __list_add(inew, iprev, inext)          \
  do {                                          \
    (inew)->next = inext;                       \
    (inew)->prev = iprev;                       \
    (inext)->prev = inew;                       \
    (iprev)->next = inew;                       \
  } while (0)

#define __list_del(iprev, inext)                \
  do {                                          \
    (inext)->prev = iprev;                      \
    (iprev)->next = inext;                      \
  } while (0)

#define list_add(inew, head)                    \
  __list_add(inew, head, (head)->next)

#define list_del(entry)                         \
  do {                                          \
    __list_del((entry)->prev, (entry)->next);   \
    __list_init(entry);                         \
  } while (0);

#define list_move(list, head)                   \
  do {                                          \
    __list_del((list)->prev, (list)->next);     \
    list_add(list, head);                       \
  } while (0);

#define list_for_each_safe(pos, n, head)        \
  for (pos = (head)->next, n = (pos)->next;     \
       pos != (head); pos = n, n = (pos)->next)

/* ===========================================================================
 *  Cache
 */
class CachePolicy;
class AbstractCache {
  public:
    AbstractCache(CachePolicy *policy);
    virtual ~AbstractCache();

    Cache::Handle *Insert (const Slice& key, uint32_t hash, void *value, size_t charge,
                           void (*deleter)(const Slice& key, void* value));
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

    uint64_t usage (void) const {
      return usage_;
    }

  private:
    CacheHandle *CreateNewHandle(const Slice& key, uint32_t hash, void *value, size_t charge,
                                 void (*deleter)(const Slice& key, void* value));
    void EntryUnref(CacheHandle *entry);
    void EntryReclaim(CacheHandle *entry);

  private:
    Atomic64 hit_;
    Atomic64 miss_;
    Atomic64 usage_;
    uint64_t capacity_;
    HandleTable table_;
    CachePolicy *policy_;
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
  : hit_(0), miss_(0), usage_(0), capacity_(0), policy_(policy)
{
}

AbstractCache::~AbstractCache() {
  policy_->Erase(this);
  delete policy_;
}

Cache::Handle *AbstractCache::Insert(const Slice& key, uint32_t hash, void *value, size_t charge,
                                     void (*deleter)(const Slice& key, void* value)) {
  CacheHandle *entry = CreateNewHandle(key, hash, value, charge, deleter);
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
    policy_->Update(this, entry);
    AtomicIncrement(&hit_, 1);
  } else {
    AtomicIncrement(&miss_, 1);
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
    size_t charge, void (*deleter)(const Slice& key, void* value)) {
  CacheHandle *entry = reinterpret_cast<CacheHandle*>(malloc(sizeof(CacheHandle)-1 + key.size()));

  entry->value = value;
  entry->deleter = deleter;

  // One from the cache, one for the user returned pointer and one for the lookup func
  entry->refs = 3;

  entry->freq = 0;
  entry->state = CACHE_ENTRY_IS_NEW;

  entry->hash = NULL;
  entry->next = entry;
  entry->prev = entry;

  entry->key_hash = hash;
  entry->charge = charge;
  entry->key_size = key.size();

  memcpy(entry->key_data, key.data(), key.size());

  AtomicIncrement(&usage_, charge);
  return(entry);
}

void AbstractCache::EntryUnref(CacheHandle *entry) {
  DCHECK_GT(entry->refs, 0);
  if (!RefCountDec(&(entry->refs))) {
    AtomicIncrement(&usage_, -entry->charge);
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
      mutex_.lock();
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
      mutex_.unlock();
    }

    void Update(AbstractCache *cache, CacheHandle *entry) {
      mutex_.lock();
      if (entry->state != CACHE_ENTRY_IS_REPLACED)
        list_move(entry, &lru_);
      mutex_.unlock();
    }

    void Remove(AbstractCache *cache, CacheHandle *entry) {
      mutex_.lock();
      list_del(entry);
      mutex_.unlock();
      EntryUnref(cache, entry);
    }

    void Reclaim(AbstractCache *cache) {
      mutex_.lock();
      CacheHandle *tail = lru_.prev;
      uint64_t usage = cache->usage();
      while (usage > cache->capacity() && tail != &lru_) {
        CacheHandle *evicted = tail;
        tail = tail->prev;
        usage -= evicted->charge;
        EntryReclaim(cache, evicted);
      }
      mutex_.unlock();
    }

  private:
    MutexType mutex_;
    CacheHandle lru_;
};

class LRUCache : public AbstractCache {
  public:
    LRUCache() : AbstractCache(new LRUCachePolicy) {};
};

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
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
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

Cache* NewLRUCache(size_t capacity) {
  return new ShardedCache<LRUCache>(capacity);
}

}  // namespace kudu
