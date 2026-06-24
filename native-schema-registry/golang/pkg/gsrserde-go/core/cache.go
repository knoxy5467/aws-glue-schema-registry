package gsrserde

import (
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

// Cache is the narrow interface every cache implementation in core/ satisfies.
// Java parity: AWSSchemaRegistryClient maintains two Caffeine caches
// (definition→schemaVersionId and schemaVersionId→schema). We keep the surface
// identical so callers (encoder.go, decoder.go) are insulated from the
// backing-store choice.
type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{})
	Close()
}

// CacheOptions configures a cache instance. The zero value is usable: TTLMillis
// and Size default to DefaultCacheTTLMillis / DefaultCacheSize, and Clock
// defaults to RealClock. Spec §3.2 / §3.3.
//
// Fields:
//   - TTLMillis: per-entry time-to-live in milliseconds. Zero or negative
//     means "use DefaultCacheTTLMillis".
//   - Size: maximum number of entries. Zero or negative means
//     "use DefaultCacheSize". When the cap is exceeded, the LRU-oldest entry
//     is evicted on insert (Java Caffeine `maximumSize` parity).
//   - Clock: the time source. nil means RealClock. Tests inject a *FakeClock
//     to drive TTL eviction deterministically without time.Sleep.
type CacheOptions struct {
	TTLMillis int64
	Size      int
	Clock     Clock
}

// cacheEntry pairs a stored value with its absolute expiry deadline. The
// deadline is computed once on Set (clock.Now() + ttl) and re-evaluated on
// every Get against the injected Clock. Spec §3.3.
type cacheEntry struct {
	value     interface{}
	expiresAt time.Time
}

// LRUCacheWrapper is the default Cache impl, backed by an in-process
// simplelru with hard size cap and per-entry TTL layered on top via the
// injected Clock. Spec §3.3.
//
// simplelru itself is not safe for concurrent use, so the wrapper serializes
// access through a single mutex. The mutex is held across the {lookup,
// expiry-check, conditional Remove} sequence in Get and across Add in Set,
// so an expired entry cannot be observed-then-clobbered by a concurrent
// Set of the same key — there is no TOCTOU window between expiry check
// and eviction (PBI-1 review NIT addressed by atomic Get path).
type LRUCacheWrapper struct {
	mu    sync.Mutex
	lru   *simplelru.LRU[string, cacheEntry]
	ttl   time.Duration
	clock Clock
}

// NewCache returns a Cache with the given TTL in milliseconds. Backward-
// compatible wrapper around NewCacheWithOptions. Per spec INV-CACHE-API,
// the signature is preserved so existing callers (encoder.go, decoder.go,
// the singleflight tests) compile unchanged. Size defaults to
// DefaultCacheSize.
func NewCache(ttlMillis int64) (Cache, error) {
	return NewCacheWithOptions(CacheOptions{TTLMillis: ttlMillis})
}

// NewCacheWithOptions is the configurable constructor. Spec §3.2 / §3.3.
// Zero / negative TTLMillis or Size are coerced to their defaults
// (DefaultCacheTTLMillis, DefaultCacheSize). A nil Clock defaults to
// RealClock.
func NewCacheWithOptions(opts CacheOptions) (Cache, error) {
	ttlMillis := opts.TTLMillis
	if ttlMillis <= 0 {
		ttlMillis = DefaultCacheTTLMillis
	}
	ttl := time.Duration(ttlMillis) * time.Millisecond

	size := opts.Size
	if size <= 0 {
		size = DefaultCacheSize
	}

	clk := opts.Clock
	if clk == nil {
		clk = RealClock
	}

	lru, err := simplelru.NewLRU[string, cacheEntry](size, nil)
	if err != nil {
		return nil, err
	}

	return &LRUCacheWrapper{
		lru:   lru,
		ttl:   ttl,
		clock: clk,
	}, nil
}

// Get returns the cached value for key, or (nil, false) if absent or
// expired. Expired entries are evicted under the same mutex hold that
// observed the expiry, so there is no window where a concurrent Set of
// the same key could be racing the eviction.
func (c *LRUCacheWrapper) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	if !c.clock.Now().Before(entry.expiresAt) {
		c.lru.Remove(key)
		return nil, false
	}
	return entry.value, true
}

// Set inserts or refreshes the entry for key. If the cache is at capacity
// the LRU-oldest entry is evicted by simplelru.Add. expiresAt is computed
// from the injected Clock, so tests can drive TTL eviction via
// FakeClock.Advance.
func (c *LRUCacheWrapper) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Add(key, cacheEntry{
		value:     value,
		expiresAt: c.clock.Now().Add(c.ttl),
	})
}

// Close drops all entries. Mirrors the prior cache wrapper's Flush()
// semantics so callers that defer Close() on test caches stay
// behaviorally identical.
func (c *LRUCacheWrapper) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Purge()
}
