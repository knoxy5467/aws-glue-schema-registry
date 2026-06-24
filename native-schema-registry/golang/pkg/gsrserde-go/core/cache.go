package gsrserde

import (
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
)

// Cache is the narrow interface every cache implementation in core/ satisfies.
// Java parity: AWSSchemaRegistryClient maintains two Caffeine caches
// (definition→schemaVersionId and schemaVersionId→schema). We keep the surface
// identical so swapping `patrickmn/go-cache` for a size-bounded LRU later
// (e.g. hashicorp/golang-lru) only requires a new implementation behind this
// interface — no caller changes.
//
// TODO(phase 4.11 PBI-2): patrickmn/go-cache is TTL-only. Java's Caffeine
// caches are also size-bounded. The size cap and the simplelru swap land in
// PBI-2; this PBI introduces only the clock-injection seam.
type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{})
	Close()
}

// CacheOptions configures a cache instance. The zero value is invalid
// (TTLMillis must be > 0); callers use NewCacheWithOptions to honor
// defaults explicitly. Spec §3.2.
//
// Fields:
//   - TTLMillis: per-entry time-to-live in milliseconds. Zero or negative
//     means "use DefaultCacheTTLMillis".
//   - Size: maximum number of entries. Zero means "use DefaultCacheSize".
//     This field is plumbed in PBI-1 but full size-cap enforcement lands
//     in PBI-2; until then it is accepted-but-ignored.
//   - Clock: the time source. nil means RealClock.
type CacheOptions struct {
	TTLMillis int64
	Size      int
	Clock     Clock
}

// GoCacheWrapper is the default Cache impl, backed by patrickmn/go-cache.
//
// When a non-RealClock is injected via CacheOptions.Clock, the wrapper
// applies the TTL itself (using the injected Clock) and stores entries
// in go-cache with NoExpiration so that go-cache's internal time.Now()
// reads can't fire eviction before the FakeClock advances. With
// RealClock the wrapper delegates expiry to go-cache directly, matching
// the pre-Phase-4.11 behavior.
type GoCacheWrapper struct {
	cache *cache.Cache
	clock Clock
	ttl   time.Duration

	// mu protects the per-key expiry map used when a non-RealClock is
	// injected. Under RealClock the map is unused and go-cache's own
	// locking serializes access.
	mu      sync.Mutex
	expires map[string]time.Time
}

// NewCache returns a Cache with the given TTL in milliseconds. Backward-
// compatible wrapper around NewCacheWithOptions. Per spec INV-CACHE-API,
// the signature is preserved so existing callers (encoder.go, decoder.go,
// the singleflight tests) compile unchanged.
func NewCache(ttlMillis int64) (Cache, error) {
	return NewCacheWithOptions(CacheOptions{TTLMillis: ttlMillis})
}

// NewCacheWithOptions is the configurable constructor. Spec §3.2. The Size
// field is plumbed but not yet enforced (PBI-2's exclusive scope).
func NewCacheWithOptions(opts CacheOptions) (Cache, error) {
	ttlMillis := opts.TTLMillis
	if ttlMillis <= 0 {
		ttlMillis = DefaultCacheTTLMillis
	}
	ttl := time.Duration(ttlMillis) * time.Millisecond

	clk := opts.Clock
	if clk == nil {
		clk = RealClock
	}

	w := &GoCacheWrapper{
		clock: clk,
		ttl:   ttl,
	}

	if _, isReal := clk.(realClock); isReal {
		// Production path: delegate expiry to go-cache itself so the
		// existing cleanup-loop / lock behavior is unchanged.
		cleanupInterval := ttl / 10
		if cleanupInterval <= 0 {
			cleanupInterval = ttl
		}
		w.cache = cache.New(ttl, cleanupInterval)
		return w, nil
	}

	// Injected (Fake) clock path: store entries with no expiry inside
	// go-cache and apply the TTL ourselves against the injected Clock.
	// This preserves the seam contract — TTL eviction is driven solely
	// by FakeClock.Advance, not by wall-clock time.
	w.cache = cache.New(cache.NoExpiration, cache.NoExpiration)
	w.expires = make(map[string]time.Time)
	return w, nil
}

func (c *GoCacheWrapper) Get(key string) (interface{}, bool) {
	if c.expires == nil {
		// RealClock path.
		return c.cache.Get(key)
	}
	// Injected-clock path: consult our expiry map first.
	c.mu.Lock()
	expiresAt, tracked := c.expires[key]
	c.mu.Unlock()
	if !tracked {
		return nil, false
	}
	if !c.clock.Now().Before(expiresAt) {
		c.mu.Lock()
		delete(c.expires, key)
		c.mu.Unlock()
		c.cache.Delete(key)
		return nil, false
	}
	return c.cache.Get(key)
}

func (c *GoCacheWrapper) Set(key string, value interface{}) {
	if c.expires == nil {
		c.cache.Set(key, value, cache.DefaultExpiration)
		return
	}
	c.mu.Lock()
	c.expires[key] = c.clock.Now().Add(c.ttl)
	c.mu.Unlock()
	c.cache.Set(key, value, cache.NoExpiration)
}

func (c *GoCacheWrapper) Close() {
	c.cache.Flush()
	if c.expires != nil {
		c.mu.Lock()
		c.expires = make(map[string]time.Time)
		c.mu.Unlock()
	}
}
