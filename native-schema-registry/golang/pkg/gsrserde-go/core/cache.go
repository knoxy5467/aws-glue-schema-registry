package gsrserde

import (
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
// TODO(phase 2): patrickmn/go-cache is TTL-only. Java's Caffeine caches are
// also size-bounded. When the integration-test scenarios in plan §5.3 #17
// ("Cache size eviction: distinct schemas beyond `cacheSize` evict the
// oldest") become a hard requirement, swap the backing store for an LRU and
// add a size knob to NewCache.
type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{})
	Close()
}

// GoCacheWrapper is the default Cache impl, backed by patrickmn/go-cache.
type GoCacheWrapper struct {
	cache *cache.Cache
}

// NewCache returns a Cache with the given TTL in milliseconds. Cleanup runs at
// TTL/10 to bound how stale a "still in the cache" entry can be.
func NewCache(ttlMillis int64) (Cache, error) {
	ttl := time.Duration(ttlMillis) * time.Millisecond
	cleanupInterval := ttl / 10
	if cleanupInterval <= 0 {
		cleanupInterval = ttl
	}

	c := cache.New(ttl, cleanupInterval)

	return &GoCacheWrapper{
		cache: c,
	}, nil
}

func (c *GoCacheWrapper) Get(key string) (interface{}, bool) {
	return c.cache.Get(key)
}

func (c *GoCacheWrapper) Set(key string, value interface{}) {
	c.cache.Set(key, value, cache.DefaultExpiration)
}

func (c *GoCacheWrapper) Close() {
	c.cache.Flush()
}
