package gsrserde

import (
	"time"

	"github.com/patrickmn/go-cache"
)

// Cache interface for schema caching
type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{})
	Close()
}

// GoCacheWrapper implements Cache using go-cache
type GoCacheWrapper struct {
	cache *cache.Cache
}

// NewCache creates a new cache with given TTL
func NewCache(ttlMillis int64) (Cache, error) {
	ttl := time.Duration(ttlMillis) * time.Millisecond
	cleanupInterval := ttl / 10 // Cleanup every 1/10th of TTL
	
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
