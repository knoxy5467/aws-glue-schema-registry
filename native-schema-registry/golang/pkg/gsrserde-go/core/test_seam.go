package gsrserde

import (
	"fmt"
	"time"
)

// This file holds the test-seam constructors that let downstream packages
// build a GsrEncoder / GsrDecoder against an in-memory fake GlueClient
// without going through NewGsrEncoder / NewGsrDecoder (which load an
// aws.Config and instantiate the real Glue SDK client).
//
// These constructors are public because the orchestrator and format-layer
// Tier-1 tests live in sibling packages, not in core itself. The
// "...ForTest" suffix is deliberate — calling these from production code
// would skip every config-key side effect that LoadConfigFromMap wires up.

// GsrEncoderOptions is the subset of Configuration that GsrEncoder reads at
// encode time. It exists so test code can pin the fields it cares about
// without round-tripping through LoadConfigFromMap.
type GsrEncoderOptions struct {
	RegistryName    string
	Compatibility   string
	Description     string
	Tags            map[string]string
	// Metadata is the parsed `metadata.<key>=<value>` map flushed to Glue via
	// PutSchemaVersionMetadata after a successful CreateSchema /
	// RegisterSchemaVersion (Java parity at AWSSchemaRegistryClient.java:264
	// and :281). Tests that exercise the metadata-flush path populate this;
	// otherwise the encoder only flushes the always-on transport entry.
	Metadata        map[string]string
	CompressionType string
	// CacheTTLMillis is the time-to-live for the schema cache. Zero means
	// "use the default" (matching LoadConfigFromMap behaviour).
	CacheTTLMillis int64
	// CacheSize is the maximum number of entries the schema cache will hold
	// before LRU-evicting the oldest. Zero means "use DefaultCacheSize"
	// (matching LoadConfigFromMap behaviour). Spec §3.2 / §3.3 and PBI-4.11-2.
	CacheSize int
	// SchemaAutoRegistrationEnabled mirrors the Java config flag and the
	// corresponding production field; tests that exercise auto-register
	// fall-through set it explicitly.
	SchemaAutoRegistrationEnabled bool
	// Clock is the time source the schema cache consumes for TTL eviction.
	// nil means RealClock (production). Tests inject a *FakeClock here to
	// drive TTL eviction deterministically via FakeClock.Advance. Spec
	// §3.2 and PBI-4.11-1.
	Clock Clock
}

// NewGsrEncoderForTest builds a *GsrEncoder wired with an injected
// GlueClient. It does not touch AWS, does not call LoadConfigFromMap, and
// returns an encoder that is safe to use from a single goroutine in a unit
// test. The returned encoder owns a fresh in-memory schema cache.
func NewGsrEncoderForTest(client GlueClient, opts GsrEncoderOptions) (*GsrEncoder, error) {
	if client == nil {
		return nil, fmt.Errorf("test seam: client must not be nil")
	}
	ttl := opts.CacheTTLMillis
	if ttl == 0 {
		ttl = DefaultCacheTTLMillis
	}
	cache, err := NewCacheWithOptions(CacheOptions{
		TTLMillis: ttl,
		Size:      opts.CacheSize,
		Clock:     opts.Clock,
	})
	if err != nil {
		return nil, fmt.Errorf("test seam: cache: %w", err)
	}
	return &GsrEncoder{
		client:                        client,
		registryName:                  opts.RegistryName,
		compatibility:                 opts.Compatibility,
		tags:                          opts.Tags,
		metadata:                      opts.Metadata,
		schemaCache:                   cache,
		description:                   opts.Description,
		schemaAutoRegistrationEnabled: opts.SchemaAutoRegistrationEnabled,
		compressionType:               opts.CompressionType,
		sleepFn:                       time.Sleep,
	}, nil
}

// GsrDecoderOptions is the subset of Configuration that GsrDecoder reads.
type GsrDecoderOptions struct {
	RegistryName   string
	CacheTTLMillis int64
	// CacheSize mirrors GsrEncoderOptions.CacheSize — see that field's comment.
	CacheSize int
	// Clock mirrors GsrEncoderOptions.Clock — see that field's comment.
	Clock Clock
}

// NewGsrDecoderForTest builds a *GsrDecoder wired with an injected
// GlueClient. Mirrors NewGsrEncoderForTest.
func NewGsrDecoderForTest(client GlueClient, opts GsrDecoderOptions) (*GsrDecoder, error) {
	if client == nil {
		return nil, fmt.Errorf("test seam: client must not be nil")
	}
	ttl := opts.CacheTTLMillis
	if ttl == 0 {
		ttl = DefaultCacheTTLMillis
	}
	cache, err := NewCacheWithOptions(CacheOptions{
		TTLMillis: ttl,
		Size:      opts.CacheSize,
		Clock:     opts.Clock,
	})
	if err != nil {
		return nil, fmt.Errorf("test seam: cache: %w", err)
	}
	return &GsrDecoder{
		client:       client,
		registryName: opts.RegistryName,
		schemaCache:  cache,
	}, nil
}

// PrimeSchemaCache is a test-only helper that seeds the decoder's
// version-id → Schema cache. It exists so format-layer Tier-1 tests can
// drive Decode through the cached fast path without dispatching a fake
// GetSchemaVersion call.
func PrimeSchemaCache(d *GsrDecoder, schemaVersionID string, schema *Schema) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.schemaCache.Set(schemaVersionID, schema)
}

// PrimeEncoderCache is the encoder analog of PrimeSchemaCache. The encoder
// caches under the key "<schemaName>:<dataFormat>" — callers that want to
// short-circuit GetSchemaByDefinition use this directly.
func PrimeEncoderCache(e *GsrEncoder, schemaName, dataFormat string, schema *Schema) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.schemaCache.Set(fmt.Sprintf("%s:%s", schemaName, dataFormat), schema)
}

// EncoderCacheHas is the read-side mirror of PrimeEncoderCache. Returns true
// iff the encoder's schemaCache currently holds an entry under the given
// (schemaName, dataFormat) key. Used by integration tests that need to
// observe LRU eviction behavior on real-Glue without depending on a Glue-side
// call counter.
func EncoderCacheHas(e *GsrEncoder, schemaName, dataFormat string) bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	_, ok := e.schemaCache.Get(fmt.Sprintf("%s:%s", schemaName, dataFormat))
	return ok
}
