package gsrserde

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncoderCacheSize_Eviction exercises DoD #3: an encoder built via
// NewGsrEncoder with cacheSize=5 evicts the LRU-oldest entry when a 6th
// distinct schema is added to its cache.
//
// The production constructor is used so the test proves the CacheSize wiring
// in NewGsrEncoder (not just NewGsrEncoderForTest). PrimeEncoderCache seeds
// the cache directly to avoid real Glue calls in a Tier-1 unit test — the
// fix under test is that NewGsrEncoder passes config.CacheSize into
// NewCacheWithOptions; without the fix the cache defaults to size 200 and
// no eviction occurs at 6 entries.
func TestEncoderCacheSize_Eviction(t *testing.T) {
	// DoD #3
	enc, err := NewGsrEncoder(map[string]string{
		"cacheSize":    "5",
		"registryName": "test-registry",
	})
	require.NoError(t, err)
	defer enc.Close()

	// Prime 5 distinct schemas — each key is "<schemaName>:<dataFormat>".
	for i := 1; i <= 5; i++ {
		PrimeEncoderCache(enc, fmt.Sprintf("schema-%d", i), "JSON", &Schema{
			SchemaName:      fmt.Sprintf("schema-%d", i),
			DataFormat:      "JSON",
			SchemaVersionID: fmt.Sprintf("uuid-%d", i),
		})
	}

	// All 5 must be present before the cap is breached.
	for i := 1; i <= 5; i++ {
		assert.Truef(t, EncoderCacheHas(enc, fmt.Sprintf("schema-%d", i), "JSON"),
			"schema-%d must be present before cap exceeded", i)
	}

	// Insert the 6th entry — schema-1 is LRU-oldest and must be evicted.
	PrimeEncoderCache(enc, "schema-6", "JSON", &Schema{
		SchemaName:      "schema-6",
		DataFormat:      "JSON",
		SchemaVersionID: "uuid-6",
	})

	assert.False(t, EncoderCacheHas(enc, "schema-1", "JSON"),
		"oldest encoder cache entry (schema-1) must be evicted when cacheSize=5 cap is exceeded")
	for i := 2; i <= 6; i++ {
		assert.Truef(t, EncoderCacheHas(enc, fmt.Sprintf("schema-%d", i), "JSON"),
			"schema-%d must remain in cache after eviction of oldest", i)
	}
}

// TestEncoderCacheSize_Default exercises DoD #4: an encoder built without a
// cacheSize key uses the default size of 200. Verified by confirming that 6
// primed entries are all retained (i.e., no eviction at 6 entries, proving
// the cache is larger than 5). This is a targeted complement to the exhaustive
// TestCache_DefaultSizeMatchesJava at the cache layer.
func TestEncoderCacheSize_Default(t *testing.T) {
	// DoD #4
	enc, err := NewGsrEncoder(map[string]string{
		"registryName": "test-registry",
		// no cacheSize key — must default to 200
	})
	require.NoError(t, err)
	defer enc.Close()

	// Prime 6 entries into a default-size encoder.
	for i := 1; i <= 6; i++ {
		PrimeEncoderCache(enc, fmt.Sprintf("schema-%d", i), "JSON", &Schema{
			SchemaName:      fmt.Sprintf("schema-%d", i),
			DataFormat:      "JSON",
			SchemaVersionID: fmt.Sprintf("uuid-%d", i),
		})
	}

	// All 6 must still be present — no eviction should occur at 6 entries
	// when the default size is 200.
	for i := 1; i <= 6; i++ {
		assert.Truef(t, EncoderCacheHas(enc, fmt.Sprintf("schema-%d", i), "JSON"),
			"schema-%d must not be evicted from a default-size encoder at only 6 entries", i)
	}
}

// TestDecoderCacheSize_Eviction exercises DoD #5: a decoder built via
// NewGsrDecoder with cacheSize=5 evicts the LRU-oldest schema-version UUID
// entry when a 6th distinct entry is added to its cache. Verified via the new
// DecoderCacheHas test seam (mirrors EncoderCacheHas).
func TestDecoderCacheSize_Eviction(t *testing.T) {
	// DoD #5
	dec, err := NewGsrDecoder(map[string]string{
		"cacheSize":    "5",
		"registryName": "test-registry",
	})
	require.NoError(t, err)
	defer dec.Close()

	// Prime 5 distinct schema-version UUIDs.
	for i := 1; i <= 5; i++ {
		uuid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		PrimeSchemaCache(dec, uuid, &Schema{
			SchemaVersionID: uuid,
			SchemaName:      fmt.Sprintf("schema-%d", i),
			DataFormat:      "JSON",
		})
	}

	// All 5 must be present before the cap is breached.
	for i := 1; i <= 5; i++ {
		uuid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		assert.Truef(t, DecoderCacheHas(dec, uuid),
			"schema-version UUID %s must be present before cap exceeded", uuid)
	}

	// Insert the 6th UUID — oldest (i=1) must be evicted.
	uuid6 := fmt.Sprintf("00000000-0000-0000-0000-%012d", 6)
	PrimeSchemaCache(dec, uuid6, &Schema{
		SchemaVersionID: uuid6,
		SchemaName:      "schema-6",
		DataFormat:      "JSON",
	})

	oldest := fmt.Sprintf("00000000-0000-0000-0000-%012d", 1)
	assert.False(t, DecoderCacheHas(dec, oldest),
		"oldest decoder cache entry must be evicted when cacheSize=5 cap is exceeded")
	for i := 2; i <= 6; i++ {
		uuid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		assert.Truef(t, DecoderCacheHas(dec, uuid),
			"schema-version UUID %s must remain in cache after eviction of oldest", uuid)
	}
}
