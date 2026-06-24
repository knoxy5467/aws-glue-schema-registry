package gsrserde

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCache_TTLEvictsAfterAdvancePastTTL replaces the prior wall-clock
// TestCacheTTL (which slept 150ms). Per spec §5 S1 and PBI-4.11-1, the
// new test uses an injected FakeClock so TTL eviction is deterministic
// and the test does not depend on goroutine scheduling.
func TestCache_TTLEvictsAfterAdvancePastTTL(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	clk := NewFakeClock(start)
	cache, err := NewCacheWithOptions(CacheOptions{
		TTLMillis: 100,
		Size:      10,
		Clock:     clk,
	})
	require.NoError(t, err)
	defer cache.Close()

	cache.Set("k", &Schema{SchemaName: "s"})

	// Before TTL: present.
	v, ok := cache.Get("k")
	require.True(t, ok, "value must exist immediately after Set")
	require.Equal(t, "s", v.(*Schema).SchemaName)

	// Advance just past TTL — no real time elapses.
	clk.Advance(150 * time.Millisecond)

	_, ok = cache.Get("k")
	require.False(t, ok, "TTL must evict after clock advance past ttlMillis")
}

func TestCacheInterface(t *testing.T) {
	cache, err := NewCache(60000)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Test non-existent key
	if _, exists := cache.Get("nonexistent"); exists {
		t.Error("Non-existent key should not exist")
	}

	// Test basic set/get
	schema := &Schema{SchemaName: "test"}
	cache.Set("key", schema)

	if value, exists := cache.Get("key"); !exists {
		t.Error("Value should exist immediately after set")
	} else if value.(*Schema).SchemaName != "test" {
		t.Error("Retrieved value doesn't match")
	}
}

func TestCacheSize(t *testing.T) {
	// Note: go-cache doesn't have built-in size limits.
	// This test verifies basic functionality only. Size-cap enforcement
	// lands in PBI-4.11-2 (LRU swap); see spec §3.3.
	cache, err := NewCache(60000)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Add items
	cache.Set("key1", &Schema{SchemaName: "schema1"})
	cache.Set("key2", &Schema{SchemaName: "schema2"})
	cache.Set("key3", &Schema{SchemaName: "schema3"})

	// All should exist (go-cache doesn't enforce size limits)
	if _, exists := cache.Get("key1"); !exists {
		t.Error("key1 should exist")
	}
	if _, exists := cache.Get("key2"); !exists {
		t.Error("key2 should exist")
	}
	if _, exists := cache.Get("key3"); !exists {
		t.Error("key3 should exist")
	}
}

// TestEncoder_TTLEviction_TriggersFreshGetSchemaByDefinition asserts the
// encoder-level TTL contract: once the cache entry for (schemaName,
// dataFormat) expires, the next encode fires a fresh GetSchemaByDefinition
// against Glue rather than returning the stale cached SchemaVersionID.
// Per spec §5 S1 and audit row §5.3 item 16 — the existing TTL test was a
// bare-cache stand-in; this validates the encoder cache contract end to end.
func TestEncoder_TTLEviction_TriggersFreshGetSchemaByDefinition(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_700_000_000, 0))
	mockClient := &MockGlueClient{}
	enc, err := NewGsrEncoderForTest(mockClient, GsrEncoderOptions{
		RegistryName:   "test-registry",
		CacheTTLMillis: 100,
		Clock:          clk,
	})
	require.NoError(t, err)

	var getCalls atomic.Int64
	versionID := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &versionID,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil).
		Run(func(_ mock.Arguments) { getCalls.Add(1) })

	// First encode populates the cache.
	_, _, err = enc.getSchemaVersionIdByDefinition("def", "s", "JSON")
	require.NoError(t, err)
	require.Equal(t, int64(1), getCalls.Load(), "first encode must call GetSchemaByDefinition once")

	// Second encode within TTL hits the cache; no extra Glue call.
	_, _, err = enc.getSchemaVersionIdByDefinition("def", "s", "JSON")
	require.NoError(t, err)
	require.Equal(t, int64(1), getCalls.Load(), "second call within TTL must hit cache")

	// Advance past TTL.
	clk.Advance(200 * time.Millisecond)

	// Third encode must re-fetch from Glue.
	_, _, err = enc.getSchemaVersionIdByDefinition("def", "s", "JSON")
	require.NoError(t, err)
	require.Equal(t, int64(2), getCalls.Load(),
		"after TTL eviction a fresh GetSchemaByDefinition must fire")
}

// TestDecoder_TTLEviction_TriggersFreshGetSchemaVersion is the symmetric
// decoder assertion. After the cache entry for schemaVersionID expires,
// the next decode must fire a fresh GetSchemaVersion. Per spec §5 S1 / §8
// D7 (test name match) and audit row §5.3 item 16.
func TestDecoder_TTLEviction_TriggersFreshGetSchemaVersion(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_700_000_000, 0))
	mockClient := &MockGlueClient{}
	dec, err := NewGsrDecoderForTest(mockClient, GsrDecoderOptions{
		RegistryName:   "test-registry",
		CacheTTLMillis: 100,
		Clock:          clk,
	})
	require.NoError(t, err)

	var getCalls atomic.Int64
	schemaDefinition := "test-definition"
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaDefinition: &schemaDefinition,
			DataFormat:       types.DataFormatJson,
		}, nil).
		Run(func(_ mock.Arguments) { getCalls.Add(1) })

	// First decode populates the cache.
	_, err = dec.getSchemaByVersionID(testUUIDString)
	require.NoError(t, err)
	require.Equal(t, int64(1), getCalls.Load(), "first decode must call GetSchemaVersion once")

	// Second decode within TTL hits the cache.
	_, err = dec.getSchemaByVersionID(testUUIDString)
	require.NoError(t, err)
	require.Equal(t, int64(1), getCalls.Load(), "second call within TTL must hit cache")

	// Advance past TTL.
	clk.Advance(200 * time.Millisecond)

	// Third decode must re-fetch from Glue.
	_, err = dec.getSchemaByVersionID(testUUIDString)
	require.NoError(t, err)
	require.Equal(t, int64(2), getCalls.Load(),
		"after TTL eviction a fresh GetSchemaVersion must fire")
}
