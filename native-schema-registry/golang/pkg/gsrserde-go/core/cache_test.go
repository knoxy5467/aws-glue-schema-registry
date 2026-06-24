package gsrserde

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCache_TTLEvictsAgainstWallClock verifies that the production RealClock path
// (i.e., nil Clock → defaults to RealClock in NewCacheWithOptions) actually evicts
// entries after their TTL elapses on the real wall clock. FakeClock tests cover the
// TTL logic under controlled time; this test exercises the wiring between cache.go
// and time.Now() so a nil-Clock edge case does not go un-exercised.
//
// The TTL is set to 50ms; the test sleeps 75ms to absorb scheduler jitter (~25ms
// slack). This is a single-goroutine timing assertion so it is race-safe under -race.
func TestCache_TTLEvictsAgainstWallClock(t *testing.T) {
	// RealClock is the production path: Clock: nil causes NewCacheWithOptions to
	// default to RealClock (clock.go).
	cache, err := NewCacheWithOptions(CacheOptions{TTLMillis: 50})
	require.NoError(t, err)
	defer cache.Close()

	cache.Set("wc", &Schema{SchemaName: "wall-clock"})

	// Entry must be present immediately after Set.
	v, ok := cache.Get("wc")
	require.True(t, ok, "entry must exist immediately after Set on real-clock path")
	require.Equal(t, "wall-clock", v.(*Schema).SchemaName)

	// Sleep past TTL + slack to let the wall clock advance.
	time.Sleep(75 * time.Millisecond)

	_, ok = cache.Get("wc")
	require.False(t, ok, "entry must be evicted after TTL elapses on real-clock path")
}

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

// TestCache_SizeEvictsOldestEntry asserts §5.3 item 17 / spec §5 S2: with
// CacheSize=N and N+1 distinct inserts (no intervening Gets), the LRU-oldest
// entry is evicted on the N+1 insert. The test deliberately does not call
// Get between inserts because simplelru promotes on Get, which would
// reshuffle the LRU order and make the eviction non-deterministic.
//
// Uses production RealClock — the size-cap scenario is orthogonal to the
// TTL seam (per spec round-2 reconciliation, the size-cap test does not
// inject FakeClock).
func TestCache_SizeEvictsOldestEntry(t *testing.T) {
	cache, err := NewCacheWithOptions(CacheOptions{
		TTLMillis: 60_000,
		Size:      3,
	})
	require.NoError(t, err)
	defer cache.Close()

	cache.Set("k1", &Schema{SchemaName: "s1"})
	cache.Set("k2", &Schema{SchemaName: "s2"})
	cache.Set("k3", &Schema{SchemaName: "s3"})

	// All three present before the cap is exceeded.
	for _, k := range []string{"k1", "k2", "k3"} {
		_, ok := cache.Get(k)
		require.Truef(t, ok, "%s should be present before cap exceeded", k)
	}

	// Insert a fourth — k1 (oldest by insertion under no intervening Gets)
	// must be evicted by LRU.
	cache.Set("k4", &Schema{SchemaName: "s4"})

	_, ok := cache.Get("k1")
	require.False(t, ok, "oldest entry (k1) must be evicted when cap exceeded")
	for _, k := range []string{"k2", "k3", "k4"} {
		_, ok := cache.Get(k)
		require.Truef(t, ok, "%s should still be present after eviction of oldest", k)
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

// TestCache_DefaultSizeMatchesJava asserts that NewCache(ttlMillis) (the legacy
// no-size constructor) creates a cache whose default size matches Java GSR's
// Caffeine `maximumSize` default of 200 (GlueSchemaRegistryConfiguration.java:50
// `private int cacheSize = 200`). This is a regression guard against silent
// breaking changes to DefaultCacheSize.
//
// Concretely: insert 250 entries (= Java default 200 + 50 overflow); verify the
// 50 LRU-oldest entries (k0..k49) are evicted and the 200 most-recent are
// retained. No Gets are performed between inserts to preserve insertion order.
func TestCache_DefaultSizeMatchesJava(t *testing.T) {
	const javaDefault = 200 // must equal DefaultCacheSize in config.go
	const overshoot = 50    // insert this many beyond the cap
	const total = javaDefault + overshoot

	// NewCache uses DefaultCacheSize (=200) — the legacy path callers use.
	// Use a very long TTL so entries never expire by time during this test.
	cache, err := NewCache(int64(24 * 60 * 60 * 1000))
	require.NoError(t, err)
	defer cache.Close()

	keys := make([]string, total)
	for i := 0; i < total; i++ {
		keys[i] = fmt.Sprintf("key-%04d", i)
		cache.Set(keys[i], &Schema{SchemaName: keys[i]})
	}

	// LRU-oldest (first 50 inserted) must be evicted.
	for i := 0; i < overshoot; i++ {
		_, ok := cache.Get(keys[i])
		require.Falsef(t, ok, "key-%04d: LRU-oldest entry must be evicted when cap=%d exceeded",
			i, javaDefault)
	}

	// The 200 most-recently inserted entries must be retained.
	for i := overshoot; i < total; i++ {
		_, ok := cache.Get(keys[i])
		require.Truef(t, ok, "key-%04d: entry must be retained within cap=%d", i, javaDefault)
	}
}
