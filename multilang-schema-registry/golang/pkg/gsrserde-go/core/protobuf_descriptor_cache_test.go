package gsrserde

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseSchemaDefinitionToDescriptor_CacheHitReturnsSamePointer asserts
// that two successive parses of the same schema text return the SAME
// *desc.FileDescriptor instance — proving the cache short-circuits the
// underlying parse. This is the contract the encoder's hot path relies on
// for the Phase 8B speedup; if a future change drops the cache, this test
// fails.
func TestParseSchemaDefinitionToDescriptor_CacheHitReturnsSamePointer(t *testing.T) {
	protoDescriptorCacheClearForTest()

	first, err := parseSchemaDefinitionToDescriptor(singleMessageSchema)
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := parseSchemaDefinitionToDescriptor(singleMessageSchema)
	require.NoError(t, err)
	require.NotNil(t, second)

	assert.Same(t, first, second,
		"expected cached descriptor: second parse should return the same *desc.FileDescriptor pointer")
}

// TestParseSchemaDefinitionToDescriptor_DistinctTextDistinctEntries
// confirms the cache keys by raw text — different schema strings produce
// different descriptors. (No alias collisions between unrelated schemas.)
func TestParseSchemaDefinitionToDescriptor_DistinctTextDistinctEntries(t *testing.T) {
	protoDescriptorCacheClearForTest()

	const otherSchema = `syntax = "proto3"; message OtherMessage { int32 id = 1; }`

	a, err := parseSchemaDefinitionToDescriptor(singleMessageSchema)
	require.NoError(t, err)
	b, err := parseSchemaDefinitionToDescriptor(otherSchema)
	require.NoError(t, err)

	assert.NotSame(t, a, b, "distinct schema text must produce distinct descriptors")
}

// TestParseSchemaDefinitionToDescriptor_MalformedSchemaErrorsNotCached
// asserts that a malformed schema returns the parse error but does NOT
// poison the cache — a subsequent call with the same malformed schema
// re-runs the parse (and may succeed if jhump behavior changes across
// versions).
func TestParseSchemaDefinitionToDescriptor_MalformedSchemaErrorsNotCached(t *testing.T) {
	protoDescriptorCacheClearForTest()

	_, err := parseSchemaDefinitionToDescriptor(`syntax = "proto3"; message {`)
	require.Error(t, err)

	// Cache must still be empty after a failed parse.
	count := 0
	protoDescriptorCache.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "errors must not be cached")
}

// TestParseSchemaDefinitionToDescriptor_EmptyDescriptorSetNotCached covers
// the corner case where the underlying parser returns (nil descriptors, nil
// error) — historically this poisoned the cache with a typed-nil entry that
// every subsequent caller would nil-deref. The uncached path now synthesizes
// an error for empty descriptor sets and the wrapper rejects it before
// LoadOrStore, so the cache stays empty.
func TestParseSchemaDefinitionToDescriptor_EmptyDescriptorSetNotCached(t *testing.T) {
	protoDescriptorCacheClearForTest()

	// A .proto with only `syntax` + an `option` produces a FileDescriptorProto
	// with zero message types but is well-formed; protoparse may return it
	// as an empty descriptor set rather than an error, depending on version.
	// Either way the wrapper must NOT cache a nil descriptor.
	const emptyProto = `syntax = "proto3"; package empty;`
	_, _ = parseSchemaDefinitionToDescriptor(emptyProto)

	// Whatever the parse outcome, no nil descriptor may sit in the cache.
	protoDescriptorCache.Range(func(_, v interface{}) bool {
		assert.NotNil(t, v, "cache must never store a nil descriptor")
		return true
	})
}

// TestParseSchemaDefinitionToDescriptor_Concurrent fires many goroutines
// against the cache simultaneously to surface data races. Run via
// `go test -race`. The cache must serialize the parse without losing
// updates or returning nil for late arrivals.
func TestParseSchemaDefinitionToDescriptor_Concurrent(t *testing.T) {
	protoDescriptorCacheClearForTest()

	const goroutines = 32
	var wg sync.WaitGroup
	results := make([]interface{}, goroutines)
	errs := make([]error, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			fd, err := parseSchemaDefinitionToDescriptor(singleMessageSchema)
			results[idx] = fd
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		require.NoErrorf(t, errs[i], "goroutine %d", i)
		require.NotNilf(t, results[i], "goroutine %d returned nil descriptor", i)
	}
	// All goroutines must observe the same cached pointer once the cache settles.
	first := results[0]
	for i := 1; i < goroutines; i++ {
		assert.Samef(t, first, results[i], "goroutine %d saw a different descriptor pointer", i)
	}
}

// BenchmarkParseSchemaDefinitionToDescriptor_Cached measures the cache-hit
// cost. The first iteration parses cold; subsequent iterations are
// dominated by sync.Map.Load. The delta to the uncached benchmark is the
// Phase 8B win on the encoder hot path.
func BenchmarkParseSchemaDefinitionToDescriptor_Cached(b *testing.B) {
	protoDescriptorCacheClearForTest()
	// Warm so the b.N loop measures hits.
	if _, err := parseSchemaDefinitionToDescriptor(singleMessageSchema); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := parseSchemaDefinitionToDescriptor(singleMessageSchema); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseSchemaDefinitionToDescriptor_Uncached is the baseline —
// calls the uncached implementation directly. Compare with the _Cached
// bench to confirm the cache delivers a meaningful speedup.
func BenchmarkParseSchemaDefinitionToDescriptor_Uncached(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := parseSchemaDefinitionToDescriptorUncached(singleMessageSchema); err != nil {
			b.Fatal(err)
		}
	}
}
