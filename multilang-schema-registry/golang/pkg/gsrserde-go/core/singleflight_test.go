package gsrserde

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestEncoder_GetSchemaVersionId_SingleflightDeduplicates verifies that N
// concurrent first-encodes for the same schema definition collapse into ONE
// GetSchemaByDefinition Glue call. Without singleflight, every goroutine
// would race the Glue API in parallel, waste quota, and risk
// AlreadyExistsException on the auto-register path.
//
// Java parity: AWSSchemaRegistryClient relies on Caffeine's LoadingCache to
// dedup loads of the same key (one load function per missing key, others
// wait); Go's idiomatic equivalent is x/sync/singleflight.
func TestEncoder_GetSchemaVersionId_SingleflightDeduplicates(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	encoder := &GsrEncoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	// Count GetSchemaByDefinition invocations across all concurrent calls.
	// Without singleflight this would be N (number of goroutines); with
	// singleflight it must be exactly 1.
	var getCalls atomic.Int64
	schemaVersionId := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &schemaVersionId,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil).
		Run(func(_ mock.Arguments) { getCalls.Add(1) })

	const N = 32
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			id, _, err := encoder.getSchemaVersionIdByDefinition("def", "schema", "JSON", "")
			assert.NoError(t, err)
			assert.Equal(t, schemaVersionId, id)
		}()
	}
	wg.Wait()

	assert.LessOrEqual(t, getCalls.Load(), int64(1),
		"singleflight must collapse N concurrent first-encodes to 1 GetSchemaByDefinition call; saw %d", getCalls.Load())
}

// TestEncoder_GetSchemaVersionId_CachedAfterFirstCall verifies that once the
// first-encode populates the cache, subsequent concurrent calls take the
// fast-path RLock branch and DON'T call Glue at all.
func TestEncoder_GetSchemaVersionId_CachedAfterFirstCall(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	encoder := &GsrEncoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	schemaVersionId := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &schemaVersionId,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)

	// Warm the cache.
	_, _, err := encoder.getSchemaVersionIdByDefinition("def", "schema", "JSON", "")
	require.NoError(t, err)

	// 100 cached lookups — none should reach Glue.
	mockClient.Calls = nil // reset call history
	mockClient.ExpectedCalls = nil
	// Drop any Glue stubs — if anyone calls Glue now, the assertion will
	// surface as an unexpected call.

	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			id, _, err := encoder.getSchemaVersionIdByDefinition("def", "schema", "JSON", "")
			assert.NoError(t, err)
			assert.Equal(t, schemaVersionId, id)
		}()
	}
	wg.Wait()

	for _, c := range mockClient.Calls {
		t.Errorf("unexpected post-warm Glue call: %s", c.Method)
	}
}

// TestDecoder_GetSchema_SingleflightDeduplicates is the symmetric assertion
// for the decoder: N concurrent first-decodes of the same UUID collapse into
// one GetSchemaVersion call.
func TestDecoder_GetSchema_SingleflightDeduplicates(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	decoder := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	var getCalls atomic.Int64
	schemaDefinition := "test-definition"
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaDefinition: &schemaDefinition,
			DataFormat:       types.DataFormatJson,
		}, nil).
		Run(func(_ mock.Arguments) { getCalls.Add(1) })

	const N = 32
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			schema, err := decoder.getSchemaByVersionID(testUUIDString)
			assert.NoError(t, err)
			assert.Equal(t, schemaDefinition, schema.SchemaDefinition)
		}()
	}
	wg.Wait()

	assert.LessOrEqual(t, getCalls.Load(), int64(1),
		"singleflight must collapse N concurrent first-decodes to 1 GetSchemaVersion call; saw %d", getCalls.Load())
}

// TestEncoder_GetSchemaVersionId_Singleflight_CreateSchemaExactlyOnce is the
// strict-equality contract test for the singleflight-around-CreateSchema
// invariant (Phase 4.11 spec §3.7, §5 S3, §8 D8). When N goroutines race the
// first-encode of a brand-new schema (GetSchemaByDefinition →
// EntityNotFoundException → CreateSchema), exactly ONE of them must reach
// CreateSchema; the rest must share that result via singleflight.
//
// Without singleflight, every racing goroutine would issue CreateSchema in
// parallel, all but one would hit *types.AlreadyExistsException, fall through
// to RegisterSchemaVersion, and double-bill the registry while creating
// spurious version-2/version-3/... entries. Java parity:
// AWSSchemaRegistryClient relies on Caffeine's LoadingCache to ensure exactly
// one CacheLoader.load() per key; Go's idiomatic equivalent is x/sync/
// singleflight per cache key.
//
// Spec §8 D8a requires a release barrier in this test — without it the
// goroutines would launch sequentially as the loop scheduled them, the first
// would populate the cache before the second even started, and singleflight
// would never be exercised. The `start := make(chan struct{})` literal +
// `<-start` in the goroutine body is the maximum-pressure setup: all 32
// goroutines park at the channel receive, then close(start) wakes them
// simultaneously so they all hit the singleflight entry in parallel. Spec
// §8 D8a calls this out as a grep-verifiable predicate.
//
// This complements (does NOT replace) the looser
// TestEncoder_GetSchemaVersionId_SingleflightDeduplicates above
// (LessOrEqual ≤ 1) which covers GetSchemaByDefinition dedup on the cached
// path; this test covers the strictly-tighter CreateSchema dedup on the
// auto-register path.
func TestEncoder_GetSchemaVersionId_Singleflight_CreateSchemaExactlyOnce(t *testing.T) {
	mockClient := &MockGlueClient{}
	// Legacy NewCache(ttlMillis) constructor — PBI-4.11-1's Clock-injected
	// constructor lands in cache.go separately; this test must stay
	// parallel-safe and not touch that file.
	cache, err := NewCache(300000)
	require.NoError(t, err)

	encoder := &GsrEncoder{
		client:                        mockClient,
		registryName:                  "test-registry",
		schemaCache:                   cache,
		schemaAutoRegistrationEnabled: true,
	}

	// GetSchemaByDefinition returns EntityNotFoundException so the encoder
	// falls through to the auto-register path (encoder.go fetchSchemaVersionID).
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())

	// CreateSchema is the contract surface under test. Count every
	// invocation and return a valid SchemaVersionId. The atomic counter is
	// read once after wg.Wait() so the happens-before is wg.Done() →
	// wg.Wait(); no data race.
	var createCalls atomic.Int64
	createdVersionID := testUUIDString
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdVersionID,
			LatestSchemaVersion: ptrInt64(1),
		}, nil).
		Run(func(_ mock.Arguments) { createCalls.Add(1) })

	// Release barrier: all 32 goroutines park on <-start before any can
	// enter the encoder. close(start) wakes them simultaneously, maximizing
	// the chance that >1 reaches the singleflight Do() entry concurrently.
	// Without this barrier, goroutines would launch and run sequentially as
	// the loop scheduled them; the first would populate the cache and the
	// rest would hit the fast path, never exercising the singleflight
	// dedup. Spec §8 D8a — grep-verifiable.
	const N = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-start
			id, _, err := encoder.getSchemaVersionIdByDefinition("def", "schema-name", "JSON", "")
			assert.NoError(t, err)
			assert.Equal(t, createdVersionID, id)
		}()
	}
	close(start)
	wg.Wait()

	// Strict equality — anything other than 1 means singleflight failed to
	// collapse the racing first-encodes. LessOrEqual would mask a regression
	// where two goroutines slipped through (e.g., per-call singleflight
	// groups instead of a per-encoder group). Spec §5 S3 prescribes the
	// `atomic.LoadInt64(&createCalls) == 1` form; atomic.Int64.Load() is the
	// idiomatic Go ≥ 1.19 spelling and is the equivalent helper the legacy
	// SingleflightDeduplicates test above uses.
	// require (not assert) so a failure aborts immediately and the test
	// message is unambiguous — final-board finding MAJOR #3.
	require.Equal(t, int64(1), createCalls.Load(),
		"singleflight must collapse %d concurrent CreateSchema calls to exactly 1; saw %d",
		N, createCalls.Load())
}

// TestEncoder_SecondCacheReturnsCachedUUID asserts that the encoder cache is
// keyed by (schemaName, dataFormat) — Java parity with the
// definition→version-id Caffeine cache — and that a hit on that key short-
// circuits the Glue call.
func TestEncoder_SchemaNameDataFormatCacheKeyHits(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	encoder := &GsrEncoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	// Pre-populate the cache with the (schemaName, dataFormat) key the
	// encoder uses internally. The mock has no Glue stubs — if the encoder
	// reaches Glue, the test surfaces an unexpected-call error.
	cache.Set("schema:JSON", &Schema{
		SchemaName:       "schema",
		SchemaDefinition: "def",
		DataFormat:       "JSON",
		SchemaVersionID:  testUUIDString,
	})

	id, _, err := encoder.getSchemaVersionIdByDefinition("def", "schema", "JSON", "")
	require.NoError(t, err)
	assert.Equal(t, testUUIDString, id)
	assert.Empty(t, mockClient.Calls, "cached path must not invoke Glue")
}

// TestDecoder_VersionIDCacheKeyHits asserts the symmetric decoder property:
// the cache is keyed by schema-version UUID (the value the wire-format header
// carries), Java parity with the second Caffeine cache.
func TestDecoder_VersionIDCacheKeyHits(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	decoder := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	cache.Set(testUUIDString, &Schema{
		SchemaName:       "schema",
		SchemaDefinition: "def",
		DataFormat:       "JSON",
		SchemaVersionID:  testUUIDString,
	})

	schema, err := decoder.getSchemaByVersionID(testUUIDString)
	require.NoError(t, err)
	assert.Equal(t, testUUIDString, schema.SchemaVersionID)
	assert.Empty(t, mockClient.Calls, "cached path must not invoke Glue")
}
