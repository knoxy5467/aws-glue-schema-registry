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
			id, _, err := encoder.getSchemaVersionIdByDefinition("def", "schema", "JSON")
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
	_, _, err := encoder.getSchemaVersionIdByDefinition("def", "schema", "JSON")
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
			id, _, err := encoder.getSchemaVersionIdByDefinition("def", "schema", "JSON")
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

	id, _, err := encoder.getSchemaVersionIdByDefinition("def", "schema", "JSON")
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
