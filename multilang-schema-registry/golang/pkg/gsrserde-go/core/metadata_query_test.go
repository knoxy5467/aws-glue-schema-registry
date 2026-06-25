// Tier-1 tests for GsrEncoder.QuerySchemaVersionMetadata and
// GsrEncoder.QuerySchemaTags.
//
// Coverage per PBI-4.10-6 / spec §3.6:
//   - AC-7 / S-11: QuerySchemaVersionMetadata flattens MetadataInfoMap into
//     map[string]string and returns the result.
//   - AC-7 error path: Glue SDK errors wrap ErrGSR.
//   - AC-8 / S-11: QuerySchemaTags calls GetSchemaByDefinition THEN GetTags
//     in that order and returns resp.Tags.
//   - AC-8-no-cache-touch: QuerySchemaTags does NOT mutate the encoder's
//     schema-definition cache.
//
// Java parity citations:
//   - QuerySchemaVersionMetadata: AWSSchemaRegistryClient.java:476-487
//   - QuerySchemaTags:            AWSSchemaRegistryClient.java:502-517

package gsrserde

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newEncoderWithMockForQuery builds a minimal GsrEncoder wired to a
// MockGlueClient for the query-method tests. These tests do not exercise the
// write path (Encode / CreateSchema / RegisterSchemaVersion), so
// schemaAutoRegistrationEnabled is left false and metadata is left nil.
func newEncoderWithMockForQuery(t *testing.T) (*GsrEncoder, *MockGlueClient) {
	t.Helper()
	mockClient := &MockGlueClient{}
	cache, err := NewCache(DefaultCacheTTLMillis)
	require.NoError(t, err)
	enc := &GsrEncoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}
	return enc, mockClient
}

// TestEncoder_QuerySchemaVersionMetadata covers AC-7 / S-11.
// The mock returns a MetadataInfoMap with two entries; the method must flatten
// it into a plain map[string]string keyed by the metadata key with the value
// taken from MetadataInfo.MetadataValue. Exactly one QuerySchemaVersionMetadata
// call must be recorded.
func TestEncoder_QuerySchemaVersionMetadata(t *testing.T) {
	enc, mockClient := newEncoderWithMockForQuery(t)

	vid := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	mockClient.On("QuerySchemaVersionMetadata", mock.Anything, &glue.QuerySchemaVersionMetadataInput{
		SchemaVersionId: aws.String(vid),
	}).Return(&glue.QuerySchemaVersionMetadataOutput{
		MetadataInfoMap: map[string]types.MetadataInfo{
			"commit": {MetadataValue: aws.String("abc123")},
			"env":    {MetadataValue: aws.String("beta")},
		},
	}, nil)

	result, err := enc.QuerySchemaVersionMetadata(context.Background(), vid)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"commit": "abc123",
		"env":    "beta",
	}, result)

	mockClient.AssertNumberOfCalls(t, "QuerySchemaVersionMetadata", 1)
}

// TestEncoder_QuerySchemaVersionMetadata_ErrorWrapsErrGSR covers AC-7 error
// path. When the SDK returns an error, the method must wrap it in ErrGSR so
// callers can errors.Is-match at either granularity.
func TestEncoder_QuerySchemaVersionMetadata_ErrorWrapsErrGSR(t *testing.T) {
	enc, mockClient := newEncoderWithMockForQuery(t)

	vid := "ffffffff-0000-1111-2222-333333333333"
	sdkErr := errors.New("access denied")
	mockClient.On("QuerySchemaVersionMetadata", mock.Anything, mock.Anything).
		Return((*glue.QuerySchemaVersionMetadataOutput)(nil), sdkErr)

	_, err := enc.QuerySchemaVersionMetadata(context.Background(), vid)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrGSR, "AC-7: errors from QuerySchemaVersionMetadata must wrap ErrGSR")
}

// TestEncoder_QuerySchemaTags covers AC-8 / S-11.
// The mock returns a SchemaArn from GetSchemaByDefinition; the mock then
// returns Tags from GetTags using that ARN. The test verifies:
//   - Call order: GetSchemaByDefinition is called before GetTags.
//   - The correct ResourceArn is forwarded to GetTags.
//   - The returned map matches the mock's Tags.
func TestEncoder_QuerySchemaTags(t *testing.T) {
	enc, mockClient := newEncoderWithMockForQuery(t)

	def := `{"type":"record","name":"Order","fields":[]}`
	name := "order-schema"
	arn := "arn:aws:glue:us-east-1:123456789012:schema/test-registry/order-schema"

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaByDefinitionOutput{
			SchemaArn: aws.String(arn),
		}, nil)
	mockClient.On("GetTags", mock.Anything, &glue.GetTagsInput{
		ResourceArn: aws.String(arn),
	}).Return(&glue.GetTagsOutput{
		Tags: map[string]string{
			"team":        "data-platform",
			"cost-center": "42",
		},
	}, nil)

	result, err := enc.QuerySchemaTags(context.Background(), def, name)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"team":        "data-platform",
		"cost-center": "42",
	}, result)

	// Verify call order: GetSchemaByDefinition before GetTags.
	mockClient.AssertNumberOfCalls(t, "GetSchemaByDefinition", 1)
	mockClient.AssertNumberOfCalls(t, "GetTags", 1)

	// Verify call order by inspecting the Calls slice.
	require.Len(t, mockClient.Calls, 2)
	require.Equal(t, "GetSchemaByDefinition", mockClient.Calls[0].Method, "AC-8: GetSchemaByDefinition must be called first")
	require.Equal(t, "GetTags", mockClient.Calls[1].Method, "AC-8: GetTags must be called second")
}

// TestEncoder_QuerySchemaTags_DoesNotMutateCache covers AC-8-no-cache-touch.
// Pre-populate the encoder's cache with one entry. Call QuerySchemaTags for a
// DIFFERENT (def, name) pair that is NOT in the cache. Assert that after the
// call the original cache entry is unchanged and no new entry has been added —
// QuerySchemaTags must bypass the cache entirely.
func TestEncoder_QuerySchemaTags_DoesNotMutateCache(t *testing.T) {
	enc, mockClient := newEncoderWithMockForQuery(t)

	// Pre-populate cache with a known entry.
	cacheKey := "cached-schema:AVRO"
	cachedSchema := &Schema{
		SchemaName:       "cached-schema",
		SchemaDefinition: `{"type":"record","name":"CachedSchema","fields":[]}`,
		DataFormat:       "AVRO",
		SchemaVersionID:  testUUIDString,
	}
	enc.schemaCache.Set(cacheKey, cachedSchema)

	// Wire up the mock for a different schema — QuerySchemaTags should NOT
	// add this to the cache.
	def := `{"type":"record","name":"Order","fields":[]}`
	name := "order-schema"
	arn := "arn:aws:glue:us-east-1:123456789012:schema/test-registry/order-schema"

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaByDefinitionOutput{
			SchemaArn: aws.String(arn),
		}, nil)
	mockClient.On("GetTags", mock.Anything, mock.Anything).
		Return(&glue.GetTagsOutput{
			Tags: map[string]string{"owner": "team-a"},
		}, nil)

	_, err := enc.QuerySchemaTags(context.Background(), def, name)
	require.NoError(t, err)

	// The pre-populated entry must still be there, unchanged.
	got, exists := enc.schemaCache.Get(cacheKey)
	require.True(t, exists, "AC-8-no-cache-touch: pre-populated cache entry must still exist")
	require.Equal(t, cachedSchema, got.(*Schema), "AC-8-no-cache-touch: pre-populated cache entry must be unchanged")

	// The queried schema must NOT have been inserted into the cache.
	queryKey := name + ":AVRO"
	_, inserted := enc.schemaCache.Get(queryKey)
	require.False(t, inserted, "AC-8-no-cache-touch: QuerySchemaTags must not insert into the cache")

	// Also check the format-agnostic key that fetchSchemaVersionID would use.
	for _, suffix := range []string{"AVRO", "JSON", "PROTOBUF"} {
		k := name + ":" + suffix
		_, found := enc.schemaCache.Get(k)
		require.False(t, found, "AC-8-no-cache-touch: no cache entry should be added for key %s", k)
	}
}
