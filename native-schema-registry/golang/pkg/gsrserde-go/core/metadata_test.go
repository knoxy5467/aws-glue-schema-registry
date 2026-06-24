// Tier-1 tests for the schema-version metadata propagation path
// (`putSchemaVersionMetadataBatch` + its encoder wire-up).
//
// Coverage per PBI-4.10-4 / spec §3.4:
//   - AC-4 / S-1: Configured metadata + transport entry reach Glue on first
//     encode via the CreateSchema success branch.
//   - AC-5 / S-2: Same set reaches Glue via the RegisterSchemaVersion
//     fallback when CreateSchema raises AlreadyExistsException.
//   - AC-9: Non-empty transport stamps the canonical x-amz-meta-transport key.
//   - AC-9b / S-10: Empty transport STILL stamps the entry (value "").
//   - AC-4-collision: Customer-configured `metadata.x-amz-meta-transport=<foo>`
//     overrides the auto-injected transport entry (Java parity at
//     AWSSchemaRegistryClient.java:419-422 — `put(transport); putAll(configured)`).
//   - INV-1 / C-10: Cache-hit fast path does NOT flush metadata.
//   - INV-6 / C-13: Per-entry PutSchemaVersionMetadata failures are logged
//     but do NOT fail Encode.
//
// All tests use the shared MockGlueClient seam from mock_glue_client_test.go;
// the metadata-pair recorder (`PutSchemaVersionMetadataPairs`) is order-
// independent — the spec deliberately does NOT pin call order (§3.4(d)).

package gsrserde

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newEncoderWithMockAndOpts builds an encoder wired with a MockGlueClient
// that also carries the configured metadata map. Mirrors newEncoderWithMock
// in glue_negatives_test.go but lets the test pin the metadata field
// directly. The tags field is left nil — these tests focus on the metadata
// flush surface, not on CreateSchema tag propagation (Phase 4.9 audit row 12,
// covered by other Tier-1 tests).
func newEncoderWithMockAndMetadata(t *testing.T, autoRegister bool, metadata map[string]string) (*GsrEncoder, *MockGlueClient) {
	t.Helper()
	mockClient := &MockGlueClient{}
	cache, err := NewCache(DefaultCacheTTLMillis)
	require.NoError(t, err)
	enc := &GsrEncoder{
		client:                        mockClient,
		registryName:                  "test-registry",
		schemaCache:                   cache,
		schemaAutoRegistrationEnabled: autoRegister,
		metadata:                      metadata,
	}
	return enc, mockClient
}

// metadataPairsAsSet converts the recorder's ordered slice into a set the
// test can compare with require.ElementsMatch. The spec (§3.4(d)) pins
// call COUNT and set-equality, NOT order — keeping the assertion on a set
// matches the contract.
func metadataPairsAsSet(pairs []MetadataPair) []MetadataPair {
	out := make([]MetadataPair, 0, len(pairs))
	out = append(out, pairs...)
	return out
}

// TestEncoder_Metadata_FlushedAfterCreateSchema covers AC-4 / S-1.
// Configured metadata = {"commit":"abc123", "env":"beta"} + transport
// "kafka-topic" → 3 PutSchemaVersionMetadata calls with the expected set.
// Java parity citation: AWSSchemaRegistryClient.java:264 (fires the flush
// from the CreateSchema success branch).
func TestEncoder_Metadata_FlushedAfterCreateSchema(t *testing.T) {
	enc, mockClient := newEncoderWithMockAndMetadata(t, true, map[string]string{
		"commit": "abc123",
		"env":    "beta",
	})

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())

	createdVid := otherUUIDString
	v := int64(1)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{
			SchemaVersionId:     aws.String(createdVid),
			LatestSchemaVersion: &v,
		}, nil)
	mockClient.On("PutSchemaVersionMetadata", mock.Anything, mock.Anything).
		Return(&glue.PutSchemaVersionMetadataOutput{}, nil)

	gotID, _, err := enc.getSchemaVersionIdByDefinition("def-1", "schema-1", "JSON", "kafka-topic")
	require.NoError(t, err)
	require.Equal(t, createdVid, gotID)

	require.Equal(t, 3, mockClient.MetadataCallCount(), "len(metadata)+1 = 3 calls (commit, env, transport)")
	require.ElementsMatch(t, []MetadataPair{
		{Key: "commit", Value: "abc123"},
		{Key: "env", Value: "beta"},
		{Key: TransportMetadataKey, Value: "kafka-topic"},
	}, metadataPairsAsSet(mockClient.MetadataPairs()))
}

// TestEncoder_Metadata_FlushedAfterRegisterSchemaVersion covers AC-5 / S-2.
// CreateSchema returns AlreadyExistsException → encoder falls through to
// RegisterSchemaVersion, which must fire the flush (Java parity at
// AWSSchemaRegistryClient.java:281).
func TestEncoder_Metadata_FlushedAfterRegisterSchemaVersion(t *testing.T) {
	enc, mockClient := newEncoderWithMockAndMetadata(t, true, map[string]string{
		"commit": "abc123",
	})

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return((*glue.CreateSchemaOutput)(nil), &types.AlreadyExistsException{Message: aws.String("race")})

	registeredVid := otherUUIDString
	v := int64(2)
	mockClient.On("RegisterSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.RegisterSchemaVersionOutput{
			SchemaVersionId: aws.String(registeredVid),
			VersionNumber:   &v,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)
	mockClient.On("PutSchemaVersionMetadata", mock.Anything, mock.Anything).
		Return(&glue.PutSchemaVersionMetadataOutput{}, nil)

	gotID, _, err := enc.getSchemaVersionIdByDefinition("def-2", "schema-2", "JSON", "kafka-topic")
	require.NoError(t, err)
	require.Equal(t, registeredVid, gotID)

	require.Equal(t, 2, mockClient.MetadataCallCount(), "len(metadata)+1 = 2 calls (commit, transport)")
	require.ElementsMatch(t, []MetadataPair{
		{Key: "commit", Value: "abc123"},
		{Key: TransportMetadataKey, Value: "kafka-topic"},
	}, metadataPairsAsSet(mockClient.MetadataPairs()))
}

// TestEncoder_Encode_InjectsTransportMetadataKey covers AC-9 / S-10
// (non-empty branch). Even with empty Config.Metadata, the transport entry
// is always present (C-14 — len(metadata)==0 still yields exactly 1 call).
func TestEncoder_Encode_InjectsTransportMetadataKey(t *testing.T) {
	enc, mockClient := newEncoderWithMockAndMetadata(t, true, nil)

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{SchemaVersionId: aws.String(otherUUIDString)}, nil)
	mockClient.On("PutSchemaVersionMetadata", mock.Anything, mock.Anything).
		Return(&glue.PutSchemaVersionMetadataOutput{}, nil)

	_, _, err := enc.getSchemaVersionIdByDefinition("def-tx", "schema-tx", "JSON", "kafka-topic")
	require.NoError(t, err)

	require.Equal(t, 1, mockClient.MetadataCallCount(), "C-14: zero configured metadata still yields the transport entry")
	require.ElementsMatch(t, []MetadataPair{
		{Key: TransportMetadataKey, Value: "kafka-topic"},
	}, metadataPairsAsSet(mockClient.MetadataPairs()))
}

// TestEncoder_Encode_EmptyTransport_StillInjects covers AC-9b / S-10 (empty
// branch). The implementor MUST NOT short-circuit the transport entry when
// transportName == "" — Java parity at
// serializer-deserializer/.../GlueSchemaRegistrySerializationFacade.java:90-95
// (unconditional `put` before the configured-metadata merge). INV-7.
func TestEncoder_Encode_EmptyTransport_StillInjects(t *testing.T) {
	enc, mockClient := newEncoderWithMockAndMetadata(t, true, nil)

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{SchemaVersionId: aws.String(otherUUIDString)}, nil)
	mockClient.On("PutSchemaVersionMetadata", mock.Anything, mock.Anything).
		Return(&glue.PutSchemaVersionMetadataOutput{}, nil)

	_, _, err := enc.getSchemaVersionIdByDefinition("def-empty", "schema-empty", "JSON", "")
	require.NoError(t, err)

	require.Equal(t, 1, mockClient.MetadataCallCount())
	pairs := mockClient.MetadataPairs()
	require.Len(t, pairs, 1)
	require.Equal(t, TransportMetadataKey, pairs[0].Key)
	require.Equal(t, "", pairs[0].Value, "AC-9b: empty transport entry value IS the empty string, the entry IS present")
}

// TestEncoder_Metadata_CustomerOverridesTransportKey covers AC-4-collision.
// When the customer configures `metadata.x-amz-meta-transport=foo`, the
// merge yields len(s.metadata) total calls (NOT +1) and the transport-key
// value is the customer's `foo`, NOT the per-call transportName. Java
// parity: AWSSchemaRegistryClient.java:419-422 places `put(transport)`
// BEFORE `putAll(configured)`, so the configured value overwrites.
func TestEncoder_Metadata_CustomerOverridesTransportKey(t *testing.T) {
	enc, mockClient := newEncoderWithMockAndMetadata(t, true, map[string]string{
		TransportMetadataKey: "foo",
		"other":              "value",
	})

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{SchemaVersionId: aws.String(otherUUIDString)}, nil)
	mockClient.On("PutSchemaVersionMetadata", mock.Anything, mock.Anything).
		Return(&glue.PutSchemaVersionMetadataOutput{}, nil)

	_, _, err := enc.getSchemaVersionIdByDefinition("def-collide", "schema-collide", "JSON", "kafka-topic")
	require.NoError(t, err)

	require.Equal(t, 2, mockClient.MetadataCallCount(), "len(s.metadata)=2 calls (configured wins over the auto-transport)")
	require.ElementsMatch(t, []MetadataPair{
		{Key: TransportMetadataKey, Value: "foo"}, // customer's `foo`, NOT "kafka-topic"
		{Key: "other", Value: "value"},
	}, metadataPairsAsSet(mockClient.MetadataPairs()))
}

// TestEncoder_Metadata_CacheHitSkipsFlush covers INV-1 / C-10. When the
// schema-version-ID lookup hits the cache, the metadata flush MUST NOT
// run — metadata is written once per schema-version (Java parity:
// AWSSchemaRegistryClient writes only on the CreateSchema / Register paths,
// not on the GetSchemaByDefinition cached lookup).
func TestEncoder_Metadata_CacheHitSkipsFlush(t *testing.T) {
	enc, mockClient := newEncoderWithMockAndMetadata(t, true, map[string]string{
		"commit": "abc123",
	})

	// Pre-populate cache directly so the encoder takes the fast path.
	enc.schemaCache.Set("schema-hit:JSON", &Schema{
		SchemaName:       "schema-hit",
		SchemaDefinition: "def-hit",
		DataFormat:       "JSON",
		SchemaVersionID:  testUUIDString,
	})

	gotID, _, err := enc.getSchemaVersionIdByDefinition("def-hit", "schema-hit", "JSON", "kafka-topic")
	require.NoError(t, err)
	require.Equal(t, testUUIDString, gotID)

	require.Equal(t, 0, mockClient.MetadataCallCount(), "INV-1: cache-hit path MUST NOT flush metadata")
	require.Empty(t, mockClient.Calls, "cache-hit path MUST NOT reach Glue at all")
}

// TestEncoder_Encode_MetadataPartialFailure_StillSucceeds covers INV-6 /
// C-13. Per-entry PutSchemaVersionMetadata failures are logged via
// stdlib log.Printf with the "gsr: " prefix and intentionally NOT
// propagated to the encoder caller. Java parity at
// AWSSchemaRegistryClient.java:425 — partial-write is a logged-only
// condition.
func TestEncoder_Encode_MetadataPartialFailure_StillSucceeds(t *testing.T) {
	enc, mockClient := newEncoderWithMockAndMetadata(t, true, map[string]string{
		"commit": "abc123",
		"env":    "beta",
	})

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{SchemaVersionId: aws.String(otherUUIDString)}, nil)

	// Fail ONE entry (the matcher fires on the "env" key); the rest succeed.
	mockClient.On("PutSchemaVersionMetadata", mock.Anything, mock.MatchedBy(func(in *glue.PutSchemaVersionMetadataInput) bool {
		return in != nil && in.MetadataKeyValue != nil && in.MetadataKeyValue.MetadataKey != nil && *in.MetadataKeyValue.MetadataKey == "env"
	})).Return((*glue.PutSchemaVersionMetadataOutput)(nil), errors.New("simulated put failure"))
	mockClient.On("PutSchemaVersionMetadata", mock.Anything, mock.Anything).
		Return(&glue.PutSchemaVersionMetadataOutput{}, nil)

	gotID, _, err := enc.getSchemaVersionIdByDefinition("def-pf", "schema-pf", "JSON", "kafka-topic")
	require.NoError(t, err, "INV-6: partial metadata-write failure MUST NOT fail the encode path")
	require.Equal(t, otherUUIDString, gotID)

	require.Equal(t, 3, mockClient.MetadataCallCount(), "all three entries are still attempted (the loop does not abort on error)")
}

// TestEncoder_Metadata_NoMetadataNoTransport_StillFiresTransportCall pins
// the C-14 contract from a different angle: with both an empty
// Config.Metadata map AND an empty transportName, the helper STILL fires
// exactly one PutSchemaVersionMetadata call for the empty-value transport
// entry. This is the AC-9b case viewed as a "minimal merged map" assertion.
func TestEncoder_Metadata_NoMetadataNoTransport_StillFiresTransportCall(t *testing.T) {
	enc, mockClient := newEncoderWithMockAndMetadata(t, true, nil)

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{SchemaVersionId: aws.String(otherUUIDString)}, nil)
	mockClient.On("PutSchemaVersionMetadata", mock.Anything, mock.Anything).
		Return(&glue.PutSchemaVersionMetadataOutput{}, nil)

	_, _, err := enc.getSchemaVersionIdByDefinition("def-min", "schema-min", "JSON", "")
	require.NoError(t, err)

	require.Equal(t, 1, mockClient.MetadataCallCount())
	require.ElementsMatch(t, []MetadataPair{
		{Key: TransportMetadataKey, Value: ""},
	}, metadataPairsAsSet(mockClient.MetadataPairs()))
}
