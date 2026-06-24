package gsrserde

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A second known UUID, used by tests that need to distinguish a Glue-returned
// UUID from the canonical testUUIDString.
const otherUUIDString = "00112233-4455-6677-8899-aabbccddeeff"

func TestSerializer_GetSchemaVersionIdByDefinition_CreateSchemaPath(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	serializer := &GsrEncoder{
		client:                        mockClient,
		registryName:                  "test-registry",
		schemaCache:                   cache,
		schemaAutoRegistrationEnabled: true,
		tags:                          map[string]string{"env": "test"},
		description:                   "test description",
		compatibility:                 "BACKWARD",
	}

	// Phase 4.5 bug 2: the encoder only falls through to CreateSchema
	// on EntityNotFoundException — an untyped errors.New("…") would
	// (correctly) propagate as a real error rather than auto-register.
	// Use the typed not-found error to drive the auto-register path
	// this test is asserting.
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		nil, newEntityNotFoundError())

	createdSchemaVersionID := otherUUIDString
	latestVersion := int64(2)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).Return(
		&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdSchemaVersionID,
			LatestSchemaVersion: &latestVersion,
		}, nil)

	schemaID, version, err := serializer.getSchemaVersionIdByDefinition("test-definition", "test-schema", "JSON", "")

	require.NoError(t, err)
	assert.Equal(t, createdSchemaVersionID, schemaID)
	assert.Equal(t, uint32(2), version)

	cached, exists := cache.Get("test-schema:JSON")
	require.True(t, exists)
	require.NotNil(t, cached)
	assert.Equal(t, createdSchemaVersionID, cached.(*Schema).SchemaVersionID)
}

// Plan §2.2 divergence (b) — live path: GetSchemaByDefinition returns the
// schema-version UUID; the cached return must match the live return. Before
// the fix, the cached path returned schema.SchemaName instead.
func TestSerializer_GetSchemaVersionIdByDefinition_GetSchemaSuccess(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	serializer := &GsrEncoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	schemaVersionId := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &schemaVersionId,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)

	schemaID, version, err := serializer.getSchemaVersionIdByDefinition("test-definition", "test-schema", "JSON", "")

	require.NoError(t, err)
	assert.Equal(t, schemaVersionId, schemaID)
	assert.Equal(t, uint32(1), version)

	cached, exists := cache.Get("test-schema:JSON")
	require.True(t, exists)
	assert.Equal(t, schemaVersionId, cached.(*Schema).SchemaVersionID)
}

func TestSerializer_GetSchemaVersionIdByDefinition_GetSchemaUnavailable(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	serializer := &GsrEncoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
		// Phase 4.5 bug 1: the encoder now honors auto-register; this
		// test exercises the "GetSchemaByDefinition returns non-Available
		// status → fall through to CreateSchema" branch, which requires
		// auto-register to be enabled.
		schemaAutoRegistrationEnabled: true,
	}

	existingVersionID := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &existingVersionID,
			Status:          types.SchemaVersionStatusPending,
		}, nil)

	createdSchemaVersionID := otherUUIDString
	latestVersion := int64(1)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).Return(
		&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdSchemaVersionID,
			LatestSchemaVersion: &latestVersion,
		}, nil)

	schemaID, version, err := serializer.getSchemaVersionIdByDefinition("test-definition", "test-schema", "JSON", "")

	require.NoError(t, err)
	assert.Equal(t, createdSchemaVersionID, schemaID)
	assert.Equal(t, uint32(1), version)
}

func TestDeserializer_CanDecodeData_EdgeCases(t *testing.T) {
	deserializer := &GsrDecoder{}

	// Pad short buffers up to 18 bytes so they pass the size check; the
	// version/compression-byte check is the differentiator. Tests that hit
	// the size-check branch live with their own buffers.
	pad := func(prefix []byte) []byte {
		out := make([]byte, WireFormatHeaderSize)
		copy(out, prefix)
		return out
	}

	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{"Empty data", []byte{}, false},
		{"Too short data (1 byte)", []byte{0x03}, false},
		{"Too short data (17 bytes — one shy)", make([]byte, WireFormatHeaderSize-1), false},
		{"Wrong header version 0x02", pad([]byte{0x02}), false},
		{"Wrong header version 0x00", pad([]byte{0x00}), false},
		{"Unknown compression byte 0x02", pad([]byte{0x03, 0x02}), false},
		{"Unknown compression byte 0x01", pad([]byte{0x03, 0x01}), false},
		{"Valid: version 0x03 + compression 0x00", pad([]byte{0x03, 0x00}), true},
		{"Valid: version 0x03 + compression 0x05", pad([]byte{0x03, 0x05}), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deserializer.CanDecodeData(tt.data)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeserializer_CanDecode_Wrapper(t *testing.T) {
	deserializer := &GsrDecoder{}

	valid := make([]byte, WireFormatHeaderSize)
	valid[0] = WireFormatVersionByte
	valid[1] = CompressionByteNone
	canDecode, err := deserializer.CanDecode(valid)

	require.NoError(t, err)
	assert.True(t, canDecode)
}

func TestCache_EdgeCases(t *testing.T) {
	cache, err := NewCache(100)
	require.NoError(t, err)

	cache.Set("key1", "value1")
	value, exists := cache.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "value1", value)

	cache.Set("key1", "new-value")
	value, exists = cache.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "new-value", value)

	_, exists = cache.Get("non-existent")
	assert.False(t, exists)

	cache.Close()
}
