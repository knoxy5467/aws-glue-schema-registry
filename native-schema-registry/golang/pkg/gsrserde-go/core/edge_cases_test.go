package gsrserde

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		nil, errors.New("schema not found"))

	// Java AWSSchemaRegistryClient.java:251 — CreateSchema returns the schema
	// version UUID; the version *number* is a separate field. The Go code
	// must return the UUID on the auto-register path, not the schema name.
	createdSchemaVersionID := "created-schema-version-uuid"
	latestVersion := int64(2)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).Return(
		&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdSchemaVersionID,
			LatestSchemaVersion: &latestVersion,
		}, nil)

	schemaID, version, err := serializer.getSchemaVersionIdByDefinition("test-definition", "test-schema", "JSON")

	assert.NoError(t, err)
	assert.Equal(t, createdSchemaVersionID, schemaID)
	assert.Equal(t, uint32(2), version)

	// Cached entry must carry the UUID so the cached path returns it on the
	// next call (Plan §2.2 divergence (b)).
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

	schemaVersionId := "test-schema-version-id"
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &schemaVersionId,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)

	schemaID, version, err := serializer.getSchemaVersionIdByDefinition("test-definition", "test-schema", "JSON")

	assert.NoError(t, err)
	// The first return value is the Glue schema-version UUID, which is the
	// value the GSR wire-format header carries (16-byte UUID after version +
	// compression bytes). Before §2.2(b) fix this asserted "test-schema",
	// which was provably wrong against the spec.
	assert.Equal(t, schemaVersionId, schemaID)
	assert.Equal(t, uint32(1), version)

	// And the cache must carry the same UUID so subsequent cache hits return it.
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
	}

	// Existing version found but not Available → encoder must fall through to
	// CreateSchema. The CreateSchema response UUID is what bubbles up.
	existingVersionID := "stale-pending-uuid"
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &existingVersionID,
			Status:          types.SchemaVersionStatusPending,
		}, nil)

	createdSchemaVersionID := "freshly-created-schema-version-uuid"
	latestVersion := int64(1)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).Return(
		&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdSchemaVersionID,
			LatestSchemaVersion: &latestVersion,
		}, nil)

	schemaID, version, err := serializer.getSchemaVersionIdByDefinition("test-definition", "test-schema", "JSON")

	assert.NoError(t, err)
	assert.Equal(t, createdSchemaVersionID, schemaID)
	assert.Equal(t, uint32(1), version)
}

func TestDeserializer_ParseGSRData_InvalidCompressionByte(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create data with invalid compression byte
	data := []byte{HeaderVersionByte, 0x02, 0x00, 0x00, 0x00, 0x04, 't', 'e', 's', 't', 0x00, 0x00, 0x00, 0x01, 'p', 'a', 'y', 'l', 'o', 'a', 'd'}
	
	_, _, err := deserializer.parseGSRData(data)
	
	// Should not error for unknown compression byte, just treat as uncompressed
	assert.NoError(t, err)
}

func TestDeserializer_ParseGSRData_ReadErrors(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	tests := []struct {
		name string
		data []byte
		expectedError string
	}{
		{
			name: "Cannot read schema ID length",
			data: []byte{HeaderVersionByte, 0x00, 0x00},
			expectedError: "data too short",
		},
		{
			name: "Cannot read schema ID",
			data: []byte{HeaderVersionByte, 0x00, 0x00, 0x00, 0x00, 0x10},
			expectedError: "failed to read schema ID",
		},
		{
			name: "Cannot read schema version",
			data: []byte{HeaderVersionByte, 0x00, 0x00, 0x00, 0x00, 0x04, 't', 'e', 's', 't'},
			expectedError: "failed to read schema version",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := deserializer.parseGSRData(tt.data)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestDeserializer_CanDecodeData_EdgeCases(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "Empty data",
			data:     []byte{},
			expected: false,
		},
		{
			name:     "Too short data",
			data:     []byte{0x03, 0x00, 0x01},
			expected: false,
		},
		{
			name:     "Wrong header version",
			data:     []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x04},
			expected: false,
		},
		{
			name:     "Wrong compression byte",
			data:     []byte{0x03, 0x02, 0x00, 0x00, 0x00, 0x04},
			expected: false,
		},
		{
			name:     "Valid format",
			data:     []byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x04},
			expected: true,
		},
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
	
	validData := []byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x04}
	canDecode, err := deserializer.CanDecode(validData)
	
	assert.NoError(t, err)
	assert.True(t, canDecode)
}

func TestCache_EdgeCases(t *testing.T) {
	cache, err := NewCache(100) // Very small TTL
	assert.NoError(t, err)
	
	// Test setting and getting
	cache.Set("key1", "value1")
	value, exists := cache.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "value1", value)
	
	// Test overwriting
	cache.Set("key1", "new-value")
	value, exists = cache.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "new-value", value)
	
	// Test non-existent key
	_, exists = cache.Get("non-existent")
	assert.False(t, exists)
	
	// Test close
	cache.Close()
}
