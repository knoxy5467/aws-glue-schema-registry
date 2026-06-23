package gsrserde

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSerializer_Encode_NilData(t *testing.T) {
	cache, _ := NewCache(300000)
	serializer := &GsrEncoder{schemaCache: cache}
	
	schema := &Schema{SchemaDefinition: "test", DataFormat: "JSON", SchemaName: "test"}
	_, err := serializer.Encode(nil, "test", schema)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data cannot be nil")
}

func TestSerializer_Encode_NilSchema(t *testing.T) {
	cache, _ := NewCache(300000)
	serializer := &GsrEncoder{schemaCache: cache}
	
	_, err := serializer.Encode([]byte("test"), "test", nil)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "schema cannot be nil")
}

func TestSerializer_Encode_Success(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	serializer := &GsrEncoder{
		client:          mockClient,
		registryName:    "test-registry",
		schemaCache:     cache,
		compressionType: "NONE",
	}
	
	// Mock successful schema retrieval
	schemaVersionId := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &schemaVersionId,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)
	
	schema := &Schema{
		SchemaDefinition: "test-definition",
		DataFormat:       "JSON",
		SchemaName:       "test-schema",
	}
	
	result, err := serializer.Encode([]byte("test-data"), "test-transport", schema)
	
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, byte(WireFormatVersionByte), result[0])
	assert.Equal(t, byte(CompressionByteNone), result[1])
}

func TestSerializer_Encode_WithZlibCompression(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	serializer := &GsrEncoder{
		client:          mockClient,
		registryName:    "test-registry",
		schemaCache:     cache,
		compressionType: "ZLIB",
	}
	
	schemaVersionId := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &schemaVersionId,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)
	
	schema := &Schema{
		SchemaDefinition: "test-definition",
		DataFormat:       "JSON",
		SchemaName:       "test-schema",
	}
	
	result, err := serializer.Encode([]byte("test-data"), "test-transport", schema)
	
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, byte(WireFormatVersionByte), result[0])
	assert.Equal(t, byte(CompressionByteZlib), result[1])
}

func TestSerializer_Encode_ProtobufFormat(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	serializer := &GsrEncoder{
		client:          mockClient,
		registryName:    "test-registry",
		schemaCache:     cache,
		compressionType: "NONE",
	}
	
	schemaVersionId := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &schemaVersionId,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)
	
	// AdditionalInfo carries the proto fully-qualified message name; the
	// encoder uses it (NOT SchemaName) to look up the message index in
	// the schema's BFS+lex-sorted descriptor list. Mismatch returns
	// wrapped ErrMessageTypeNotFound; see protobuf_utils.go and §2.2 of
	// the plan.
	schema := &Schema{
		SchemaDefinition: `syntax = "proto3"; message Test { string name = 1; }`,
		DataFormat:       "PROTOBUF",
		SchemaName:       "Test",
		AdditionalInfo:   "Test",
	}

	result, err := serializer.Encode([]byte("test-data"), "test-transport", schema)

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestSerializer_CreateSchema_Success(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	serializer := &GsrEncoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
		tags:         map[string]string{"env": "test"},
		description:  "test description",
	}
	
	// Mock schema not found, then successful creation
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		nil, errors.New("schema not found"))

	latestVersion := int64(1)
	createdSchemaVersionID := otherUUIDString
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).Return(
		&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdSchemaVersionID,
			LatestSchemaVersion: &latestVersion,
		}, nil)

	versionID, version, err := serializer.createSchema("test-schema", "JSON", "test-definition")

	assert.NoError(t, err)
	// createSchema must return the UUID from CreateSchemaOutput.SchemaVersionId
	// (Java AWSSchemaRegistryClient.java:242 returns UUID, not the version
	// number) — the wire-format header carries the UUID.
	assert.Equal(t, createdSchemaVersionID, versionID)
	assert.Equal(t, uint32(1), version)
}

func TestSerializer_CreateSchema_Error(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	serializer := &GsrEncoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}
	
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).Return(
		nil, errors.New("creation failed"))

	_, _, err := serializer.createSchema("test-schema", "JSON", "test-definition")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create schema")
}

func TestSerializer_GetSchemaVersionIdByDefinition_Cached(t *testing.T) {
	cache, _ := NewCache(300000)
	serializer := &GsrEncoder{schemaCache: cache}

	// Pre-populate cache with a fully-formed entry (including the
	// SchemaVersionID UUID that the cached path must echo back). Before the
	// §2.2(b) fix this test asserted the schema *name* came out of the cache,
	// which was a stub-tracking lie (encoder.go:125 was returning
	// schema.SchemaName).
	cachedVersionID := testUUIDString
	schema := &Schema{
		SchemaName:       "test-schema",
		SchemaDefinition: "test",
		DataFormat:       "JSON",
		SchemaVersionID:  cachedVersionID,
	}
	cache.Set("test-schema:JSON", schema)

	schemaID, version, err := serializer.getSchemaVersionIdByDefinition("test", "test-schema", "JSON")

	assert.NoError(t, err)
	assert.Equal(t, cachedVersionID, schemaID)
	assert.Equal(t, uint32(1), version)
}
