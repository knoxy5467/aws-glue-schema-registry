package gsrserde

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockGlueClient for testing
type MockGlueClient struct {
	mock.Mock
}

func (m *MockGlueClient) GetSchemaByDefinition(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaByDefinitionOutput), args.Error(1)
}

func (m *MockGlueClient) CreateSchema(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.CreateSchemaOutput), args.Error(1)
}

func (m *MockGlueClient) GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaVersionOutput), args.Error(1)
}

func (m *MockGlueClient) RegisterSchemaVersion(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.RegisterSchemaVersionOutput), args.Error(1)
}

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
	schemaVersionId := "test-schema-id"
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
	assert.Equal(t, byte(HeaderVersionByte), result[0])
	assert.Equal(t, byte(0x00), result[1]) // No compression
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
	
	schemaVersionId := "test-schema-id"
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
	assert.Equal(t, byte(HeaderVersionByte), result[0])
	assert.Equal(t, byte(0x01), result[1]) // ZLIB compression
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
	
	schemaVersionId := "test-schema-id"
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &schemaVersionId,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)
	
	schema := &Schema{
		SchemaDefinition: "syntax = \"proto3\"; message Test { string name = 1; }",
		DataFormat:       "PROTOBUF",
		SchemaName:       "test-schema",
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
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).Return(
		&glue.CreateSchemaOutput{
			LatestSchemaVersion: &latestVersion,
		}, nil)
	
	version, err := serializer.createSchema("test-schema", "JSON", "test-definition")
	
	assert.NoError(t, err)
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
	
	_, err := serializer.createSchema("test-schema", "JSON", "test-definition")
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create schema")
}

func TestSerializer_GetSchemaVersionIdByDefinition_Cached(t *testing.T) {
	cache, _ := NewCache(300000)
	serializer := &GsrEncoder{schemaCache: cache}
	
	// Pre-populate cache
	schema := &Schema{SchemaName: "test-schema", SchemaDefinition: "test", DataFormat: "JSON"}
	cache.Set("test-schema:JSON", schema)
	
	schemaID, version, err := serializer.getSchemaVersionIdByDefinition("test", "test-schema", "JSON")
	
	assert.NoError(t, err)
	assert.Equal(t, "test-schema", schemaID)
	assert.Equal(t, uint32(1), version)
}
