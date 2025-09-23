package gsrserde

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSerializer_Encode_GetSchemaError(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	serializer := &GsrEncoder{
		client:          mockClient,
		registryName:    "test-registry",
		schemaCache:     cache,
		compressionType: "NONE",
	}
	
	// Mock schema retrieval failure
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(
		nil, errors.New("schema not found"))
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).Return(
		nil, errors.New("create failed"))
	
	schema := &Schema{
		SchemaDefinition: "test-definition",
		DataFormat:       "JSON",
		SchemaName:       "test-schema",
	}
	
	_, err := serializer.Encode([]byte("test-data"), "test-transport", schema)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get schema")
}
