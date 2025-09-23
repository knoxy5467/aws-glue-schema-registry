package gsrserde

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeserializer_extractSchemaName_Coverage(t *testing.T) {
	deserializer := &GsrDecoder{registryName: "test-registry"}

	tests := []struct {
		name     string
		schemaID string
		expected string
	}{
		{
			name:     "ARN format",
			schemaID: "arn:aws:glue:us-east-1:123456789012:schema/test-registry/test-schema",
			expected: "test-schema",
		},
		{
			name:     "Simple name",
			schemaID: "simple-schema",
			expected: "simple-schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deserializer.extractSchemaName(tt.schemaID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertBase64SchemaToStringSchema_Coverage(t *testing.T) {
	// Test with invalid base64
	_, err := ConvertBase64SchemaToStringSchema("invalid-base64!")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode base64 schema")

	// Test with valid base64 but invalid protobuf
	_, err = ConvertBase64SchemaToStringSchema("aGVsbG8=") // "hello" in base64
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal FileDescriptorProto")
}

func TestDeserializer_DecodeSchema_InvalidFormat_Coverage(t *testing.T) {
	cache, _ := NewCache(300000)
	deserializer := &GsrDecoder{
		registryName: "test-registry",
		schemaCache:  cache,
	}

	// Test with invalid data (too short)
	testData := []byte{0x03, 0x00}
	
	_, err := deserializer.DecodeSchema(testData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid GSR format")
}
