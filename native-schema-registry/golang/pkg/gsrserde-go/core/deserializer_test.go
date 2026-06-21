package gsrserde

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDeserializer_Decode_Success(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}
	
	// Mock schema retrieval
	schemaDefinition := "test-definition"
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaVersionOutput{
			SchemaDefinition: &schemaDefinition,
			DataFormat:       types.DataFormatJson,
		}, nil)
	
	// Create valid GSR data
	data := createValidGSRData(t, "test-schema", 1, []byte("test-payload"))
	
	result, err := deserializer.Decode(data)
	
	assert.NoError(t, err)
	assert.Equal(t, []byte("test-payload"), result)
}

func TestDeserializer_Decode_ProtobufFormat(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}
	
	schemaDefinition := "syntax = \"proto3\"; message Test { string name = 1; }"
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaVersionOutput{
			SchemaDefinition: &schemaDefinition,
			DataFormat:       types.DataFormatProtobuf,
		}, nil)
	
	// Wire payload is <varint(message_index) || protobuf bytes>. For a single
	// top-level message the index is 0, so the varint is a single 0x00 byte
	// and the encoded payload bytes follow unchanged. Decode must consume
	// exactly that one byte and return the rest.
	wirePayload := append([]byte{0x00}, []byte("test-payload")...)
	data := createValidGSRData(t, "test-schema", 1, wirePayload)

	result, err := deserializer.Decode(data)

	require.NoError(t, err)
	assert.Equal(t, []byte("test-payload"), result)
}

func TestDeserializer_Decode_WithCompression(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}
	
	schemaDefinition := "test-definition"
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaVersionOutput{
			SchemaDefinition: &schemaDefinition,
			DataFormat:       types.DataFormatJson,
		}, nil)
	
	// Create compressed data
	originalPayload := []byte("test-payload")
	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)
	writer.Write(originalPayload)
	writer.Close()
	compressedPayload := buf.Bytes()
	
	data := createValidGSRDataWithCompression(t, "test-schema", 1, compressedPayload)
	
	result, err := deserializer.Decode(data)
	
	assert.NoError(t, err)
	assert.Equal(t, compressedPayload, result) // No decompression since we used 0x00
}

func TestDeserializer_DecodeSchema_Success(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}
	
	schemaDefinition := "test-definition"
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaVersionOutput{
			SchemaDefinition: &schemaDefinition,
			DataFormat:       types.DataFormatJson,
		}, nil)
	
	data := createValidGSRData(t, "test-schema", 1, []byte("test-payload"))
	
	schema, err := deserializer.DecodeSchema(data)
	
	assert.NoError(t, err)
	assert.Equal(t, "test-schema", schema.SchemaName)
	assert.Equal(t, "test-definition", schema.SchemaDefinition)
	assert.Equal(t, "JSON", schema.DataFormat)
}

func TestDeserializer_GetSchema_Cached(t *testing.T) {
	cache, _ := NewCache(300000)
	deserializer := &GsrDecoder{schemaCache: cache}
	
	// Pre-populate cache
	expectedSchema := &Schema{SchemaName: "test", SchemaDefinition: "def", DataFormat: "JSON"}
	cache.Set("test-schema:1", expectedSchema)
	
	schema, err := deserializer.getSchema("test-schema", 1)
	
	assert.NoError(t, err)
	assert.Equal(t, expectedSchema, schema)
}

func TestDeserializer_GetSchema_Error(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}
	
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		nil, errors.New("schema not found"))
	
	_, err := deserializer.getSchema("test-schema", 1)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get schema")
}

func TestDeserializer_ExtractSchemaName(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	tests := []struct {
		input    string
		expected string
	}{
		{"simple-name", "simple-name"},
		{"arn:aws:glue:us-east-1:123456789:schema/registry/schema-name", "schema-name"},
		{"arn:aws:glue:us-west-2:987654321:schema/my-registry/my-schema", "my-schema"},
		{"invalid:arn:format", "invalid:arn:format"}, // Simple name case
	}
	
	for _, test := range tests {
		result := deserializer.extractSchemaName(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestDeserializer_ExtractSchemaNameFromArn(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"arn:aws:glue:us-east-1:123456789:schema/registry/schema-name", "schema-name"},
		{"arn:aws:glue:us-west-2:987654321:schema/my-registry/my-schema", "my-schema"},
		{"invalid-arn", ""},
		{"arn:aws:glue:us-east-1:123456789:schema/registry", ""},
	}
	
	for _, test := range tests {
		result := extractSchemaNameFromArn(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestDeserializer_ParseGSRData_CompressionError(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create data with invalid compression
	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(0x01) // ZLIB compression
	binary.Write(&buf, binary.BigEndian, uint32(4))
	buf.WriteString("test")
	binary.Write(&buf, binary.BigEndian, uint32(1))
	buf.Write([]byte("invalid-zlib-data"))
	
	_, _, err := deserializer.parseGSRData(buf.Bytes())
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create zlib reader")
}

// Helper functions
func createValidGSRData(t *testing.T, schemaID string, version uint32, payload []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(0x00) // No compression
	binary.Write(&buf, binary.BigEndian, uint32(len(schemaID)))
	buf.WriteString(schemaID)
	binary.Write(&buf, binary.BigEndian, version)
	buf.Write(payload)
	return buf.Bytes()
}

func createValidGSRDataWithCompression(t *testing.T, schemaID string, version uint32, compressedPayload []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(0x00) // Change to no compression since CanDecodeData only accepts 0x00
	binary.Write(&buf, binary.BigEndian, uint32(len(schemaID)))
	buf.WriteString(schemaID)
	binary.Write(&buf, binary.BigEndian, version)
	buf.Write(compressedPayload)
	return buf.Bytes()
}
