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

func TestDeserializer_Decode_Success(t *testing.T) {
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

	data := createValidGSRData(t, testUUIDString, []byte("test-payload"))

	result, err := deserializer.Decode(data)

	require.NoError(t, err)
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

	schemaDefinition := `syntax = "proto3"; message Test { string name = 1; }`
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaVersionOutput{
			SchemaDefinition: &schemaDefinition,
			DataFormat:       types.DataFormatProtobuf,
		}, nil)

	// Wire payload is <varint(message_index) || protobuf bytes>. Single
	// top-level message → index 0 → single 0x00 byte prefix.
	wirePayload := append([]byte{0x00}, []byte("test-payload")...)
	data := createValidGSRData(t, testUUIDString, wirePayload)

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

	originalPayload := []byte("test-payload-for-compression-round-trip")
	compressed, err := ZlibCompressionHandler{}.Compress(originalPayload)
	require.NoError(t, err)

	data, err := EncodeWireFormat(testUUIDString, CompressionByteZlib, compressed)
	require.NoError(t, err)

	result, err := deserializer.Decode(data)

	require.NoError(t, err)
	assert.Equal(t, originalPayload, result)
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
	schemaArn := "arn:aws:glue:us-east-1:123456789012:schema/test-registry/test-schema"
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		&glue.GetSchemaVersionOutput{
			SchemaDefinition: &schemaDefinition,
			DataFormat:       types.DataFormatJson,
			SchemaArn:        &schemaArn,
		}, nil)

	data := createValidGSRData(t, testUUIDString, []byte("test-payload"))

	schema, err := deserializer.DecodeSchema(data)

	require.NoError(t, err)
	assert.Equal(t, "test-schema", schema.SchemaName)
	assert.Equal(t, "test-definition", schema.SchemaDefinition)
	assert.Equal(t, "JSON", schema.DataFormat)
	assert.Equal(t, testUUIDString, schema.SchemaVersionID)
}

func TestDeserializer_GetSchemaByVersionID_Cached(t *testing.T) {
	cache, _ := NewCache(300000)
	deserializer := &GsrDecoder{schemaCache: cache}

	expectedSchema := &Schema{
		SchemaName:       "test",
		SchemaDefinition: "def",
		DataFormat:       "JSON",
		SchemaVersionID:  testUUIDString,
	}
	cache.Set(testUUIDString, expectedSchema)

	schema, err := deserializer.getSchemaByVersionID(testUUIDString)

	require.NoError(t, err)
	assert.Equal(t, expectedSchema, schema)
}

func TestDeserializer_GetSchemaByVersionID_Error(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		nil, errors.New("schema not found"))

	_, err := deserializer.getSchemaByVersionID(testUUIDString)

	require.Error(t, err)
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
		{"invalid:arn:format", "invalid:arn:format"},
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

func TestDeserializer_Decode_BadCompressionByteIsIncompatibleData(t *testing.T) {
	deserializer := &GsrDecoder{}

	// 18 bytes: valid version, invalid compression byte (0x02 not in {0,5}).
	bad := make([]byte, WireFormatHeaderSize+1)
	bad[0] = WireFormatVersionByte
	bad[1] = 0x02
	_, err := deserializer.Decode(bad)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleData))
}

// createValidGSRData builds a valid 18-byte-prefixed wire-format payload for
// tests. uuid must be a parseable UUID string; payload is appended verbatim
// (no compression). Helper kept package-level so multiple tests can share it.
func createValidGSRData(t *testing.T, schemaVersionID string, payload []byte) []byte {
	t.Helper()
	out, err := EncodeWireFormat(schemaVersionID, CompressionByteNone, payload)
	require.NoError(t, err)
	return out
}
