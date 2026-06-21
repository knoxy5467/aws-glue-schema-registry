package gsrserde

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDeserializer_Decode_GetSchemaError(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		nil, errors.New("schema not found"))

	data := createValidGSRData(t, testUUIDString, []byte("test-payload"))

	_, err := deserializer.Decode(data)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get schema")
}

func TestDeserializer_DecodeSchema_GetSchemaError(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		nil, errors.New("schema not found"))

	data := createValidGSRData(t, testUUIDString, []byte("test-payload"))

	_, err := deserializer.DecodeSchema(data)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get schema")
}

func TestDeserializer_DecodeSchema_TruncatedPrefixIsIncompatibleData(t *testing.T) {
	deserializer := &GsrDecoder{}

	// 17 bytes — one short of the 18-byte wire-format header.
	short := make([]byte, WireFormatHeaderSize-1)
	short[0] = WireFormatVersionByte
	_, err := deserializer.DecodeSchema(short)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleData))
}

func TestDeserializer_Decode_ZlibPayloadRoundTrips(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	// Pre-populate the cache so the decode path stays inside the test
	// harness — Decode → decompress → cache hit → no Glue call.
	cache.Set(testUUIDString, &Schema{
		SchemaName:       "test-schema",
		SchemaDefinition: "test-definition",
		DataFormat:       "JSON",
		SchemaVersionID:  testUUIDString,
	})

	original := []byte("payload that gets zlib-compressed and round-tripped")
	compressed, err := ZlibCompressionHandler{}.Compress(original)
	require.NoError(t, err)

	wire, err := EncodeWireFormat(testUUIDString, CompressionByteZlib, compressed)
	require.NoError(t, err)

	got, err := deserializer.Decode(wire)
	require.NoError(t, err)
	assert.Equal(t, original, got)
}

func TestDeserializer_Decode_ZlibCorruptedPayloadErrors(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)

	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}

	// Wire format says zlib, payload is garbage.
	wire, err := EncodeWireFormat(testUUIDString, CompressionByteZlib, []byte("not-zlib-data"))
	require.NoError(t, err)

	_, err = deserializer.Decode(wire)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decompress")
}
