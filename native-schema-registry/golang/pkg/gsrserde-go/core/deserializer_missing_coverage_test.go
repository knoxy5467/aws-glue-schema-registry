package gsrserde

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDeserializer_Decode_GetSchemaError(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, _ := NewCache(300000)
	
	deserializer := &GsrDecoder{
		client:       mockClient,
		registryName: "test-registry",
		schemaCache:  cache,
	}
	
	// Mock schema retrieval error
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		nil, errors.New("schema not found"))
	
	data := createValidGSRData(t, "test-schema", 1, []byte("test-payload"))
	
	_, err := deserializer.Decode(data)
	
	assert.Error(t, err)
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
	
	// Mock schema retrieval error
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).Return(
		nil, errors.New("schema not found"))
	
	data := createValidGSRData(t, "test-schema", 1, []byte("test-payload"))
	
	_, err := deserializer.DecodeSchema(data)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get schema")
}

func TestDeserializer_DecodeSchema_ParseError(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create data that passes CanDecodeData but fails in parseGSRData
	// Need exactly 6 bytes to pass CanDecodeData but fail schema ID length read
	data := []byte{HeaderVersionByte, 0x00, 0x00, 0x00, 0x00, 0x10} // Schema ID length = 16 but no data
	
	_, err := deserializer.DecodeSchema(data)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read schema ID")
}

func TestDeserializer_ParseGSRData_ZlibDecompressionError(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create data with valid zlib header but invalid compressed data
	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(0x01) // ZLIB compression
	binary.Write(&buf, binary.BigEndian, uint32(4))
	buf.WriteString("test")
	binary.Write(&buf, binary.BigEndian, uint32(1))
	
	// Create valid zlib header but truncated data
	var zlibBuf bytes.Buffer
	writer := zlib.NewWriter(&zlibBuf)
	writer.Write([]byte("test-payload"))
	writer.Close()
	
	// Truncate the zlib data to cause decompression error
	truncatedZlib := zlibBuf.Bytes()[:len(zlibBuf.Bytes())-5]
	buf.Write(truncatedZlib)
	
	_, _, err := deserializer.parseGSRData(buf.Bytes())
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decompress data")
}

func TestDeserializer_ParseGSRData_ValidZlibDecompression(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create properly compressed data
	originalPayload := []byte("test-payload-for-compression")
	var zlibBuf bytes.Buffer
	writer := zlib.NewWriter(&zlibBuf)
	writer.Write(originalPayload)
	writer.Close()
	compressedData := zlibBuf.Bytes()
	
	// Create valid GSR data with compression
	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(0x01) // ZLIB compression
	schemaID := "test-schema"
	binary.Write(&buf, binary.BigEndian, uint32(len(schemaID)))
	buf.WriteString(schemaID)
	binary.Write(&buf, binary.BigEndian, uint32(1))
	buf.Write(compressedData)
	
	schemaInfo, payload, err := deserializer.parseGSRData(buf.Bytes())
	
	assert.NoError(t, err)
	assert.Equal(t, "test-schema", schemaInfo.SchemaID)
	assert.Equal(t, uint32(1), schemaInfo.SchemaVersion)
	assert.Equal(t, originalPayload, payload)
}

func TestDeserializer_ParseGSRData_InvalidZlibReader(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create data with compression flag but invalid zlib data
	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(0x01) // ZLIB compression
	schemaID := "test-schema"
	binary.Write(&buf, binary.BigEndian, uint32(len(schemaID)))
	buf.WriteString(schemaID)
	binary.Write(&buf, binary.BigEndian, uint32(1))
	buf.Write([]byte("not-zlib-data"))
	
	_, _, err := deserializer.parseGSRData(buf.Bytes())
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create zlib reader")
}

func TestDeserializer_ParseGSRData_ZlibReadAllError(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create GSR data with compression flag but data that will fail ReadAll
	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(0x01) // ZLIB compression
	schemaID := "test-schema"
	binary.Write(&buf, binary.BigEndian, uint32(len(schemaID)))
	buf.WriteString(schemaID)
	binary.Write(&buf, binary.BigEndian, uint32(1))
	
	// Add zlib header but truncated/corrupted data
	zlibHeader := []byte{0x78, 0x9c} // Valid zlib header
	buf.Write(zlibHeader)
	buf.Write([]byte{0xFF, 0xFF, 0xFF}) // Corrupted data
	
	_, _, err := deserializer.parseGSRData(buf.Bytes())
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decompress data")
}
