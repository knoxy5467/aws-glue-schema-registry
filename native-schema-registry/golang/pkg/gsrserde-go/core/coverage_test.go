package gsrserde

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewGsrDecoder_Coverage(t *testing.T) {
	decoder, err := NewGsrDecoder(map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, decoder)
	decoder.Close()
}

func TestNewGsrEncoder_Coverage(t *testing.T) {
	encoder, err := NewGsrEncoder(map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, encoder)
	encoder.Close()
}

func TestGsrDecoder_Decode_Coverage(t *testing.T) {
	cache, _ := NewCache(300000)
	decoder := &GsrDecoder{
		registryName: "test-registry",
		schemaCache:  cache,
	}

	// Test with invalid data
	_, err := decoder.Decode([]byte{0x01, 0x02})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid GSR format")
}

func TestGsrDecoder_parseGSRData_Coverage(t *testing.T) {
	decoder := &GsrDecoder{}

	// Test with invalid data (too short)
	_, _, err := decoder.parseGSRData([]byte{0x03})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data too short")

	// Test with wrong magic byte
	_, _, err = decoder.parseGSRData([]byte{0x02, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x00, 0x00, 0x00, 0x01})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read schema version")
}

func TestGsrDecoder_Close_Coverage(t *testing.T) {
	cache, _ := NewCache(300000)
	decoder := &GsrDecoder{
		schemaCache: cache,
	}

	err := decoder.Close()
	assert.NoError(t, err)

	// Test closing already closed decoder
	err = decoder.Close()
	assert.NoError(t, err)
}

func TestGsrEncoder_Close_Coverage(t *testing.T) {
	cache, _ := NewCache(300000)
	encoder := &GsrEncoder{
		schemaCache: cache,
	}

	err := encoder.Close()
	assert.NoError(t, err)

	// Test closing already closed encoder
	err = encoder.Close()
	assert.NoError(t, err)
}
