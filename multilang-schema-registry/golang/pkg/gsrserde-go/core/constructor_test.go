package gsrserde

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewGsrDecoder_Success(t *testing.T) {
	decoder, err := NewGsrDecoder(map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, decoder)
	decoder.Close()
}

func TestNewGsrEncoder_Success(t *testing.T) {
	encoder, err := NewGsrEncoder(map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, encoder)
	encoder.Close()
}
