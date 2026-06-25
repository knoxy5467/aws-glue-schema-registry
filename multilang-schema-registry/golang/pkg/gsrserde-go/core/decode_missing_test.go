package gsrserde

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeserializer_Decode_TruncatedPrefixIsIncompatibleData(t *testing.T) {
	deserializer := &GsrDecoder{}

	// 6 bytes — passes the old "looks like header" check that used to live
	// in CanDecodeData, but is one shy of the 18-byte wire-format header.
	short := []byte{WireFormatVersionByte, CompressionByteNone, 0x00, 0x00, 0x00, 0x10}

	_, err := deserializer.Decode(short)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleData),
		"truncated wire-format prefix must surface as ErrIncompatibleData")
}
