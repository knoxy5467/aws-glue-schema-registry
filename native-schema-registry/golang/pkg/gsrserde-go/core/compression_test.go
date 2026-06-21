package gsrserde

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressionFactory_HandlerForType(t *testing.T) {
	cf := CompressionFactory{}

	cases := []struct {
		in       CompressionType
		wantNil  bool
		wantByte byte
	}{
		{"", true, 0},
		{"NONE", true, 0},
		{"none", true, 0},
		{"ZLIB", false, 0x05},
		{"zlib", false, 0x05},
	}
	for _, c := range cases {
		h, err := cf.HandlerForType(c.in)
		require.NoError(t, err, "input=%q", c.in)
		if c.wantNil {
			assert.Nil(t, h, "input=%q", c.in)
		} else {
			require.NotNil(t, h, "input=%q", c.in)
			assert.Equal(t, c.wantByte, h.CompressionByte(), "input=%q", c.in)
		}
	}
}

func TestCompressionFactory_HandlerForType_UnknownErrors(t *testing.T) {
	_, err := CompressionFactory{}.HandlerForType("LZ4")
	assert.Error(t, err)
}

func TestCompressionFactory_HandlerForByte(t *testing.T) {
	cf := CompressionFactory{}

	h, err := cf.HandlerForByte(CompressionByteNone)
	require.NoError(t, err)
	assert.Nil(t, h)

	h, err = cf.HandlerForByte(CompressionByteZlib)
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.Equal(t, byte(0x05), h.CompressionByte())
}

func TestCompressionFactory_HandlerForByte_UnknownErrorsWithIncompatibleData(t *testing.T) {
	_, err := CompressionFactory{}.HandlerForByte(0x01)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleData))
}

func TestZlib_RoundTrip(t *testing.T) {
	h := ZlibCompressionHandler{}
	original := []byte("this is a meaningful round-trip test for zlib compression")

	compressed, err := h.Compress(original)
	require.NoError(t, err)
	assert.NotEqual(t, original, compressed)

	decompressed, err := h.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestZlib_LevelPinning_SameInputDeterministicallyCompresses(t *testing.T) {
	// Two compresses of the same input must produce byte-identical output.
	// If the level isn't pinned, level autotuning could drift between runs,
	// which would break Java golden-byte fixtures (plan §5.5).
	h := ZlibCompressionHandler{}
	input := bytes.Repeat([]byte("0123456789ABCDEF"), 64)

	a, err := h.Compress(input)
	require.NoError(t, err)
	b, err := h.Compress(input)
	require.NoError(t, err)
	assert.Equal(t, a, b, "zlib level must be pinned so same input → same bytes")
}

func TestZlib_CompressedSmallerThanInput_ForCompressibleInput(t *testing.T) {
	h := ZlibCompressionHandler{}
	// Highly compressible: 4 KiB of repeated bytes.
	input := bytes.Repeat([]byte("aaaaaaaa"), 512)
	compressed, err := h.Compress(input)
	require.NoError(t, err)
	assert.Less(t, len(compressed), len(input),
		"compressed=%d input=%d — zlib should shrink trivially compressible input",
		len(compressed), len(input))
}

func TestZlib_DecompressRejectsGarbage(t *testing.T) {
	h := ZlibCompressionHandler{}
	_, err := h.Decompress([]byte("not zlib"))
	assert.Error(t, err)
}
