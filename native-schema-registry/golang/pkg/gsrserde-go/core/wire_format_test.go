package gsrserde

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test vector — UUID with all bits known, picked so MSB/LSB endianness is
// trivially diagnosable from the byte slice. Anchored to Java
// SerializationDataEncoder.java:81-86 which writes ByteBuffer.putLong(MSB)
// then putLong(LSB) — both big-endian.
const testUUIDString = "11223344-5566-7788-99aa-bbccddeeff00"

var testUUIDBytes = []byte{
	0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // MSB big-endian
	0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, // LSB big-endian
}

func TestEncodeWireFormat_HeaderLayoutNoCompression(t *testing.T) {
	payload := []byte("hello")

	out, err := EncodeWireFormat(testUUIDString, CompressionByteNone, payload)
	require.NoError(t, err)
	require.Len(t, out, WireFormatHeaderSize+len(payload))

	// Byte 0: version 0x03.
	assert.Equal(t, byte(0x03), out[0])
	// Byte 1: compression 0x00.
	assert.Equal(t, byte(0x00), out[1])
	// Bytes 2..17: UUID, MSB-first big-endian (8 bytes), then LSB-first big-endian (8 bytes).
	assert.Equal(t, testUUIDBytes, out[2:WireFormatHeaderSize])
	// Bytes 18..: payload, verbatim.
	assert.Equal(t, payload, out[WireFormatHeaderSize:])
}

func TestEncodeWireFormat_HeaderLayoutZlibCompression(t *testing.T) {
	// Payload bytes are NOT compressed here — EncodeWireFormat trusts the caller.
	payload := []byte{0x78, 0x9c, 0x00} // bogus "looks like zlib" prefix; doesn't matter
	out, err := EncodeWireFormat(testUUIDString, CompressionByteZlib, payload)
	require.NoError(t, err)

	assert.Equal(t, byte(0x03), out[0])
	assert.Equal(t, byte(0x05), out[1])
	assert.Equal(t, testUUIDBytes, out[2:WireFormatHeaderSize])
	assert.Equal(t, payload, out[WireFormatHeaderSize:])
}

func TestEncodeWireFormat_RejectsUnknownCompressionByte(t *testing.T) {
	_, err := EncodeWireFormat(testUUIDString, 0x01, []byte("x"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleData))
}

func TestEncodeWireFormat_RejectsMalformedUUID(t *testing.T) {
	_, err := EncodeWireFormat("not-a-uuid", CompressionByteNone, []byte("x"))
	require.Error(t, err)
	// Wrap chain reaches uuid.Parse error; ErrIncompatibleData is for the
	// wire-format bytes, not for input validation, so we don't assert
	// errors.Is here. The error message must mention the input failure mode.
	assert.Contains(t, err.Error(), "UUID")
}

func TestDecodeWireFormat_RoundTrip(t *testing.T) {
	original := []byte("round-trip payload")

	encoded, err := EncodeWireFormat(testUUIDString, CompressionByteNone, original)
	require.NoError(t, err)

	gotID, gotComp, gotPayload, err := DecodeWireFormat(encoded)
	require.NoError(t, err)

	// uuid.Parse normalizes case; assert against the parsed canonical form.
	wantID := uuid.MustParse(testUUIDString).String()
	assert.Equal(t, wantID, gotID)
	assert.Equal(t, CompressionByteNone, gotComp)
	assert.Equal(t, original, gotPayload)
}

func TestDecodeWireFormat_RejectsTooShort(t *testing.T) {
	// 17 bytes = one short of header size.
	short := make([]byte, WireFormatHeaderSize-1)
	short[0] = 0x03
	_, _, _, err := DecodeWireFormat(short)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleData))
}

func TestDecodeWireFormat_RejectsWrongVersionByte(t *testing.T) {
	buf := make([]byte, WireFormatHeaderSize+1)
	buf[0] = 0x02 // not 0x03
	_, _, _, err := DecodeWireFormat(buf)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleData))
}

func TestDecodeWireFormat_RejectsUnknownCompressionByte(t *testing.T) {
	buf := make([]byte, WireFormatHeaderSize+1)
	buf[0] = 0x03
	buf[1] = 0x02 // not 0x00 or 0x05
	_, _, _, err := DecodeWireFormat(buf)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleData))
}

func TestDecodeWireFormat_EmptyPayloadIsValid(t *testing.T) {
	// Encoder is allowed to emit a header-only buffer with no payload.
	out, err := EncodeWireFormat(testUUIDString, CompressionByteNone, nil)
	require.NoError(t, err)
	require.Len(t, out, WireFormatHeaderSize)

	_, _, payload, err := DecodeWireFormat(out)
	require.NoError(t, err)
	assert.Len(t, payload, 0)
}

func TestDecodeWireFormat_UUIDEndianness(t *testing.T) {
	// Construct a buffer by hand to lock down Java parity at the byte
	// level. MSB big-endian then LSB big-endian.
	manual := append([]byte{0x03, 0x00}, testUUIDBytes...)
	manual = append(manual, []byte("payload")...)

	gotID, _, gotPayload, err := DecodeWireFormat(manual)
	require.NoError(t, err)

	wantID := uuid.MustParse(testUUIDString).String()
	assert.Equal(t, wantID, gotID)
	assert.Equal(t, []byte("payload"), gotPayload)
}

// Encoding/decoding a UUID by way of EncodeWireFormat must produce the same
// 16-byte slice you'd get if you wrote uuid.UUID directly — i.e. uuid.UUID's
// internal byte layout IS the Java putLong(MSB)+putLong(LSB) byte layout.
// Lock that property down so a future maintainer that swaps the uuid package
// catches the regression.
func TestEncodeWireFormat_UUIDLayoutMatchesUUIDPackage(t *testing.T) {
	u := uuid.MustParse(testUUIDString)
	encoded, err := EncodeWireFormat(testUUIDString, CompressionByteNone, nil)
	require.NoError(t, err)
	assert.Equal(t, u[:], encoded[2:WireFormatHeaderSize])
}
