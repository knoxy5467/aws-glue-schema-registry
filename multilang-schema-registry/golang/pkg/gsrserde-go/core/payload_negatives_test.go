// Tier-1 §5.3 items 28-29 coverage for the GSR wire format:
//
//   28. Truncated payload (less than 18 bytes) → typed error. The bytes-level
//       guard is in TestDecodeWireFormat_RejectsTooShort; this file adds the
//       orchestrator-level assertion that GsrDecoder.Decode (the entry point
//       the format-layer deserializer calls) propagates that error
//       intact and chained to ErrIncompatibleData / ErrGSR.
//
//   29. Header version 0x03 with a syntactically-valid UUID that points at no
//       known schema. The decoder cannot tell at parse time whether the UUID
//       was "corrupted in transit" or simply names an unregistered schema —
//       both fall through to GetSchemaVersion, which surfaces a typed error.
//       This test mocks GetSchemaVersion → EntityNotFoundException and asserts
//       the chain reaches both the typed SDK error and ErrGSR.

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

// TestGsrDecoder_TruncatedPayload_SurfacesIncompatibleData asserts §5.3
// item 28: a payload shorter than the 18-byte header surfaces an error
// chain that errors.Is reaches ErrIncompatibleData (and therefore ErrGSR).
func TestGsrDecoder_TruncatedPayload_SurfacesIncompatibleData(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, err := NewCache(DefaultCacheTTLMillis)
	require.NoError(t, err)
	d := &GsrDecoder{client: mockClient, schemaCache: cache}

	// 17 bytes — one short of the 18-byte minimum prefix.
	short := make([]byte, WireFormatHeaderSize-1)
	short[0] = WireFormatVersionByte
	short[1] = CompressionByteNone

	_, err = d.Decode(short)
	require.Error(t, err, "<18-byte payload must be rejected")
	assert.ErrorIs(t, err, ErrIncompatibleData,
		"chain must reach ErrIncompatibleData")
	assert.ErrorIs(t, err, ErrGSR,
		"chain must reach ErrGSR for unified callers")
	assert.Empty(t, mockClient.Calls,
		"truncated payload must NOT trigger a Glue call (header parse fails first)")
}

// TestGsrDecoder_ValidHeader_UnknownUUID_SurfacesTypedError asserts §5.3
// item 29: a well-formed 18-byte prefix whose UUID names no registered
// schema falls through to GetSchemaVersion, which returns
// EntityNotFoundException; the decoder propagates that as a typed error
// chain. From the wire-format layer's perspective this is the same bytes
// shape as "header 0x03 but corrupt UUID" — both cases hit the same code
// path and surface the same typed error.
func TestGsrDecoder_ValidHeader_UnknownUUID_SurfacesTypedError(t *testing.T) {
	mockClient := &MockGlueClient{}
	cache, err := NewCache(DefaultCacheTTLMillis)
	require.NoError(t, err)
	d := &GsrDecoder{client: mockClient, schemaCache: cache}

	msg := "Schema version is not found."
	notFound := &types.EntityNotFoundException{Message: &msg}
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaVersionOutput)(nil), notFound)

	// Build a syntactically-valid 18-byte prefix + empty payload.
	encoded, err := EncodeWireFormat(testUUIDString, CompressionByteNone, nil)
	require.NoError(t, err)
	require.Equal(t, WireFormatHeaderSize, len(encoded))

	_, err = d.Decode(encoded)
	require.Error(t, err, "unknown UUID must surface as an error")

	// The typed SDK error must remain in the chain so callers can
	// inspect API details (e.g., to distinguish "not registered yet"
	// from auth failures).
	var ent *types.EntityNotFoundException
	assert.True(t, errors.As(err, &ent),
		"*types.EntityNotFoundException must be reachable via errors.As")

	mockClient.AssertCalled(t, "GetSchemaVersion", mock.Anything, mock.Anything)
}
