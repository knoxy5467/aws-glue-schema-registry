package gsrserde

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializationError_Message(t *testing.T) {
	err := NewSerializationError("test message")
	assert.Equal(t, "serialization error: test message", err.Error())
}

func TestDeserializationError_Message(t *testing.T) {
	err := NewDeserializationError("test message")
	assert.Equal(t, "deserialization error: test message", err.Error())
}

func TestSerializationError_WrapsCause(t *testing.T) {
	cause := errors.New("upstream failure")
	err := WrapSerializationError("encode failed", cause)
	assert.Contains(t, err.Error(), "encode failed")
	assert.Contains(t, err.Error(), "upstream failure")
	assert.True(t, errors.Is(err, cause), "WrapSerializationError must chain Unwrap to cause")
}

func TestDeserializationError_WrapsCause(t *testing.T) {
	cause := errors.New("upstream failure")
	err := WrapDeserializationError("decode failed", cause)
	assert.True(t, errors.Is(err, cause))
}

// Java parity: IncompatibleData extends AWSSchemaRegistryException, so
// catching AWSSchemaRegistryException also catches IncompatibleData. The
// Go equivalent is errors.Is(err, ErrGSR) returning true for any
// ErrIncompatibleData-derived error.
func TestErrIncompatibleData_IsErrGSR(t *testing.T) {
	assert.True(t, errors.Is(ErrIncompatibleData, ErrGSR))
}

func TestErrMessageTypeNotFound_IsErrGSR(t *testing.T) {
	assert.True(t, errors.Is(ErrMessageTypeNotFound, ErrGSR))
}

func TestErrSchemaAutoRegistrationDisabled_IsErrGSR(t *testing.T) {
	assert.True(t, errors.Is(ErrSchemaAutoRegistrationDisabled, ErrGSR))
}

func TestErrInvalidProtobufPayload_IsErrGSR(t *testing.T) {
	assert.True(t, errors.Is(ErrInvalidProtobufPayload, ErrGSR))
}

func TestErrInvalidProtobufPayload_ChainsThroughFmtErrorf(t *testing.T) {
	wrapped := fmt.Errorf("protobuf: %w", ErrInvalidProtobufPayload)
	assert.True(t, errors.Is(wrapped, ErrInvalidProtobufPayload))
	assert.True(t, errors.Is(wrapped, ErrGSR))
}

func TestSerializationError_IsErrGSR(t *testing.T) {
	err := NewSerializationError("anything")
	assert.True(t, errors.Is(err, ErrGSR))
}

func TestDeserializationError_IsErrGSR(t *testing.T) {
	err := NewDeserializationError("anything")
	assert.True(t, errors.Is(err, ErrGSR))
}

func TestErrors_As_SerializationError(t *testing.T) {
	original := &SerializationError{Message: "boom"}
	wrapped := fmt.Errorf("outer: %w", original)

	var se *SerializationError
	assert.True(t, errors.As(wrapped, &se))
	assert.Equal(t, "boom", se.Message)
}

func TestErrors_As_DeserializationError(t *testing.T) {
	original := &DeserializationError{Message: "boom"}
	wrapped := fmt.Errorf("outer: %w", original)

	var de *DeserializationError
	assert.True(t, errors.As(wrapped, &de))
	assert.Equal(t, "boom", de.Message)
}

// Lock down the wrap chain so a future refactor that breaks Unwrap surfaces
// immediately: an ErrMessageTypeNotFound wrapped via fmt.Errorf must still
// errors.Is back to the sentinel.
func TestErrMessageTypeNotFound_ChainsThroughFmtErrorf(t *testing.T) {
	wrapped := fmt.Errorf("encoder: %w", ErrMessageTypeNotFound)
	assert.True(t, errors.Is(wrapped, ErrMessageTypeNotFound))
	assert.True(t, errors.Is(wrapped, ErrGSR))
}

func TestErrIncompatibleData_ChainsThroughFmtErrorf(t *testing.T) {
	wrapped := fmt.Errorf("decoder: %w", ErrIncompatibleData)
	assert.True(t, errors.Is(wrapped, ErrIncompatibleData))
	assert.True(t, errors.Is(wrapped, ErrGSR))
}

// TestErrors_NewMalformedSentinels_ChainToErrGSR locks the sentinel-shape
// invariants for the three Phase 4.12 §3.9 / §4 per-format sentinels:
//   - each sentinel resolves to ErrGSR via errors.Is (so umbrella callers
//     still match), AND
//   - the sibling sentinels do NOT cross-resolve (Avro is not Protobuf, etc.),
//     which guards against a future refactor that accidentally collapses the
//     three sentinels into a single chain.
//
// The wrapper-struct half of AC-3 (errors.Is(&FormatDeserializationError{
// Cause: ErrMalformed<Format>}, ErrMalformed<Format>)) cannot live here
// because the core module is published independently from the deserializer
// packages and importing them would introduce a dependency cycle. Those
// assertions live in each deserializer's own *_test.go file added by
// PBI-4.12-1.
func TestErrors_NewMalformedSentinels_ChainToErrGSR(t *testing.T) {
	cases := []struct {
		name     string
		sentinel error
		siblings []error
	}{
		{"ErrMalformedJSON", ErrMalformedJSON, []error{ErrMalformedAvro, ErrMalformedProtobuf}},
		{"ErrMalformedAvro", ErrMalformedAvro, []error{ErrMalformedJSON, ErrMalformedProtobuf}},
		{"ErrMalformedProtobuf", ErrMalformedProtobuf, []error{ErrMalformedJSON, ErrMalformedAvro}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, errors.Is(tc.sentinel, ErrGSR),
				"%s must chain to ErrGSR via errors.Is", tc.name)
			for _, sibling := range tc.siblings {
				assert.False(t, errors.Is(tc.sentinel, sibling),
					"%s must NOT cross-resolve to sibling sentinel %v", tc.name, sibling)
			}
			// And the inverse: wrapping the sentinel preserves both matches.
			wrapped := fmt.Errorf("deserializer: %w", tc.sentinel)
			assert.True(t, errors.Is(wrapped, tc.sentinel),
				"%s wrapped via fmt.Errorf must still errors.Is back to itself", tc.name)
			assert.True(t, errors.Is(wrapped, ErrGSR),
				"%s wrapped via fmt.Errorf must transitively reach ErrGSR", tc.name)
		})
	}
}
