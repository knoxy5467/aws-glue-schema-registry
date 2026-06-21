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
