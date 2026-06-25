package protobuf

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
)

// TestProtobufDeserializationError_Is_ErrGSR locks the Is(target)→ErrGSR shim
// (Phase 4.12 §4 invariant). PBI-4.12-1 introduced the wrapper struct with
// the shim for parity with JsonDeserializationError / AvroDeserializationError.
func TestProtobufDeserializationError_Is_ErrGSR(t *testing.T) {
	err := &ProtobufDeserializationError{Message: "anything"}
	assert.True(t, errors.Is(err, gsrcore.ErrGSR),
		"ProtobufDeserializationError must match ErrGSR via the Is() shim")
}

// TestProtobufDeserializationError_UnwrapsToMalformedSentinel locks AC-3 of
// PBI-4.12-1: when Cause is the matching per-format sentinel, errors.Is
// against the wrapper must resolve through Unwrap() to the sentinel AND
// (transitively) to ErrGSR.
func TestProtobufDeserializationError_UnwrapsToMalformedSentinel(t *testing.T) {
	wrapped := &ProtobufDeserializationError{
		Message: "proto unmarshal failure",
		Cause:   fmt.Errorf("%w: simulated", gsrcore.ErrMalformedProtobuf),
	}
	assert.True(t, errors.Is(wrapped, gsrcore.ErrMalformedProtobuf),
		"wrapper Cause carrying ErrMalformedProtobuf must resolve via Unwrap()")
	assert.True(t, errors.Is(wrapped, gsrcore.ErrGSR),
		"ErrMalformedProtobuf transitively chains to ErrGSR")
	assert.False(t, errors.Is(wrapped, gsrcore.ErrMalformedJSON))
	assert.False(t, errors.Is(wrapped, gsrcore.ErrMalformedAvro))
}
