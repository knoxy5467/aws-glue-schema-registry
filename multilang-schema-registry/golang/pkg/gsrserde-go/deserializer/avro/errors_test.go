package avro

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
)

// TestAvroDeserializationError_Is_ErrGSR locks the Is(target)→ErrGSR shim
// (Phase 4.12 §4 invariant). PBI-4.12-1 added the shim for parity with
// SerializationError / DeserializationError in core.
func TestAvroDeserializationError_Is_ErrGSR(t *testing.T) {
	err := &AvroDeserializationError{Message: "anything"}
	assert.True(t, errors.Is(err, gsrcore.ErrGSR),
		"AvroDeserializationError must match ErrGSR via the Is() shim")
}

// TestAvroDeserializationError_UnwrapsToMalformedSentinel locks AC-3 of
// PBI-4.12-1: when Cause is the matching per-format sentinel, errors.Is
// against the wrapper must resolve through Unwrap() to the sentinel AND
// (transitively) to ErrGSR.
func TestAvroDeserializationError_UnwrapsToMalformedSentinel(t *testing.T) {
	wrapped := &AvroDeserializationError{
		Message: "decode failure",
		Cause:   fmt.Errorf("%w: simulated", gsrcore.ErrMalformedAvro),
	}
	assert.True(t, errors.Is(wrapped, gsrcore.ErrMalformedAvro),
		"wrapper Cause carrying ErrMalformedAvro must resolve via Unwrap()")
	assert.True(t, errors.Is(wrapped, gsrcore.ErrGSR),
		"ErrMalformedAvro transitively chains to ErrGSR")
	assert.False(t, errors.Is(wrapped, gsrcore.ErrMalformedJSON))
	assert.False(t, errors.Is(wrapped, gsrcore.ErrMalformedProtobuf))
}
