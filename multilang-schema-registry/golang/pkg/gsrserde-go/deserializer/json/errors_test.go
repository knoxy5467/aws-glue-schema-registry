package json

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
)

// TestJsonDeserializationError_Is_ErrGSR locks the Is(target)→ErrGSR shim
// (Phase 4.12 §4 invariant). PBI-4.12-1 added the shim for parity with
// SerializationError / DeserializationError in core.
func TestJsonDeserializationError_Is_ErrGSR(t *testing.T) {
	err := &JsonDeserializationError{Message: "anything"}
	assert.True(t, errors.Is(err, gsrcore.ErrGSR),
		"JsonDeserializationError must match ErrGSR via the Is() shim")
}

// TestJsonDeserializationError_UnwrapsToMalformedSentinel locks AC-3 of
// PBI-4.12-1: when Cause is the matching per-format sentinel, errors.Is
// against the wrapper must resolve through Unwrap() to the sentinel AND
// (transitively) to ErrGSR. This proves the reachability the PBI-4.12-2/3/4
// assertions will depend on, without yet wiring the deserializer code itself
// (those wraps land in PBI-4.12-2/3/4).
func TestJsonDeserializationError_UnwrapsToMalformedSentinel(t *testing.T) {
	wrapped := &JsonDeserializationError{
		Message: "not valid JSON",
		Cause:   fmt.Errorf("%w: simulated", gsrcore.ErrMalformedJSON),
	}
	assert.True(t, errors.Is(wrapped, gsrcore.ErrMalformedJSON),
		"wrapper Cause carrying ErrMalformedJSON must resolve via Unwrap()")
	assert.True(t, errors.Is(wrapped, gsrcore.ErrGSR),
		"ErrMalformedJSON transitively chains to ErrGSR")
	// And the sibling sentinels must NOT cross-resolve.
	assert.False(t, errors.Is(wrapped, gsrcore.ErrMalformedAvro))
	assert.False(t, errors.Is(wrapped, gsrcore.ErrMalformedProtobuf))
}
