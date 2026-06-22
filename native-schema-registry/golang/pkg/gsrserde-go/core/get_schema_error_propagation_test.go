// Tier-1 tests for the GetSchemaByDefinition error-propagation contract.
//
// Phase 4.5 bug 2: encoder.go:197-218 falls through to createSchema on
// ANY non-nil GetSchemaByDefinition error, masking the original typed
// error and write-amplifying the failure (a denied-read becomes a
// denied-write attempt; a throttled-read becomes a doubled-throttle).
// Java parity (AWSSchemaRegistryClient.java:151): fall through to
// CreateSchema ONLY on EntityNotFoundException.
//
// These tests pin the post-fix contract:
//   - EntityNotFound → fall through to CreateSchema (the only auto-
//     register trigger).
//   - Any other typed error → propagate directly; CreateSchema NOT
//     called.

package gsrserde

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestEncoder_GetSchemaError_AccessDenied_DoesNotFallThrough asserts
// the post-fix contract: AccessDenied from GetSchemaByDefinition is
// returned directly; CreateSchema is NOT attempted.
//
// Today's strings.Contains-driven fall-through (bug 2) double-bills
// IAM by silently retrying as CreateSchema; a partial-permission
// posture (read denied, write allowed) silently mutates the registry.
func TestEncoder_GetSchemaError_AccessDenied_DoesNotFallThrough(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, true)

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newAccessDeniedError())
	// CreateSchema intentionally not wired — mock will panic if invoked.

	_, _, err := enc.getSchemaVersionIdByDefinition("def-acd", "schema-acd", "JSON")
	require.Error(t, err, "AccessDenied must surface as an error")

	var ade *types.AccessDeniedException
	require.True(t, errors.As(err, &ade),
		"AccessDeniedException must remain in the error chain (got %T: %v)", err, err)

	mockClient.AssertNotCalled(t, "CreateSchema", mock.Anything, mock.Anything)
}

// TestEncoder_GetSchemaError_Throttling_DoesNotFallThrough asserts the
// same contract for ThrottlingException: the encoder must not double
// the load on an already-throttled service.
func TestEncoder_GetSchemaError_Throttling_DoesNotFallThrough(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, true)

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newThrottlingError())

	_, _, err := enc.getSchemaVersionIdByDefinition("def-thr", "schema-thr", "JSON")
	require.Error(t, err)

	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr), "smithy.APIError must remain in the error chain")
	require.Equal(t, "ThrottlingException", apiErr.ErrorCode())

	mockClient.AssertNotCalled(t, "CreateSchema", mock.Anything, mock.Anything)
}

// TestEncoder_GetSchemaError_InvalidInput_DoesNotFallThrough covers a
// third typed error: InvalidInputException (e.g., a malformed
// definition rejected by Glue). Today's encoder hides this behind a
// CreateSchema attempt; post-fix it must propagate directly.
func TestEncoder_GetSchemaError_InvalidInput_DoesNotFallThrough(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, true)

	invalid := &types.InvalidInputException{Message: aws.String("Schema definition is invalid")}
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), invalid)

	_, _, err := enc.getSchemaVersionIdByDefinition("def-inv", "schema-inv", "JSON")
	require.Error(t, err)

	var iie *types.InvalidInputException
	require.True(t, errors.As(err, &iie),
		"InvalidInputException must remain in the error chain (got %T: %v)", err, err)

	mockClient.AssertNotCalled(t, "CreateSchema", mock.Anything, mock.Anything)
}

// TestEncoder_GetSchemaError_EntityNotFound_StillFallsThrough is the
// counterpart: EntityNotFoundException IS the documented auto-register
// trigger, so the fall-through must still happen. Companion to
// TestEncoder_EntityNotFound_AutoRegisterTrue_FallsThroughToCreateSchema
// in glue_negatives_test.go; this one explicitly asserts CreateSchema
// IS called via the post-fix typed-error gate.
func TestEncoder_GetSchemaError_EntityNotFound_StillFallsThrough(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, true)

	createdVersionID := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdVersionID,
			LatestSchemaVersion: ptrInt64(1),
		}, nil)

	id, _, err := enc.getSchemaVersionIdByDefinition("def-enf", "schema-enf", "JSON")
	require.NoError(t, err)
	require.Equal(t, createdVersionID, id)

	mockClient.AssertCalled(t, "CreateSchema", mock.Anything, mock.Anything)
}
