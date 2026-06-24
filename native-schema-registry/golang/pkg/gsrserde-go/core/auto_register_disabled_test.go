// Tier-1 tests for SchemaAutoRegistrationEnabled=false enforcement.
//
// Phase 4.5 bug 1: the encoder stored schemaAutoRegistrationEnabled
// (encoder.go:47, set at :81) but never read it; fetchSchemaVersionID
// always called createSchema on a GetSchemaByDefinition miss. The
// sentinel ErrSchemaAutoRegistrationDisabled in errors.go:46 was
// declared but never returned. Plan §5.3 item 14.
//
// Real-world impact: a customer setting auto-register=false to keep
// their producers out of the registry's write path (compliance,
// approval-gate workflows) silently got the opposite behavior.

package gsrserde

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestEncoder_AutoRegisterDisabled_UnknownSchemaReturnsSentinel asserts
// the §5.3 item 14 contract: when SchemaAutoRegistrationEnabled=false
// and GetSchemaByDefinition returns EntityNotFoundException, the
// encoder returns ErrSchemaAutoRegistrationDisabled WITHOUT calling
// CreateSchema.
func TestEncoder_AutoRegisterDisabled_UnknownSchemaReturnsSentinel(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, false)

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	// CreateSchema deliberately not wired — mock panics if called.

	_, _, err := enc.getSchemaVersionIdByDefinition("def-auto-off", "schema-auto-off", "JSON", "")
	require.Error(t, err, "auto-register=false + unknown schema must error")
	require.True(t, errors.Is(err, ErrSchemaAutoRegistrationDisabled),
		"error must wrap ErrSchemaAutoRegistrationDisabled (got %T: %v)", err, err)

	mockClient.AssertNotCalled(t, "CreateSchema", mock.Anything, mock.Anything)
}

// TestEncoder_AutoRegisterEnabled_UnknownSchemaCreates is the
// counterpart: with the flag on, the encoder DOES fall through to
// CreateSchema (the §5.3 item 13 / item 25 contract). Mirrors the
// existing TestEncoder_EntityNotFound_AutoRegisterTrue_FallsThroughToCreateSchema
// but kept here next to the auto-register=false test so the contrast
// is grep-able.
func TestEncoder_AutoRegisterEnabled_UnknownSchemaCreates(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, true)

	createdID := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdID,
			LatestSchemaVersion: ptrInt64(1),
		}, nil)

	id, _, err := enc.getSchemaVersionIdByDefinition("def-auto-on", "schema-auto-on", "JSON", "")
	require.NoError(t, err)
	require.Equal(t, createdID, id)

	mockClient.AssertCalled(t, "CreateSchema", mock.Anything, mock.Anything)
}
