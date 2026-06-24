// Tier-1 tests for the CreateSchema → RegisterSchemaVersion fall-through
// when Glue returns AlreadyExistsException (race with another producer).
//
// Phase 4.5 bug 3: the existing fall-through uses
// `strings.Contains(err.Error(), "AlreadyExistsException")` at
// encoder.go:221, which is brittle to SDK error-formatting changes,
// localized messages, and middleware-stripped error prefixes. The
// typed AWS SDK error is *types.AlreadyExistsException; errors.As is
// the right test. These tests pin both paths:
//
//   - typed error WITH the "AlreadyExistsException" substring: matches
//     both the old strings.Contains AND the new errors.As fix.
//   - typed error WITHOUT the substring (ErrorCodeOverride): matches
//     errors.As but NOT the substring check. This is the red test
//     that drives the fix.

package gsrserde

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// alreadyExistsRecoveryFixture wires the AlreadyExists fall-through:
// GetSchemaByDefinition returns EntityNotFound (drives the fall-through
// into createSchema); createSchema returns the supplied AlreadyExists
// error; RegisterSchemaVersion returns a fresh UUID.
func alreadyExistsRecoveryFixture(t *testing.T, createErr error) (*GsrEncoder, *MockGlueClient, string) {
	t.Helper()
	enc, mockClient := newEncoderWithMock(t, true)

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())

	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return((*glue.CreateSchemaOutput)(nil), createErr)

	const recoveredID = "00000000-0000-0000-0000-aaaabbbbcccc"
	v := int64(2)
	mockClient.On("RegisterSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.RegisterSchemaVersionOutput{
			SchemaVersionId: aws.String(recoveredID),
			VersionNumber:   &v,
		}, nil)

	return enc, mockClient, recoveredID
}

// TestEncoder_AlreadyExists_TypedErrorWithSubstring_RecoversViaRegister
// covers the "happy" typed-error case: the SDK returns
// *types.AlreadyExistsException whose Error() string contains
// "AlreadyExistsException". Both the legacy strings.Contains code and
// the errors.As fix recover.
func TestEncoder_AlreadyExists_TypedErrorWithSubstring_RecoversViaRegister(t *testing.T) {
	createErr := &types.AlreadyExistsException{Message: aws.String("Schema already exists")}
	enc, mockClient, recoveredID := alreadyExistsRecoveryFixture(t, createErr)

	gotID, gotVer, err := enc.getSchemaVersionIdByDefinition("def-1", "schema-1", "JSON", "")
	require.NoError(t, err)
	require.Equal(t, recoveredID, gotID)
	require.Equal(t, uint32(2), gotVer)

	mockClient.AssertCalled(t, "RegisterSchemaVersion", mock.Anything, mock.Anything)
}

// TestEncoder_AlreadyExists_TypedErrorWithoutSubstring_RecoversViaRegister
// is the red test driving Phase 4.5 bug 3. *types.AlreadyExistsException
// carries an `ErrorCodeOverride` field — when set, Error() emits the
// override code, not "AlreadyExistsException". A future SDK release,
// localized response, or middleware that rewrites the message can
// make Error() NOT contain the canonical substring while the typed
// shape is preserved. The strings.Contains code at encoder.go:221
// MISSES this case and surfaces "failed to create schema" instead of
// falling through to RegisterSchemaVersion.
//
// This test fails under strings.Contains and passes under errors.As.
func TestEncoder_AlreadyExists_TypedErrorWithoutSubstring_RecoversViaRegister(t *testing.T) {
	createErr := &types.AlreadyExistsException{
		Message:           aws.String("collision"),
		ErrorCodeOverride: aws.String("SchemaCollision"), // no "AlreadyExistsException" substring
	}
	// Sanity check: the override does change Error() output so the
	// strings.Contains path genuinely misses.
	require.NotContains(t, createErr.Error(), "AlreadyExistsException",
		"fixture invariant: ErrorCodeOverride must make Error() miss the legacy substring")
	require.NotContains(t, createErr.Error(), "already exists",
		"fixture invariant: Message must miss the legacy fallback substring")

	enc, mockClient, recoveredID := alreadyExistsRecoveryFixture(t, createErr)

	gotID, gotVer, err := enc.getSchemaVersionIdByDefinition("def-2", "schema-2", "JSON", "")
	require.NoError(t, err)
	require.Equal(t, recoveredID, gotID)
	require.Equal(t, uint32(2), gotVer)

	mockClient.AssertCalled(t, "RegisterSchemaVersion", mock.Anything, mock.Anything)
}

// TestEncoder_AlreadyExists_UnrelatedTypedError_DoesNotRecover guards
// the negative direction: an error that is NOT *types.AlreadyExistsException
// (e.g. *types.InvalidInputException for an incompatible schema
// rejection) must NOT trigger the Register fall-through. The legacy
// code would mis-trigger if the error message happened to contain
// "already exists" as a substring (e.g. localized "field 'name'
// already exists" from a different validation path). errors.As-only
// matching prevents that.
func TestEncoder_AlreadyExists_UnrelatedTypedError_DoesNotRecover(t *testing.T) {
	createErr := &types.InvalidInputException{
		Message: aws.String("Field name already exists in another schema"), // adversarial substring
	}

	enc, mockClient := newEncoderWithMock(t, true)
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return((*glue.CreateSchemaOutput)(nil), createErr)
	// RegisterSchemaVersion expectation is INTENTIONALLY OMITTED — the
	// mock's MethodCalled will fail loudly if the encoder fires this
	// path against an unrelated typed error.

	_, _, err := enc.getSchemaVersionIdByDefinition("def-3", "schema-3", "JSON", "")
	require.Error(t, err, "unrelated typed error must NOT be recovered via RegisterSchemaVersion")

	mockClient.AssertNotCalled(t, "RegisterSchemaVersion", mock.Anything, mock.Anything)
}
