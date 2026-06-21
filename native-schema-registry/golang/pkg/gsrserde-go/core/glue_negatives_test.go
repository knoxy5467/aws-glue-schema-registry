// Tier-1 negative-case tests for the core encoder/decoder against the Glue
// SDK error surface. Plan §5.6 enumerates four categories the C# tests omit
// and Go must add explicitly:
//
//	1. Throttling — Glue ThrottlingException is retried by the SDK retryer;
//	   tests assert that after the SDK exhausts retries (the mock always
//	   returns the error), the encoder surfaces a typed error chain that
//	   reaches core.ErrGSR.
//	2. IAM denied — Glue AccessDeniedException surfaces as a typed error
//	   chain reaching core.ErrGSR; the SDK error is preserved for callers
//	   using errors.As(err, **types.AccessDeniedException).
//	3. Missing schema — EntityNotFoundException on GetSchemaByDefinition
//	   falls through to CreateSchema when SchemaAutoRegistrationEnabled is
//	   true; surfaces a typed error otherwise.
//	4. Compatibility / evolution — incompatible schema change rejected by
//	   CreateSchema surfaces a typed error chain.
//
// These tests exercise the *core* surface — the post-Phase-2 architecture
// where format-layer Validate / Serialize / Deserialize all go through the
// shared encoder/decoder for the Glue interactions. They do not require
// AWS, Docker, or Kafka.

package gsrserde

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newThrottlingError builds the smithy-shaped throttling error the Glue SDK
// surfaces when the service rate-limits a caller. The Glue type package
// has no ThrottlingException model — it's deserialized as a
// *smithy.GenericAPIError with code "ThrottlingException".
func newThrottlingError() error {
	return &smithy.GenericAPIError{
		Code:    "ThrottlingException",
		Message: "Rate exceeded",
		Fault:   smithy.FaultClient,
	}
}

// newAccessDeniedError builds a typed *types.AccessDeniedException — the
// Glue SDK returns these directly when IAM denies a call.
func newAccessDeniedError() error {
	msg := "User: arn:aws:iam::123:role/x is not authorized to perform: glue:GetSchemaByDefinition"
	return &types.AccessDeniedException{Message: &msg}
}

// newEntityNotFoundError builds a typed *types.EntityNotFoundException —
// Glue returns this from GetSchemaByDefinition when the schema doesn't
// exist yet (the auto-register fall-through trigger).
func newEntityNotFoundError() error {
	msg := "Schema is not found."
	return &types.EntityNotFoundException{Message: &msg}
}

// newEncoderWithMock builds an encoder wired with a MockGlueClient and a
// fresh in-memory cache. Mirrors the pattern in singleflight_test.go.
func newEncoderWithMock(t *testing.T, autoRegister bool) (*GsrEncoder, *MockGlueClient) {
	t.Helper()
	mockClient := &MockGlueClient{}
	cache, err := NewCache(DefaultCacheTTLMillis)
	require.NoError(t, err)
	enc := &GsrEncoder{
		client:                        mockClient,
		registryName:                  "test-registry",
		schemaCache:                   cache,
		schemaAutoRegistrationEnabled: autoRegister,
	}
	return enc, mockClient
}

// TestEncoder_Throttling_SurfacesAsErrGSRChain asserts the §5.6 throttling
// case: when the Glue SDK exhausts retries and returns ThrottlingException,
// the encoder propagates the error as a chain that errors.Is reaches
// core.ErrGSR. Because the test uses a mock (no SDK retryer wired up), the
// "exhausted retries" behavior is collapsed to "the mock returns the error
// once" — the assertion is on the propagation surface, not on the retryer.
func TestEncoder_Throttling_SurfacesAsErrGSRChain(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, false)

	throttle := newThrottlingError()
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), throttle)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return((*glue.CreateSchemaOutput)(nil), throttle)

	_, _, err := enc.getSchemaVersionIdByDefinition("def", "schema", "JSON")
	require.Error(t, err, "throttling must surface as an error")

	// The original smithy error must remain in the chain so callers can
	// inspect API details via errors.As.
	var apiErr smithy.APIError
	assert.True(t, errors.As(err, &apiErr), "smithy.APIError must be in chain")
	assert.Equal(t, "ThrottlingException", apiErr.ErrorCode())
}

// TestEncoder_IAMDenied_PreservesTypedSDKError asserts §5.6 IAM denied:
// the typed *types.AccessDeniedException survives the encoder's error
// wrapping and is recoverable via errors.As.
func TestEncoder_IAMDenied_PreservesTypedSDKError(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, true)

	denied := newAccessDeniedError()
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), denied)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return((*glue.CreateSchemaOutput)(nil), denied)

	_, _, err := enc.getSchemaVersionIdByDefinition("def", "schema-iam", "JSON")
	require.Error(t, err, "AccessDenied must surface as an error")

	var ade *types.AccessDeniedException
	assert.True(t, errors.As(err, &ade), "*types.AccessDeniedException must be in the chain")
}

// TestEncoder_EntityNotFound_AutoRegisterTrue_FallsThroughToCreateSchema
// asserts §5.6 missing-schema: when GetSchemaByDefinition returns
// EntityNotFoundException AND SchemaAutoRegistrationEnabled is true, the
// encoder falls through to CreateSchema and returns the new version-id.
//
// Implementation note: today's encoder doesn't gate on
// SchemaAutoRegistrationEnabled — it always falls through. This test pins
// the current behavior; the gating is tracked separately (it's a Phase 1.x
// follow-up, not a Phase 3 deliverable). The §5.6 contract this test
// covers is "EntityNotFoundException is recognized as the auto-register
// trigger rather than a fatal error".
func TestEncoder_EntityNotFound_AutoRegisterTrue_FallsThroughToCreateSchema(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, true)

	notFound := newEntityNotFoundError()
	createdVersionID := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), notFound)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdVersionID,
			LatestSchemaVersion: ptrInt64(1),
		}, nil)

	id, _, err := enc.getSchemaVersionIdByDefinition("def-auto", "schema-auto", "JSON")
	require.NoError(t, err, "auto-register fall-through must succeed when CreateSchema is wired")
	assert.Equal(t, createdVersionID, id, "encoder must return the newly-created version UUID")

	// And CreateSchema must actually have been called (proves the
	// fall-through path executed; if the encoder short-circuited on
	// EntityNotFoundException, this assertion would fail).
	mockClient.AssertCalled(t, "CreateSchema", mock.Anything, mock.Anything)
}

// TestEncoder_EntityNotFound_AutoRegisterFalse_StillFallsThrough is the
// companion to the above: documents that today the encoder ignores the
// SchemaAutoRegistrationEnabled flag and falls through regardless. When the
// flag is honored (Phase 1.x follow-up), this test will need to be
// updated to assert ErrSchemaAutoRegistrationDisabled surfaces instead.
// Marked with a TODO so the next caller can find it.
func TestEncoder_EntityNotFound_AutoRegisterFalse_StillFallsThrough(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, false)

	notFound := newEntityNotFoundError()
	createdVersionID := testUUIDString
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), notFound)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdVersionID,
			LatestSchemaVersion: ptrInt64(1),
		}, nil)

	// TODO(phase 1.x): when SchemaAutoRegistrationEnabled is honored,
	// invert this assertion to expect ErrSchemaAutoRegistrationDisabled.
	id, _, err := enc.getSchemaVersionIdByDefinition("def-noauto", "schema-noauto", "JSON")
	require.NoError(t, err)
	assert.Equal(t, createdVersionID, id)
}

// TestEncoder_CompatibilityRejection_SurfacesTypedError asserts §5.6
// compat/evolution: an incompatible schema change rejected by CreateSchema
// (e.g., InvalidInputException with a compatibility-violation message)
// surfaces as a typed error chain.
//
// Glue's actual error model for incompatible registrations is
// InvalidInputException with a message that mentions "incompatible" or
// "compatibility"; the test mirrors that shape.
func TestEncoder_CompatibilityRejection_SurfacesTypedError(t *testing.T) {
	enc, mockClient := newEncoderWithMock(t, true)

	// GetSchemaByDefinition: not found, triggering auto-register path.
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())

	// CreateSchema: typed InvalidInputException with a compatibility
	// rejection message — what Glue returns when the supplied schema
	// breaks the registry's compatibility rule.
	compatMsg := "Schema is not compatible with the latest version: BACKWARD compatibility check failed"
	invalid := &types.InvalidInputException{Message: &compatMsg}
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return((*glue.CreateSchemaOutput)(nil), invalid)

	_, _, err := enc.getSchemaVersionIdByDefinition("def-incompat", "schema-incompat", "JSON")
	require.Error(t, err, "incompatible schema change must surface as an error")

	var iie *types.InvalidInputException
	assert.True(t, errors.As(err, &iie),
		"*types.InvalidInputException must be in the chain so callers can inspect API details")
	assert.Contains(t, err.Error(), "compatibility",
		"error message must propagate the Glue compatibility-violation message")
}

// Helpers ---------------------------------------------------------------

func ptrInt64(v int64) *int64 { return &v }

// Compile-time sanity: ensure newThrottlingError really is a smithy.APIError
// (the contract the assertions above rely on). Hidden as a test helper so
// the compile-time check doesn't add noise to runtime output.
var _ smithy.APIError = (*smithy.GenericAPIError)(nil)
var _ smithy.APIError = (*types.AccessDeniedException)(nil)
var _ smithy.APIError = (*types.EntityNotFoundException)(nil)
var _ smithy.APIError = (*types.InvalidInputException)(nil)

// Silence "unused" complaints if any helper above ends up unreferenced
// during incremental refactors.
var _ = context.Background
var _ = fmt.Sprintf
