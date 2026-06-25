// Tier-1 tests for the RegisterSchemaVersion PENDING→AVAILABLE poll loop.
//
// Coverage per PBI-4.10-5 / spec §3.5:
//   - AC-6 (happy path): mock returns PENDING N times then AVAILABLE;
//     GetSchemaVersion is called exactly N times; nil error.
//   - AC-6-exhaustion: mock returns PENDING 10+ times;
//     GetSchemaVersion is called exactly 10 times; error wraps ErrGSR.
//   - AC-6-non-pending: mock returns DELETING on first call;
//     error wraps ErrGSR; no further GetSchemaVersion calls.
//   - AC-6-already-available: RegisterSchemaVersion returns AVAILABLE
//     immediately; GetSchemaVersion is called 0 times.
//   - AC-6-metadata-flush-on-success: poll succeeds; PutSchemaVersionMetadata
//     is called (metadata flush fires after poll).
//   - AC-6-no-flush-on-exhaustion: poll exhausts; PutSchemaVersionMetadata
//     is never called.
//
// sleepFn discipline (INV-2 / C-19):
//
//	The GsrEncoder.sleepFn field is package-local (lowercase). Tests
//	override it via in-package _test.go access — no public setter is
//	needed or provided. The no-op sleepFn makes the poll loop run
//	instantly so the test suite completes in <1ms per test.
//
// Java parity citation: AWSSchemaRegistryClient.java:373-411
// (waitForSchemaEvolutionCheckToComplete).

package gsrserde

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// noopSleep is the injectable sleep for poll_test.go tests.
// It makes schemaEvolutionMaxWaitInterval a no-op so tests run instantly
// (INV-2 / C-19). Do NOT use time.Sleep in any poll_test.go test — always
// set enc.sleepFn = noopSleep before driving the encoder.
func noopSleep(_ time.Duration) {}

// newPollEncoder builds an encoder wired with a MockGlueClient, no-op sleepFn,
// and no configured metadata. Tests that need metadata set the metadata field
// directly. schemaAutoRegistrationEnabled is true — poll tests drive
// registerSchemaVersion via the AlreadyExists recovery path.
func newPollEncoder(t *testing.T) (*GsrEncoder, *MockGlueClient) {
	t.Helper()
	mockClient := &MockGlueClient{}
	cache, err := NewCache(DefaultCacheTTLMillis)
	require.NoError(t, err)
	enc := &GsrEncoder{
		client:                        mockClient,
		registryName:                  "test-registry",
		schemaCache:                   cache,
		schemaAutoRegistrationEnabled: true,
		sleepFn:                       noopSleep,
	}
	return enc, mockClient
}

// wirePollPath sets up the GetSchemaByDefinition + CreateSchema(AlreadyExists)
// stubs that route execution into registerSchemaVersion. The caller adds the
// RegisterSchemaVersion and GetSchemaVersion stubs for the specific scenario.
func wirePollPath(t *testing.T, mockClient *MockGlueClient) {
	t.Helper()
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return((*glue.CreateSchemaOutput)(nil), &types.AlreadyExistsException{Message: aws.String("race")})
}

// TestEncoder_RegisterSchemaVersion_PollsPendingToAvailable covers AC-6
// (happy path). The mock returns PENDING twice then AVAILABLE; the encoder
// must call GetSchemaVersion exactly 3 times (one per iteration before
// receiving AVAILABLE) and return nil error.
//
// Java parity: AWSSchemaRegistryClient.java:380-410 — the poll loop
// sleeps first then calls GetSchemaVersion; on AVAILABLE it returns.
func TestEncoder_RegisterSchemaVersion_PollsPendingToAvailable(t *testing.T) {
	enc, mockClient := newPollEncoder(t)
	wirePollPath(t, mockClient)

	const vid = "poll-happy-uuid-0001-000000000001"
	v := int64(2)
	// RegisterSchemaVersion returns PENDING (triggers the poll).
	mockClient.On("RegisterSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.RegisterSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			VersionNumber:   &v,
			Status:          types.SchemaVersionStatusPending,
		}, nil)

	// GetSchemaVersion: PENDING twice, then AVAILABLE.
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			Status:          types.SchemaVersionStatusPending,
		}, nil).Once()
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			Status:          types.SchemaVersionStatusPending,
		}, nil).Once()
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			Status:          types.SchemaVersionStatusAvailable,
		}, nil).Once()

	gotID, _, err := enc.getSchemaVersionIdByDefinition("def-poll", "schema-poll", "JSON", "")
	require.NoError(t, err, "AC-6: poll must succeed when AVAILABLE is reached within max attempts")
	require.Equal(t, vid, gotID)

	require.Equal(t, 3, mockClient.GetSchemaVersionCalls,
		"AC-6: GetSchemaVersion must be called exactly 3 times (2×PENDING + 1×AVAILABLE)")
}

// TestEncoder_RegisterSchemaVersion_PollExhaustionErrors covers AC-6-exhaustion.
// The mock returns PENDING on every call; the encoder must call GetSchemaVersion
// exactly schemaEvolutionMaxAttempts (10) times and then return an ErrGSR-
// wrapped error.
func TestEncoder_RegisterSchemaVersion_PollExhaustionErrors(t *testing.T) {
	enc, mockClient := newPollEncoder(t)
	wirePollPath(t, mockClient)

	const vid = "poll-exhaust-uuid-000000000002"
	v := int64(2)
	mockClient.On("RegisterSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.RegisterSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			VersionNumber:   &v,
			Status:          types.SchemaVersionStatusPending,
		}, nil)

	// Always PENDING — loop will exhaust.
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			Status:          types.SchemaVersionStatusPending,
		}, nil)

	_, _, err := enc.getSchemaVersionIdByDefinition("def-exhaust", "schema-exhaust", "JSON", "")
	require.Error(t, err, "AC-6-exhaustion: exhausted poll must return error")
	require.True(t, errors.Is(err, ErrGSR),
		"AC-6-exhaustion: error must wrap ErrGSR, got: %v", err)

	require.Equal(t, schemaEvolutionMaxAttempts, mockClient.GetSchemaVersionCalls,
		"AC-6-exhaustion: GetSchemaVersion must be called exactly %d times", schemaEvolutionMaxAttempts)
}

// TestEncoder_RegisterSchemaVersion_NonPendingStatusErrors covers
// AC-6-non-pending. When GetSchemaVersion returns a status other than
// PENDING or AVAILABLE (e.g. DELETING), registerSchemaVersion must return
// a typed ErrGSR-wrapped error and stop polling immediately.
func TestEncoder_RegisterSchemaVersion_NonPendingStatusErrors(t *testing.T) {
	enc, mockClient := newPollEncoder(t)
	wirePollPath(t, mockClient)

	const vid = "poll-nonpend-uuid-0000000000003"
	v := int64(2)
	mockClient.On("RegisterSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.RegisterSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			VersionNumber:   &v,
			Status:          types.SchemaVersionStatusPending,
		}, nil)

	// First call returns DELETING — unexpected non-PENDING status.
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			Status:          types.SchemaVersionStatusDeleting,
		}, nil).Once()

	_, _, err := enc.getSchemaVersionIdByDefinition("def-nonpend", "schema-nonpend", "JSON", "")
	require.Error(t, err, "AC-6-non-pending: unexpected status must return error")
	require.True(t, errors.Is(err, ErrGSR),
		"AC-6-non-pending: error must wrap ErrGSR, got: %v", err)

	require.Equal(t, 1, mockClient.GetSchemaVersionCalls,
		"AC-6-non-pending: must stop polling after unexpected status; got %d calls", mockClient.GetSchemaVersionCalls)
}

// TestEncoder_RegisterSchemaVersion_AlreadyAvailable covers the early-return
// path per spec §3.5: when RegisterSchemaVersion returns Status==AVAILABLE,
// the encoder returns immediately without calling GetSchemaVersion at all.
func TestEncoder_RegisterSchemaVersion_AlreadyAvailable(t *testing.T) {
	enc, mockClient := newPollEncoder(t)
	wirePollPath(t, mockClient)

	const vid = "poll-avail-uuid-00000000000004"
	v := int64(2)
	mockClient.On("RegisterSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.RegisterSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			VersionNumber:   &v,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)

	gotID, _, err := enc.getSchemaVersionIdByDefinition("def-avail", "schema-avail", "JSON", "")
	require.NoError(t, err, "AC-6-already-available: immediate AVAILABLE must succeed")
	require.Equal(t, vid, gotID)

	require.Equal(t, 0, mockClient.GetSchemaVersionCalls,
		"AC-6-already-available: GetSchemaVersion must NOT be called when RegisterSchemaVersion returns AVAILABLE")
}

// TestEncoder_RegisterSchemaVersion_FlushAfterPollSuccess covers
// AC-6-metadata-flush-on-success. When the poll succeeds, the metadata flush
// from PBI-4.10-4 fires AFTER the poll completes — verified by asserting a
// PutSchemaVersionMetadata call count > 0 combined with no error.
func TestEncoder_RegisterSchemaVersion_FlushAfterPollSuccess(t *testing.T) {
	enc, mockClient := newPollEncoder(t)
	// Add a metadata entry so we can count the flush.
	enc.metadata = map[string]string{"env": "staging"}
	wirePollPath(t, mockClient)

	const vid = "poll-flush-uuid-000000000000005"
	v := int64(2)
	// RegisterSchemaVersion returns PENDING (triggers the poll).
	mockClient.On("RegisterSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.RegisterSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			VersionNumber:   &v,
			Status:          types.SchemaVersionStatusPending,
		}, nil)

	// GetSchemaVersion: PENDING once, then AVAILABLE.
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			Status:          types.SchemaVersionStatusPending,
		}, nil).Once()
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			Status:          types.SchemaVersionStatusAvailable,
		}, nil).Once()

	mockClient.On("PutSchemaVersionMetadata", mock.Anything, mock.Anything).
		Return(&glue.PutSchemaVersionMetadataOutput{}, nil)

	gotID, _, err := enc.getSchemaVersionIdByDefinition("def-flush", "schema-flush", "JSON", "kafka")
	require.NoError(t, err)
	require.Equal(t, vid, gotID)

	// len(metadata)+transport = 2 PutSchemaVersionMetadata calls.
	require.Equal(t, 2, mockClient.MetadataCallCount(),
		"AC-6-metadata-flush-on-success: metadata flush must fire after successful poll")
}

// TestEncoder_RegisterSchemaVersion_NoFlushOnExhaustion covers
// AC-6-no-flush-on-exhaustion. When the poll exhausts, PutSchemaVersionMetadata
// must NOT be called — the encoder errors out before the flush call site.
func TestEncoder_RegisterSchemaVersion_NoFlushOnExhaustion(t *testing.T) {
	enc, mockClient := newPollEncoder(t)
	enc.metadata = map[string]string{"env": "staging"}
	wirePollPath(t, mockClient)

	const vid = "poll-noflush-uuid-00000000000006"
	v := int64(2)
	mockClient.On("RegisterSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.RegisterSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			VersionNumber:   &v,
			Status:          types.SchemaVersionStatusPending,
		}, nil)

	// Always PENDING.
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(vid),
			Status:          types.SchemaVersionStatusPending,
		}, nil)

	_, _, err := enc.getSchemaVersionIdByDefinition("def-noflush", "schema-noflush", "JSON", "kafka")
	require.Error(t, err, "poll exhaustion must return error")
	require.True(t, errors.Is(err, ErrGSR))

	require.Equal(t, 0, mockClient.MetadataCallCount(),
		"AC-6-no-flush-on-exhaustion: PutSchemaVersionMetadata must NOT be called on poll exhaustion")
}
