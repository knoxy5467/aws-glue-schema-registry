//go:build integration
// +build integration

package integration_tests

// Tier-2 metadata-lifecycle cells — spec §3.11 cells 1-2 + spec §5.2 cell 3.
//
// Cells 1 and 2 require real AWS Glue (a configured AWS account).
// Gate: scenarioGate(t, requiresReal=true, false) + AWS_INTEGRATION=1 + GSR_GLUE=real.
//
// Cell 3 uses fakeglue and does NOT require real AWS.
// Gate: scenarioGate(t, requiresReal=false, requiresFake=true).
//
// Default `go test ./...` (no -tags integration) does NOT compile or execute
// any of these tests (INV-10, AC-16, C-6).

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
)

const metadataLifecycleSchema = `{"type":"record","name":"MetadataLifecycleUser","fields":[{"name":"id","type":"string"}]}`

// TestMetadataLifecycle_TagsAndDescriptionPropagate — spec §3.11 cell 1.
//
// Registers a schema via the GsrEncoder with configured Tags, Description, and
// Metadata, then asserts via direct Glue API calls that:
//   - GetSchema returns the configured description verbatim.
//   - GetTags (via encoder.QuerySchemaTags) returns the configured tags.
//   - QuerySchemaVersionMetadata returns the configured metadata plus the
//     always-on x-amz-meta-transport entry.
//
// AC-14, C-6, C-8.
func TestMetadataLifecycle_TagsAndDescriptionPropagate(t *testing.T) {
	scenarioGate(t, true, false)
	h := newGlueHandle(t)

	schemaName := randomGlueName(t, "meta-lifecycle-1")
	h.Cleanup.TrackSchema(testRegistryName, schemaName)

	const configuredDescription = "test description"
	configuredTags := map[string]string{"team": "gsr-go"}
	configuredMetadata := map[string]string{"commit": "abc123"}

	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  testRegistryName,
		Compatibility:                 "NONE",
		SchemaAutoRegistrationEnabled: true,
		Description:                   configuredDescription,
		Tags:                          configuredTags,
		Metadata:                      configuredMetadata,
	})
	require.NoError(t, err, "NewGsrEncoderForTest should succeed")

	// Encode one message — this triggers CreateSchema with the configured
	// tags, description, and metadata flush via PutSchemaVersionMetadata.
	encodedBytes, err := enc.Encode([]byte(`{"id":"user-1"}`), schemaName, &gsrcore.Schema{
		SchemaDefinition: metadataLifecycleSchema,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.NoError(t, err, "Encode should succeed and register schema in real Glue")

	// Extract the schema-version UUID from the wire-format prefix.
	schemaVersionID, _, _, err := gsrcore.DecodeWireFormat(encodedBytes)
	require.NoError(t, err, "DecodeWireFormat should parse encoded bytes")

	ctx, cancel := context.WithTimeout(context.Background(), defaultScenarioCtxTimeout)
	defer cancel()

	// AC-14 assertion 1: GetSchema returns the configured description verbatim.
	// h.Real.Client is the raw *glue.Client; GetSchema is not on the
	// gsrcore.GlueClient interface (description read is Tier-2 only).
	getSchemaResp, err := h.Real.Client.GetSchema(ctx, &glue.GetSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: strPtr(testRegistryName),
			SchemaName:   strPtr(schemaName),
		},
	})
	require.NoError(t, err, "GetSchema should succeed")
	require.NotNil(t, getSchemaResp.Description, "GetSchema.Description should be non-nil")
	require.Equal(t, configuredDescription, *getSchemaResp.Description,
		"GetSchema.Description must match the configured description verbatim")

	// AC-14 assertion 2: QuerySchemaTags returns the configured tags.
	gotTags, err := enc.QuerySchemaTags(ctx, metadataLifecycleSchema, schemaName)
	require.NoError(t, err, "QuerySchemaTags should succeed")
	for k, v := range configuredTags {
		require.Equal(t, v, gotTags[k],
			"QuerySchemaTags result must contain tag %q=%q", k, v)
	}

	// AC-14 assertion 3: QuerySchemaVersionMetadata returns the configured
	// metadata PLUS the always-on x-amz-meta-transport entry (INV-7).
	gotMeta, err := enc.QuerySchemaVersionMetadata(ctx, schemaVersionID)
	require.NoError(t, err, "QuerySchemaVersionMetadata should succeed")
	require.Equal(t, "abc123", gotMeta["commit"],
		"QuerySchemaVersionMetadata must contain configured metadata key 'commit'")
	require.Contains(t, gotMeta, gsrcore.TransportMetadataKey,
		"QuerySchemaVersionMetadata must include always-on %q entry", gsrcore.TransportMetadataKey)
	t.Logf("✅ Cell 1 passed: description=%q tags=%v meta=%v", configuredDescription, configuredTags, configuredMetadata)
}

// TestMetadataLifecycle_QueryMetadataAndTagsFlow — spec §3.11 cell 2.
//
// Registers a fresh schema and explicitly exercises the encoder's
// QuerySchemaVersionMetadata and QuerySchemaTags read methods to verify the
// round-trip after a register. Asserts the returned maps match the configured
// input.
//
// AC-15, C-6, C-8.
func TestMetadataLifecycle_QueryMetadataAndTagsFlow(t *testing.T) {
	scenarioGate(t, true, false)
	h := newGlueHandle(t)

	schemaName := randomGlueName(t, "meta-lifecycle-2")
	h.Cleanup.TrackSchema(testRegistryName, schemaName)

	configuredTags := map[string]string{"env": "test", "owner": "gsr-go"}
	configuredMetadata := map[string]string{"version": "1.0", "source": "unit-test"}

	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  testRegistryName,
		Compatibility:                 "NONE",
		SchemaAutoRegistrationEnabled: true,
		Tags:                          configuredTags,
		Metadata:                      configuredMetadata,
	})
	require.NoError(t, err, "NewGsrEncoderForTest should succeed")

	// Register the schema by encoding a message.
	encodedBytes, err := enc.Encode([]byte(`{"id":"user-2"}`), schemaName, &gsrcore.Schema{
		SchemaDefinition: metadataLifecycleSchema,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.NoError(t, err, "Encode should succeed and register schema in real Glue")

	schemaVersionID, _, _, err := gsrcore.DecodeWireFormat(encodedBytes)
	require.NoError(t, err, "DecodeWireFormat should parse encoded bytes")

	ctx, cancel := context.WithTimeout(context.Background(), defaultScenarioCtxTimeout)
	defer cancel()

	// AC-15 assertion 1: QuerySchemaVersionMetadata round-trip.
	gotMeta, err := enc.QuerySchemaVersionMetadata(ctx, schemaVersionID)
	require.NoError(t, err, "QuerySchemaVersionMetadata should succeed")
	for k, v := range configuredMetadata {
		require.Equal(t, v, gotMeta[k],
			"QuerySchemaVersionMetadata result must contain configured key %q=%q", k, v)
	}
	require.Contains(t, gotMeta, gsrcore.TransportMetadataKey,
		"QuerySchemaVersionMetadata must include always-on %q entry", gsrcore.TransportMetadataKey)

	// AC-15 assertion 2: QuerySchemaTags round-trip.
	gotTags, err := enc.QuerySchemaTags(ctx, metadataLifecycleSchema, schemaName)
	require.NoError(t, err, "QuerySchemaTags should succeed")
	for k, v := range configuredTags {
		require.Equal(t, v, gotTags[k],
			"QuerySchemaTags result must contain configured tag %q=%q", k, v)
	}
	t.Logf("✅ Cell 2 passed: meta=%v tags=%v", configuredMetadata, configuredTags)
}

// TestMetadataLifecycle_PollPendingToAvailable — spec §5.2 PBI-11 / Cell 3.
//
// Uses fakeglue (not real Glue) to verify the PENDING→AVAILABLE poll loop
// without AWS billing. The fakeglue ForcePending affordance scripts:
//
//	RegisterSchemaVersion returns PENDING → GetSchemaVersion called N times
//	with PENDING → GetSchemaVersion returns AVAILABLE → Encode succeeds.
//
// The test forces CreateSchema to return AlreadyExistsException so the encoder
// falls into the registerSchemaVersion path (the only path that can see
// PENDING from Glue). ForcePendingCount=2 means 2 PENDING poll responses
// before AVAILABLE; the encoder must call GetSchemaVersion exactly 3 times.
//
// sleepFn runs at its default (time.Sleep × 3 s per attempt) — 2 PENDING
// responses × 3 s ≤ 6 s, well within the 90-s scenario timeout.
//
// Gate: scenarioGate(t, requiresReal=false, requiresFake=true) — this cell
// uses the fakeglue backend; GSR_GLUE=real skips it.
//
// AC-20, spec §5.2.
func TestMetadataLifecycle_PollPendingToAvailable(t *testing.T) {
	scenarioGate(t, false, true)
	h := newGlueHandle(t)

	// ForceCreateError → AlreadyExistsException routes Encode into the
	// registerSchemaVersion (poll) path rather than the createSchema path.
	h.Fake.ForceCreateError = &types.AlreadyExistsException{
		Message: aws.String("schema already exists (forced for poll-path test)"),
	}
	// Script 2 PENDING responses from GetSchemaVersion before AVAILABLE.
	h.Fake.ForceRegisterPending = true
	h.Fake.ForcePendingCount = 2

	const pendingCount = 2

	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "NONE",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err, "NewGsrEncoderForTest should succeed")

	schemaName := randomGlueName(t, "poll-pending")

	_, encodeErr := enc.Encode([]byte(`{"id":"poll-1"}`), schemaName, &gsrcore.Schema{
		SchemaDefinition: metadataLifecycleSchema,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.NoError(t, encodeErr,
		"Cell 3: Encode must succeed after PENDING→AVAILABLE poll resolves")

	// Poll loop calls GetSchemaVersion pendingCount times returning PENDING,
	// then once more returning AVAILABLE — total = pendingCount + 1.
	gotVersionCalls := h.Fake.Count("GetSchemaVersion")
	require.Equal(t, pendingCount+1, gotVersionCalls,
		"Cell 3: GetSchemaVersion must be called exactly %d times (%d PENDING + 1 AVAILABLE), got %d",
		pendingCount+1, pendingCount, gotVersionCalls)

	require.Equal(t, 1, h.Fake.Count("RegisterSchemaVersion"),
		"Cell 3: RegisterSchemaVersion must be called exactly once")

	// AC-14: metadata flush must fire AFTER the poll resolves. Even with no
	// configured metadata, the encoder always flushes the always-on
	// TransportMetadataKey entry (INV-7 / PBI-4.10-4).
	require.GreaterOrEqual(t, h.Fake.Count("PutSchemaVersionMetadata"), 1,
		"Cell 3: metadata flush (PutSchemaVersionMetadata) must fire after poll resolves to AVAILABLE")

	t.Logf("✅ Cell 3 passed: poll loop resolved after %d PENDING responses (GetSchemaVersion called %d times)",
		pendingCount, gotVersionCalls)
}

// strPtr returns a pointer to the given string. Used only in this file to
// keep the Glue API call sites readable.
func strPtr(s string) *string {
	return &s
}
