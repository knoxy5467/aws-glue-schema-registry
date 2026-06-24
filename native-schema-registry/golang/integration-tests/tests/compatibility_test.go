//go:build integration

package integration_tests

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
)

// Compatibility / evolution scenarios from plan §5.3 items 18-22.
//
// Real cross-version compatibility enforcement happens on the Glue
// service side. fakeglue does NOT model Glue's BACKWARD / FORWARD /
// FULL compatibility-check semantics; that would require porting the
// JSON Schema / Avro / Protobuf compatibility checkers into Go, which
// is out of scope for Phase 4.
//
// So these tests verify the surface the Go client *can* control: that
// the compatibility config flows into CreateSchema's request, and
// that the encoder surfaces server-side compatibility errors when
// the (fake) Glue rejects an evolution. Real evolution-against-real-
// Glue lives in the AWS_INTEGRATION=1 nightly runs.

const (
	schemaV1 = `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`
	schemaV2 = `{"type":"record","name":"User","fields":[{"name":"id","type":"string"},{"name":"email","type":["null","string"],"default":null}]}`
	schemaV3 = `{"type":"record","name":"User","fields":[{"name":"id","type":"string"},{"name":"email","type":["null","string"],"default":null},{"name":"age","type":["null","int"],"default":null}]}`
)

// compatibilityRoundTrip runs the same fresh-encoder-per-iteration
// loop used by items 18 / 19 / 20 / 21. For each schema definition
// in defs, it builds a NEW encoder (so the per-name cache keyed by
// `schemaName:dataFormat` does not short-circuit later iterations
// into reusing v1's version-id) and asserts encode/decode succeed.
//
// IMPORTANT — what this DOES and does NOT prove:
//   - It DOES prove that every definition in defs serializes through
//     the wire-format and round-trips through the GlueClient under
//     this Compatibility= setting.
//   - It DOES exercise the per-name cache-defeat property — but only
//     STRUCTURALLY (fresh encoders). Whether the cache actually had
//     to be bypassed cannot be asserted backend-agnostically; items 19
//     and 21 add a fakeglue Snapshot-based total-count assertion to
//     close that gap.
//   - It does NOT prove server-side compatibility enforcement. On
//     real Glue, iter 2's CreateSchema hits AlreadyExistsException
//     and the encoder falls through to RegisterSchemaVersion, which
//     carries no Compatibility field — so a regression that dropped
//     `Compatibility` from the CreateSchemaInput would still let
//     this test pass (iter 1 would create the schema with the server
//     default of NONE, iter 2's v2 would register cleanly via the
//     fall-through). Server-side BACKWARD enforcement is proven
//     exclusively by TestCompatibility_IncompatibleRejected_Real,
//     which pushes a deliberately incompatible v2.
//
// Phase 4.8 review finding #8: extracted from items 18/19/20/21 to
// keep the four tests in lock-step if the round-trip envelope (e.g.,
// wire-format prefix assertions) ever tightens.
func compatibilityRoundTrip(t *testing.T, h *glueHandle, schemaName, compatMode string, defs []string) {
	t.Helper()
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	for i, def := range defs {
		enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
			RegistryName:                  "default-registry",
			Compatibility:                 compatMode,
			SchemaAutoRegistrationEnabled: true,
		})
		require.NoError(t, err)

		encoded, err := enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
			SchemaDefinition: def,
			SchemaName:       schemaName,
			DataFormat:       "AVRO",
		})
		require.NoError(t, err, "iteration %d (Compatibility=%s) encode", i+1, compatMode)

		decoded, err := dec.Decode(encoded)
		require.NoError(t, err, "iteration %d decode", i+1)
		require.Equal(t, []byte("payload"), decoded)
	}
}

// §5.3 item 18 — BACKWARD evolution v1→v2: producer publishes v1,
// consumer with v2 reads successfully.
//
// In Glue, "BACKWARD compatibility" is a property of the schema-name,
// not of a specific encode. The Go side's job is to send the v1
// schema definition to the encoder; the wire format carries the v1
// version-id, and the consumer's decoder looks up that exact v1
// schema. That bytes-flow is what this test pins — it runs against
// either backend.
//
// What this test does NOT prove: server-side BACKWARD enforcement.
// See compatibilityRoundTrip's docstring and
// TestCompatibility_IncompatibleRejected_Real for the canonical
// server-side enforcement test.
func TestCompatibility_BackwardV1ToV2(t *testing.T) {
	t.Parallel()
	h := newGlueHandle(t)
	schemaName := randomGlueName(t, "compat-18")
	if h.Cleanup != nil {
		h.Cleanup.TrackSchema("default-registry", schemaName)
	}
	compatibilityRoundTrip(t, h, schemaName, "BACKWARD", []string{schemaV1, schemaV2})
}

// §5.3 item 19 — BACKWARD_ALL across three versions. Builds on
// compatibilityRoundTrip and adds the fakeglue Snapshot-based
// total-count assertion that ACTUALLY proves the cache-defeat
// pattern (item 18 / 20 cannot make this assertion because they're
// backend-agnostic).
//
// Phase 4.7: requiresFake=true. The Snapshot()-based total assertion
// uses fakeglue introspection.
func TestCompatibility_BackwardAll_ThreeVersions(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)

	schemaName := randomGlueName(t, "compat-19")
	compatibilityRoundTrip(t, h, schemaName, "BACKWARD_ALL", []string{schemaV1, schemaV2, schemaV3})

	// Each fresh encoder + distinct definition forces a new Glue
	// resolution: iter 1 CreateSchema, iters 2-3 RegisterSchemaVersion
	// (fakeglue's CreateSchema-on-existing returns a fresh UUID; real
	// Glue would 409 and the encoder would fall through to Register).
	// Total CreateSchema+RegisterSchemaVersion should equal 3 — this
	// is the assertion that proves the cache-defeat structurally
	// worked (vs. the encoder silently short-circuiting on v2/v3).
	total := h.Fake.Snapshot()["CreateSchema"] + h.Fake.Snapshot()["RegisterSchemaVersion"]
	require.Equal(t, 3, total,
		"BACKWARD_ALL across three versions must reach Glue three times (CreateSchema or RegisterSchemaVersion); saw %d", total)
}

// §5.3 item 20 — FORWARD evolution v2→v1. Producer publishes v2,
// consumer reads with v1 schema. Same wire-flow contract as item 18
// (v2's bytes carry v2's version-id; consumer fetches v2's schema).
// Backend-agnostic. v1 is forward-compatible from v2: v2 adds a
// nullable field, so reading v2 bytes with a v1 reader-schema
// drops the field. See compatibilityRoundTrip's docstring for the
// "what this does NOT prove" boundary.
func TestCompatibility_ForwardV2ToV1(t *testing.T) {
	t.Parallel()
	h := newGlueHandle(t)
	schemaName := randomGlueName(t, "compat-20")
	if h.Cleanup != nil {
		h.Cleanup.TrackSchema("default-registry", schemaName)
	}
	compatibilityRoundTrip(t, h, schemaName, "FORWARD", []string{schemaV2, schemaV1})
}

// §5.3 item 21 — FULL evolution both directions. Builds on
// compatibilityRoundTrip and adds the fakeglue Snapshot-based
// total-count assertion to prove the cache-defeat structurally
// worked (same shape as item 19).
//
// Phase 4.7: requiresFake=true.
func TestCompatibility_FullBothDirections(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)

	schemaName := randomGlueName(t, "compat-21")
	compatibilityRoundTrip(t, h, schemaName, "FULL", []string{schemaV1, schemaV2})

	total := h.Fake.Snapshot()["CreateSchema"] + h.Fake.Snapshot()["RegisterSchemaVersion"]
	require.Equal(t, 2, total,
		"FULL evolution v1+v2 must reach Glue twice; saw %d", total)
}

// §5.3 item 22 — incompatible schema change rejected at CreateSchema
// time when compatibility is set. Drive this on fake by making
// CreateSchema return an InvalidInputException; the real-mode
// companion (TestCompatibility_IncompatibleRejected_Real) drives a
// genuinely incompatible evolution through actual Glue server-side
// enforcement.
//
// Phase 4.7: requiresFake=true. The Force* mechanism is fake-only.
func TestCompatibility_IncompatibleRejected(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	h.Fake.ForceCreateError = &types.InvalidInputException{Message: ptr("Schema version is incompatible with the existing schema")}

	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "BACKWARD",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "compat-22")
	_, err = enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
		SchemaDefinition: schemaV1,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.Error(t, err, "encoder must surface Glue's compatibility rejection")
	var inv *types.InvalidInputException
	require.True(t, errors.As(err, &inv), "error must wrap Glue's InvalidInputException (got %T: %v)", err, err)
}

// TestCompatibility_IncompatibleRejected_Real is the requiresReal
// companion. Against real Glue:
//   - register v1 of a schema with BACKWARD compatibility set,
//   - attempt to register an *incompatible* v2 (e.g., a renamed required
//     field), and
//   - assert the encoder returns an error.
//
// This is the only way to prove the *server* enforces compatibility,
// which is the entire point of §5.3 item 22. The fake's
// ForceCreateError version proves only that the encoder doesn't
// swallow the SDK exception.
func TestCompatibility_IncompatibleRejected_Real(t *testing.T) {
	t.Parallel()
	scenarioGate(t, true, false)
	h := newGlueHandle(t)

	schemaName := randomGlueName(t, "compat-22-real")
	h.Cleanup.TrackSchema("default-registry", schemaName)

	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "BACKWARD",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)

	_, err = enc.Encode([]byte("payload-v1"), schemaName, &gsrcore.Schema{
		SchemaDefinition: schemaV1,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.NoError(t, err, "v1 should register cleanly")

	// v2-incompatible: rename "id" → "userId", a non-default-bearing
	// required field swap. BACKWARD requires consumers using v2 schema
	// to read v1 data; renaming a required field breaks that.
	incompatibleV2 := `{"type":"record","name":"User","fields":[{"name":"userId","type":"string"}]}`
	encFresh, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "BACKWARD",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)
	_, err = encFresh.Encode([]byte("payload-v2"), schemaName, &gsrcore.Schema{
		SchemaDefinition: incompatibleV2,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.Error(t, err, "real Glue must reject incompatible v2 under BACKWARD compatibility")

	// Code-review finding #3 (Phase 4.7): plain require.Error here
	// passes on ANY error — a transient ThrottlingException or
	// IAM-denied would falsely satisfy the test and we'd ship a
	// regression where the encoder silently dropped server-side
	// compatibility enforcement. Tighten: the error must either be a
	// typed Glue InvalidInputException OR a smithy.APIError whose
	// code/message names compatibility / invalid input. ThrottlingException
	// (Code = "ThrottlingException") would NOT satisfy this — it would
	// fail the test, which is the right behavior under throttling
	// because we cannot prove anything about compatibility under a
	// throttled call.
	var inv *types.InvalidInputException
	if errors.As(err, &inv) {
		return // valid; this is the explicit Glue type for compat rejection
	}
	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr),
		"compatibility-rejection error must wrap a typed Glue / smithy error (got %T: %v)", err, err)
	require.NotEqual(t, "ThrottlingException", apiErr.ErrorCode(),
		"ThrottlingException is not a valid compatibility-rejection — re-run after throttling clears")
	lowerMsg := strings.ToLower(apiErr.ErrorMessage())
	require.True(t,
		strings.Contains(lowerMsg, "compat") || strings.Contains(lowerMsg, "invalid"),
		"compatibility-rejection error message must mention compatibility or invalid input (got code=%q msg=%q)",
		apiErr.ErrorCode(), apiErr.ErrorMessage())
}

func ptr[T any](v T) *T { return &v }

// --- Tier-2 _Real companions (items 18-21) ---
//
// Each test is gated scenarioGate(t, true, false) + requireAWSIntegration(t)
// so it only runs when GSR_GLUE=real AND AWS_INTEGRATION=1. Under the
// default fake backend all four tests SKIP immediately.
//
// Per spec §10 non-goal #5: these tests assert bytes-flow round-trip
// (each version encodes to its own GSR wire-frame UUID; a shared
// GsrDecoder resolves those UUIDs via real Glue and returns the original
// payload verbatim). They do NOT assert Avro reader-schema projection
// (that the v2-aware reader drops extra fields when reading v1 bytes).
// Reader-schema projection is the Phase-5 canary's responsibility:
// plan §6 Canary Test Plan / §7 Phase 5.
//
// Cleanup: every _Real test registers created schemas via
// h.Cleanup.TrackSchema so they are deleted during t.Cleanup even on
// test failure.

// TestCompatibility_BackwardV1ToV2_Real is the requiresReal companion for
// §5.3 item 18 (BACKWARD evolution v1→v2). Against real Glue it:
//  1. Registers v1 (CreateSchema) and v2 (RegisterSchemaVersion) with
//     Compatibility="BACKWARD".
//  2. Encodes a payload for each version via compatibilityRoundTrip.
//  3. Decodes every produced byte stream through a shared GsrDecoder and
//     asserts verbatim equality — this is the round-2 spec M1 requirement.
//
// What this test does NOT prove:
//   - Avro reader-schema projection (§10 non-goal #5): asserts bytes-flow
//     round-trip only; actual projection deferred to Phase-5 canary.
func TestCompatibility_BackwardV1ToV2_Real(t *testing.T) {
	t.Parallel()
	scenarioGate(t, true, false)
	requireAWSIntegration(t)

	h := newGlueHandle(t)
	schemaName := randomGlueName(t, "compat-18-real")
	h.Cleanup.TrackSchema("default-registry", schemaName)

	compatibilityRoundTrip(t, h, schemaName, "BACKWARD", []string{schemaV1, schemaV2})
}

// TestCompatibility_BackwardAll_ThreeVersions_Real is the requiresReal
// companion for §5.3 item 19 (BACKWARD_ALL across v1, v2, v3). Against real
// Glue it:
//  1. Registers v1 via CreateSchema, then v2 and v3 via RegisterSchemaVersion
//     (Glue auto-falls-through on AlreadyExists in subsequent iterations of
//     compatibilityRoundTrip's per-encoder loop).
//  2. Encodes a payload for each version.
//  3. Decodes all three produced byte streams through a shared GsrDecoder and
//     asserts verbatim equality.
//
// Note: the fakeglue Snapshot()-based total-call assertion from the fake
// companion (TestCompatibility_BackwardAll_ThreeVersions) is intentionally
// omitted here — real Glue has no call-count introspection surface.
//
// What this test does NOT prove:
//   - Avro reader-schema projection (§10 non-goal #5): bytes-flow round-trip
//     only; projection deferred to Phase-5 canary.
func TestCompatibility_BackwardAll_ThreeVersions_Real(t *testing.T) {
	t.Parallel()
	scenarioGate(t, true, false)
	requireAWSIntegration(t)

	h := newGlueHandle(t)
	schemaName := randomGlueName(t, "compat-19-real")
	h.Cleanup.TrackSchema("default-registry", schemaName)

	compatibilityRoundTrip(t, h, schemaName, "BACKWARD_ALL", []string{schemaV1, schemaV2, schemaV3})
}

// TestCompatibility_ForwardV2ToV1_Real is the requiresReal companion for §5.3
// item 20 (FORWARD evolution v2→v1). Against real Glue:
//  1. Registers v2 (CreateSchema) and v1 (RegisterSchemaVersion) under
//     Compatibility="FORWARD".
//  2. Encodes a payload for each definition.
//  3. Decodes both produced byte streams through a shared GsrDecoder and
//     asserts verbatim equality — the decoder resolves each version-id from
//     real Glue's schema store.
//
// FORWARD compatibility means v2 was the first registered schema and v1 is
// registered as a compatible predecessor. compatibilityRoundTrip iterates
// [schemaV2, schemaV1] so v2 is registered first via CreateSchema; v1 is then
// registered via the fall-through to RegisterSchemaVersion.
//
// What this test does NOT prove:
//   - Avro reader-schema projection (§10 non-goal #5): this test exercises
//     bytes-flow round-trip only (the wire-format UUID for each version is
//     resolved by GsrDecoder and the original payload bytes are returned
//     verbatim). Actual Avro reader-schema projection — where a v1-schema
//     Avro reader drops the extra fields present in v2 bytes — is deferred
//     to the Phase-5 canary.
func TestCompatibility_ForwardV2ToV1_Real(t *testing.T) {
	t.Parallel()
	scenarioGate(t, true, false)
	requireAWSIntegration(t)

	h := newGlueHandle(t)
	schemaName := randomGlueName(t, "compat-20-real")
	h.Cleanup.TrackSchema("default-registry", schemaName)

	compatibilityRoundTrip(t, h, schemaName, "FORWARD", []string{schemaV2, schemaV1})
}

// TestCompatibility_FullBothDirections_Real is the requiresReal companion for
// §5.3 item 21 (FULL evolution — compatible in both directions). Against real
// Glue:
//  1. Registers v1 (CreateSchema) and v2 (RegisterSchemaVersion) under
//     Compatibility="FULL".
//  2. Encodes a payload for each version.
//  3. Decodes both produced byte streams through a shared GsrDecoder and
//     asserts the decoded bytes are the original payload verbatim (round-2
//     spec M1 fix: per-version decode-back is required, not optional).
//
// Note: the fakeglue Snapshot()-based total-call assertion from the fake
// companion (TestCompatibility_FullBothDirections) is intentionally omitted —
// real Glue has no call-count introspection surface.
//
// What this test does NOT prove:
//   - Avro reader-schema projection (§10 non-goal #5): asserts bytes-flow
//     round-trip only (return the original payload verbatim); projection
//     deferred to Phase-5 canary.
func TestCompatibility_FullBothDirections_Real(t *testing.T) {
	t.Parallel()
	scenarioGate(t, true, false)
	requireAWSIntegration(t)

	h := newGlueHandle(t)
	schemaName := randomGlueName(t, "compat-21-real")
	h.Cleanup.TrackSchema("default-registry", schemaName)

	compatibilityRoundTrip(t, h, schemaName, "FULL", []string{schemaV1, schemaV2})
}
