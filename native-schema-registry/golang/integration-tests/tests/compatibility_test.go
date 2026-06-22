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

// §5.3 item 18 — BACKWARD evolution v1→v2: producer publishes v1,
// consumer with v2 reads successfully.
//
// In Glue, "BACKWARD compatibility" is a property of the schema-name,
// not of a specific encode. The Go side's job is to send the v1
// schema definition to the encoder; the wire format carries the v1
// version-id, and the consumer's decoder looks up that exact v1
// schema. That bytes-flow is what this test pins — it runs against
// either backend.
func TestCompatibility_BackwardV1ToV2(t *testing.T) {
	t.Parallel()
	h := newGlueHandle(t)
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "BACKWARD",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "compat-18")
	if h.Cleanup != nil {
		h.Cleanup.TrackSchema("default-registry", schemaName)
	}
	encoded, err := enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
		SchemaDefinition: schemaV1,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.NoError(t, err)

	// Decode resolves the wire-format version-id back through Glue —
	// proving v1's bytes land at v1's schema.
	decoded, err := dec.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), decoded)
}

// §5.3 item 19 — BACKWARD_ALL across three versions. The encoder's
// cache is keyed by `schemaName:dataFormat` (encoder.go:154), so a
// single encoder instance reused across iterations would short-circuit
// on the cached version-id for iterations 2 and 3 — fakeglue would
// never see v2/v3 and the BACKWARD_ALL contract would not actually
// be exercised. Build a fresh encoder per iteration to defeat the
// cache, and assert CallCounts["CreateSchema"] tracks the iteration
// count so any regression in v2/v3 registration surfaces here.
//
// Phase 4.7: requiresFake=true. The Snapshot()-based total assertion
// uses fakeglue introspection.
func TestCompatibility_BackwardAll_ThreeVersions(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "compat-19")
	for i, def := range []string{schemaV1, schemaV2, schemaV3} {
		enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
			RegistryName:                  "default-registry",
			Compatibility:                 "BACKWARD_ALL",
			SchemaAutoRegistrationEnabled: true,
		})
		require.NoError(t, err)

		encoded, err := enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
			SchemaDefinition: def,
			SchemaName:       schemaName,
			DataFormat:       "AVRO",
		})
		require.NoError(t, err, "iteration %d (definition %d)", i, i+1)
		decoded, err := dec.Decode(encoded)
		require.NoError(t, err, "iteration %d decode", i)
		require.Equal(t, []byte("payload"), decoded)
	}

	// Each fresh encoder + distinct definition forces a new Glue
	// resolution: iter 1 CreateSchema, iters 2-3 RegisterSchemaVersion
	// (fakeglue's CreateSchema-on-existing returns a fresh UUID; real
	// Glue would 409 and the encoder would fall through to Register).
	// Total CreateSchema+RegisterSchemaVersion should equal 3.
	total := h.Fake.Snapshot()["CreateSchema"] + h.Fake.Snapshot()["RegisterSchemaVersion"]
	require.Equal(t, 3, total,
		"BACKWARD_ALL across three versions must reach Glue three times (CreateSchema or RegisterSchemaVersion); saw %d", total)
}

// §5.3 item 20 — FORWARD evolution v2→v1. Producer publishes v2,
// consumer reads with v1 schema. Same wire-flow contract as item 18
// (v2's bytes carry v2's version-id; consumer fetches v2's schema).
// Backend-agnostic.
func TestCompatibility_ForwardV2ToV1(t *testing.T) {
	t.Parallel()
	h := newGlueHandle(t)
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "FORWARD",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "compat-20")
	if h.Cleanup != nil {
		h.Cleanup.TrackSchema("default-registry", schemaName)
	}
	encoded, err := enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
		SchemaDefinition: schemaV2,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.NoError(t, err)
	decoded, err := dec.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), decoded)
}

// §5.3 item 21 — FULL evolution both directions. Same cache-defeat
// pattern as item 19: fresh encoder per iteration so the cache
// doesn't silently elide the second Glue resolution.
//
// Phase 4.7: requiresFake=true. Same Snapshot()-based assertion.
func TestCompatibility_FullBothDirections(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "compat-21")
	for i, def := range []string{schemaV1, schemaV2} {
		enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
			RegistryName:                  "default-registry",
			Compatibility:                 "FULL",
			SchemaAutoRegistrationEnabled: true,
		})
		require.NoError(t, err)

		encoded, err := enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
			SchemaDefinition: def,
			SchemaName:       schemaName,
			DataFormat:       "AVRO",
		})
		require.NoError(t, err, "iteration %d (definition %d)", i, i+1)
		decoded, err := dec.Decode(encoded)
		require.NoError(t, err, "iteration %d decode", i)
		require.Equal(t, []byte("payload"), decoded)
	}

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
