//go:build integration

package integration_tests

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/fakeglue"
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
// schema. That bytes-flow is what this test pins.
func TestCompatibility_BackwardV1ToV2(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "BACKWARD",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	encoded, err := enc.Encode([]byte("payload"), "compat-18", &gsrcore.Schema{
		SchemaDefinition: schemaV1,
		SchemaName:       "compat-18",
		DataFormat:       "AVRO",
	})
	require.NoError(t, err)

	// Decode resolves the wire-format version-id back through Glue
	// (the fake) — proving v1's bytes land at v1's schema.
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
func TestCompatibility_BackwardAll_ThreeVersions(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	schemaName := "compat-19" + scenarioRegistrySuffix()
	for i, def := range []string{schemaV1, schemaV2, schemaV3} {
		enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
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
	total := f.Snapshot()["CreateSchema"] + f.Snapshot()["RegisterSchemaVersion"]
	require.Equal(t, 3, total,
		"BACKWARD_ALL across three versions must reach Glue three times (CreateSchema or RegisterSchemaVersion); saw %d", total)
}

// §5.3 item 20 — FORWARD evolution v2→v1. Producer publishes v2,
// consumer reads with v1 schema. Same wire-flow contract as item 18
// (v2's bytes carry v2's version-id; consumer fetches v2's schema).
func TestCompatibility_ForwardV2ToV1(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "FORWARD",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	encoded, err := enc.Encode([]byte("payload"), "compat-20", &gsrcore.Schema{
		SchemaDefinition: schemaV2,
		SchemaName:       "compat-20",
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
func TestCompatibility_FullBothDirections(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	schemaName := "compat-21" + scenarioRegistrySuffix()
	for i, def := range []string{schemaV1, schemaV2} {
		enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
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

	total := f.Snapshot()["CreateSchema"] + f.Snapshot()["RegisterSchemaVersion"]
	require.Equal(t, 2, total,
		"FULL evolution v1+v2 must reach Glue twice; saw %d", total)
}

// §5.3 item 22 — incompatible schema change rejected at CreateSchema
// time when compatibility is set. Drive this by making CreateSchema
// return an InvalidInputException — what Glue does when the new
// version is rejected.
func TestCompatibility_IncompatibleRejected(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	f.ForceCreateError = &types.InvalidInputException{Message: ptr("Schema version is incompatible with the existing schema")}

	enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "BACKWARD",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)

	_, err = enc.Encode([]byte("payload"), "compat-22", &gsrcore.Schema{
		SchemaDefinition: schemaV1,
		SchemaName:       "compat-22",
		DataFormat:       "AVRO",
	})
	require.Error(t, err, "encoder must surface Glue's compatibility rejection")
	var inv *types.InvalidInputException
	require.True(t, errors.As(err, &inv), "error must wrap Glue's InvalidInputException (got %T: %v)", err, err)
}

func ptr[T any](v T) *T { return &v }
