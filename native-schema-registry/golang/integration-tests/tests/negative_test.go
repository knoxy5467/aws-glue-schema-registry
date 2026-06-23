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

// throttlingError stands in for Glue's ThrottlingException — the
// Glue v2 SDK doesn't export a typed ThrottlingException struct; it
// surfaces throttling via the smithy.APIError-typed generic error
// code "ThrottlingException". The retry middleware matches on that
// code, and the encoder's err.Error() contains "ThrottlingException"
// regardless. Tests assert on the API code so a real Throttling
// response is matched the same way.
type throttlingError struct{}

func (throttlingError) Error() string                     { return "ThrottlingException: rate limited" }
func (throttlingError) ErrorCode() string                 { return "ThrottlingException" }
func (throttlingError) ErrorMessage() string              { return "rate limited" }
func (throttlingError) ErrorFault() smithy.ErrorFault     { return smithy.FaultClient }
var _ smithy.APIError = throttlingError{}

// Negative / error-path scenarios from plan §5.3 items 23-29 at the
// integration level. These mirror the Tier-1 negatives Phase 3
// already landed in pkg/gsrserde-go/core/{glue_negatives_test.go,
// payload_negatives_test.go} but drive them through the same fake
// GlueClient the lifecycle/compatibility tests use, so the integration
// tag's "everything compiles together" guarantee covers the negative
// branches too.

const negativeAvroSchema = `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`

// §5.3 item 23 — IAM denied → returns a typed error wrapping the SDK's
// AccessDeniedException. Phase 4.5 bug 2 fix: only ForceGetSchemaError
// needs to be set now that the encoder gates fall-through on the
// EntityNotFoundException type. The post-fix contract is that
// CreateSchema is NEVER called when GetSchemaByDefinition returns a
// non-EntityNotFound error — asserted explicitly via CallCounts.
//
// Phase 4.7: requiresFake=true. Driving an IAM-denied response on
// real Glue would require a separate role + AssumeRole flow, which
// is Phase 5 canary territory. The fake's Force* path proves the
// encoder's response-handling code; the canary will prove the IAM
// resolver chain itself.
func TestNegative_IAMDenied(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	h.Fake.ForceGetSchemaError = &types.AccessDeniedException{Message: ptr("denied")}
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "neg-23")
	_, err = enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
		SchemaDefinition: negativeAvroSchema,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.Error(t, err)
	var ad *types.AccessDeniedException
	require.True(t, errors.As(err, &ad), "encoder must surface AccessDeniedException via errors.As (got %T: %v)", err, err)
	require.Equal(t, 0, h.Fake.Count("CreateSchema"),
		"AccessDenied on GetSchemaByDefinition must NOT trigger a write-amplifying CreateSchema attempt")
}

// §5.3 item 24 — Throttling. Glue's ThrottlingException is retryable;
// the AWS SDK retry middleware swallows transient errors and only
// surfaces after retries are exhausted. With the test seam (which
// does not run middleware), ForceGetSchemaError = ThrottlingException
// surfaces on the very first call. Phase 4.5 bug 2 fix: the encoder
// no longer attempts CreateSchema on a throttled read, doubling load
// on an already-throttled Glue.
//
// Phase 4.7: requiresFake=true. Real throttling is intentionally
// rare and difficult to provoke deterministically; the canary
// harness (Phase 5) will exercise the SDK retry path against real
// Glue under load.
func TestNegative_Throttling(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	h.Fake.ForceGetSchemaError = throttlingError{}
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "neg-24")
	_, err = enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
		SchemaDefinition: negativeAvroSchema,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.Error(t, err)
	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr), "encoder must surface a smithy.APIError-typed error (got %T: %v)", err, err)
	require.Equal(t, "ThrottlingException", apiErr.ErrorCode(),
		"the surfaced error must carry the ThrottlingException API code so the retry middleware matches")
	require.Equal(t, 0, h.Fake.Count("CreateSchema"),
		"ThrottlingException on GetSchemaByDefinition must NOT trigger an additional CreateSchema call (which would double the load)")
}

// §5.3 item 25 — EntityNotFoundException on GetSchemaVersion +
// schemaAutoRegistrationEnabled=true → falls through to
// CreateSchema. The fakeglue.Fake's empty-state path is exactly this:
// GetSchemaByDefinition returns EntityNotFound, the encoder falls
// through to CreateSchema, and the encode succeeds.
//
// Phase 4.7: requiresFake=true. The exact-CallCount asserts are
// fake-only. Real Glue exercises the same fall-through path via
// auto-register-on-fresh-schema-name in TestCompatibility_BackwardV1ToV2
// and friends.
func TestNegative_EntityNotFoundFallsThroughToCreate(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "neg-25")
	encoded, err := enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
		SchemaDefinition: negativeAvroSchema,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	})
	require.NoError(t, err)
	require.NotEmpty(t, encoded)
	require.Equal(t, 1, h.Fake.Count("CreateSchema"), "auto-register=true should fall through to CreateSchema")
	require.Equal(t, 1, h.Fake.Count("GetSchemaByDefinition"), "first Glue call is GetSchemaByDefinition")
}

// §5.3 item 26 — malformed payload surfaces an error at the wire-
// format layer (truncated, bad version byte, bad compression byte).
// The byte-level wire-format negatives are covered by the dedicated
// items 28-29 below; this test pins the contract that core.GsrDecoder
// surfaces a non-nil error on a corrupted-header payload, which is
// the most common form of "malformed payload" a producer of a wrong
// wire-format dialect would emit.
//
// Per-format payload-body negatives (malformed JSON / Avro / Protobuf
// after a valid wire-format header) are covered exhaustively in
// pkg/gsrserde-go/deserializer/{json,avro}/*_malformed_test.go and
// pkg/gsrserde-go/core/payload_negatives_test.go — the Tier-1 layer
// is the right home for those because they don't need a Glue seam.
// An earlier draft of this test claimed to cover them at the
// integration level, but the assertion path bailed at schema lookup
// long before any format deserializer was reached; the review
// correctly flagged that as a misleading green. Cross-reference the
// Tier-1 coverage in the comment instead of pretending to mirror it.
func TestNegative_MalformedDecodePayload(t *testing.T) {
	t.Parallel()
	// Phase 4.8 nit #6: gate requiresFake=true. The decoder does
	// reach the GlueClient here (GetSchemaVersion lookup of an
	// unknown UUID), and the two backends return DIFFERENT typed
	// errors for THIS payload (real Glue rejects the all-zero UUID
	// as InvalidInputException; fakeglue's empty-map miss is
	// EntityNotFoundException). The assertion is "any non-nil
	// error", so the typed-error difference is not covered either
	// way, but be aware that gating this on fake DOES drop the
	// all-zero-UUID InvalidInputException path from real-mode runs.
	// TestNegative_UnknownVersionUUID below uses a non-zero UUID
	// and covers the EntityNotFound path on either backend; the
	// InvalidInputException path for all-zero UUIDs is not covered
	// in real mode after this gate. Trade-off accepted because the
	// assertion was already loose; tracked here so the next person
	// auditing real-mode coverage knows what they're looking at.
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	// A payload with a valid version + compression byte but a
	// schema-version-id Glue has never seen. Backend behavior:
	//   - fakeglue: returns *types.EntityNotFoundException from
	//     GetSchemaVersion (the empty in-memory map miss).
	//   - real Glue: returns *types.InvalidInputException for this
	//     specific all-zero UUID (server rejects "00000000-..." as
	//     not a valid version id) — NOT EntityNotFound. A random
	//     non-zero unknown UUID would surface as EntityNotFound.
	// Either way the decoder MUST surface a non-nil error rather than
	// silently returning empty, which is the only assertion below.
	// Phase 4.8 nit #11: doc updated to reflect real-Glue's actual
	// error type for the all-zero UUID case.
	bad := []byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		'm', 'a', 'l', 'f', 'o', 'r', 'm', 'e', 'd'}
	_, err = dec.Decode(bad)
	require.Error(t, err, "decoder must error when wire-format references an unknown schema-version-id")
}

// §5.3 item 27 — UTF-8 vs non-UTF-8 JSON payload rejected.
//
// To actually exercise the JSON deserializer path we must (a) seed
// fakeglue with a JSON schema version, (b) build a wire-format payload
// whose UUID resolves to that schema and whose body is non-UTF-8.
// An earlier draft of this test used a zero UUID — fakeglue returned
// EntityNotFound and the body bytes never reached the JSON layer,
// rendering the assertion trivially-true. Fixed by priming the
// decoder cache directly via gsrcore.PrimeSchemaCache so we don't
// need to also wire up CreateSchema flow.
func TestNegative_NonUTF8Path(t *testing.T) {
	t.Parallel()
	// Phase 4.8 nit #6: although the body of this test primes the
	// decoder cache directly via PrimeSchemaCache and never reaches
	// the underlying GlueClient, newGlueHandle still pays the
	// LoadDefaultConfig + Credentials.Retrieve cost in real mode.
	// scenarioGate(t, false, true) so the real-mode run skips it
	// without paying that cost — there is no real-mode value to be
	// gained since GlueClient is never called.
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	// Prime the cache so DecodeWireFormat -> schema lookup -> JSON
	// deserialize is the actual code path.
	const versionUUID = "00112233-4455-6677-8899-aabbccddeeff"
	gsrcore.PrimeSchemaCache(dec, versionUUID, &gsrcore.Schema{
		SchemaName:       "neg-27",
		SchemaDefinition: `{"type":"object"}`,
		DataFormat:       "JSON",
		SchemaVersionID:  versionUUID,
	})

	uuidBytes := []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
		0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
	payload := []byte{0x03, 0x00}
	payload = append(payload, uuidBytes...)
	// Invalid UTF-8 (lone continuation bytes); a regression that lets
	// the decoder return these bytes uncritically would fail downstream
	// JSON parsing rather than be caught at the wire-format layer.
	payload = append(payload, 0xff, 0xfe, 0xfd, 0xfc)

	// The core decoder is currently format-agnostic and returns raw
	// payload bytes; format-layer UTF-8 rejection lives in the JSON
	// deserializer (pkg/gsrserde-go/deserializer/json/). Lock down
	// THIS layer's contract — bytes flow through verbatim — with an
	// unconditional require.NoError + require.Equal. Earlier draft
	// wrapped the assertion in `if err == nil`, which made the test a
	// no-op if Decode ever started returning an error (the assertion
	// never ran). Phase 4 review flagged that as the exact false-green
	// pattern the §5.3 row 27 was supposed to catch.
	got, err := dec.Decode(payload)
	require.NoError(t, err, "core decoder is format-agnostic and must return raw payload bytes here")
	require.Equal(t, []byte{0xff, 0xfe, 0xfd, 0xfc}, got,
		"core decoder must return the non-UTF-8 bytes verbatim; UTF-8 rejection is the JSON deserializer's job")
}

// §5.3 item 28 — truncated payload (< 18 bytes) → typed error.
func TestNegative_TruncatedPayload(t *testing.T) {
	t.Parallel()
	// Phase 4.8 nit #6: gate requiresFake=true. The truncation check
	// short-circuits inside DecodeWireFormat (size guard) before any
	// GlueClient call, so there is nothing for real-Glue mode to
	// contribute. Skip on real to avoid paying the LoadDefaultConfig
	// + Credentials.Retrieve probe.
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	short := []byte{0x03, 0x00, 0x01, 0x02}
	_, err = dec.Decode(short)
	require.Error(t, err)
	require.True(t,
		errors.Is(err, gsrcore.ErrIncompatibleData) || strings.Contains(err.Error(), "size"),
		"truncated payload must surface ErrIncompatibleData (got: %v)", err)
}

// §5.3 item 29 — header version byte 0x03 but corrupt UUID → typed
// error.
//
// "Corrupt UUID" can mean one of two things: (a) bytes that don't
// decode to a valid UUID (impossible at the wire-format level since
// 16 bytes always decode to *some* UUID), or (b) a UUID that
// references a schema-version-id Glue doesn't recognize. (b) is the
// behavior the encoder/decoder actually hit at runtime, so that's
// what this test pins.
func TestNegative_UnknownVersionUUID(t *testing.T) {
	t.Parallel()
	// Unknown-UUID lookup goes through GetSchemaVersion; both backends
	// surface an error here. Backend-agnostic.
	h := newGlueHandle(t)
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	// Hand-build a wire-format payload with a UUID Glue doesn't know.
	pkt := make([]byte, gsrcore.WireFormatHeaderSize+3)
	pkt[0] = 0x03
	pkt[1] = 0x00
	for i := 0; i < 16; i++ {
		pkt[2+i] = byte(0xA0 + i)
	}
	pkt[gsrcore.WireFormatHeaderSize] = 'a'
	pkt[gsrcore.WireFormatHeaderSize+1] = 'b'
	pkt[gsrcore.WireFormatHeaderSize+2] = 'c'

	_, err = dec.Decode(pkt)
	require.Error(t, err, "decoder must surface an error when Glue can't resolve the schema-version UUID")
}
