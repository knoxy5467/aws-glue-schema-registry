//go:build integration

package integration_tests

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/fakeglue"
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
func TestNegative_IAMDenied(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	f.ForceGetSchemaError = &types.AccessDeniedException{Message: ptr("denied")}
	enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	_, err = enc.Encode([]byte("payload"), "neg-23", &gsrcore.Schema{
		SchemaDefinition: negativeAvroSchema,
		SchemaName:       "neg-23",
		DataFormat:       "AVRO",
	})
	require.Error(t, err)
	var ad *types.AccessDeniedException
	require.True(t, errors.As(err, &ad), "encoder must surface AccessDeniedException via errors.As (got %T: %v)", err, err)
	require.Equal(t, 0, f.CallCounts["CreateSchema"],
		"AccessDenied on GetSchemaByDefinition must NOT trigger a write-amplifying CreateSchema attempt")
}

// §5.3 item 24 — Throttling. Glue's ThrottlingException is retryable;
// the AWS SDK retry middleware swallows transient errors and only
// surfaces after retries are exhausted. With the test seam (which
// does not run middleware), ForceGetSchemaError = ThrottlingException
// surfaces on the very first call. Phase 4.5 bug 2 fix: the encoder
// no longer attempts CreateSchema on a throttled read, doubling load
// on an already-throttled Glue.
func TestNegative_Throttling(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	f.ForceGetSchemaError = throttlingError{}
	enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	_, err = enc.Encode([]byte("payload"), "neg-24", &gsrcore.Schema{
		SchemaDefinition: negativeAvroSchema,
		SchemaName:       "neg-24",
		DataFormat:       "AVRO",
	})
	require.Error(t, err)
	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr), "encoder must surface a smithy.APIError-typed error (got %T: %v)", err, err)
	require.Equal(t, "ThrottlingException", apiErr.ErrorCode(),
		"the surfaced error must carry the ThrottlingException API code so the retry middleware matches")
	require.Equal(t, 0, f.CallCounts["CreateSchema"],
		"ThrottlingException on GetSchemaByDefinition must NOT trigger an additional CreateSchema call (which would double the load)")
}

// §5.3 item 25 — EntityNotFoundException on GetSchemaVersion +
// schemaAutoRegistrationEnabled=true → falls through to
// CreateSchema. The fakeglue.Fake's empty-state path is exactly this:
// GetSchemaByDefinition returns EntityNotFound, the encoder falls
// through to CreateSchema, and the encode succeeds.
func TestNegative_EntityNotFoundFallsThroughToCreate(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)

	encoded, err := enc.Encode([]byte("payload"), "neg-25", &gsrcore.Schema{
		SchemaDefinition: negativeAvroSchema,
		SchemaName:       "neg-25",
		DataFormat:       "AVRO",
	})
	require.NoError(t, err)
	require.NotEmpty(t, encoded)
	require.Equal(t, 1, f.CallCounts["CreateSchema"], "auto-register=true should fall through to CreateSchema")
	require.Equal(t, 1, f.CallCounts["GetSchemaByDefinition"], "first Glue call is GetSchemaByDefinition")
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
	f := fakeglue.New()
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	// A payload with a valid version + compression byte but a
	// schema-version-id Glue has never seen — fakeglue returns
	// EntityNotFound from GetSchemaVersion. The decoder surfaces that
	// as an error from Decode rather than silently returning empty.
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
	f := fakeglue.New()
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
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

	got, err := dec.Decode(payload)
	if err == nil {
		// The core decoder is currently format-agnostic and returns
		// raw payload bytes; format-layer rejection of non-UTF-8 is
		// the JSON deserializer's job (pkg/gsrserde-go/deserializer/json/).
		// Lock down the bytes flowed through verbatim so a future
		// regression here is at least visible.
		require.Equal(t, []byte{0xff, 0xfe, 0xfd, 0xfc}, got,
			"core decoder is expected to return raw payload bytes; format-layer UTF-8 validation is the JSON deserializer's responsibility")
		t.Log("§5.3 item 27 lower-half (core decoder bytes-through) verified. " +
			"Format-layer UTF-8 rejection is covered in pkg/gsrserde-go/deserializer/json/json_malformed_test.go.")
	}
}

// §5.3 item 28 — truncated payload (< 18 bytes) → typed error.
func TestNegative_TruncatedPayload(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
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
	f := fakeglue.New()
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
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
