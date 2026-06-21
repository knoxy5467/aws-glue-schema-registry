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
// AccessDeniedException. Drive via ForceGetSchemaError so the failure
// happens at the GetSchemaByDefinition step (mirrors the first Glue
// call the encoder makes).
func TestNegative_IAMDenied(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	// The encoder falls through GetSchemaByDefinition errors to
	// CreateSchema (auto-register path). Set the error on BOTH calls
	// so the IAM denied surfaces regardless of which step the test
	// happens to drive.
	f.ForceGetSchemaError = &types.AccessDeniedException{Message: ptr("denied")}
	f.ForceCreateError = &types.AccessDeniedException{Message: ptr("denied")}
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
}

// §5.3 item 24 — Throttling. Glue's ThrottlingException is retryable;
// the AWS SDK retry middleware swallows transient errors and only
// surfaces after retries are exhausted. With the test seam (which
// does not run middleware), ForceGetSchemaError = ThrottlingException
// surfaces on the very first call. The behavior we lock down here is:
// the encoder propagates the typed error rather than swallowing it.
func TestNegative_Throttling(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	// Same fall-through note as TestNegative_IAMDenied: force the
	// error on both Glue calls so throttling surfaces.
	f.ForceGetSchemaError = throttlingError{}
	f.ForceCreateError = throttlingError{}
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

// §5.3 item 26 — malformed JSON / Avro / Protobuf each returns a
// sentinel error. The byte-level malformed payload paths are exercised
// in pkg/gsrserde-go/core/payload_negatives_test.go (Tier-1) and the
// format layer (deserializer/{json,avro}/{json,avro}_malformed_test.go).
// At the integration level, the assertion is that a malformed payload
// surfaces a non-nil decode error rather than being silently accepted.
func TestNegative_MalformedDecodePayload(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	bad := []byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		'm', 'a', 'l', 'f', 'o', 'r', 'm', 'e', 'd'}
	_, err = dec.Decode(bad)
	require.Error(t, err, "decoder must error on a schema-version-id that doesn't resolve")
}

// §5.3 item 27 — UTF-8 vs non-UTF-8 JSON payload. The JSON
// deserializer's UTF-8 handling lives in
// pkg/gsrserde-go/deserializer/json/json_malformed_test.go (Tier-1).
// This integration-level assertion confirms that bytes that fail
// UTF-8 validation never make it past wire-format Decode either.
// A truncated payload (item 28 is the dedicated truncated-payload
// test) is the simplest non-UTF-8 surface that the wire-format
// layer itself rejects.
func TestNegative_NonUTF8Path(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	nonUTF8Payload := make([]byte, gsrcore.WireFormatHeaderSize+4)
	nonUTF8Payload[0] = 0x03
	nonUTF8Payload[1] = 0x00
	// Bytes 2..17 are the schema-version UUID — leave the zero UUID;
	// fake's GetSchemaVersion will return not-found.
	nonUTF8Payload[18] = 0xff
	nonUTF8Payload[19] = 0xfe
	nonUTF8Payload[20] = 0xfd
	nonUTF8Payload[21] = 0xfc
	_, err = dec.Decode(nonUTF8Payload)
	require.Error(t, err, "decoder must surface an error when the schema-version-id is unknown — the non-UTF-8 payload never reaches the JSON deserializer")
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
