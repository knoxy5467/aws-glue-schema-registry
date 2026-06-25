//go:build integration

package integration_tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
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

// TestNegative_MalformedDecodePayload is the unknown-UUID guard for §5.3
// items 26/29 — NOT the canonical item-26 per-format sentinel coverage.
//
// This test feeds a GSR-framed payload whose UUID is the all-zeros value
// and asserts that the decoder returns a non-nil error. The error origin
// varies by backend (fakeglue: EntityNotFoundException; real Glue:
// InvalidInputException for the all-zero UUID). In both cases the contract
// is simply "a non-nil error is surfaced" — this is the unknown-UUID guard,
// not a per-format sentinel test.
//
// Canonical item-26 Tier-2 coverage — per-format ErrMalformedJSON /
// ErrMalformedAvro / ErrMalformedProtobuf sentinel tests that prime the
// decoder cache and exercise the format deserializer — lives in
// payload_negatives_test.go (TestPayloadNegatives_MalformedJSON_SurfacesSentinel,
// TestPayloadNegatives_MalformedAvro_SurfacesSentinel,
// TestPayloadNegatives_MalformedProtobuf_SurfacesSentinel). The earlier
// draft of this test claimed to cover that path, but the assertion bailed
// at schema lookup long before any format deserializer was reached; the
// Phase 4.9 audit correctly flagged that gap. Cross-reference the
// Tier-1 coverage in pkg/gsrserde-go/deserializer/{json,avro,protobuf}/
// *_malformed_test.go and pkg/gsrserde-go/core/payload_negatives_test.go.
//
// Phase 4.12 §3.9 last paragraph — docstring-only update; test body
// unchanged (PBI-4.12-8 AC-9).
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

// spyGlueClient wraps any gsrcore.GlueClient and counts CreateSchema calls
// via an atomic counter. All other methods delegate unchanged to the inner
// client. Used by TestNegative_EntityNotFound_FallsThroughToCreate_Real to
// assert exactly two CreateSchema calls (initial auto-register + re-register
// after out-of-band schema deletion) without Glue-side call-count APIs
// (which real Glue doesn't expose).
//
// The counter is shared across both encoder instances built in the test so
// the total CreateSchema count across the full test body is observable.
type spyGlueClient struct {
	inner              gsrcore.GlueClient
	createSchemaCount  atomic.Int32
}

func (s *spyGlueClient) CreateSchema(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
	s.createSchemaCount.Add(1)
	return s.inner.CreateSchema(ctx, params, optFns...)
}

func (s *spyGlueClient) GetSchemaByDefinition(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
	return s.inner.GetSchemaByDefinition(ctx, params, optFns...)
}

func (s *spyGlueClient) GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	return s.inner.GetSchemaVersion(ctx, params, optFns...)
}

func (s *spyGlueClient) RegisterSchemaVersion(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
	return s.inner.RegisterSchemaVersion(ctx, params, optFns...)
}

func (s *spyGlueClient) PutSchemaVersionMetadata(ctx context.Context, params *glue.PutSchemaVersionMetadataInput, optFns ...func(*glue.Options)) (*glue.PutSchemaVersionMetadataOutput, error) {
	return s.inner.PutSchemaVersionMetadata(ctx, params, optFns...)
}

func (s *spyGlueClient) QuerySchemaVersionMetadata(ctx context.Context, params *glue.QuerySchemaVersionMetadataInput, optFns ...func(*glue.Options)) (*glue.QuerySchemaVersionMetadataOutput, error) {
	return s.inner.QuerySchemaVersionMetadata(ctx, params, optFns...)
}

func (s *spyGlueClient) GetTags(ctx context.Context, params *glue.GetTagsInput, optFns ...func(*glue.Options)) (*glue.GetTagsOutput, error) {
	return s.inner.GetTags(ctx, params, optFns...)
}

// §5.3 item 25 — _Real companion.
//
// Exercises the EntityNotFound auto-register fall-through path against real
// AWS Glue (account 850995546034, region determined by local AWS config). The
// companion fake-gated test is TestNegative_EntityNotFoundFallsThroughToCreate above.
//
// Flow (spec Architecture §4):
//  A. Encode with schema A on enc1 → GetSchemaByDefinition (EntityNotFound) →
//     CreateSchema → encode succeeds. Counter == 1.
//  B. Encode again with same schema → cache hit on enc1. Counter still 1.
//  C. Delete schema A directly via h.Real.DeleteSchema (out-of-band).
//  D. Close enc1, build enc2 with the same spy and options (cold cache).
//  E. Encode with schema A on enc2 → GetSchemaByDefinition (EntityNotFound
//     because deleted) → CreateSchema (fall-through) → encode succeeds.
//     Counter == 2.
//
// Assertions (DoD #6, INV-REALTEST-SKIP):
//   - Each encode step succeeds (no error).
//   - After Step E: spy.createSchemaCount == 2 (initial + recovery).
//   - Schema A exists in Glue after the test (verified via GetSchemaVersion on
//     the UUID returned by enc2's encode). Teardown via h.Cleanup.TrackSchema.
//
// Gated: AWS_INTEGRATION=1 AND GSR_GLUE=real (scenarioGate requiresReal=true).
// Skip-mode: scenarioGate skips the test when GSR_GLUE != "real" — the test
// NEVER fails due to missing credentials; it only fails if it runs and an
// assertion is wrong (INV-REALTEST-SKIP).
func TestNegative_EntityNotFound_FallsThroughToCreate_Real(t *testing.T) {
	// INV-REALTEST-SKIP: skip (not fail) when real Glue is not selected.
	scenarioGate(t, true, false)
	requireAWSIntegration(t)

	startTime := time.Now()
	h := newGlueHandle(t)

	spy := &spyGlueClient{inner: h.Client}

	schemaName := randomGlueName(t, "neg-25-real")
	schema := &gsrcore.Schema{
		SchemaDefinition: negativeAvroSchema,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	}
	h.Cleanup.TrackSchema(testRegistryName, schemaName)

	// Step A — first encode: GetSchemaByDefinition (EntityNotFound) →
	// CreateSchema → encode succeeds. createSchemaCount becomes 1.
	enc1, err := gsrcore.NewGsrEncoderForTest(spy, gsrcore.GsrEncoderOptions{
		RegistryName:                  testRegistryName,
		Compatibility:                 "NONE",
		SchemaAutoRegistrationEnabled: true,
		CacheSize:                     10,
	})
	require.NoError(t, err)

	out1, err := enc1.Encode([]byte("payload-A"), schemaName, schema)
	require.NoError(t, err, "Step A: first encode must succeed")
	require.NotEmpty(t, out1)
	require.EqualValues(t, 1, spy.createSchemaCount.Load(),
		"Step A: exactly one CreateSchema call after initial register")

	// Step B — second encode on enc1: cache hit, no Glue call.
	out2, err := enc1.Encode([]byte("payload-B"), schemaName, schema)
	require.NoError(t, err, "Step B: second encode (cache hit) must succeed")
	require.NotEmpty(t, out2)
	require.EqualValues(t, 1, spy.createSchemaCount.Load(),
		"Step B: createSchemaCount must stay 1 on cache hit")

	// Step C — delete schema A from real Glue out-of-band, then wait for the
	// deletion to propagate. Real Glue's DeleteSchema is eventually consistent:
	// GetSchemaByDefinition may still return success for a short period after
	// DeleteSchema returns 200. We poll until EntityNotFound is observed (up to
	// 30 s) before continuing to Step D so enc2's encode reliably triggers the
	// fall-through path rather than hitting a stale success response.
	ctx := context.Background()
	_, err = h.Real.DeleteSchema(ctx, &glue.DeleteSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(testRegistryName),
			SchemaName:   aws.String(schemaName),
		},
	})
	require.NoError(t, err, "Step C: out-of-band DeleteSchema must succeed")

	// Wait for deletion propagation: poll until GetSchemaByDefinition returns
	// EntityNotFoundException (schema is gone). Capped at 30 s to keep the
	// test from running indefinitely.
	//
	// We break ONLY on EntityNotFoundException — the definitive "schema is gone"
	// signal. Transient errors (network blip, throttle) are not treated as
	// propagation-complete because the schema may still be AVAILABLE; continuing
	// the poll on transient errors avoids a confusing downstream assertion failure.
	deadline := time.Now().Add(30 * time.Second)
	propagated := false
	for time.Now().Before(deadline) {
		probeResp, probeErr := h.Real.GetSchemaByDefinition(ctx, &glue.GetSchemaByDefinitionInput{
			SchemaId: &types.SchemaId{
				RegistryName: aws.String(testRegistryName),
				SchemaName:   aws.String(schemaName),
			},
			SchemaDefinition: aws.String(negativeAvroSchema),
		})
		if probeErr != nil {
			var enf *types.EntityNotFoundException
			if errors.As(probeErr, &enf) {
				// Schema is definitively gone — propagation complete.
				propagated = true
				break
			}
			// Transient error (throttle, network). Schema may still be
			// AVAILABLE; keep polling so enc2 reliably hits EntityNotFound.
			time.Sleep(1 * time.Second)
			continue
		}
		if probeResp.Status != types.SchemaVersionStatusAvailable {
			// Not AVAILABLE — deletion is in progress; treat as propagated.
			propagated = true
			break
		}
		// Schema still visible; wait and retry.
		time.Sleep(1 * time.Second)
	}
	require.True(t, propagated, "schema deletion did not propagate within 30s — enc2 encode may return stale success")

	// Step D — close enc1 and build enc2 with same spy + options (cold cache).
	// This guarantees the next encode for schema A reaches Glue rather than
	// serving the stale version ID from enc1's LRU.
	require.NoError(t, enc1.Close())

	enc2, err := gsrcore.NewGsrEncoderForTest(spy, gsrcore.GsrEncoderOptions{
		RegistryName:                  testRegistryName,
		Compatibility:                 "NONE",
		SchemaAutoRegistrationEnabled: true,
		CacheSize:                     10,
	})
	require.NoError(t, err)

	// Step E — re-encode on enc2: cache miss → GetSchemaByDefinition
	// (EntityNotFound because we deleted it) → CreateSchema fall-through →
	// encode succeeds. createSchemaCount becomes 2.
	out3, err := enc2.Encode([]byte("payload-C"), schemaName, schema)
	require.NoError(t, err, "Step E: re-encode after delete must succeed via fall-through")
	require.NotEmpty(t, out3)
	require.EqualValues(t, 2, spy.createSchemaCount.Load(),
		"Step E: exactly two total CreateSchema calls (initial + recovery after delete)")

	// Verify schema A exists in Glue after recovery: extract the version UUID
	// from the wire-format header (bytes 2..17) and call GetSchemaVersion.
	require.GreaterOrEqual(t, len(out3), gsrcore.WireFormatHeaderSize,
		"encoded output must be at least WireFormatHeaderSize bytes")
	versionUUIDBytes := out3[2:gsrcore.WireFormatHeaderSize]
	versionUUID := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		versionUUIDBytes[0:4],
		versionUUIDBytes[4:6],
		versionUUIDBytes[6:8],
		versionUUIDBytes[8:10],
		versionUUIDBytes[10:16])
	getVersionOut, err := h.Real.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
		SchemaVersionId: aws.String(versionUUID),
	})
	require.NoError(t, err, "schema A must exist in Glue after recovery (GetSchemaVersion must succeed)")
	require.Equal(t, types.SchemaVersionStatusAvailable, getVersionOut.Status,
		"recovered schema version must be AVAILABLE")

	// Write test-artifact log capturing run metadata for developer inspection.
	// Written to t.TempDir() so it never dirties the worktree. The path is
	// printed via t.Logf so operators can retrieve it from `go test -v` output
	// or the test runner's artifact capture.
	logPath := fmt.Sprintf("%s/phase-4.14-entity-not-found-real.log", t.TempDir())
	logContent := fmt.Sprintf(
		"TestNegative_EntityNotFound_FallsThroughToCreate_Real\n"+
			"timestamp:         %s\n"+
			"account:           850995546034\n"+
			"region:            %s\n"+
			"registry:          %s\n"+
			"schema_name:       %s\n"+
			"version_uuid:      %s\n"+
			"create_schema_count: %d\n"+
			"duration_ms:       %d\n"+
			"result:            PASS\n",
		startTime.UTC().Format(time.RFC3339),
		h.Real.Region,
		testRegistryName,
		schemaName,
		versionUUID,
		spy.createSchemaCount.Load(),
		time.Since(startTime).Milliseconds(),
	)
	if err := os.WriteFile(logPath, []byte(logContent), 0o644); err == nil {
		t.Logf("test artifact written to %s", logPath)
	}
}
