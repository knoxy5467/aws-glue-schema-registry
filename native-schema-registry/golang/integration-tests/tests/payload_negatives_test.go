//go:build integration

package integration_tests

// payload_negatives_test.go — Tier-2 fake-backend per-format malformed-payload
// sentinel tests for §5.3 item 26.
//
// Each test:
//   1. Creates a fake GlueClient via newGlueHandle (fakeglue.Fake).
//   2. Builds a GsrDecoder and primes its schema cache directly via
//      gsrcore.PrimeSchemaCache so no real GetSchemaVersion call is needed.
//   3. Wraps the primed decoder in a format-specific deserializer.Deserializer
//      via deserializer.NewDeserializerWithDecoder.
//   4. Constructs a wire-format payload with a valid 18-byte GSR header
//      (pointing at the primed version UUID) but intentionally malformed body
//      bytes for the target format.
//   5. Calls Deserializer.Deserialize and asserts the per-format sentinel.
//
// Gating: scenarioGate(t, false, true) — requiresFake=true. The decoder
// cache is primed directly (PrimeSchemaCache) so the GlueClient is never
// called, but newGlueHandle still pays LoadDefaultConfig + creds.Retrieve in
// real mode. Gating on fake avoids that cost and keeps CI clean.
//
// Phase 4.12 §3.9 / DoD #11.

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	avroDeser "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer/avro"
	protobufDeser "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer/protobuf"
)

// payloadNegUUID is the fixed version UUID primed into the decoder cache for
// all three payload-negatives tests.
const payloadNegUUID = "deadbeef-dead-dead-dead-deaddeadbeef"

// buildWirePayload returns a GSR-framed wire-format payload whose 18-byte
// header references payloadNegUUID and whose body is the supplied bytes.
// Panics (via require.NoError) on encode failure — only valid in test code.
func buildWirePayload(t testing.TB, body []byte) []byte {
	t.Helper()
	out, err := gsrcore.EncodeWireFormat(payloadNegUUID, gsrcore.CompressionByteNone, body)
	require.NoError(t, err, "buildWirePayload: EncodeWireFormat must not error")
	return out
}

// TestPayloadNegatives_MalformedJSON_SurfacesSentinel asserts §5.3 item 26
// (JSON path) end-to-end at the Tier-2 integration level.
//
// The test primes the decoder cache with a JSON schema, constructs a
// wire-format payload whose body is syntactically broken JSON, drives it
// through the full deserializer.Deserializer pipeline, and asserts:
//   - errors.Is(err, gsrcore.ErrMalformedJSON) — per-format sentinel.
//   - errors.Is(err, gsrcore.ErrGSR) — umbrella sentinel (transitive via
//     ErrMalformedJSON wrapping ErrGSR).
//
// JSON sentinel assertion is library-agnostic (Phase 4.11 coupling — the JSON
// validate library is being migrated from xeipuuv/gojsonschema to
// santhosh-tekuri/jsonschema/v6; the assertion chains through ErrMalformedJSON
// regardless of which lib wraps the underlying parse error). No errors.As
// assertion on JsonDeserializationError at the integration layer per spec §9:
// Tier-2 asserts errors.Is(err, gsrcore.ErrGSR) only for the JSON path.
//
// Phase 4.12 §3.9 / PBI-4.12-8 AC-5.
func TestPayloadNegatives_MalformedJSON_SurfacesSentinel(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)

	h := newGlueHandle(t)

	// Prime the decoder cache with a minimal JSON schema so GetSchemaVersion
	// is never called (fakeglue's empty state would return EntityNotFound).
	coreDecoder, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	gsrcore.PrimeSchemaCache(coreDecoder, payloadNegUUID, &gsrcore.Schema{
		SchemaName:       "neg-26-json",
		SchemaDefinition: `{"type":"object"}`,
		DataFormat:       "JSON",
		SchemaVersionID:  payloadNegUUID,
	})

	cfg := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatJSON,
	})
	des, err := deserializer.NewDeserializerWithDecoder(cfg, coreDecoder)
	require.NoError(t, err)

	// Malformed JSON body: a colon-only fragment that fails encoding/json
	// unmarshal. The schema cache hit means the decoder returns these bytes
	// from Decode; the JSON deserializer then rejects them.
	malformedBody := []byte(`{"x":}`)
	wirePayload := buildWirePayload(t, malformedBody)

	_, err = des.Deserialize("topic", wirePayload)
	require.Error(t, err, "malformed JSON body must surface an error end-to-end")

	// Coupling: Phase 4.11 — see spec.md §3.9. The JSON validate library is
	// migrating from xeipuuv/gojsonschema to santhosh-tekuri/jsonschema/v6.
	// Both libs are wrapped under ErrMalformedJSON in the JSON deserializer,
	// so errors.Is resolves regardless of which library is active.
	require.True(t, errors.Is(err, gsrcore.ErrMalformedJSON),
		"errors.Is must reach gsrcore.ErrMalformedJSON through the deserializer chain (got: %v)", err)
	require.True(t, errors.Is(err, gsrcore.ErrGSR),
		"errors.Is must reach gsrcore.ErrGSR transitively (ErrMalformedJSON wraps ErrGSR)")
}

// TestPayloadNegatives_MalformedAvro_SurfacesSentinel asserts §5.3 item 26
// (Avro path) end-to-end at the Tier-2 integration level.
//
// The test primes the decoder cache with an Avro schema, constructs a
// wire-format payload whose body is deliberately invalid Avro binary, drives
// it through the full deserializer.Deserializer pipeline, and asserts:
//   - errors.Is(err, gsrcore.ErrMalformedAvro) — per-format sentinel.
//   - errors.Is(err, gsrcore.ErrGSR) — umbrella sentinel (transitive).
//   - errors.As(err, &*avro.AvroDeserializationError{}) — typed wrapper
//     retention for format-discrimination callers (spec §3.9 round-2 M3).
//
// Phase 4.12 §3.9 / PBI-4.12-8 AC-6.
func TestPayloadNegatives_MalformedAvro_SurfacesSentinel(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)

	h := newGlueHandle(t)

	coreDecoder, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	// Schema declares a record with a mandatory string field. The malformed
	// bytes below trigger a specific hamba/avro rejection path.
	const avroSchemaDef = `{"type":"record","name":"NegTest","fields":[{"name":"message","type":"string"}]}`

	gsrcore.PrimeSchemaCache(coreDecoder, payloadNegUUID, &gsrcore.Schema{
		SchemaName:       "neg-26-avro",
		SchemaDefinition: avroSchemaDef,
		DataFormat:       "AVRO",
		SchemaVersionID:  payloadNegUUID,
	})

	cfg := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatAvro,
		common.AvroRecordTypeKey: common.AvroRecordTypeGeneric,
	})
	des, err := deserializer.NewDeserializerWithDecoder(cfg, coreDecoder)
	require.NoError(t, err)

	// Malformed Avro binary: 5-byte varint 0xFE 0xFF 0xFF 0xFF 0x0F
	// zig-zag-decodes to ~2^31 — a string length far beyond
	// hamba/avro's MaxByteSliceSize. hamba rejects it with a typed error.
	// (Plain truncation isn't a reliable trigger because hamba zero-values
	// missing fields when unmarshalling into map[string]any.)
	// Mirror of pkg/gsrserde-go/deserializer/avro/avro_malformed_test.go.
	malformedBody := []byte{0xFE, 0xFF, 0xFF, 0xFF, 0x0F}
	wirePayload := buildWirePayload(t, malformedBody)

	_, err = des.Deserialize("topic", wirePayload)
	require.Error(t, err, "malformed Avro body must surface an error end-to-end")

	require.True(t, errors.Is(err, gsrcore.ErrMalformedAvro),
		"errors.Is must reach gsrcore.ErrMalformedAvro through the deserializer chain (got: %v)", err)
	require.True(t, errors.Is(err, gsrcore.ErrGSR),
		"errors.Is must reach gsrcore.ErrGSR transitively")

	// Typed wrapper retention — format-discrimination callers (spec §3.9 M3).
	var avroErr *avroDeser.AvroDeserializationError
	require.True(t, errors.As(err, &avroErr),
		"errors.As must resolve *avro.AvroDeserializationError (got: %T: %v)", err, err)
}

// TestPayloadNegatives_MalformedProtobuf_SurfacesSentinel asserts §5.3 item
// 26 (Protobuf path) end-to-end at the Tier-2 integration level.
//
// The test primes the decoder cache with a Protobuf schema, constructs a
// wire-format payload whose body is a valid message-index varint (0x00 = index
// 0) followed by deliberately malformed Protobuf binary, drives it through the
// full deserializer.Deserializer pipeline, and asserts:
//   - errors.Is(err, gsrcore.ErrMalformedProtobuf) — per-format sentinel.
//   - errors.Is(err, gsrcore.ErrGSR) — umbrella sentinel (transitive).
//   - errors.As(err, &*protobuf.ProtobufDeserializationError{}) — typed wrapper
//     retention for format-discrimination callers (spec §3.9 round-2 M3).
//
// Wire-format note: GsrDecoder.Decode strips the 18-byte GSR prefix; for
// PROTOBUF it also strips the leading message-index varint before handing
// the remaining bytes to the format deserializer. The body placed in the wire
// payload must therefore be 0x00 (message-index = 0 as a single-byte varint)
// followed by malformed proto bytes. The deserializer receives only the
// malformed proto bytes — the message-index prefix is consumed by the decoder.
//
// Phase 4.12 §3.9 / PBI-4.12-8 AC-7.
func TestPayloadNegatives_MalformedProtobuf_SurfacesSentinel(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)

	h := newGlueHandle(t)

	coreDecoder, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	// Build a minimal Protobuf MessageDescriptor inline using descriptorpb +
	// protodesc, mirroring the pattern in pkg/gsrserde-go/deserializer/
	// protobuf/protobuf_deserializer_test.go. This avoids depending on
	// testpb-generated code (which requires protoc at build time).
	fileDescProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("payload_neg_test.proto"),
		Package: proto.String("integration_tests"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("PayloadNegMsg"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}
	fileDesc, err := protodesc.NewFile(fileDescProto, protoregistry.GlobalFiles)
	require.NoError(t, err, "protodesc.NewFile must succeed for inline descriptor")
	msgDesc := fileDesc.Messages().ByName("PayloadNegMsg")
	require.NotNil(t, msgDesc, "PayloadNegMsg must be found in inline descriptor")

	// The schema definition string for Protobuf is the proto source text.
	// GsrDecoder uses DataFormat for stripping the message-index prefix; the
	// exact SchemaDefinition value is passed to the format deserializer but is
	// not parsed by it (the MessageDescriptor is the authoritative descriptor).
	const protoSchemaDef = `syntax = "proto3"; message PayloadNegMsg { string id = 1; }`

	gsrcore.PrimeSchemaCache(coreDecoder, payloadNegUUID, &gsrcore.Schema{
		SchemaName:       "neg-26-proto",
		SchemaDefinition: protoSchemaDef,
		DataFormat:       "PROTOBUF",
		SchemaVersionID:  payloadNegUUID,
	})

	cfg := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey:           common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: msgDesc,
	})
	des, err := deserializer.NewDeserializerWithDecoder(cfg, coreDecoder)
	require.NoError(t, err)

	// Wire-format body: 0x00 (message-index varint = 0, consumed by
	// GsrDecoder.Decode's stripMessageIndex) followed by 0x81, 0x82, 0x83
	// (incomplete varint — fails proto.Unmarshal). The format deserializer
	// receives only the trailing 3 bytes.
	malformedBody := []byte{0x00, 0x81, 0x82, 0x83}
	wirePayload := buildWirePayload(t, malformedBody)

	_, err = des.Deserialize("topic", wirePayload)
	require.Error(t, err, "malformed Protobuf body must surface an error end-to-end")

	require.True(t, errors.Is(err, gsrcore.ErrMalformedProtobuf),
		"errors.Is must reach gsrcore.ErrMalformedProtobuf through the deserializer chain (got: %v)", err)
	require.True(t, errors.Is(err, gsrcore.ErrGSR),
		"errors.Is must reach gsrcore.ErrGSR transitively")

	// Typed wrapper retention — format-discrimination callers (spec §3.9 M3).
	var protoErr *protobufDeser.ProtobufDeserializationError
	require.True(t, errors.As(err, &protoErr),
		"errors.As must resolve *protobuf.ProtobufDeserializationError (got: %T: %v)", err, err)
}
