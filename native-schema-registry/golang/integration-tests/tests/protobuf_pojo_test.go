//go:build integration
// +build integration

package integration_tests

// Tier-2 integration test for the Protobuf POJO deserialization dispatch
// path (Phase 4.14 PBI-05, DoD #11, #13).
//
// Uses the fakeglue backend to prove that a full encode → schema registration
// → decode round-trip with ProtobufMessageType=POJO returns a concrete
// proto.Message (*testpb.TestMessage) rather than a *dynamicpb.Message.
//
// Gating: scenarioGate(t, false, true) — requiresFake=true.
// This test MUST skip (not fail) when GSR_GLUE=real; real-AWS is PBI-06's lane.
// There is no Protobuf POJO _Real companion in Phase 4.14.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/dynamicpb"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// TestProtobuf_POJO_RoundTrip_Fake encodes a protobuf payload and decodes it
// with POJO configuration, asserting:
//  1. The returned value is *testpb.TestMessage (not *dynamicpb.Message).
//  2. All fields are populated with the original values.
//
// Gating: scenarioGate(t, requiresReal=false, requiresFake=true).
// Real-AWS: this test skips when GSR_GLUE=real — PBI-06 owns real-AWS coverage.
//
// DoD #11 (integration-level end-to-end side), #13 (-race green).
func TestProtobuf_POJO_RoundTrip_Fake(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)

	h := newGlueHandle(t)

	// --- Encoder ---
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "NONE",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err, "NewGsrEncoderForTest must succeed")

	original := &testpb.TestMessage{
		Id:    "pojo-round-trip-id",
		Name:  "POJO Round Trip Name",
		Age:   28,
		Email: "pojo@example.com",
		Tags:  []string{"pbi05", "protobuf", "pojo"},
	}

	// Encoder configuration: the serializer encodes testpb.TestMessage to
	// protobuf binary, and the GSR header wraps the result.
	serCfg := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: original.ProtoReflect().Descriptor(),
	})
	ser, err := serializer.NewSerializerWithEncoder(serCfg, enc)
	require.NoError(t, err, "NewSerializerWithEncoder must succeed")
	t.Cleanup(func() { _ = ser.Close() })

	topic := "protobuf-pojo-fake"

	// Encode: the serializer marshals original via proto.Marshal and registers
	// the schema with fakeglue via auto-registration.
	encoded, err := ser.Serialize(topic, original)
	require.NoError(t, err, "Serialize must succeed against fakeglue")
	require.NotEmpty(t, encoded, "encoded bytes must not be empty")

	// Exactly one CreateSchema call is expected (auto-register path).
	require.Equal(t, 1, h.Fake.Count("CreateSchema"),
		"auto-register: exactly one CreateSchema call expected on first encode")

	// --- Decoder ---
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err, "NewGsrDecoderForTest must succeed")

	// POJO configuration: provide a *testpb.TestMessage as the unmarshal
	// template. The deserializer clones it via proto.Clone before each decode.
	desCfg := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: original.ProtoReflect().Descriptor(),
		common.ProtobufMessageTypeKey:       common.ProtobufMessageTypePOJO,
		common.ProtobufPOJOTypeKey:          &testpb.TestMessage{},
	})
	des, err := deserializer.NewDeserializerWithDecoder(desCfg, dec)
	require.NoError(t, err, "NewDeserializerWithDecoder must succeed")
	t.Cleanup(func() { _ = des.Close() })

	// Decode: the POJO path must return a *testpb.TestMessage, not *dynamicpb.Message.
	got, err := des.Deserialize(topic, encoded)
	require.NoError(t, err, "Deserialize must succeed for POJO path")
	require.NotNil(t, got, "deserialized value must not be nil")

	// Type assertion: POJO must return the concrete generated type, not dynamicpb.
	typedResult, ok := got.(*testpb.TestMessage)
	require.True(t, ok,
		"POJO must return *testpb.TestMessage (not *dynamicpb.Message), got %T: %v", got, got)

	// Confirm not a dynamicpb.Message (belt-and-suspenders assertion for clarity).
	_, isDynamic := got.(*dynamicpb.Message)
	require.False(t, isDynamic,
		"POJO dispatch must NOT return *dynamicpb.Message")

	// Field fidelity: all fields must match the original.
	require.Equal(t, original.GetId(), typedResult.GetId(),
		"Id field must round-trip correctly")
	require.Equal(t, original.GetName(), typedResult.GetName(),
		"Name field must round-trip correctly")
	require.Equal(t, original.GetAge(), typedResult.GetAge(),
		"Age field must round-trip correctly")
	require.Equal(t, original.GetEmail(), typedResult.GetEmail(),
		"Email field must round-trip correctly")
	require.Equal(t, original.GetTags(), typedResult.GetTags(),
		"Tags field must round-trip correctly")

	t.Logf("✅ Protobuf POJO round-trip passed: got %T{Id=%q, Name=%q, Age=%d, Email=%q, Tags=%v}",
		typedResult,
		typedResult.GetId(), typedResult.GetName(),
		typedResult.GetAge(), typedResult.GetEmail(),
		typedResult.GetTags())
}
