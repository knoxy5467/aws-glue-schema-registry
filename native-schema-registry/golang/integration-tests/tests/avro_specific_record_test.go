//go:build integration
// +build integration

package integration_tests

// Tier-2 integration test for the Avro SPECIFIC_RECORD deserialization dispatch
// path (Phase 4.14 PBI-05, DoD #9, #13).
//
// Uses the fakeglue backend to prove that a full encode → schema registration
// → decode round-trip with AvroRecordType=SPECIFIC_RECORD returns a typed Go
// struct (*specificAvroTestRecord) rather than a map[string]interface{}.
//
// Gating: scenarioGate(t, false, true) — requiresFake=true.
// This test MUST skip (not fail) when GSR_GLUE=real; real-AWS is PBI-06's lane.
// There is no Avro SPECIFIC_RECORD _Real companion in Phase 4.14.

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	gsravro "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
)

// specificAvroTestRecord is the concrete Go type the SPECIFIC_RECORD
// deserializer must produce. Field tags match the Avro schema below so
// hamba/avro's reflect path can populate them on decode.
type specificAvroTestRecord struct {
	ID   string `avro:"id"`
	Name string `avro:"name"`
	Age  int    `avro:"age"`
}

// avroSpecificRecordSchema is the Avro schema for specificAvroTestRecord.
// Must match the struct field names exactly (hamba/avro uses avro tags).
const avroSpecificRecordSchema = `{
	"type": "record",
	"name": "SpecificRecordTestUser",
	"fields": [
		{"name": "id",   "type": "string"},
		{"name": "name", "type": "string"},
		{"name": "age",  "type": "int"}
	]
}`

// TestAvro_SpecificRecord_RoundTrip_Fake encodes an Avro payload and decodes
// it with SPECIFIC_RECORD configuration, asserting:
//  1. The returned value is *specificAvroTestRecord (not map[string]interface{}).
//  2. All fields are populated with the original values.
//
// Gating: scenarioGate(t, requiresReal=false, requiresFake=true).
// Real-AWS: this test skips when GSR_GLUE=real — PBI-06 owns real-AWS coverage.
//
// DoD #9 (integration-level end-to-end side), #13 (-race green).
func TestAvro_SpecificRecord_RoundTrip_Fake(t *testing.T) {
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

	serCfg := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatAvro,
		common.AvroRecordTypeKey: common.AvroRecordTypeGeneric,
	})
	ser, err := serializer.NewSerializerWithEncoder(serCfg, enc)
	require.NoError(t, err, "NewSerializerWithEncoder must succeed")
	t.Cleanup(func() { _ = ser.Close() })

	// Input: the same schema + data we expect to get back as a typed struct.
	original := &specificAvroTestRecord{
		ID:   "specific-record-id",
		Name: "Specific Record Name",
		Age:  42,
	}
	topic := "avro-specific-record-fake"

	// Encode: Serialize wraps original in an AvroRecord and writes to fakeglue.
	encoded, err := ser.Serialize(topic, &gsravro.AvroRecord{
		Schema: avroSpecificRecordSchema,
		Data:   original,
	})
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

	// SPECIFIC_RECORD configuration: tell the deserializer to allocate and
	// populate a *specificAvroTestRecord on each decode call.
	desCfg := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey:   common.DataFormatAvro,
		common.AvroRecordTypeKey:   common.AvroRecordTypeSpecific,
		common.AvroSpecificTypeKey: reflect.TypeOf(specificAvroTestRecord{}),
	})
	des, err := deserializer.NewDeserializerWithDecoder(desCfg, dec)
	require.NoError(t, err, "NewDeserializerWithDecoder must succeed")
	t.Cleanup(func() { _ = des.Close() })

	// Decode: the SPECIFIC_RECORD path must return a *specificAvroTestRecord.
	got, err := des.Deserialize(topic, encoded)
	require.NoError(t, err, "Deserialize must succeed for SPECIFIC_RECORD path")
	require.NotNil(t, got, "deserialized value must not be nil")

	// Type assertion: SPECIFIC_RECORD must return a concrete typed pointer,
	// not map[string]interface{} (which is what GENERIC_RECORD returns).
	typedResult, ok := got.(*specificAvroTestRecord)
	require.True(t, ok,
		"SPECIFIC_RECORD must return *specificAvroTestRecord, got %T: %v", got, got)

	// Field fidelity: all fields must match the original.
	require.Equal(t, original.ID, typedResult.ID,
		"ID field must round-trip correctly")
	require.Equal(t, original.Name, typedResult.Name,
		"Name field must round-trip correctly")
	require.Equal(t, original.Age, typedResult.Age,
		"Age field must round-trip correctly")

	t.Logf("✅ Avro SPECIFIC_RECORD round-trip passed: got %T{ID=%q, Name=%q, Age=%d}",
		typedResult, typedResult.ID, typedResult.Name, typedResult.Age)
}
