// Tier-1 tests for schema evolution and the cached-second-encode
// round-trip the plan §5.3 calls out for items 18–21 (BACKWARD /
// BACKWARD_ALL / FORWARD / FULL compatibility) and item 25
// (EntityNotFoundException + auto-register fall-through).
//
// PBI-4.12-6 seeds this file with the item-25 cached-second-encode
// test only. PBI-4.12-7 appends the items 18–21 wire-flow tests.
// Keep shared helpers grouped at the bottom so PBI-7's additions
// can be inserted between the test functions and the helpers
// without renumbering.

package gsrserde

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestEncoder_EntityNotFound_TwoEncodes_OnlyOneCreateSchema pins the
// §3.8 / §5.3 item-25 full-round-trip contract:
//
//   1. First Encode of a brand-new schema: GetSchemaByDefinition
//      returns EntityNotFoundException, the encoder falls through to
//      CreateSchema, caches the returned version-id, and produces a
//      wire-format payload whose bytes[2..17] are the UUID
//      CreateSchema returned.
//   2. Second Encode of the SAME (schemaName, dataFormat) pair: the
//      cache short-circuits the lookup BEFORE any SDK call. Cumulative
//      mock-call counts for both GetSchemaByDefinition AND CreateSchema
//      must remain at 1 — proving neither was re-invoked.
//   3. The wire-format UUID portion of both encodes is byte-identical:
//      same schema → same cached version-id → same prefix.
//
// The existing TestEncoder_EntityNotFound_AutoRegisterTrue_FallsThroughToCreateSchema
// asserts only the first half (CreateSchema fires once). This test
// closes the cached-second-encode loop and is the Tier-1 anchor for
// item 25.
func TestEncoder_EntityNotFound_TwoEncodes_OnlyOneCreateSchema(t *testing.T) {
	const (
		schemaName       = "phase412-item25-schema"
		schemaDefinition = `{"type":"string"}`
		dataFormat       = "JSON"
	)
	createdVersionID := testUUIDString

	enc, mockClient := newEncoderWithMock(t, true)

	// GetSchemaByDefinition returns EntityNotFoundException only on the
	// first encode; the second encode is expected to never call it
	// (cache hit). Wiring it as a standing expectation (no .Once()) is
	// safe because we assert the cumulative call count, not the queue.
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{
			SchemaVersionId:     &createdVersionID,
			LatestSchemaVersion: ptrInt64(1),
		}, nil)

	schema := &Schema{
		SchemaName:       schemaName,
		SchemaDefinition: schemaDefinition,
		DataFormat:       dataFormat,
	}

	// First encode: cache miss → fall-through path.
	firstOut, err := enc.Encode([]byte("payload-1"), "topic", schema)
	require.NoError(t, err, "first encode must succeed via auto-register fall-through")
	require.GreaterOrEqual(t, len(firstOut), WireFormatHeaderSize,
		"first encode must produce a full wire-format prefix (%d bytes) plus payload", WireFormatHeaderSize)

	mockClient.AssertNumberOfCalls(t, "GetSchemaByDefinition", 1)
	mockClient.AssertNumberOfCalls(t, "CreateSchema", 1)

	// Second encode: same (schemaName, dataFormat) → cached path. The
	// encoder MUST NOT invoke either Glue API again. AssertNumberOfCalls
	// is cumulative across the whole test, so the expected counts stay
	// at 1, not 0.
	secondOut, err := enc.Encode([]byte("payload-2"), "topic", schema)
	require.NoError(t, err, "second encode must succeed from the cached version-id")
	require.GreaterOrEqual(t, len(secondOut), WireFormatHeaderSize,
		"second encode must produce a full wire-format prefix (%d bytes) plus payload", WireFormatHeaderSize)

	mockClient.AssertNumberOfCalls(t, "GetSchemaByDefinition", 1)
	mockClient.AssertNumberOfCalls(t, "CreateSchema", 1)

	// AC-3: bytes[2..17] (the 16-byte UUID portion of the wire-format
	// header — see wire_format.go layout comment) must be identical
	// across both encodes. Different payloads, same schema, same
	// cached version-id ⇒ same prefix UUID.
	firstUUIDBytes := firstOut[2:WireFormatHeaderSize]
	secondUUIDBytes := secondOut[2:WireFormatHeaderSize]
	require.True(t, bytes.Equal(firstUUIDBytes, secondUUIDBytes),
		"wire-format UUID portion must be identical across two encodes of the same schema (first=%x, second=%x)",
		firstUUIDBytes, secondUUIDBytes)
}

// Shared schema fixtures for the items 18–21 wire-flow tests. Each is a
// minimal JSON-formatted Avro record; v2 adds one optional field to v1,
// v3 adds a second optional field on top of v2. Wire-flow assertions
// don't care about Avro projection semantics (§10 non-goal #5) — the
// definitions just need to be distinct strings so each fresh encoder
// cache-misses and triggers the Glue auto-register fall-through.
const (
	evolutionSchemaName = "phase412-evolution-schema"
	evolutionV1Def      = `{"type":"record","name":"Payload","namespace":"phase412","fields":[{"name":"id","type":"string"}]}`
	evolutionV2Def      = `{"type":"record","name":"Payload","namespace":"phase412","fields":[{"name":"id","type":"string"},{"name":"note","type":["null","string"],"default":null}]}`
	evolutionV3Def      = `{"type":"record","name":"Payload","namespace":"phase412","fields":[{"name":"id","type":"string"},{"name":"note","type":["null","string"],"default":null},{"name":"flag","type":["null","boolean"],"default":null}]}`

	evolutionUUIDV1 = "11111111-1111-1111-1111-111111111111"
	evolutionUUIDV2 = "22222222-2222-2222-2222-222222222222"
	evolutionUUIDV3 = "33333333-3333-3333-3333-333333333333"
)

// encodeFreshWithCreateSchema wires a fresh encoder + fresh mock so the
// per-instance cache is empty for this version's encode. The mock is
// staged so GetSchemaByDefinition returns EntityNotFoundException
// (cache-miss surrogate for a brand-new definition) and CreateSchema
// returns the supplied UUID — proving the encoder threaded that UUID
// through the wire-format header.
//
// Returns the encoded output and the mock so the caller can assert on
// call counts. Each encoder is independent, defeating the cache the way
// PBI-4.12-7's "fresh encoder per version" guidance prescribes.
func encodeFreshWithCreateSchema(t *testing.T, schemaName, schemaDef, dataFormat, versionUUID string, payload []byte) ([]byte, *MockGlueClient) {
	t.Helper()
	enc, mockClient := newEncoderWithMock(t, true)

	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return((*glue.GetSchemaByDefinitionOutput)(nil), newEntityNotFoundError())
	mockClient.On("CreateSchema", mock.Anything, mock.Anything).
		Return(&glue.CreateSchemaOutput{
			SchemaVersionId:     &versionUUID,
			LatestSchemaVersion: ptrInt64(1),
		}, nil)

	out, err := enc.Encode(payload, "topic", &Schema{
		SchemaName:       schemaName,
		SchemaDefinition: schemaDef,
		DataFormat:       dataFormat,
	})
	require.NoError(t, err, "fresh encoder must auto-register %q via CreateSchema fall-through", versionUUID)
	require.GreaterOrEqual(t, len(out), WireFormatHeaderSize,
		"encode must produce a full %d-byte wire-format prefix plus payload", WireFormatHeaderSize)
	return out, mockClient
}

// assertWirePrefixMatchesUUID checks that bytes[2..18] of the encoded
// payload equal the 16-byte binary form of expectedUUID. This is the
// per-version wire-flow assertion items 18–21 all share.
func assertWirePrefixMatchesUUID(t *testing.T, encoded []byte, expectedUUID, label string) {
	t.Helper()
	require.GreaterOrEqual(t, len(encoded), WireFormatHeaderSize,
		"%s: encoded payload shorter than the %d-byte wire-format header", label, WireFormatHeaderSize)
	wantBytes := uuid.MustParse(expectedUUID)
	gotBytes := encoded[2:WireFormatHeaderSize]
	require.True(t, bytes.Equal(gotBytes, wantBytes[:]),
		"%s: wire-format UUID prefix mismatch (want=%x, got=%x)", label, wantBytes[:], gotBytes)
}

// TestEvolution_BackwardV1ToV2_WireFlow pins the §3.1 / §5.3 item-18
// Tier-1 wire-flow contract for BACKWARD compatibility:
//
//   - Producer registers v1 via a fresh encoder → wire prefix bytes[2..17]
//     carry UUID_v1 returned by CreateSchema.
//   - Producer evolves to v2 via a SECOND fresh encoder (different
//     definition, distinct mock-returned UUID_v2) → wire prefix carries
//     UUID_v2.
//   - UUID_v1 ≠ UUID_v2.
//   - The fresh-encoder #2 invokes GetSchemaByDefinition exactly once,
//     proving its cache is genuinely empty (no leak from encoder #1's
//     now-discarded cache — the §3.1 "fresh GsrEncoder" guidance).
//
// Per spec §10 non-goal #5 this is a WIRE-FLOW only assertion. The Go
// decoder is schema-agnostic — it dispatches on whichever UUID the
// prefix carries. The Tier-2 _Real companion in PBI-4.12-8 exercises
// actual Glue compatibility-mode enforcement.
func TestEvolution_BackwardV1ToV2_WireFlow(t *testing.T) {
	v1Out, v1Mock := encodeFreshWithCreateSchema(t, evolutionSchemaName,
		evolutionV1Def, "AVRO", evolutionUUIDV1, []byte(`{"id":"v1"}`))
	v2Out, v2Mock := encodeFreshWithCreateSchema(t, evolutionSchemaName,
		evolutionV2Def, "AVRO", evolutionUUIDV2, []byte(`{"id":"v2","note":null}`))

	assertWirePrefixMatchesUUID(t, v1Out, evolutionUUIDV1, "v1 encode")
	assertWirePrefixMatchesUUID(t, v2Out, evolutionUUIDV2, "v2 encode")

	require.NotEqual(t, v1Out[2:WireFormatHeaderSize], v2Out[2:WireFormatHeaderSize],
		"BACKWARD evolution must yield distinct UUIDs in the wire prefix")

	v1Mock.AssertNumberOfCalls(t, "CreateSchema", 1)
	v2Mock.AssertNumberOfCalls(t, "CreateSchema", 1)
	// Fresh-encoder cache-defeat: encoder #2 must hit Glue exactly once;
	// it shares NO state with encoder #1.
	v2Mock.AssertNumberOfCalls(t, "GetSchemaByDefinition", 1)
}

// TestEvolution_BackwardAll_ThreeVersions_WireFlow pins the §3.2 /
// §5.3 item-19 Tier-1 wire-flow contract for BACKWARD_ALL across three
// versions: three fresh encoders, three distinct mock-returned UUIDs,
// three distinct wire prefixes.
//
// Modeling note (PBI AC-2 explicitly allows either reading): this test
// commits to "each fresh encoder cache-misses → CreateSchema fires for
// every encoder" — the simpler shape. The Tier-2 _Real companion in
// PBI-4.12-8 will exercise the "single schema, three versions" Glue
// modeling where v2/v3 land via RegisterSchemaVersion.
//
// Per spec §10 non-goal #5 this asserts WIRE-FLOW only.
func TestEvolution_BackwardAll_ThreeVersions_WireFlow(t *testing.T) {
	v1Out, v1Mock := encodeFreshWithCreateSchema(t, evolutionSchemaName,
		evolutionV1Def, "AVRO", evolutionUUIDV1, []byte(`{"id":"v1"}`))
	v2Out, v2Mock := encodeFreshWithCreateSchema(t, evolutionSchemaName,
		evolutionV2Def, "AVRO", evolutionUUIDV2, []byte(`{"id":"v2","note":null}`))
	v3Out, v3Mock := encodeFreshWithCreateSchema(t, evolutionSchemaName,
		evolutionV3Def, "AVRO", evolutionUUIDV3, []byte(`{"id":"v3","note":null,"flag":null}`))

	assertWirePrefixMatchesUUID(t, v1Out, evolutionUUIDV1, "v1 encode")
	assertWirePrefixMatchesUUID(t, v2Out, evolutionUUIDV2, "v2 encode")
	assertWirePrefixMatchesUUID(t, v3Out, evolutionUUIDV3, "v3 encode")

	// All three UUIDs must be pair-wise distinct on the wire.
	require.NotEqual(t, v1Out[2:WireFormatHeaderSize], v2Out[2:WireFormatHeaderSize],
		"v1 vs v2 wire UUIDs must differ")
	require.NotEqual(t, v2Out[2:WireFormatHeaderSize], v3Out[2:WireFormatHeaderSize],
		"v2 vs v3 wire UUIDs must differ")
	require.NotEqual(t, v1Out[2:WireFormatHeaderSize], v3Out[2:WireFormatHeaderSize],
		"v1 vs v3 wire UUIDs must differ")

	v1Mock.AssertNumberOfCalls(t, "CreateSchema", 1)
	v2Mock.AssertNumberOfCalls(t, "CreateSchema", 1)
	v3Mock.AssertNumberOfCalls(t, "CreateSchema", 1)
}

// TestEvolution_ForwardV2ToV1_WireFlow pins the §3.3 / §5.3 item-20
// Tier-1 wire-flow contract for FORWARD compatibility — the mirror of
// item 18 with the registration order reversed (v2 first, then v1).
// Distinct UUIDs per version; the wire prefix carries each one.
//
// Per spec §10 non-goal #5 this asserts WIRE-FLOW only. The Tier-2
// _Real companion in PBI-4.12-8 differentiates FORWARD from BACKWARD
// against real Glue's compatibility enforcement.
func TestEvolution_ForwardV2ToV1_WireFlow(t *testing.T) {
	v2Out, v2Mock := encodeFreshWithCreateSchema(t, evolutionSchemaName,
		evolutionV2Def, "AVRO", evolutionUUIDV2, []byte(`{"id":"v2","note":null}`))
	v1Out, v1Mock := encodeFreshWithCreateSchema(t, evolutionSchemaName,
		evolutionV1Def, "AVRO", evolutionUUIDV1, []byte(`{"id":"v1"}`))

	assertWirePrefixMatchesUUID(t, v2Out, evolutionUUIDV2, "v2 encode (registered first)")
	assertWirePrefixMatchesUUID(t, v1Out, evolutionUUIDV1, "v1 encode (registered second)")

	require.NotEqual(t, v2Out[2:WireFormatHeaderSize], v1Out[2:WireFormatHeaderSize],
		"FORWARD evolution must yield distinct UUIDs in the wire prefix")

	v2Mock.AssertNumberOfCalls(t, "CreateSchema", 1)
	v1Mock.AssertNumberOfCalls(t, "CreateSchema", 1)
}

// TestEvolution_FullBothDirections_WireFlow pins the §3.4 / §5.3
// item-21 Tier-1 wire-flow contract for FULL compatibility — both
// directions across a single evolution step. Structurally similar to
// item 18 (two versions, two distinct UUIDs) but semantically distinct:
// FULL allows readers in either direction. The Tier-1 wire-flow
// assertion is the same — distinct UUIDs in the prefix — and the
// Tier-2 _Real companion in PBI-4.12-8 differentiates FULL from
// BACKWARD against real Glue.
//
// Per spec §10 non-goal #5 this asserts WIRE-FLOW only.
func TestEvolution_FullBothDirections_WireFlow(t *testing.T) {
	v1Out, v1Mock := encodeFreshWithCreateSchema(t, evolutionSchemaName,
		evolutionV1Def, "AVRO", evolutionUUIDV1, []byte(`{"id":"v1"}`))
	v2Out, v2Mock := encodeFreshWithCreateSchema(t, evolutionSchemaName,
		evolutionV2Def, "AVRO", evolutionUUIDV2, []byte(`{"id":"v2","note":null}`))

	assertWirePrefixMatchesUUID(t, v1Out, evolutionUUIDV1, "v1 encode")
	assertWirePrefixMatchesUUID(t, v2Out, evolutionUUIDV2, "v2 encode")

	require.NotEqual(t, v1Out[2:WireFormatHeaderSize], v2Out[2:WireFormatHeaderSize],
		"FULL evolution must yield distinct UUIDs in the wire prefix")

	v1Mock.AssertNumberOfCalls(t, "CreateSchema", 1)
	v2Mock.AssertNumberOfCalls(t, "CreateSchema", 1)
}
