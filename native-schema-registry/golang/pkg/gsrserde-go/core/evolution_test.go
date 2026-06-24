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
