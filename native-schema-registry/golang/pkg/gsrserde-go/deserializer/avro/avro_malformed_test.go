// Tier-1 §5.3 item 26 coverage for Avro: malformed AVRO binary surfaces as
// a typed *AvroDeserializationError (errors.Is reaches the local sentinel),
// not a generic / silently-zero result. C# tests omit this; the Go library
// adds it explicitly per plan §5.6.

package avro

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// TestAvroDeserializer_MalformedBytes_SurfacesTypedError exercises §5.3
// item 26 ("malformed Avro returns a sentinel error"). The schema declares
// a record with mandatory fields whose binary encoding requires multiple
// varints + payloads; the supplied bytes are a single byte — far less than
// the schema demands.
func TestAvroDeserializer_MalformedBytes_SurfacesTypedError(t *testing.T) {
	cfg := &common.Configuration{}
	des, err := NewAvroDeserializer(cfg)
	require.NoError(t, err)

	// Schema asks for one string. The 5-byte varint 0xFE 0xFF 0xFF 0xFF 0x0F
	// zig-zag-decodes to ~2^31 — a string length far beyond
	// Config.MaxByteSliceSize. hamba/avro rejects that with a typed error.
	// (Plain truncation isn't a reliable trigger because hamba leaves
	// missing fields zero-valued when unmarshalling into map[string]any.)
	schema := &gsrcore.Schema{
		SchemaName:       "TestRecord",
		SchemaDefinition: `{"type":"record","name":"TestRecord","fields":[{"name":"message","type":"string"}]}`,
		DataFormat:       "AVRO",
	}
	mangled := []byte{0xFE, 0xFF, 0xFF, 0xFF, 0x0F}

	result, err := des.Deserialize(mangled, schema)
	require.Error(t, err, "malformed Avro must surface as an error")
	assert.Nil(t, result, "result must be nil on parse failure")

	var avroErr *AvroDeserializationError
	require.True(t, errors.As(err, &avroErr), "must be a typed AvroDeserializationError")
	assert.NotNil(t, avroErr.Cause,
		"the underlying hamba/avro parse error must be preserved for diagnostics")
}

// TestAvroDeserializer_Malformed_PreservesErrDeserializationFailed asserts
// cross-format parity for ErrDeserializationFailed (board-fixes MAJOR-1).
// All three deserializers (JSON, Avro, Protobuf) must preserve their
// per-format ErrDeserializationFailed sentinel so callers can discriminate
// "deserialization failed" from "generic GSR error" using errors.Is without
// importing each format package. This test covers the Avro format.
func TestAvroDeserializer_Malformed_PreservesErrDeserializationFailed(t *testing.T) {
	cfg := &common.Configuration{}
	des, err := NewAvroDeserializer(cfg)
	require.NoError(t, err)

	// Same 5-byte malformed varint payload used in the sentinel test above.
	schema := &gsrcore.Schema{
		SchemaName:       "TestRecord",
		SchemaDefinition: `{"type":"record","name":"TestRecord","fields":[{"name":"message","type":"string"}]}`,
		DataFormat:       "AVRO",
	}
	mangled := []byte{0xFE, 0xFF, 0xFF, 0xFF, 0x0F}

	_, deserErr := des.Deserialize(mangled, schema)
	require.Error(t, deserErr, "malformed Avro must surface as an error")

	// Cross-format parity assertion: errors.Is must resolve to the per-format
	// ErrDeserializationFailed sentinel (MAJOR-1 board fix).
	assert.True(t, errors.Is(deserErr, ErrDeserializationFailed),
		"errors.Is must resolve to avro.ErrDeserializationFailed on malformed-payload errors")

	// Umbrella sentinel must still resolve (regression guard).
	assert.True(t, errors.Is(deserErr, gsrcore.ErrMalformedAvro),
		"errors.Is must also resolve to gsrcore.ErrMalformedAvro")
	assert.True(t, errors.Is(deserErr, gsrcore.ErrGSR),
		"errors.Is must also resolve transitively to gsrcore.ErrGSR")
}

// TestAvroDeserializer_Malformed_SurfacesMalformedAvroSentinel exercises
// §3.9 item 26 (Avro). The supplied bytes fail Avro binary decode against
// a record schema; the deserializer must surface the per-format
// ErrMalformedAvro sentinel via errors.Is in addition to the pre-existing
// *AvroDeserializationError wrapper resolution.
//
// Phase 4.12 §3.9 / PBI-4.12-3 AC-1, AC-2.
func TestAvroDeserializer_Malformed_SurfacesMalformedAvroSentinel(t *testing.T) {
	cfg := &common.Configuration{}
	des, err := NewAvroDeserializer(cfg)
	require.NoError(t, err)

	// Same payload as the typed-error test above: 5-byte varint that
	// zig-zag-decodes to ~2^31, which exceeds Config.MaxByteSliceSize and
	// causes hamba/avro to reject the binary input as malformed.
	schema := &gsrcore.Schema{
		SchemaName:       "TestRecord",
		SchemaDefinition: `{"type":"record","name":"TestRecord","fields":[{"name":"message","type":"string"}]}`,
		DataFormat:       "AVRO",
	}
	mangled := []byte{0xFE, 0xFF, 0xFF, 0xFF, 0x0F}

	result, err := des.Deserialize(mangled, schema)
	require.Error(t, err, "malformed Avro must surface as an error")
	assert.Nil(t, result, "result must be nil on parse failure")

	// Per-format sentinel: errors.Is must resolve through the wrapper's
	// Unwrap() to gsrcore.ErrMalformedAvro.
	assert.True(t, errors.Is(err, gsrcore.ErrMalformedAvro),
		"errors.Is must resolve to gsrcore.ErrMalformedAvro")

	// Umbrella sentinel: ErrMalformedAvro wraps ErrGSR transitively.
	assert.True(t, errors.Is(err, gsrcore.ErrGSR),
		"errors.Is must resolve transitively to gsrcore.ErrGSR")

	// Wrapper type retention: existing tests asserting *AvroDeserializationError
	// via errors.As must keep working. AC-4 regression guard.
	var avroErr *AvroDeserializationError
	require.True(t, errors.As(err, &avroErr),
		"errors.As must still resolve the *AvroDeserializationError wrapper")
	assert.NotNil(t, avroErr.Cause,
		"the underlying hamba/avro error must be preserved for diagnostics")
}
