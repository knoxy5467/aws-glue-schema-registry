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
