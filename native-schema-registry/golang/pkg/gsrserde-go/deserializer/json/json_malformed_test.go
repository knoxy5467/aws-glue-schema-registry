// Tier-1 §5.3 item 27 coverage for JSON: non-UTF-8 byte sequences must
// surface as the malformed-JSON sentinel rather than corrupting downstream
// data. encoding/json rejects invalid UTF-8, so this test asserts the
// JsonDeserializer's wrapping preserves the typed error chain.

package json

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// TestJsonDeserializer_NonUtf8_SurfacesMalformedJsonError exercises §5.3
// item 27. The supplied bytes are valid JSON syntax shape but contain a
// lone 0xFF byte that doesn't form a valid UTF-8 code point — encoding/json
// rejects it, the wrapper surfaces *JsonDeserializationError with the
// "data is not valid JSON" message and the encoding/json error preserved.
func TestJsonDeserializer_NonUtf8_SurfacesMalformedJsonError(t *testing.T) {
	cfg := &common.Configuration{}
	des, err := NewJsonDeserializer(cfg)
	require.NoError(t, err)

	// {"name": "<lone 0xFF byte>"} — invalid UTF-8 string body inside a
	// well-formed JSON skeleton.
	nonUTF8 := []byte{'{', '"', 'n', '"', ':', '"', 0xFF, '"', '}'}

	schema := &gsrcore.Schema{
		SchemaDefinition: `{"type":"object","properties":{"n":{"type":"string"}}}`,
		DataFormat:       "JSON",
	}

	result, err := des.Deserialize(nonUTF8, schema)
	require.Error(t, err, "non-UTF-8 must surface as a malformed-JSON error")
	assert.Nil(t, result, "result must be nil on parse failure")

	var jsonErr *JsonDeserializationError
	require.True(t, errors.As(err, &jsonErr), "must be a typed JsonDeserializationError")
	assert.Contains(t, jsonErr.Error(), "not valid JSON",
		"the wrapper message must signal the malformed-JSON sentinel surface")
	assert.NotNil(t, jsonErr.Cause,
		"the underlying encoding/json error must be preserved for diagnostics")
}
