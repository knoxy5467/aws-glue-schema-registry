// Tier-1 §5.3 item 26 (JSON) + item 27 coverage. Item 26: payload bytes
// that fail JSON parsing must surface the per-format malformed sentinel
// (gsrcore.ErrMalformedJSON) so callers can use errors.Is to discriminate
// format-specific malformations from the umbrella ErrGSR. Item 27: the
// non-UTF-8 path participates in the same sentinel chain.
//
// Coupling: Phase 4.11 — see spec.md §3.9. The JSON validate library is
// migrating from xeipuuv/gojsonschema to santhosh-tekuri/jsonschema/v6 in
// Phase 4.11. These tests assert at the sentinel layer (errors.Is against
// gsrcore.ErrMalformedJSON / ErrGSR), NOT at the underlying validate
// library's error type, value, or message substring — both libs are
// wrapped under ErrMalformedJSON, so the assertions are library-agnostic.

package json

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// TestJsonDeserializer_Malformed_SurfacesMalformedJSONSentinel exercises
// §5.3 item 26 (JSON). The supplied bytes are syntactically broken JSON
// (a property name followed by a colon with no value, then a closing
// brace). encoding/json rejects them; the deserializer must surface the
// per-format ErrMalformedJSON sentinel via errors.Is in addition to the
// pre-existing *JsonDeserializationError wrapper resolution.
func TestJsonDeserializer_Malformed_SurfacesMalformedJSONSentinel(t *testing.T) {
	cfg := &common.Configuration{}
	des, err := NewJsonDeserializer(cfg)
	require.NoError(t, err)

	// Deliberately broken JSON: value missing after the colon.
	malformed := []byte(`{"x":}`)

	schema := &gsrcore.Schema{
		SchemaDefinition: `{"type":"object"}`,
		DataFormat:       "JSON",
	}

	result, err := des.Deserialize(malformed, schema)
	require.Error(t, err, "malformed JSON must surface as an error")
	assert.Nil(t, result, "result must be nil on parse failure")

	// Per-format sentinel: errors.Is must resolve through the wrapper's
	// Unwrap() to gsrcore.ErrMalformedJSON.
	assert.True(t, errors.Is(err, gsrcore.ErrMalformedJSON),
		"errors.Is must resolve to gsrcore.ErrMalformedJSON")

	// Umbrella sentinel: ErrMalformedJSON wraps ErrGSR transitively.
	assert.True(t, errors.Is(err, gsrcore.ErrGSR),
		"errors.Is must resolve transitively to gsrcore.ErrGSR")

	// Wrapper type retention: existing tests asserting *JsonDeserializationError
	// via errors.As must keep working. PBI-4.12-2 AC-3 regression guard.
	var jsonErr *JsonDeserializationError
	require.True(t, errors.As(err, &jsonErr),
		"errors.As must still resolve the *JsonDeserializationError wrapper")
	assert.NotNil(t, jsonErr.Cause,
		"the underlying parse error must be preserved for diagnostics")
}

// TestJsonDeserializer_NonUtf8_SurfacesMalformedJsonError exercises §5.3
// item 27. The supplied bytes are valid JSON syntax shape but contain a
// lone 0xFF byte that doesn't form a valid UTF-8 code point — encoding/json
// rejects it, the wrapper surfaces *JsonDeserializationError with the
// "data is not valid JSON" message and the encoding/json error preserved.
//
// Phase 4.12 §3.10 extension: the chain MUST also resolve to
// gsrcore.ErrMalformedJSON and gsrcore.ErrGSR via errors.Is.
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

	// Existing wrapper assertions — must remain green (AC-2 / GR-3).
	var jsonErr *JsonDeserializationError
	require.True(t, errors.As(err, &jsonErr), "must be a typed JsonDeserializationError")
	assert.Contains(t, jsonErr.Error(), "not valid JSON",
		"the wrapper message must signal the malformed-JSON sentinel surface")
	assert.NotNil(t, jsonErr.Cause,
		"the underlying encoding/json error must be preserved for diagnostics")

	// Phase 4.12 §3.10 additions: per-format and umbrella sentinel chain.
	assert.True(t, errors.Is(err, gsrcore.ErrMalformedJSON),
		"errors.Is must resolve to gsrcore.ErrMalformedJSON")
	assert.True(t, errors.Is(err, gsrcore.ErrGSR),
		"errors.Is must resolve transitively to gsrcore.ErrGSR")
}
