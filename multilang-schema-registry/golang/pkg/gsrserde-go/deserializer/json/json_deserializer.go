package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrInvalidJsonData is returned when the data is not valid JSON
	ErrInvalidJsonData = fmt.Errorf("data must be valid JSON")

	// ErrNilData is returned when a nil data is provided
	ErrNilData = fmt.Errorf("data cannot be nil")

	// ErrDeserialization is returned when JSON deserialization fails
	ErrDeserialization = fmt.Errorf("JSON deserialization failed")

	// ErrValidation is returned when JSON validation fails
	ErrValidation = fmt.Errorf("JSON validation failed")

	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = fmt.Errorf("invalid JSON schema")

	// ErrDeserializationFailed is returned when JSON deserialization fails due
	// to a malformed payload. Preserved in the error chain so callers can use
	// errors.Is(err, ErrDeserializationFailed) for cross-format parity with
	// avro.ErrDeserializationFailed and protobuf.ErrDeserializationFailed.
	// Phase 4.12 board-fixes MAJOR-1.
	ErrDeserializationFailed = fmt.Errorf("JSON deserializer: deserialization failed")
)

// JsonDeserializationError represents an error that occurred during JSON deserialization
type JsonDeserializationError struct {
	Message string
	Cause   error
}

func (e *JsonDeserializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("JSON deserialization error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("JSON deserialization error: %s", e.Message)
}

func (e *JsonDeserializationError) Unwrap() error {
	return e.Cause
}

// Is supports errors.Is(jsonErr, gsrcore.ErrGSR) — every JsonDeserializationError
// is by definition a Glue Schema Registry error. Matches the parity contract
// in pkg/gsrserde-go/core/errors.go: SerializationError, DeserializationError.
// Note: this only fast-paths the ErrGSR match. Other targets (e.g.,
// gsrcore.ErrMalformedJSON) flow through Unwrap() against the Cause chain.
// Phase 4.12 §4 invariant.
func (e *JsonDeserializationError) Is(target error) bool {
	return target == gsrcore.ErrGSR
}

// JsonValidationError represents an error that occurred during JSON validation
type JsonValidationError struct {
	Message string
	Cause   error
}

func (e *JsonValidationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("JSON validation error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("JSON validation error: %s", e.Message)
}

func (e *JsonValidationError) Unwrap() error {
	return e.Cause
}

// JsonDeserializer handles deserialization of JSON data with schema validation.
// It returns JsonDataWithSchema wrapper objects containing both the schema and deserialized data.
type JsonDeserializer struct {
	// This deserializer is stateless and can be safely used concurrently
	config *common.Configuration
}

// NewJsonDeserializer creates a new JSON deserializer instance.
func NewJsonDeserializer(config *common.Configuration) (*JsonDeserializer, error) {
	if config == nil {
		return nil, common.ErrNilConfig
	}
	return &JsonDeserializer{
		config: config,
	}, nil
}

// Deserialize deserializes JSON data bytes and returns the validated JSON payload as a string.
// The schema parameter contains the JSON schema definition for validation.
//
// Parameters:
//
//	data: The JSON data bytes to deserialize
//	schema: The schema object containing the JSON schema definition
//
// Returns:
//
//	interface{}: The validated JSON payload as a string
//	error: Any error that occurred during deserialization
func (j *JsonDeserializer) Deserialize(data []byte, schema *gsrcore.Schema) (interface{}, error) {
	if data == nil {
		return nil, &JsonDeserializationError{
			Message: "cannot deserialize nil data",
			Cause:   ErrNilData,
		}
	}

	if schema == nil {
		return nil, &JsonDeserializationError{
			Message: "schema cannot be nil",
			Cause:   ErrInvalidSchema,
		}
	}

	// Handle empty data case
	if len(data) == 0 {
		return "", nil
	}

	// RFC 8259 §8.1: JSON text exchanged between systems must be encoded
	// in UTF-8. encoding/json itself silently replaces invalid UTF-8 in
	// string bodies with U+FFFD, which corrupts data without surfacing.
	// Reject invalid UTF-8 up front so callers see a typed
	// JsonDeserializationError instead of a quietly-mangled payload.
	//
	// Phase 4.12 §3.9 / §3.10: wrap the Cause with gsrcore.ErrMalformedJSON
	// so callers can resolve errors.Is(err, gsrcore.ErrMalformedJSON) (and
	// transitively errors.Is(err, gsrcore.ErrGSR)) through the wrapper's
	// Unwrap() chain. The pre-existing ErrInvalidJsonData sentinel is
	// preserved further down the chain for diagnostic continuity.
	if !utf8.Valid(data) {
		return nil, &JsonDeserializationError{
			Message: "data is not valid JSON: non-UTF-8 bytes",
			Cause:   fmt.Errorf("%w: %w: %w", gsrcore.ErrMalformedJSON, ErrDeserializationFailed, ErrInvalidJsonData),
		}
	}

	// Validate that data is valid JSON.
	//
	// Phase 4.12 §3.9: wrap the Cause with gsrcore.ErrMalformedJSON so the
	// per-format malformed sentinel resolves via errors.Is. The underlying
	// encoding/json error is preserved further down the chain so callers
	// can still drill into the parse-failure detail if needed.
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, &JsonDeserializationError{
			Message: "data is not valid JSON",
			Cause:   fmt.Errorf("%w: %w: %w", gsrcore.ErrMalformedJSON, ErrDeserializationFailed, err),
		}
	}

	// Get schema definition from schema object
	schemaDefinition := schema.SchemaDefinition
	if strings.TrimSpace(schemaDefinition) == "" {
		return nil, &JsonDeserializationError{
			Message: "schema definition is empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Validate data against schema
	if err := j.validateAgainstSchema(schemaDefinition, data); err != nil {
		return nil, &JsonDeserializationError{
			Message: "data validation against schema failed",
			Cause:   err,
		}
	}

	// Return the validated JSON payload as a string
	return string(data), nil
}

// validateAgainstSchema validates JSON data against a schema definition using
// santhosh-tekuri/jsonschema/v6. The JSON Schema draft is auto-detected from
// the schema document's `$schema` field; absent that, the v6 default (Draft
// 2020-12) is used. Both Draft-07 and Draft 2020-12 schemas validate without
// extra branching.
//
// Parameters:
//
//	schemaDefinition: The JSON schema definition string
//	data: The JSON data bytes to validate
//
// Returns:
//
//	error: Any validation error, nil if valid
func (j *JsonDeserializer) validateAgainstSchema(schemaDefinition string, data []byte) error {
	if data == nil {
		return &JsonValidationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	if len(data) == 0 {
		// Empty data is valid for optional fields
		return nil
	}

	if strings.TrimSpace(schemaDefinition) == "" {
		return &JsonValidationError{
			Message: "schema definition cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Parse the schema document.
	schemaRaw, err := jsonschema.UnmarshalJSON(strings.NewReader(schemaDefinition))
	if err != nil {
		return &JsonValidationError{
			Message: "validation failed: invalid schema JSON",
			Cause:   err,
		}
	}

	// Compile the schema. Pin Draft-07 as the default for schemas without an
	// explicit `$schema` field, preserving xeipuuv/gojsonschema legacy behavior.
	// v6's out-of-the-box default is Draft 2020-12, which would be a silent
	// breaking change for existing GSR JSON schemas. Schemas that DO include
	// `$schema` continue to use their declared draft (auto-detected by v6).
	//
	// Remote $ref safety: v6's default URLLoader is FileLoader (file:// only);
	// http/https $ref resolution is already closed-by-default without any
	// explicit configuration — no UseLoader call needed.
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft7)
	const schemaURL = "inmem:///schema.json"
	if err := compiler.AddResource(schemaURL, schemaRaw); err != nil {
		return &JsonValidationError{
			Message: "validation failed: adding schema resource",
			Cause:   err,
		}
	}
	sch, err := compiler.Compile(schemaURL)
	if err != nil {
		return &JsonValidationError{
			Message: "validation failed: compiling schema",
			Cause:   err,
		}
	}

	// Parse the document.
	docRaw, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return &JsonValidationError{
			Message: "validation failed: parsing document",
			Cause:   err,
		}
	}

	// Validate.
	if err := sch.Validate(docRaw); err != nil {
		return &JsonValidationError{
			Message: fmt.Sprintf("validation errors: %s", err.Error()),
			Cause:   ErrValidation,
		}
	}

	return nil
}
