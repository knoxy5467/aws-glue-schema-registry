package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrInvalidJsonData is returned when the data is not a JsonDataWithSchema wrapper
	ErrInvalidJsonData = fmt.Errorf("data must be JsonDataWithSchema wrapper")

	// ErrNilData is returned when a nil data is provided
	ErrNilData = fmt.Errorf("data cannot be nil")

	// ErrSchemaGeneration is returned when schema generation fails
	ErrSchemaGeneration = fmt.Errorf("failed to generate JSON schema")

	// ErrSerialization is returned when JSON serialization fails
	ErrSerialization = fmt.Errorf("JSON serialization failed")

	// ErrValidation is returned when JSON validation fails
	ErrValidation = fmt.Errorf("JSON validation failed")

	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = fmt.Errorf("invalid JSON schema")
)

// JsonSerializationError represents an error that occurred during JSON serialization
type JsonSerializationError struct {
	Message string
	Cause   error
}

func (e *JsonSerializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("JSON serialization error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("JSON serialization error: %s", e.Message)
}

func (e *JsonSerializationError) Unwrap() error {
	return e.Cause
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

// JsonSerializer handles serialization of JSON data with schema validation.
// It only accepts JsonDataWithSchema wrapper objects to ensure schema validation.
type JsonSerializer struct {
	// This serializer is stateless and can be safely used concurrently
	config *common.Configuration
}

// NewJsonSerializer creates a new JSON serializer instance.
func NewJsonSerializer(config *common.Configuration) *JsonSerializer {
	if config == nil {
		panic("configuration cannot be nil")
	}
	return &JsonSerializer{
		config: config,
	}
}

// Serialize serializes a JsonDataWithSchema wrapper to JSON bytes.
// This method only accepts JsonDataWithSchema wrapper objects and returns an error for any other type.
//
// Parameters:
//   data: Must be a *JsonDataWithSchema wrapper object
//
// Returns:
//   []byte: The serialized JSON data
//   error: Any error that occurred during serialization
func (j *JsonSerializer) Serialize(data interface{}) ([]byte, error) {
	if data == nil {
		return nil, &JsonSerializationError{
			Message: "cannot serialize nil data",
			Cause:   ErrNilData,
		}
	}

	// Runtime validation: ensure data is JsonDataWithSchema wrapper
	wrapper, ok := data.(*JsonDataWithSchema)
	if !ok {
		return nil, &JsonSerializationError{
			Message: fmt.Sprintf("JSON serializer only accepts JsonDataWithSchema wrapper objects, got %T", data),
			Cause:   ErrInvalidJsonData,
		}
	}

	// Validate the wrapper object
	if err := j.ValidateObject(wrapper); err != nil {
		return nil, &JsonSerializationError{
			Message: "wrapper validation failed",
			Cause:   err,
		}
	}

	// Get payload from wrapper
	payload := wrapper.GetPayload()
	if payload == "" {
		return []byte{}, nil
	}

	// Validate that payload is valid JSON
	var jsonData interface{}
	if err := json.Unmarshal([]byte(payload), &jsonData); err != nil {
		return nil, &JsonSerializationError{
			Message: "payload is not valid JSON",
			Cause:   err,
		}
	}

	// Validate payload against schema
	schema := wrapper.GetSchema()
	if err := j.Validate(schema, []byte(payload)); err != nil {
		return nil, &JsonSerializationError{
			Message: "payload validation against schema failed",
			Cause:   err,
		}
	}

	// Return the payload as bytes (it's already JSON)
	return []byte(payload), nil
}

// GetSchemaDefinition extracts the JSON schema definition from a JsonDataWithSchema wrapper.
//
// Parameters:
//   data: Must be a *JsonDataWithSchema wrapper object
//
// Returns:
//   string: The JSON schema definition
//   error: Any error that occurred during schema extraction
func (j *JsonSerializer) GetSchemaDefinition(data interface{}) (string, error) {
	if data == nil {
		return "", &JsonSerializationError{
			Message: "cannot get schema from nil data",
			Cause:   ErrNilData,
		}
	}

	// Runtime validation: ensure data is JsonDataWithSchema wrapper
	wrapper, ok := data.(*JsonDataWithSchema)
	if !ok {
		return "", &JsonSerializationError{
			Message: fmt.Sprintf("JSON serializer only accepts JsonDataWithSchema wrapper objects, got %T", data),
			Cause:   ErrInvalidJsonData,
		}
	}

	schema := wrapper.GetSchema()
	if strings.TrimSpace(schema) == "" {
		return "", &JsonSerializationError{
			Message: "wrapper contains empty schema",
			Cause:   ErrInvalidSchema,
		}
	}

	return schema, nil
}

// Validate validates JSON data against a schema definition using
// santhosh-tekuri/jsonschema/v6. The JSON Schema draft is auto-detected from
// the schema document's `$schema` field; absent that, the v6 default (Draft
// 2020-12) is used. Both Draft-07 and Draft 2020-12 schemas validate without
// extra branching.
//
// Parameters:
//   schemaDefinition: The JSON schema definition string
//   data: The JSON data bytes to validate
//
// Returns:
//   error: Any validation error, nil if valid
func (j *JsonSerializer) Validate(schemaDefinition string, data []byte) error {
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

// ValidateObject validates a JsonDataWithSchema wrapper object.
//
// Parameters:
//   data: Must be a *JsonDataWithSchema wrapper object
//
// Returns:
//   error: Any validation error, nil if valid
func (j *JsonSerializer) ValidateObject(data interface{}) error {
	if data == nil {
		return &JsonValidationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	// Runtime validation: ensure data is JsonDataWithSchema wrapper
	wrapper, ok := data.(*JsonDataWithSchema)
	if !ok {
		return &JsonValidationError{
			Message: fmt.Sprintf("JSON serializer only accepts JsonDataWithSchema wrapper objects, got %T", data),
			Cause:   ErrInvalidJsonData,
		}
	}

	// Validate schema is not empty
	schema := wrapper.GetSchema()
	if strings.TrimSpace(schema) == "" {
		return &JsonValidationError{
			Message: "JsonDataWithSchema contains empty schema",
			Cause:   ErrInvalidSchema,
		}
	}

	// Validate schema is valid JSON
	var schemaData interface{}
	if err := json.Unmarshal([]byte(schema), &schemaData); err != nil {
		return &JsonValidationError{
			Message: "JsonDataWithSchema contains invalid JSON schema",
			Cause:   err,
		}
	}

	// Validate payload is valid JSON (if not empty)
	payload := wrapper.GetPayload()
	if payload != "" {
		var jsonData interface{}
		if err := json.Unmarshal([]byte(payload), &jsonData); err != nil {
			return &JsonValidationError{
				Message: "JsonDataWithSchema payload is not valid JSON",
				Cause:   err,
			}
		}
	}

	return nil
}

// SetAdditionalSchemaInfo sets additional schema information in the schema object.
// For JSON, this includes metadata about the JsonDataWithSchema wrapper.
//
// Parameters:
//   data: Must be a *JsonDataWithSchema wrapper object
//   schema: The schema object to update
//
// Returns:
//   error: Any error that occurred during schema update
func (j *JsonSerializer) SetAdditionalSchemaInfo(data interface{}, schema *gsrcore.Schema) error {
	if data == nil {
		return &JsonSerializationError{
			Message: "data cannot be nil",
			Cause:   ErrNilData,
		}
	}

	if schema == nil {
		return &JsonSerializationError{
			Message: "schema cannot be nil",
			Cause:   fmt.Errorf("schema is nil"),
		}
	}

	// Runtime validation: ensure data is JsonDataWithSchema wrapper
	_, ok := data.(*JsonDataWithSchema)
	if !ok {
		return &JsonSerializationError{
			Message: fmt.Sprintf("JSON serializer only accepts JsonDataWithSchema wrapper objects, got %T", data),
			Cause:   ErrInvalidJsonData,
		}
	}

	// Set additional info for JSON data with schema
	schema.AdditionalInfo = "JsonDataWithSchema"

	// Ensure DataFormat is set correctly
	if schema.DataFormat == "" {
		schema.DataFormat = "JSON"
	}

	return nil
}
