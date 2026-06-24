package avro

import (
	"fmt"
	"reflect"

	hambaavro "github.com/hamba/avro/v2"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrInvalidAvroData is returned when the data is not valid for AVRO deserialization
	ErrInvalidAvroData = fmt.Errorf("data must be valid AVRO binary")

	// ErrNilData is returned when a nil data is provided
	ErrNilData = fmt.Errorf("avro data cannot be nil")

	// ErrDeserialization is returned when AVRO deserialization fails
	ErrDeserialization = fmt.Errorf("avro deserialization failed")

	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = fmt.Errorf("invalid avro schema")

	// ErrDeserializationFailed is returned when Avro deserialization fails due
	// to a malformed payload. Preserved in the error chain so callers can use
	// errors.Is(err, ErrDeserializationFailed) for cross-format parity with
	// json.ErrDeserializationFailed and protobuf.ErrDeserializationFailed.
	// Phase 4.12 board-fixes MAJOR-1.
	ErrDeserializationFailed = fmt.Errorf("avro deserializer: deserialization failed")

	// ErrMissingAvroSpecificType is returned at Deserialize time when
	// AvroRecordType == AvroRecordTypeSpecific but AvroSpecificType is nil.
	// This is a caller-configuration error (analogous to an invalid argument),
	// not a Glue service error, so it intentionally does NOT wrap ErrGSR.
	// Phase 4.14 PBI-02.
	ErrMissingAvroSpecificType = fmt.Errorf("avro deserializer: SPECIFIC_RECORD requires AvroSpecificType in configuration")
)

// AvroDeserializationError represents an error that occurred during AVRO deserialization
type AvroDeserializationError struct {
	Message string
	Cause   error
}

func (e *AvroDeserializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("avro deserialization error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("avro deserialization error: %s", e.Message)
}

func (e *AvroDeserializationError) Unwrap() error {
	return e.Cause
}

// Is supports errors.Is(avroErr, gsrcore.ErrGSR) — every AvroDeserializationError
// is by definition a Glue Schema Registry error. Matches the parity contract
// in pkg/gsrserde-go/core/errors.go: SerializationError, DeserializationError.
// Note: this only fast-paths the ErrGSR match. Other targets (e.g.,
// gsrcore.ErrMalformedAvro) flow through Unwrap() against the Cause chain.
// Phase 4.12 §4 invariant.
func (e *AvroDeserializationError) Is(target error) bool {
	return target == gsrcore.ErrGSR
}

// AvroDeserializer handles deserialization of AVRO messages using goavro.
// It deserializes AVRO binary data back to Go structs.
type AvroDeserializer struct {
	// This deserializer is stateless and can be safely used concurrently
	config *common.Configuration
}

// NewAvroDeserializer creates a new AVRO deserializer instance.
func NewAvroDeserializer(config *common.Configuration) (*AvroDeserializer, error) {
	if config == nil {
		return nil, common.ErrNilConfig
	}
	return &AvroDeserializer{
		config: config,
	}, nil
}

// Deserialize deserializes AVRO binary data to Go struct using goavro.
// This method matches the DataFormatDeserializer interface.
//
// Parameters:
//
//	data: The AVRO binary data to deserialize
//	schema: The AVRO schema object
//
// Returns:
//
//	interface{}: The deserialized Go struct/data
//	error: Any error that occurred during deserialization
func (d *AvroDeserializer) Deserialize(data []byte, schema *gsrcore.Schema) (interface{}, error) {
	if len(data) == 0 {
		return nil, &AvroDeserializationError{
			Message: "cannot deserialize empty data",
			Cause:   ErrNilData,
		}
	}

	if schema == nil {
		return nil, &AvroDeserializationError{
			Message: "schema cannot be nil",
			Cause:   ErrInvalidSchema,
		}
	}

	if schema.SchemaDefinition == "" {
		return nil, &AvroDeserializationError{
			Message: "schema definition cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Parse AVRO schema using hamba/avro
	avroSchema, err := hambaavro.Parse(schema.SchemaDefinition)
	if err != nil {
		return nil, &AvroDeserializationError{
			Message: "failed to parse AVRO schema",
			Cause:   err,
		}
	}

	// Dispatch based on AvroRecordType.
	//
	// SPECIFIC_RECORD: unmarshal into a freshly-allocated instance of the
	// caller-provided reflect.Type. The caller sets AvroSpecificType on the
	// common.Configuration (programmatic Go API). If they forgot to set it,
	// return ErrMissingAvroSpecificType — a caller-configuration error that
	// intentionally does NOT wrap ErrGSR.
	//
	// GENERIC_RECORD (default/unknown): existing behavior — unmarshal into
	// interface{}, which hamba/avro populates as a map[string]interface{} for
	// Avro records. INV-GENERIC-DEFAULT: this path must not regress.
	//
	// Phase 4.12 §3.9: binary-decode failures on EITHER path wrap the cause
	// with gsrcore.ErrMalformedAvro so callers can use errors.Is for
	// cross-format parity. The underlying hamba/avro error is preserved further
	// down the chain for diagnostic continuity. Schema-parse failures above
	// are NOT wrapped — those are schema problems, not malformed payloads.
	if d.config.AvroRecordType == common.AvroRecordTypeSpecific {
		if d.config.AvroSpecificType == nil {
			return nil, fmt.Errorf("%w: AvroSpecificType must be set on Configuration when AvroRecordType == SPECIFIC_RECORD", ErrMissingAvroSpecificType)
		}
		// Normalize pointer types: if the caller passed reflect.TypeOf(&MyRecord{})
		// (Kind == Ptr), reflect.New(t) would produce **MyRecord which hamba/avro
		// cannot populate. Deref to the element type so reflect.New always yields
		// a single-level pointer (*MyRecord) regardless of input form.
		t := d.config.AvroSpecificType
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		target := reflect.New(t).Interface()
		if err := hambaavro.Unmarshal(avroSchema, data, target); err != nil {
			return nil, &AvroDeserializationError{
				Message: "failed to deserialize AVRO data",
				Cause:   fmt.Errorf("%w: %w: %w", gsrcore.ErrMalformedAvro, ErrDeserializationFailed, err),
			}
		}
		return target, nil
	}

	// Default: GENERIC_RECORD (or unknown/unset) — interface{} path.
	var result interface{}
	if err := hambaavro.Unmarshal(avroSchema, data, &result); err != nil {
		return nil, &AvroDeserializationError{
			Message: "failed to deserialize AVRO data",
			Cause:   fmt.Errorf("%w: %w: %w", gsrcore.ErrMalformedAvro, ErrDeserializationFailed, err),
		}
	}

	return result, nil
}


// ValidateData validates AVRO binary data against a schema.
//
// Parameters:
//
//	data: The AVRO binary data to validate
//	schemaString: The AVRO schema as JSON string
//
// Returns:
//
//	error: Any validation error, nil if valid
func (d *AvroDeserializer) ValidateData(data []byte, schemaString string) error {
	if len(data) == 0 {
		return &AvroDeserializationError{
			Message: "data cannot be empty",
			Cause:   ErrNilData,
		}
	}

	if schemaString == "" {
		return &AvroDeserializationError{
			Message: "schema string cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Parse the schema
	avroSchema, err := hambaavro.Parse(schemaString)
	if err != nil {
		return &AvroDeserializationError{
			Message: "failed to parse AVRO schema",
			Cause:   err,
		}
	}

	// Try to unmarshal the data to validate it
	var result interface{}
	if err := hambaavro.Unmarshal(avroSchema, data, &result); err != nil {
		return &AvroDeserializationError{
			Message: "failed to validate AVRO data against schema",
			Cause:   err,
		}
	}

	return nil
}

// ValidateSchema validates an AVRO schema string.
//
// Parameters:
//
//	schemaString: The AVRO schema as JSON string
//
// Returns:
//
//	error: Any validation error, nil if valid
func (d *AvroDeserializer) ValidateSchema(schemaString string) error {
	if schemaString == "" {
		return &AvroDeserializationError{
			Message: "schema string cannot be empty",
			Cause:   ErrInvalidSchema,
		}
	}

	// Try to parse the schema to validate it
	_, err := hambaavro.Parse(schemaString)
	if err != nil {
		return &AvroDeserializationError{
			Message: "failed to parse AVRO schema",
			Cause:   err,
		}
	}

	return nil
}

// GetConfiguration returns the current configuration
func (d *AvroDeserializer) GetConfiguration() *common.Configuration {
	return d.config
}
