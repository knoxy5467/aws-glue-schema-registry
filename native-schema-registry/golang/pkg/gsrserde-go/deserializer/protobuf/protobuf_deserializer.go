package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

var (
	// ErrNilData is returned when nil data is provided
	ErrNilData = fmt.Errorf("protobuf deserializer: data cannot be nil")

	// ErrEmptyData is returned when empty data is provided
	ErrEmptyData = fmt.Errorf("protobuf deserializer: data cannot be empty")

	// ErrNilSchema is returned when nil schema is provided
	ErrNilSchema = fmt.Errorf("protobuf deserializer: schema cannot be nil")

	// ErrInvalidSchema is returned when schema is invalid
	ErrInvalidSchema = fmt.Errorf("protobuf deserializer: invalid schema definition")

	// ErrSchemaNotProtobuf is returned when schema is not a protobuf schema
	ErrSchemaNotProtobuf = fmt.Errorf("protobuf deserializer: schema is not a protobuf schema")

	// ErrMessageDescriptorNotFound is returned when the message descriptor cannot be found
	ErrMessageDescriptorNotFound = fmt.Errorf("protobuf deserializer: message descriptor not found")

	// ErrDeserializationFailed is returned when protobuf deserialization fails
	ErrDeserializationFailed = fmt.Errorf("protobuf deserializer: deserialization failed")

	// ErrNilDescriptor is returned by NewProtobufDeserializer when the
	// configuration omits ProtobufMessageDescriptor. Chained through
	// core.ErrInvalidProtobufPayload (and therefore core.ErrGSR) so callers
	// can detect "protobuf misconfigured" without importing this package.
	// Java parity: ProtobufDeserializer's constructor surfaces missing
	// descriptor state via AWSSchemaRegistryException, not a JVM panic.
	ErrNilDescriptor = fmt.Errorf("%w: protobuf deserializer: message descriptor cannot be nil", gsrcore.ErrInvalidProtobufPayload)

	// ErrMissingProtobufPOJOType is returned when ProtobufMessageType is set
	// to POJO but ProtobufPOJOMessage is nil in the configuration. This is a
	// caller-side configuration mistake surfaced at Deserialize time (not at
	// config parse time, because ProtobufPOJOMessage is set via the
	// programmatic Go API rather than a string-keyed configMap). Intentionally
	// does NOT wrap ErrGSR — this is not a Glue service error. Java parity:
	// analogous to the IllegalArgumentException thrown when POJO mode is used
	// without a registered message class.
	ErrMissingProtobufPOJOType = fmt.Errorf("protobuf deserializer: POJO requires ProtobufPOJOMessage in configuration")
)

// ProtobufDeserializationError represents an error that occurred during Protobuf
// deserialization. Mirrors JsonDeserializationError / AvroDeserializationError
// shape so the three format wrappers have a consistent surface (Phase 4.12 §4
// sentinel registry: row "*protobuf.ProtobufDeserializationError"). PBI-4.12-4
// uses this wrapper when wiring proto.Unmarshal failures with
// gsrcore.ErrMalformedProtobuf in the Cause chain so callers can match either
// via errors.As against the wrapper type OR via errors.Is against the sentinel.
type ProtobufDeserializationError struct {
	Message string
	Cause   error
}

func (e *ProtobufDeserializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("protobuf deserialization error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("protobuf deserialization error: %s", e.Message)
}

// Unwrap exposes the Cause so errors.Is / errors.As traverse the chain.
// Required by PBI-4.12-4 so errors.Is(err, gsrcore.ErrMalformedProtobuf)
// resolves through this wrapper to the sentinel embedded in Cause.
func (e *ProtobufDeserializationError) Unwrap() error {
	return e.Cause
}

// Is supports errors.Is(protobufErr, gsrcore.ErrGSR) — every
// ProtobufDeserializationError is by definition a Glue Schema Registry error.
// Matches the parity contract on JsonDeserializationError /
// AvroDeserializationError. Other targets (e.g., gsrcore.ErrMalformedProtobuf)
// flow through Unwrap() against the Cause chain. Phase 4.12 §4 invariant.
func (e *ProtobufDeserializationError) Is(target error) bool {
	return target == gsrcore.ErrGSR
}

// ProtobufDeserializer implements the DataFormatDeserializer interface for protobuf messages.
// It supports proto2 and proto3 without extensions or groups.
type ProtobufDeserializer struct {
	// messageDescriptor holds the protobuf message descriptor for deserialization
	messageDescriptor protoreflect.MessageDescriptor
	// config holds the configuration for the deserializer
	config *common.Configuration
}

// NewProtobufDeserializer creates a new ProtobufDeserializer instance.
// The configuration must contain the protobuf message descriptor for
// deserialization; a nil descriptor surfaces as the typed sentinel
// ErrNilDescriptor (whose chain reaches core.ErrInvalidProtobufPayload and
// core.ErrGSR) — not a panic. Java parity: ProtobufDeserializer reports
// missing descriptor via AWSSchemaRegistryException, recoverable by callers.
func NewProtobufDeserializer(config *common.Configuration) (*ProtobufDeserializer, error) {
	if config == nil {
		return nil, common.ErrNilConfig
	}
	if config.ProtobufMessageDescriptor == nil {
		return nil, ErrNilDescriptor
	}
	return &ProtobufDeserializer{
		config:            config,
		messageDescriptor: config.ProtobufMessageDescriptor,
	}, nil
}

// Deserialize takes encoded protobuf data and a schema, and returns the deserialized message.
// The schema must be a protobuf schema with a valid schema definition.
//
// Parameters:
//
//	data: The encoded protobuf byte data to deserialize
//	schema: The protobuf schema information needed for deserialization
//
// Returns:
//
//	interface{}: The deserialized protobuf message as a dynamic message
//	error: Any error that occurred during deserialization
func (pd *ProtobufDeserializer) Deserialize(data []byte, schema *gsrcore.Schema) (interface{}, error) {
	// Validate input parameters
	if data == nil {
		return nil, ErrNilData
	}

	if len(data) == 0 {
		return nil, ErrEmptyData
	}

	if schema == nil {
		return nil, ErrNilSchema
	}

	// Verify this is a protobuf schema
	if schema.DataFormat != "PROTOBUF" {
		return nil, ErrSchemaNotProtobuf
	}

	if schema.SchemaDefinition == "" {
		return nil, ErrInvalidSchema
	}

	// POJO dispatch: when the caller has configured ProtobufMessageTypePOJO,
	// unmarshal into a clone of the caller-provided concrete proto.Message
	// rather than a dynamicpb.Message. This takes precedence over the default
	// dynamic path so POJO callers get the concrete type they registered.
	// Java parity: ProtobufMessageType.POJO routes to the caller's registered
	// message class via reflection; here we use proto.Clone for allocation.
	if pd.config.ProtobufMessageType == common.ProtobufMessageTypePOJO {
		if pd.config.ProtobufPOJOMessage == nil {
			return nil, fmt.Errorf("%w: missing ProtobufPOJOMessage in configuration", ErrMissingProtobufPOJOType)
		}
		dst := proto.Clone(pd.config.ProtobufPOJOMessage)
		if err := proto.Unmarshal(data, dst); err != nil {
			return nil, &ProtobufDeserializationError{
				Message: "failed to deserialize Protobuf data (POJO)",
				Cause:   fmt.Errorf("%w: %w: %v", gsrcore.ErrMalformedProtobuf, ErrDeserializationFailed, err),
			}
		}
		return dst, nil
	}

	// Create a new dynamic message instance
	dynamicMessage := dynamicpb.NewMessage(pd.messageDescriptor)

	// Unmarshal the protobuf data.
	//
	// Phase 4.12 §3.9 item 26 (Protobuf): wrap the proto.Unmarshal failure
	// in the *ProtobufDeserializationError struct (introduced by PBI-4.12-1
	// for JSON / Avro parity) and embed gsrcore.ErrMalformedProtobuf in the
	// Cause chain so callers can resolve errors.Is(err, ErrMalformedProtobuf)
	// (and transitively errors.Is(err, ErrGSR)) through the wrapper's
	// Unwrap(). The pre-existing ErrDeserializationFailed sentinel and its
	// "protobuf deserializer: deserialization failed:" diagnostic substring
	// are preserved inside the same Cause chain so prior errors.Is /
	// error-message regression assertions stay green.
	if err := proto.Unmarshal(data, dynamicMessage); err != nil {
		return nil, &ProtobufDeserializationError{
			Message: "failed to deserialize Protobuf data",
			Cause:   fmt.Errorf("%w: %w: %v", gsrcore.ErrMalformedProtobuf, ErrDeserializationFailed, err),
		}
	}

	return dynamicMessage, nil
}


