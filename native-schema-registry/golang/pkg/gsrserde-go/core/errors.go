package gsrserde

import (
	"errors"
	"fmt"
)

// The Go error surface maps Java's two exception classes —
// AWSSchemaRegistryException (general) and
// GlueSchemaRegistryIncompatibleDataException (wire-format failure) — onto
// Go-idiomatic sentinel errors usable with errors.Is / errors.As, plus two
// wrapping types (SerializationError, DeserializationError) for failures
// that originate inside the encode/decode flow.
//
// Java parity:
//   - AWSSchemaRegistryException                              ⇒ ErrGSR
//   - GlueSchemaRegistryIncompatibleDataException             ⇒ ErrIncompatibleData
//   - GlueSchemaRegistryIncompatibleDataException::UNKNOWN_DATA_ERROR_MESSAGE          ⇒ wrapped under ErrIncompatibleData
//   - GlueSchemaRegistryIncompatibleDataException::UNKNOWN_HEADER_VERSION_BYTE_ERROR_MESSAGE ⇒ same
//   - GlueSchemaRegistryIncompatibleDataException::UNKNOWN_COMPRESSION_BYTE_ERROR_MESSAGE    ⇒ same
//
// Java's hierarchy is `IncompatibleData extends AWSSchemaRegistryException`,
// so an errors.Is(err, ErrIncompatibleData) caller also satisfies
// errors.Is(err, ErrGSR) — locked down by errors_test.go.

// ErrGSR is the umbrella sentinel for any GSR-originated failure.
// Anchored to Java AWSSchemaRegistryException.
var ErrGSR = errors.New("AWS Glue Schema Registry error")

// ErrIncompatibleData is the sentinel for payloads whose 18-byte prefix is
// missing or malformed. Anchored to Java
// GlueSchemaRegistryIncompatibleDataException. Wraps ErrGSR so callers can
// errors.Is at either granularity.
var ErrIncompatibleData = fmt.Errorf("%w: payload is not in GSR wire format", ErrGSR)

// ErrMessageTypeNotFound is returned when a protobuf message-type name is not
// present in the schema's BFS+lex-sorted descriptor list. Wraps ErrGSR.
//
// Java parity: AWSSchemaRegistryException thrown at
// serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/protobuf/MessageIndexFinder.java:35-39.
var ErrMessageTypeNotFound = fmt.Errorf("%w: protobuf message type not found in schema", ErrGSR)

// ErrSchemaAutoRegistrationDisabled is returned when the encoder hits an
// unknown schema and SchemaAutoRegistrationEnabled is false. Mirrors Java
// AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG.
var ErrSchemaAutoRegistrationDisabled = fmt.Errorf("%w: schema auto-registration is disabled", ErrGSR)

// ErrInvalidProtobufPayload is returned by the protobuf format layer when a
// candidate value (object or byte slice) is rejected as not a valid protobuf
// message. Wraps ErrGSR so callers can errors.Is at either granularity.
//
// Java parity: AWSSchemaRegistryException thrown at
// serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/protobuf/ProtobufSerializer.java:113-115
// when an object is not an instance of com.google.protobuf.Message.
var ErrInvalidProtobufPayload = fmt.Errorf("%w: value is not a valid protobuf message", ErrGSR)

// ErrMalformedJSON is the per-format sentinel for a JSON payload that fails
// the JSON deserializer's malformed-payload contract: syntactic parse failures
// (encoding/json rejection) and non-UTF-8 payloads. Does NOT cover
// schema-validation failures — those surface as *JsonValidationError without
// ErrMalformedJSON in the chain. Wraps ErrGSR so callers can errors.Is at
// either granularity. Phase 4.12 §3.9 item 26 / §4 / board-fixes MAJOR-1.
var ErrMalformedJSON = fmt.Errorf("%w: malformed JSON", ErrGSR)

// ErrMalformedAvro is the per-format sentinel for an Avro payload that fails
// the Avro deserializer's malformed-payload contract (decode failure against
// the writer schema, or invalid Avro binary). Wraps ErrGSR. Phase 4.12 §3.9
// item 26 / §4.
var ErrMalformedAvro = fmt.Errorf("%w: malformed Avro", ErrGSR)

// ErrMalformedProtobuf is the per-format sentinel for a Protobuf payload that
// fails the Protobuf deserializer's malformed-payload contract (proto.Unmarshal
// failure, or message-type mismatch on a structurally valid payload). Wraps
// ErrGSR. Note: this sentinel is narrower than ErrInvalidProtobufPayload —
// ErrInvalidProtobufPayload signals "value is not a protobuf at all" (e.g., a
// nil descriptor or non-Message Go object), whereas ErrMalformedProtobuf
// signals "bytes were submitted but failed unmarshal against the descriptor".
// Both wrap ErrGSR. Phase 4.12 §3.9 item 26 / §4.
var ErrMalformedProtobuf = fmt.Errorf("%w: malformed Protobuf", ErrGSR)

// SerializationError wraps a failure that originated inside Encode. It carries
// the original error so errors.Unwrap, errors.Is, and errors.As all chain
// through to the underlying cause (sentinel, SDK error, parser error, …).
type SerializationError struct {
	Message string
	Cause   error
}

func (e *SerializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("serialization error: %s: %s", e.Message, e.Cause)
	}
	return fmt.Sprintf("serialization error: %s", e.Message)
}

func (e *SerializationError) Unwrap() error { return e.Cause }

// Is supports errors.Is(serErr, ErrGSR) — every SerializationError is by
// definition a Glue Schema Registry error.
func (e *SerializationError) Is(target error) bool {
	return target == ErrGSR
}

// NewSerializationError builds a SerializationError with no underlying cause.
func NewSerializationError(message string) error {
	return &SerializationError{Message: message}
}

// WrapSerializationError builds a SerializationError that wraps cause.
func WrapSerializationError(message string, cause error) error {
	return &SerializationError{Message: message, Cause: cause}
}

// DeserializationError mirrors SerializationError for the decode path.
type DeserializationError struct {
	Message string
	Cause   error
}

func (e *DeserializationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("deserialization error: %s: %s", e.Message, e.Cause)
	}
	return fmt.Sprintf("deserialization error: %s", e.Message)
}

func (e *DeserializationError) Unwrap() error { return e.Cause }

func (e *DeserializationError) Is(target error) bool {
	return target == ErrGSR
}

// NewDeserializationError builds a DeserializationError with no cause.
func NewDeserializationError(message string) error {
	return &DeserializationError{Message: message}
}

// WrapDeserializationError builds a DeserializationError that wraps cause.
func WrapDeserializationError(message string, cause error) error {
	return &DeserializationError{Message: message, Cause: cause}
}
