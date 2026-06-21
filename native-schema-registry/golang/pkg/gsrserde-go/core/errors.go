package gsrserde

import (
	"errors"
	"fmt"
)

// ErrMessageTypeNotFound is returned when a protobuf message-type name is not
// present in the schema's BFS+lex-sorted descriptor list.
//
// Java parity: AWSSchemaRegistryException thrown at
// serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/protobuf/MessageIndexFinder.java:35-39.
//
// Callers MUST check this with errors.Is so the encoder refuses to ship a
// payload tagged with the prefix for the first sorted message-type (a silent
// 0 return would corrupt the wire format — see §2.2 of GSR-Golang-Plan-revision.md).
var ErrMessageTypeNotFound = errors.New("protobuf message type not found in schema")

// SerializationError represents a serialization error
type SerializationError struct {
	Message string
}

func (e *SerializationError) Error() string {
	return fmt.Sprintf("serialization error: %s", e.Message)
}

// NewSerializationError creates a new serialization error
func NewSerializationError(message string) error {
	return &SerializationError{Message: message}
}

// DeserializationError represents a deserialization error
type DeserializationError struct {
	Message string
}

func (e *DeserializationError) Error() string {
	return fmt.Sprintf("deserialization error: %s", e.Message)
}

// NewDeserializationError creates a new deserialization error
func NewDeserializationError(message string) error {
	return &DeserializationError{Message: message}
}
