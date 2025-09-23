package gsrserde

import "fmt"

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
