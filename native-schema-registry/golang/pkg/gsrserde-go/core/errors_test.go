package gsrserde

import (
	"testing"
)

func TestSerializationError(t *testing.T) {
	err := NewSerializationError("test message")
	
	if err.Error() != "serialization error: test message" {
		t.Errorf("Expected 'serialization error: test message', got '%s'", err.Error())
	}
}

func TestDeserializationError(t *testing.T) {
	err := NewDeserializationError("test message")
	
	if err.Error() != "deserialization error: test message" {
		t.Errorf("Expected 'deserialization error: test message', got '%s'", err.Error())
	}
}

func TestErrorTypes(t *testing.T) {
	serErr := &SerializationError{Message: "test"}
	if serErr.Error() != "serialization error: test" {
		t.Errorf("SerializationError.Error() failed")
	}
	
	deserErr := &DeserializationError{Message: "test"}
	if deserErr.Error() != "deserialization error: test" {
		t.Errorf("DeserializationError.Error() failed")
	}
}
