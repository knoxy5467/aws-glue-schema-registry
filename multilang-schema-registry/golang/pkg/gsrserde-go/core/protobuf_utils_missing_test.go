package gsrserde

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertBase64SchemaToStringSchema_InvalidBase64(t *testing.T) {
	_, err := ConvertBase64SchemaToStringSchema("invalid-base64!")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode base64 schema")
}

func TestConvertBase64SchemaToStringSchema_InvalidProto(t *testing.T) {
	// Valid base64 but invalid protobuf data
	invalidProto := "aGVsbG8gd29ybGQ=" // "hello world" in base64
	_, err := ConvertBase64SchemaToStringSchema(invalidProto)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal FileDescriptorProto")
}

func TestConvertBase64SchemaToStringSchema_InvalidFileDescriptor(t *testing.T) {
	// Create a minimal but invalid FileDescriptorProto
	emptyProto := "Cg==" // Empty protobuf message in base64
	_, err := ConvertBase64SchemaToStringSchema(emptyProto)
	assert.Error(t, err)
	// Should fail at some stage
	assert.NotNil(t, err)
}
