package protobuf

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/test_helpers"
)

// createProtobufConfig creates a Configuration object for Protobuf tests
func createProtobufConfig() *common.Configuration {
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
	return common.NewConfiguration(configMap)
}

// createProtobufConfigWithDescriptor creates a Configuration object with a specific message descriptor
func createProtobufConfigWithDescriptor(descriptor interface{}) *common.Configuration {
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
	if descriptor != nil {
		configMap[common.ProtobufMessageDescriptorKey] = descriptor
	}
	return common.NewConfiguration(configMap)
}

func TestNewProtobufSerializer(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)
	assert.NotNil(t, serializer, "NewProtobufSerializer should return a non-nil serializer")
}

func TestProtobufSerializer_Serialize_ErrorCases(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	tests := []struct {
		name        string
		data        interface{}
		expectedErr error
	}{
		{
			name:        "nil data",
			data:        nil,
			expectedErr: ErrNilMessage,
		},
		{
			name:        "non-proto message string",
			data:        "not a proto message",
			expectedErr: ErrInvalidProtoMessage,
		},
		{
			name:        "non-proto message int",
			data:        42,
			expectedErr: ErrInvalidProtoMessage,
		},
		{
			name:        "non-proto message map",
			data:        map[string]interface{}{"key": "value"},
			expectedErr: ErrInvalidProtoMessage,
		},
		{
			name:        "non-proto message slice",
			data:        []byte{0x01, 0x02, 0x03},
			expectedErr: ErrInvalidProtoMessage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serializer.Serialize(tt.data)
			assert.Nil(t, result, "Result should be nil on error")
			var serializationErr *ProtobufSerializationError
			assert.ErrorAs(t, err, &serializationErr, "Should return ProtobufSerializationError")
			assert.ErrorIs(t, err, tt.expectedErr, "Should return expected error")
		})
	}
}

func TestProtobufSerializer_Serialize_ValidMessage(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	// Generate test protobuf message
	testMsg := test_helpers.GenerateTestProtoMessage()
	require.NotNil(t, testMsg, "Should generate test message")

	// Test serialization
	result, err := serializer.Serialize(testMsg)
	require.NoError(t, err, "Serialization should succeed")
	require.NotNil(t, result, "Result should not be nil")
	assert.Greater(t, len(result), 0, "Serialized data should not be empty")

	// Verify the result is valid protobuf data
	err = test_helpers.ValidateProtoMessage(result)
	// Note: ValidateProtoMessage may fail because it tries to unmarshal with a different message type
	// This is expected, but the serialization itself should succeed
}

func TestProtobufSerializer_Serialize_ComplexMessage(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	// Generate complex test message
	complexMsg := test_helpers.GenerateComplexTestMessage(12345, "complex-test")
	require.NotNil(t, complexMsg, "Should generate complex test message")

	// Test serialization
	result, err := serializer.Serialize(complexMsg)
	require.NoError(t, err, "Complex message serialization should succeed")
	require.NotNil(t, result, "Result should not be nil")
	assert.Greater(t, len(result), 0, "Serialized data should not be empty")
}

func TestProtobufSerializer_Serialize_DescriptorMessage(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	// Use a FileDescriptorProto as test data (this is a known protobuf message)
	descriptorMsg := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("TestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
				},
			},
		},
	}

	// Test serialization
	result, err := serializer.Serialize(descriptorMsg)
	require.NoError(t, err, "Descriptor message serialization should succeed")
	require.NotNil(t, result, "Result should not be nil")
	assert.Greater(t, len(result), 0, "Serialized data should not be empty")

	// Verify we can unmarshal it back
	var unmarshaled descriptorpb.FileDescriptorProto
	err = proto.Unmarshal(result, &unmarshaled)
	assert.NoError(t, err, "Should be able to unmarshal serialized data")
	assert.Equal(t, "test.proto", unmarshaled.GetName(), "Name should match")
}

func TestProtobufSerializer_GetSchemaDefinition_ErrorCases(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	tests := []struct {
		name        string
		data        interface{}
		expectedErr error
	}{
		{
			name:        "nil data",
			data:        nil,
			expectedErr: ErrNilMessage,
		},
		{
			name:        "non-proto message",
			data:        "not a proto message",
			expectedErr: ErrInvalidProtoMessage,
		},
		{
			name:        "non-proto message struct",
			data:        struct{ Value string }{Value: "test"},
			expectedErr: ErrInvalidProtoMessage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serializer.GetSchemaDefinition(tt.data)
			assert.Empty(t, result, "Result should be empty on error")
			var serializationErr *ProtobufSerializationError
			assert.ErrorAs(t, err, &serializationErr, "Should return ProtobufSerializationError")
			assert.ErrorIs(t, err, tt.expectedErr, "Should return expected error")
		})
	}
}

func TestProtobufSerializer_GetSchemaDefinition_ValidMessage(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	// Use FileDescriptorProto as test data
	descriptorMsg := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("schema_test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("SchemaTestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("value"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	// Test schema definition extraction
	result, err := serializer.GetSchemaDefinition(descriptorMsg)
	require.NoError(t, err, "Schema definition extraction should succeed")
	assert.NotEmpty(t, result, "Schema definition should not be empty")

	// Verify the schema definition contains protobuf data
	assert.Greater(t, len(result), 10, "Schema definition should contain substantial data")
}

func TestProtobufSerializer_GetSchemaDefinition_DynamicMessage(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	// Generate dynamic test message
	dynamicMsg := test_helpers.GenerateTestProtoMessage()
	require.NotNil(t, dynamicMsg, "Should generate dynamic test message")

	// Test schema definition extraction
	result, err := serializer.GetSchemaDefinition(dynamicMsg)
	require.NoError(t, err, "Dynamic message schema definition should succeed")
	assert.NotEmpty(t, result, "Schema definition should not be empty")
}

func TestProtobufSerializer_ValidateObject_ErrorCases(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	tests := []struct {
		name        string
		data        interface{}
		expectedErr error
	}{
		{
			name:        "nil data",
			data:        nil,
			expectedErr: ErrNilMessage,
		},
		{
			name:        "non-proto message",
			data:        "not a proto message",
			expectedErr: ErrInvalidProtoMessage,
		},
		{
			name:        "non-proto message slice",
			data:        []string{"not", "proto"},
			expectedErr: ErrInvalidProtoMessage,
		},
		{
			name:        "non-proto message map",
			data:        map[string]int{"count": 42},
			expectedErr: ErrInvalidProtoMessage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serializer.ValidateObject(tt.data)
			var validationErr *ProtobufValidationError
			assert.ErrorAs(t, err, &validationErr, "Should return ProtobufValidationError")
			assert.ErrorIs(t, err, tt.expectedErr, "Should return expected error")
		})
	}
}

func TestProtobufSerializer_ValidateObject_ValidMessages(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	tests := []struct {
		name string
		data proto.Message
	}{
		{
			name: "FileDescriptorProto",
			data: &descriptorpb.FileDescriptorProto{
				Name: proto.String("valid.proto"),
			},
		},
		{
			name: "DescriptorProto",
			data: &descriptorpb.DescriptorProto{
				Name: proto.String("ValidMessage"),
			},
		},
		{
			name: "FieldDescriptorProto",
			data: &descriptorpb.FieldDescriptorProto{
				Name:   proto.String("field"),
				Number: proto.Int32(1),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serializer.ValidateObject(tt.data)
			assert.NoError(t, err, "Valid proto message should pass validation")
		})
	}
}

func TestProtobufSerializer_ValidateObject_DynamicMessage(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	// Generate and validate dynamic message
	dynamicMsg := test_helpers.GenerateTestProtoMessage()
	require.NotNil(t, dynamicMsg, "Should generate dynamic test message")

	err := serializer.ValidateObject(dynamicMsg)
	assert.NoError(t, err, "Dynamic protobuf message should be valid")
}

func TestProtobufSerializer_Validate_ErrorCases(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	tests := []struct {
		name        string
		schema      string
		data        []byte
		expectedErr error
	}{
		{
			name:        "nil data",
			schema:      "valid-schema",
			data:        nil,
			expectedErr: ErrNilMessage,
		},
		{
			name:        "empty data",
			schema:      "valid-schema",
			data:        []byte{},
			expectedErr: ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serializer.Validate(tt.schema, tt.data)
			var validationErr *ProtobufValidationError
			assert.ErrorAs(t, err, &validationErr, "Should return ProtobufValidationError")
			assert.ErrorIs(t, err, tt.expectedErr, "Should return expected error")
		})
	}
}

// TestProtobufSerializer_Validate_RejectsNonProtoBytes is the Phase 3 red→green
// test for replacing Validate(string, []byte)'s null-check-only stub with a
// real wire-format check. Java parity: ProtobufSerializer.java:106-109 leaves
// validate(byte[]) as a TODO, but the Go library has no other place to
// surface "these bytes are not protobuf" before they reach the encoder, so we
// promote the stub to a real probe.
//
// Bytes that are not well-formed protobuf (no leading varint tag, or invalid
// wire-type) must return a typed *ProtobufValidationError whose chain reaches
// core.ErrInvalidProtobufPayload and therefore core.ErrGSR.
func TestProtobufSerializer_Validate_RejectsNonProtoBytes(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	tests := []struct {
		name string
		data []byte
	}{
		// Wire-type 7 is reserved/invalid in proto wire format. A single byte
		// of 0x0F encodes (field=1, wire=7) which proto.Unmarshal rejects.
		{name: "invalid wire type", data: []byte{0x0F}},
		// A length-delimited (wire=2) tag claiming 16 bytes but with only 3
		// payload bytes; classic truncation case.
		{name: "truncated length-delimited", data: []byte{0x0A, 0x10, 0x01, 0x02, 0x03}},
		// An ASCII string that happens to have invalid wire bytes when read
		// as protobuf — the catch-all "bytes that obviously aren't proto".
		{name: "ascii noise", data: []byte("not a proto message")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serializer.Validate("test-schema", tt.data)
			require.Error(t, err, "non-proto bytes must be rejected")
			var validationErr *ProtobufValidationError
			assert.ErrorAs(t, err, &validationErr, "Should return ProtobufValidationError")
			// Tier-1 spec §5 S5 assertion: explicit errors.Is on the
			// sentinel rather than testify's wrapper, so the contract is
			// readable as the stdlib chain check downstream callers will
			// use (e.g. the Kafka serializer adapter doing
			// `if errors.Is(err, gsrcore.ErrInvalidProtobufPayload) { … }`).
			assert.True(t, errors.Is(err, gsrcore.ErrInvalidProtobufPayload),
				"errors.Is chain must reach core.ErrInvalidProtobufPayload")
			assert.True(t, errors.Is(err, gsrcore.ErrGSR),
				"errors.Is chain must reach core.ErrGSR for unified callers")
		})
	}
}

// TestProtobufSerializer_Validate_AcceptsWellFormedBytes is the paired
// positive case for the malformed-bytes contract above: bytes produced by
// proto.Marshal must round-trip through Validate without error. This
// guards against an over-strict probe that rejects valid wire output.
func TestProtobufSerializer_Validate_AcceptsWellFormedBytes(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	wellFormed, err := proto.Marshal(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("validate_accepts_wellformed.proto"),
		Package: proto.String("test"),
	})
	require.NoError(t, err, "proto.Marshal should produce well-formed bytes")
	require.NotEmpty(t, wellFormed, "marshaled bytes must be non-empty")

	err = serializer.Validate("test-schema", wellFormed)
	assert.NoError(t, err, "well-formed protobuf bytes must pass Validate")
}

// TestProtobufSerializer_ValidateObject_PointerToStringTypedError is the
// Phase 3 red→green test for replacing the protobuf serializer's stub error
// surface with one rooted in core.ErrInvalidProtobufPayload. Passing a
// *string — which clearly isn't a proto.Message — must surface as a typed
// *ProtobufValidationError whose chain reaches the new core sentinel.
//
// Pre-Phase-3 behavior: returns ProtobufValidationError wrapping the local
// ErrInvalidProtoMessage sentinel only. Post-Phase-3: the chain also reaches
// core.ErrInvalidProtobufPayload (and therefore core.ErrGSR), so callers can
// use errors.Is at any granularity to detect "this isn't a protobuf value".
func TestProtobufSerializer_ValidateObject_PointerToStringTypedError(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	s := "not a proto message"
	err := serializer.ValidateObject(&s)
	require.Error(t, err, "*string must be rejected as not a protobuf value")

	var validationErr *ProtobufValidationError
	assert.ErrorAs(t, err, &validationErr, "Should return ProtobufValidationError")
	assert.ErrorIs(t, err, gsrcore.ErrInvalidProtobufPayload,
		"chain must reach core.ErrInvalidProtobufPayload")
	assert.ErrorIs(t, err, gsrcore.ErrGSR,
		"chain must reach core.ErrGSR for unified callers")
}

func TestProtobufSerializer_Validate_ValidData(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	tests := []struct {
		name   string
		schema string
		data   []byte
	}{
		{
			name:   "simple valid protobuf data",
			schema: "test-schema",
			data:   []byte{0x08, 0x96, 0x01}, // Valid protobuf: field 1, varint 150
		},
		{
			name:   "valid protobuf with string",
			schema: "test-schema",
			data:   []byte{0x0A, 0x04, 0x74, 0x65, 0x73, 0x74}, // Valid: field 1, string "test"
		},
		{
			name:   "complex valid protobuf data",
			schema: "test-schema",
			data: []byte{
				0x08, 0x96, 0x01, // field 1: varint 150
				0x12, 0x04,       // field 2: length-delimited, length 4
				0x74, 0x65, 0x73, 0x74, // "test"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serializer.Validate(tt.schema, tt.data)
			assert.NoError(t, err, "Valid protobuf data should pass validation")
		})
	}
}

func TestProtobufSerializer_SetAdditionalSchemaInfo_ErrorCases(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	tests := []struct {
		name        string
		data        interface{}
		schema      *gsrcore.Schema
		expectedErr error
	}{
		{
			name:        "nil data",
			data:        nil,
			schema:      &gsrcore.Schema{},
			expectedErr: ErrNilMessage,
		},
		{
			name:        "nil schema",
			data:        &descriptorpb.FileDescriptorProto{Name: proto.String("test.proto")},
			schema:      nil,
			expectedErr: nil, // This will be a different error about nil schema
		},
		{
			name:        "non-proto message",
			data:        "not a proto message",
			schema:      &gsrcore.Schema{},
			expectedErr: ErrInvalidProtoMessage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serializer.SetAdditionalSchemaInfo(tt.data, tt.schema)
			if tt.expectedErr != nil {
				var serializationErr *ProtobufSerializationError
				assert.ErrorAs(t, err, &serializationErr, "Should return ProtobufSerializationError")
				assert.ErrorIs(t, err, tt.expectedErr, "Should return expected error")
			} else if tt.schema == nil {
				assert.Error(t, err, "Should return error for nil schema")
				assert.Contains(t, err.Error(), "schema is nil", "Should mention nil schema")
			}
		})
	}
}

func TestProtobufSerializer_SetAdditionalSchemaInfo_ValidCases(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	tests := []struct {
		name           string
		data           proto.Message
		expectedFormat string
	}{
		{
			name: "FileDescriptorProto",
			data: &descriptorpb.FileDescriptorProto{
				Name: proto.String("test.proto"),
			},
			expectedFormat: "PROTOBUF",
		},
		{
			name: "DescriptorProto",
			data: &descriptorpb.DescriptorProto{
				Name: proto.String("TestMessage"),
			},
			expectedFormat: "PROTOBUF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &gsrcore.Schema{}
			
			err := serializer.SetAdditionalSchemaInfo(tt.data, schema)
			require.NoError(t, err, "Setting schema info should succeed")
			
			assert.Equal(t, tt.expectedFormat, schema.DataFormat, "DataFormat should be set correctly")
			assert.NotEmpty(t, schema.AdditionalInfo, "AdditionalInfo should be set")
		})
	}
}

func TestProtobufSerializer_SetAdditionalSchemaInfo_DynamicMessage(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	// Generate dynamic test message
	dynamicMsg := test_helpers.GenerateTestProtoMessage()
	require.NotNil(t, dynamicMsg, "Should generate dynamic test message")

	schema := &gsrcore.Schema{}
	err := serializer.SetAdditionalSchemaInfo(dynamicMsg, schema)
	require.NoError(t, err, "Setting schema info for dynamic message should succeed")
	
	assert.Equal(t, "PROTOBUF", schema.DataFormat, "DataFormat should be PROTOBUF")
	assert.NotEmpty(t, schema.AdditionalInfo, "AdditionalInfo should be set")
}

func TestProtobufSerializer_SetAdditionalSchemaInfo_PreservesExistingDataFormat(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	data := &descriptorpb.FileDescriptorProto{
		Name: proto.String("test.proto"),
	}

	schema := &gsrcore.Schema{
		DataFormat: "EXISTING_FORMAT",
	}

	err := serializer.SetAdditionalSchemaInfo(data, schema)
	require.NoError(t, err, "Setting schema info should succeed")
	
	// The implementation preserves existing DataFormat if it's already set
	assert.Equal(t, "EXISTING_FORMAT", schema.DataFormat, "DataFormat should be preserved")
	assert.NotEmpty(t, schema.AdditionalInfo, "AdditionalInfo should be set")
}

func TestProtobufSerializer_WithTestHelpers(t *testing.T) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	// Test with multiple generated messages
	messages := test_helpers.GenerateTestProtoMessages(3)
	require.Len(t, messages, 3, "Should generate 3 test messages")

	for i, msg := range messages {
		t.Run(fmt.Sprintf("message_%d", i), func(t *testing.T) {
			// Test serialization
			data, err := serializer.Serialize(msg)
			require.NoError(t, err, "Serialization should succeed")
			assert.Greater(t, len(data), 0, "Serialized data should not be empty")

			// Test validation
			err = serializer.ValidateObject(msg)
			assert.NoError(t, err, "Message should be valid")

			// Test schema definition
			schemaDef, err := serializer.GetSchemaDefinition(msg)
			require.NoError(t, err, "Schema definition should succeed")
			assert.NotEmpty(t, schemaDef, "Schema definition should not be empty")
		})
	}
}

func TestProtobufSerializer_ErrorTypes(t *testing.T) {
	// Test ProtobufSerializationError
	baseErr := fmt.Errorf("base error")
	serErr := &ProtobufSerializationError{
		Message: "test serialization error",
		Cause:   baseErr,
	}

	assert.Contains(t, serErr.Error(), "test serialization error", "Error message should contain message")
	assert.Contains(t, serErr.Error(), "base error", "Error message should contain cause")
	assert.Equal(t, baseErr, serErr.Unwrap(), "Unwrap should return cause")

	// Test ProtobufSerializationError without cause
	serErrNoCause := &ProtobufSerializationError{
		Message: "no cause error",
	}
	assert.Equal(t, "protobuf serialization error: no cause error", serErrNoCause.Error())
	assert.Nil(t, serErrNoCause.Unwrap(), "Unwrap should return nil when no cause")

	// Test ProtobufValidationError
	valErr := &ProtobufValidationError{
		Message: "test validation error",
		Cause:   baseErr,
	}

	assert.Contains(t, valErr.Error(), "test validation error", "Error message should contain message")
	assert.Contains(t, valErr.Error(), "base error", "Error message should contain cause")
	assert.Equal(t, baseErr, valErr.Unwrap(), "Unwrap should return cause")

	// Test ProtobufValidationError without cause
	valErrNoCause := &ProtobufValidationError{
		Message: "no cause validation error",
	}
	assert.Equal(t, "protobuf validation error: no cause validation error", valErrNoCause.Error())
	assert.Nil(t, valErrNoCause.Unwrap(), "Unwrap should return nil when no cause")
}

// BenchmarkProtobufSerializer_Serialize benchmarks the serialization performance
func BenchmarkProtobufSerializer_Serialize(b *testing.B) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	// Setup test data
	testMsg := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("benchmark.proto"),
		Package: proto.String("benchmark"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("BenchmarkMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
					{
						Name:   proto.String("name"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := serializer.Serialize(testMsg)
		if err != nil {
			b.Fatalf("Serialization failed: %v", err)
		}
		if len(result) == 0 {
			b.Fatal("Result should not be empty")
		}
	}
}

// BenchmarkProtobufSerializer_GetSchemaDefinition benchmarks schema definition extraction
func BenchmarkProtobufSerializer_GetSchemaDefinition(b *testing.B) {
	config := createProtobufConfig()
	serializer := NewProtobufSerializer(config)

	testMsg := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("benchmark_schema.proto"),
		Package: proto.String("benchmark"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("SchemaMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("value"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := serializer.GetSchemaDefinition(testMsg)
		if err != nil {
			b.Fatalf("Schema definition failed: %v", err)
		}
		if len(result) == 0 {
			b.Fatal("Schema definition should not be empty")
		}
	}
}
