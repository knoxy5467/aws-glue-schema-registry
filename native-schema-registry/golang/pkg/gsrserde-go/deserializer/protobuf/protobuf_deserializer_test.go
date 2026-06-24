package protobuf

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/test_helpers"
)

// createProtobufDeserializerConfig creates a Configuration object for Protobuf deserializer tests
func createProtobufDeserializerConfig() *common.Configuration {
	// Create a simple message descriptor for testing
	desc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test_deserializer.proto"),
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
					{
						Name:   proto.String("name"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	// Create file descriptor
	fileDesc, _ := protodesc.NewFile(desc, protoregistry.GlobalFiles)

	// Get message descriptor
	msgDesc := fileDesc.Messages().ByName("TestMessage")

	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
	configMap[common.ProtobufMessageDescriptorKey] = msgDesc
	return common.NewConfiguration(configMap)
}

// createProtobufDeserializerConfigWithDescriptor creates a Configuration object with a specific message descriptor
func createProtobufDeserializerConfigWithDescriptor(descriptor protoreflect.MessageDescriptor) *common.Configuration {
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
	if descriptor != nil {
		configMap[common.ProtobufMessageDescriptorKey] = descriptor
	}
	return common.NewConfiguration(configMap)
}

// createValidProtobufSchema creates a valid protobuf schema for testing
func createValidProtobufSchema() *gsrcore.Schema {
	return &gsrcore.Schema{
		SchemaName:     "TestSchema",
		SchemaDefinition:     test_helpers.CreateTestProtoSchema(),
		DataFormat:     "PROTOBUF",
		AdditionalInfo: "test.TestMessage",
	}
}

// createInvalidProtobufSchema creates an invalid protobuf schema for testing
func createInvalidProtobufSchema(dataFormat, definition string) *gsrcore.Schema {
	return &gsrcore.Schema{
		SchemaName:     "InvalidSchema",
		SchemaDefinition:     definition,
		DataFormat:     dataFormat,
		AdditionalInfo: "invalid",
	}
}

// generateValidProtobufData creates valid protobuf byte data for testing
func generateValidProtobufData() []byte {
	// Create a simple protobuf message
	msg := &descriptorpb.FileDescriptorProto{
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

	data, err := proto.Marshal(msg)
	if err != nil {
		panic(fmt.Sprintf("failed to generate valid protobuf data: %v", err))
	}
	return data
}

// generateInvalidProtobufData creates invalid protobuf byte data for testing
func generateInvalidProtobufData() []byte {
	// Return data that looks like protobuf but is actually invalid
	return []byte{0x81, 0x82, 0x83} // Invalid: incomplete varint encoding
}

func TestNewProtobufDeserializer(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	assert.NotNil(t, deserializer, "NewProtobufDeserializer should return a non-nil deserializer")
	assert.Equal(t, config, deserializer.config, "Deserializer should store the provided config")
	assert.NotNil(t, deserializer.messageDescriptor, "Deserializer should have a message descriptor")
}

func TestNewProtobufDeserializer_ErrorCases(t *testing.T) {
	tests := []struct {
		name   string
		config *common.Configuration
	}{
		{
			name:   "nil config",
			config: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deserializer, err := NewProtobufDeserializer(tt.config)
			assert.Nil(t, deserializer, "NewProtobufDeserializer should return a nil deserializer")
			assert.Error(t, err, "Should return an error")
		})
	}
}

// TestNewProtobufDeserializer_NilDescriptor_ReturnsTypedError is the Phase 3
// red→green test for replacing the panic-on-nil-descriptor with a typed
// error return. Java parity: ProtobufDeserializer's constructor surfaces
// the missing-descriptor case via AWSSchemaRegistryException, not a JVM
// panic. The Go equivalent is core.ErrInvalidProtobufPayload (or a typed
// constructor-level error) returned from NewProtobufDeserializer.
//
// Pre-Phase-3 behavior: a non-nil config with a nil ProtobufMessageDescriptor
// triggered `panic("protobuf message descriptor cannot be nil")` inside the
// constructor — unrecoverable from typical Go control flow.
//
// Phase 4.11 S6 strengthens this test with the full errors.Is chain
// assertion — the nil-descriptor sentinel must chain through
// core.ErrInvalidProtobufPayload (the format-layer sentinel) up to
// core.ErrGSR (the root GSR sentinel), so callers can distinguish a
// protobuf-misconfiguration error without importing this package.
func TestNewProtobufDeserializer_NilDescriptor_ReturnsTypedError(t *testing.T) {
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
	// Intentionally omit ProtobufMessageDescriptorKey so the descriptor
	// stays nil after NewConfiguration.
	cfg := common.NewConfiguration(configMap)
	require.NotNil(t, cfg, "config builder must succeed")
	require.Nil(t, cfg.ProtobufMessageDescriptor, "descriptor must be unset for this scenario")

	// Constructor must NOT panic — instead it must return (nil, error)
	// whose error chain reaches the new typed sentinel and core.ErrGSR.
	require.NotPanics(t, func() {
		des, err := NewProtobufDeserializer(cfg)
		assert.Nil(t, des, "deserializer must be nil on missing descriptor")
		require.Error(t, err, "constructor must return an error, not panic")
		require.True(t, errors.Is(err, ErrNilDescriptor),
			"nil-descriptor must be the typed sentinel ErrNilDescriptor")
		require.True(t, errors.Is(err, gsrcore.ErrInvalidProtobufPayload),
			"nil-descriptor must chain through core.ErrInvalidProtobufPayload")
		require.True(t, errors.Is(err, gsrcore.ErrGSR),
			"all GSR-typed errors must chain through core.ErrGSR")
	})
}

func TestProtobufDeserializer_Deserialize_ErrorCases(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	tests := []struct {
		name        string
		data        []byte
		schema      *gsrcore.Schema
		expectedErr error
	}{
		{
			name:        "nil data",
			data:        nil,
			schema:      createValidProtobufSchema(),
			expectedErr: ErrNilData,
		},
		{
			name:        "empty data",
			data:        []byte{},
			schema:      createValidProtobufSchema(),
			expectedErr: ErrEmptyData,
		},
		{
			name:        "nil schema",
			data:        generateValidProtobufData(),
			schema:      nil,
			expectedErr: ErrNilSchema,
		},
		{
			name:        "non-protobuf schema",
			data:        generateValidProtobufData(),
			schema:      createInvalidProtobufSchema("AVRO", "valid definition"),
			expectedErr: ErrSchemaNotProtobuf,
		},
		{
			name:        "invalid schema - empty definition",
			data:        generateValidProtobufData(),
			schema:      createInvalidProtobufSchema("PROTOBUF", ""),
			expectedErr: ErrInvalidSchema,
		},
		{
			name:        "deserialization failed - invalid protobuf data",
			data:        generateInvalidProtobufData(),
			schema:      createValidProtobufSchema(),
			expectedErr: ErrDeserializationFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := deserializer.Deserialize(tt.data, tt.schema)
			assert.Nil(t, result, "Result should be nil on error")
			assert.Error(t, err, "Should return an error")
			assert.ErrorIs(t, err, tt.expectedErr, "Should return expected error type")
		})
	}
}

func TestProtobufDeserializer_Deserialize_ValidCases(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	tests := []struct {
		name   string
		data   []byte
		schema *gsrcore.Schema
	}{
		{
			name:   "valid protobuf data with valid schema",
			data:   generateValidProtobufData(),
			schema: createValidProtobufSchema(),
		},
		{
			name: "simple protobuf message",
			data: []byte{0x08, 0x96, 0x01}, // Valid protobuf: field 1, varint 150
			schema: &gsrcore.Schema{
				SchemaName:     "SimpleSchema",
				SchemaDefinition:     "syntax = \"proto3\"; message Simple { int64 value = 1; }",
				DataFormat:     "PROTOBUF",
				AdditionalInfo: "Simple",
			},
		},
		{
			name: "protobuf with string field",
			data: []byte{0x0A, 0x04, 0x74, 0x65, 0x73, 0x74}, // Valid: field 1, string "test"
			schema: &gsrcore.Schema{
				SchemaName:     "StringSchema",
				SchemaDefinition:     "syntax = \"proto3\"; message StringMsg { string text = 1; }",
				DataFormat:     "PROTOBUF",
				AdditionalInfo: "StringMsg",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := deserializer.Deserialize(tt.data, tt.schema)
			require.NoError(t, err, "Deserialization should succeed")
			assert.NotNil(t, result, "Result should not be nil")

			// Verify that result is a dynamic message
			dynamicMsg, ok := result.(*dynamicpb.Message)
			assert.True(t, ok, "Result should be a dynamic protobuf message")
			assert.NotNil(t, dynamicMsg, "Dynamic message should not be nil")
		})
	}
}

func TestProtobufDeserializer_Deserialize_ComplexMessage(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	// Create a FileDescriptorProto as test data
	testMsg := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("complex_test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("ComplexMessage"),
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

	// Serialize the message
	data, err := proto.Marshal(testMsg)
	require.NoError(t, err, "Serialization should succeed")

	schema := createValidProtobufSchema()

	// Test deserialization
	result, err := deserializer.Deserialize(data, schema)
	require.NoError(t, err, "Deserialization should succeed")
	assert.NotNil(t, result, "Result should not be nil")

	// Verify that result is a dynamic message
	dynamicMsg, ok := result.(*dynamicpb.Message)
	assert.True(t, ok, "Result should be a dynamic protobuf message")
	assert.NotNil(t, dynamicMsg, "Dynamic message should not be nil")
}

func TestProtobufDeserializer_Deserialize_DynamicMessage(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	// Generate dynamic test message and serialize it
	testMsg := test_helpers.GenerateTestProtoMessage()
	require.NotNil(t, testMsg, "Should generate test message")

	data, err := proto.Marshal(testMsg)
	require.NoError(t, err, "Serialization should succeed")

	schema := createValidProtobufSchema()

	// Test deserialization
	result, err := deserializer.Deserialize(data, schema)
	require.NoError(t, err, "Deserialization should succeed")
	assert.NotNil(t, result, "Result should not be nil")

	// Verify that result is a dynamic message
	dynamicMsg, ok := result.(*dynamicpb.Message)
	assert.True(t, ok, "Result should be a dynamic protobuf message")
	assert.NotNil(t, dynamicMsg, "Dynamic message should not be nil")
}

func TestProtobufDeserializer_RoundTrip(t *testing.T) {
	// This test requires both serializer and deserializer working together
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	// Create test data
	originalMsg := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("roundtrip_test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("RoundTripMessage"),
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

	// Serialize the original message
	serializedData, err := proto.Marshal(originalMsg)
	require.NoError(t, err, "Serialization should succeed")

	schema := createValidProtobufSchema()

	// Deserialize using our deserializer
	result, err := deserializer.Deserialize(serializedData, schema)
	require.NoError(t, err, "Deserialization should succeed")
	assert.NotNil(t, result, "Result should not be nil")

	// Verify the result is a dynamic message
	dynamicMsg, ok := result.(*dynamicpb.Message)
	assert.True(t, ok, "Result should be a dynamic protobuf message")
	assert.NotNil(t, dynamicMsg, "Dynamic message should not be nil")
}

func TestProtobufDeserializer_WithTestHelpers(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	// Test with multiple generated messages
	messages := test_helpers.GenerateTestProtoMessages(3)
	require.Len(t, messages, 3, "Should generate 3 test messages")

	schema := createValidProtobufSchema()

	for i, msg := range messages {
		t.Run(fmt.Sprintf("message_%d", i), func(t *testing.T) {
			// Serialize the message
			data, err := proto.Marshal(msg)
			require.NoError(t, err, "Serialization should succeed")

			// Deserialize the message
			result, err := deserializer.Deserialize(data, schema)
			require.NoError(t, err, "Deserialization should succeed")
			assert.NotNil(t, result, "Result should not be nil")

			// Verify that result is a dynamic message
			dynamicMsg, ok := result.(*dynamicpb.Message)
			assert.True(t, ok, "Result should be a dynamic protobuf message")
			assert.NotNil(t, dynamicMsg, "Dynamic message should not be nil")
		})
	}
}

func TestProtobufDeserializer_ComplexTestMessage(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	// Generate complex test message
	complexMsg := test_helpers.GenerateComplexTestMessage(99999, "complex-deserializer-test")
	require.NotNil(t, complexMsg, "Should generate complex test message")

	// Serialize it
	data, err := proto.Marshal(complexMsg)
	require.NoError(t, err, "Complex message serialization should succeed")

	schema := createValidProtobufSchema()

	// Test deserialization
	result, err := deserializer.Deserialize(data, schema)
	require.NoError(t, err, "Complex message deserialization should succeed")
	assert.NotNil(t, result, "Result should not be nil")

	// Verify that result is a dynamic message
	dynamicMsg, ok := result.(*dynamicpb.Message)
	assert.True(t, ok, "Result should be a dynamic protobuf message")
	assert.NotNil(t, dynamicMsg, "Dynamic message should not be nil")
}

func TestProtobufDeserializer_ErrorWrapping(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	// Test with invalid protobuf data to trigger deserialization error
	invalidData := []byte{0xFF, 0xFF, 0xFF, 0xFF} // Invalid protobuf data
	schema := createValidProtobufSchema()

	result, err := deserializer.Deserialize(invalidData, schema)
	assert.Nil(t, result, "Result should be nil on error")
	assert.Error(t, err, "Should return an error")
	assert.ErrorIs(t, err, ErrDeserializationFailed, "Should wrap with ErrDeserializationFailed")
	assert.Contains(t, err.Error(), "protobuf deserializer: deserialization failed:", "Error should contain wrapped message")
}

// TestProtobufDeserializer_Malformed_SurfacesMalformedProtobufSentinel exercises
// §3.9 item 26 (Protobuf). Bytes that fail proto.Unmarshal against the
// configured message descriptor must surface the per-format
// ErrMalformedProtobuf sentinel via errors.Is, AND retain the
// *ProtobufDeserializationError wrapper for errors.As resolution.
//
// Phase 4.12 §3.9 / PBI-4.12-4 AC-1, AC-2.
func TestProtobufDeserializer_Malformed_SurfacesMalformedProtobufSentinel(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	require.NoError(t, err, "deserializer construction must succeed")

	// Bytes that look like a varint but fail proto.Unmarshal: 0x81 sets the
	// continuation bit on the first byte, so the parser expects at least one
	// more byte to complete the field tag — the buffer ends mid-tag and
	// proto.Unmarshal returns an error. This is the same shape as the
	// generateInvalidProtobufData helper used elsewhere in this file.
	mangled := []byte{0x81, 0x82, 0x83}
	schema := createValidProtobufSchema()

	result, err := deserializer.Deserialize(mangled, schema)
	require.Error(t, err, "malformed protobuf bytes must surface as an error")
	assert.Nil(t, result, "result must be nil on parse failure")

	// Per-format sentinel: errors.Is must resolve through the wrapper's
	// Unwrap() to gsrcore.ErrMalformedProtobuf.
	assert.True(t, errors.Is(err, gsrcore.ErrMalformedProtobuf),
		"errors.Is must resolve to gsrcore.ErrMalformedProtobuf")

	// Umbrella sentinel: ErrMalformedProtobuf wraps ErrGSR transitively.
	assert.True(t, errors.Is(err, gsrcore.ErrGSR),
		"errors.Is must resolve transitively to gsrcore.ErrGSR")

	// Wrapper-type retention: callers using errors.As against the wrapper
	// struct must still resolve. AC-2 regression guard mirroring the JSON
	// and Avro tests.
	var protoErr *ProtobufDeserializationError
	require.True(t, errors.As(err, &protoErr),
		"errors.As must resolve the *ProtobufDeserializationError wrapper")
	assert.NotNil(t, protoErr.Cause,
		"the underlying proto.Unmarshal error must be preserved for diagnostics")

	// Pre-existing diagnostic sentinel: the inner ErrDeserializationFailed
	// reachability and its substring must remain intact so prior assertions
	// (TestProtobufDeserializer_ErrorWrapping, TestProtobufDeserializer_DataValidation)
	// continue to pass without modification.
	assert.True(t, errors.Is(err, ErrDeserializationFailed),
		"errors.Is must still reach ErrDeserializationFailed through the inner chain")
}

func TestProtobufDeserializer_SchemaValidation(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")
	validData := generateValidProtobufData()

	tests := []struct {
		name          string
		dataFormat    string
		definition    string
		expectedError error
	}{
		{
			name:          "JSON format should fail",
			dataFormat:    "JSON",
			definition:    "valid definition",
			expectedError: ErrSchemaNotProtobuf,
		},
		{
			name:          "AVRO format should fail",
			dataFormat:    "AVRO",
			definition:    "valid definition",
			expectedError: ErrSchemaNotProtobuf,
		},
		{
			name:          "empty definition should fail",
			dataFormat:    "PROTOBUF",
			definition:    "",
			expectedError: ErrInvalidSchema,
		},
		{
			name:          "valid PROTOBUF format should work",
			dataFormat:    "PROTOBUF",
			definition:    "syntax = \"proto3\"; message Test { string name = 1; }",
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := createInvalidProtobufSchema(tt.dataFormat, tt.definition)

			result, err := deserializer.Deserialize(validData, schema)

			if tt.expectedError != nil {
				assert.Error(t, err, "Should return an error")
				assert.ErrorIs(t, err, tt.expectedError, "Should return expected error type")
				assert.Nil(t, result, "Result should be nil on error")
			} else {
				assert.NoError(t, err, "Should not return an error for valid schema")
				assert.NotNil(t, result, "Result should not be nil for valid schema")
			}
		})
	}
}

// BenchmarkProtobufDeserializer_Deserialize benchmarks the deserialization performance
func BenchmarkProtobufDeserializer_Deserialize(b *testing.B) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(b, err, "err should be nil")

	// Setup test data
	testMsg := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("benchmark_deserializer.proto"),
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

	serializedData, err := proto.Marshal(testMsg)
	if err != nil {
		b.Fatalf("Failed to serialize test message: %v", err)
	}

	schema := createValidProtobufSchema()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := deserializer.Deserialize(serializedData, schema)
		if err != nil {
			b.Fatalf("Deserialization failed: %v", err)
		}
		if result == nil {
			b.Fatal("Result should not be nil")
		}
	}
}

// BenchmarkProtobufDeserializer_LargeMessage benchmarks deserialization of large messages
func BenchmarkProtobufDeserializer_LargeMessage(b *testing.B) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(b, err, "err should be nil")

	// Create a large test message
	largeMsg := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("large_benchmark.proto"),
		Package:     proto.String("benchmark"),
		MessageType: make([]*descriptorpb.DescriptorProto, 100),
	}

	// Fill with many message types
	for i := 0; i < 100; i++ {
		largeMsg.MessageType[i] = &descriptorpb.DescriptorProto{
			Name: proto.String(fmt.Sprintf("Message%d", i)),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:   proto.String("field1"),
					Number: proto.Int32(1),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				},
				{
					Name:   proto.String("field2"),
					Number: proto.Int32(2),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				},
			},
		}
	}

	serializedData, err := proto.Marshal(largeMsg)
	if err != nil {
		b.Fatalf("Failed to serialize large test message: %v", err)
	}

	schema := createValidProtobufSchema()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := deserializer.Deserialize(serializedData, schema)
		if err != nil {
			b.Fatalf("Large message deserialization failed: %v", err)
		}
		if result == nil {
			b.Fatal("Result should not be nil")
		}
	}
}

func TestProtobufDeserializer_ErrorMessages(t *testing.T) {
	// Test that all error constants have proper messages
	errors := []error{
		ErrNilData,
		ErrEmptyData,
		ErrNilSchema,
		ErrInvalidSchema,
		ErrSchemaNotProtobuf,
		ErrMessageDescriptorNotFound,
		ErrDeserializationFailed,
	}

	expectedPrefixes := []string{
		"protobuf deserializer: data cannot be nil",
		"protobuf deserializer: data cannot be empty",
		"protobuf deserializer: schema cannot be nil",
		"protobuf deserializer: invalid schema definition",
		"protobuf deserializer: schema is not a protobuf schema",
		"protobuf deserializer: message descriptor not found",
		"protobuf deserializer: deserialization failed",
	}

	for i, err := range errors {
		t.Run(fmt.Sprintf("error_%d", i), func(t *testing.T) {
			assert.Equal(t, expectedPrefixes[i], err.Error(), "Error message should match expected format")
		})
	}
}

func TestProtobufDeserializer_ConfigurationFields(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")

	// Verify that the deserializer properly stores configuration fields
	assert.NotNil(t, deserializer.config, "Config should not be nil")
	assert.NotNil(t, deserializer.messageDescriptor, "Message descriptor should not be nil")
	assert.Equal(t, config.ProtobufMessageDescriptor, deserializer.messageDescriptor, "Message descriptor should match config")
}

// TestProtobufDeserializer_Dynamic_Default verifies INV-DYNAMIC-DEFAULT: when no
// ProtobufMessageType is configured (zero value / Unknown), the existing
// auto-dispatch path returns a *dynamicpb.Message. This is a regression guard
// ensuring the POJO branch added in Phase 4.14 does not alter the default path.
func TestProtobufDeserializer_Dynamic_Default(t *testing.T) {
	// Config deliberately omits ProtobufMessageTypeKey — zero value means Dynamic.
	config := createProtobufDeserializerConfig()
	require.Equal(t, common.ProtobufMessageTypeUnknown, config.ProtobufMessageType,
		"default config must have ProtobufMessageTypeUnknown (zero value)")

	deserializer, err := NewProtobufDeserializer(config)
	require.NoError(t, err, "constructor must succeed")

	data := generateValidProtobufData()
	schema := createValidProtobufSchema()

	result, err := deserializer.Deserialize(data, schema)
	require.NoError(t, err, "dynamic default path must succeed")
	require.NotNil(t, result, "result must not be nil")

	_, isDynamic := result.(*dynamicpb.Message)
	assert.True(t, isDynamic, "INV-DYNAMIC-DEFAULT: result must be *dynamicpb.Message when ProtobufMessageType is not set")
}

// TestProtobufDeserializer_POJO_TypedMessage verifies DoD #11: when the caller
// registers a concrete proto.Message via ProtobufPOJOMessage, Deserialize
// returns a populated concrete instance of that exact type (not *dynamicpb.Message).
// Uses *descriptorpb.FileDescriptorProto as the concrete type because it is
// already available in the test file and is a valid generated proto.Message.
func TestProtobufDeserializer_POJO_TypedMessage(t *testing.T) {
	// Build a FileDescriptorProto and marshal it — this is both the template
	// AND the source data, since FileDescriptorProto can unmarshal its own wire bytes.
	templateMsg := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("pojo_test.proto"),
		Package: proto.String("pojo"),
	}
	data, err := proto.Marshal(templateMsg)
	require.NoError(t, err, "marshal must succeed")

	// Build config with POJO mode and the concrete message template.
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
	configMap[common.ProtobufMessageDescriptorKey] = createProtobufDeserializerConfig().ProtobufMessageDescriptor
	configMap[common.ProtobufMessageTypeKey] = common.ProtobufMessageTypePOJO
	configMap[common.ProtobufPOJOTypeKey] = templateMsg
	config := common.NewConfiguration(configMap)
	require.Equal(t, common.ProtobufMessageTypePOJO, config.ProtobufMessageType)
	require.NotNil(t, config.ProtobufPOJOMessage)

	deserializer, err := NewProtobufDeserializer(config)
	require.NoError(t, err, "constructor must succeed")

	schema := createValidProtobufSchema()

	result, err := deserializer.Deserialize(data, schema)
	require.NoError(t, err, "POJO deserialization must succeed")
	require.NotNil(t, result, "result must not be nil")

	// DoD #11: result must be concrete *descriptorpb.FileDescriptorProto, not *dynamicpb.Message.
	concreteMsg, ok := result.(*descriptorpb.FileDescriptorProto)
	require.True(t, ok, "DoD #11: result must be *descriptorpb.FileDescriptorProto, not *dynamicpb.Message")
	assert.Equal(t, "pojo_test.proto", concreteMsg.GetName(),
		"deserialized concrete message must have correct field value")
	assert.Equal(t, "pojo", concreteMsg.GetPackage(),
		"deserialized concrete message must have correct package field")

	// Confirm the template was NOT mutated — proto.Clone must have been used.
	assert.Equal(t, "pojo_test.proto", templateMsg.GetName(),
		"template proto.Message must not be mutated by Deserialize")
}

// TestProtobufDeserializer_POJO_MissingType verifies DoD #12: when POJO mode
// is configured but ProtobufPOJOMessage is nil, Deserialize returns an error
// wrapping ErrMissingProtobufPOJOType so callers can test via errors.Is.
func TestProtobufDeserializer_POJO_MissingType(t *testing.T) {
	// Config: POJO mode set, but ProtobufPOJOMessage intentionally absent (nil).
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
	configMap[common.ProtobufMessageDescriptorKey] = createProtobufDeserializerConfig().ProtobufMessageDescriptor
	configMap[common.ProtobufMessageTypeKey] = common.ProtobufMessageTypePOJO
	// Deliberately omit ProtobufPOJOTypeKey.
	config := common.NewConfiguration(configMap)
	require.Equal(t, common.ProtobufMessageTypePOJO, config.ProtobufMessageType)
	require.Nil(t, config.ProtobufPOJOMessage, "ProtobufPOJOMessage must be nil for this scenario")

	deserializer, err := NewProtobufDeserializer(config)
	require.NoError(t, err, "constructor must succeed — POJO nil check is at Deserialize time")

	data := generateValidProtobufData()
	schema := createValidProtobufSchema()

	result, err := deserializer.Deserialize(data, schema)
	require.Error(t, err, "must return an error when ProtobufPOJOMessage is nil in POJO mode")
	assert.Nil(t, result, "result must be nil on error")

	// DoD #12: errors.Is must resolve to ErrMissingProtobufPOJOType.
	assert.True(t, errors.Is(err, ErrMissingProtobufPOJOType),
		"DoD #12: errors.Is must resolve to ErrMissingProtobufPOJOType")
	assert.Contains(t, err.Error(), "POJO requires ProtobufPOJOMessage",
		"error message must contain the expected diagnostic substring")
}

func TestProtobufDeserializer_DataValidation(t *testing.T) {
	config := createProtobufDeserializerConfig()
	deserializer, err := NewProtobufDeserializer(config)
	assert.Nil(t, err, "err should be nil")
	schema := createValidProtobufSchema()

	tests := []struct {
		name        string
		data        []byte
		expectError bool
		errorType   error
	}{
		{
			name:        "nil data",
			data:        nil,
			expectError: true,
			errorType:   ErrNilData,
		},
		{
			name:        "empty data",
			data:        []byte{},
			expectError: true,
			errorType:   ErrEmptyData,
		},
		{
			name:        "incomplete protobuf data - should fail",
			data:        []byte{0x08}, // Incomplete: field tag without value
			expectError: true,
			errorType:   ErrDeserializationFailed,
		},
		{
			name:        "valid single field protobuf data",
			data:        []byte{0x08, 0x00}, // Valid: field 1, varint value 0
			expectError: false,
		},
		{
			name:        "valid multi-byte data",
			data:        []byte{0x08, 0x96, 0x01},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := deserializer.Deserialize(tt.data, schema)

			if tt.expectError {
				assert.Error(t, err, "Should return an error")
				assert.ErrorIs(t, err, tt.errorType, "Should return expected error type")
				assert.Nil(t, result, "Result should be nil on error")
			} else {
				assert.NoError(t, err, "Should not return an error for valid data")
				assert.NotNil(t, result, "Result should not be nil for valid data")
			}
		})
	}
}
