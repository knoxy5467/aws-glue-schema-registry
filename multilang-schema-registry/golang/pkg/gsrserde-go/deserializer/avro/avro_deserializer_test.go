package avro

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	hambaavro "github.com/hamba/avro/v2"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
)

// avroTestRecord is a minimal Go struct for SPECIFIC_RECORD dispatch tests.
// Field names and avro tags match the "TestRecord" schema used in the PBI-02
// test functions below. A single string field is sufficient to demonstrate
// typed struct unmarshaling — keep it minimal per project conventions.
// Phase 4.14 PBI-02.
type avroTestRecord struct {
	Name string `avro:"name"`
}

// createAvroConfig creates a Configuration object for AVRO tests
func createAvroConfig() *common.Configuration {
	configMap := make(map[string]interface{})
	configMap[common.DataFormatTypeKey] = common.DataFormatAvro
	return common.NewConfiguration(configMap)
}

// createAvroData creates AVRO binary data for testing
func createAvroData(schema, data interface{}) ([]byte, error) {
	avroSchema, err := hambaavro.Parse(schema.(string))
	if err != nil {
		return nil, err
	}
	return hambaavro.Marshal(avroSchema, data)
}

func TestNewAvroDeserializer(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		config := createAvroConfig()
		deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")
		assert.NotNil(t, deserializer, "NewAvroDeserializer should return a non-nil deserializer")
		assert.Equal(t, config, deserializer.config)
	})

	t.Run("NilConfiguration", func(t *testing.T) {
		deserializer, err := NewAvroDeserializer(nil)
		assert.Nil(t, deserializer, "deserializer should be nil")
		assert.EqualError(t,err,common.ErrNilConfig.Error())
	})
}

func TestAvroDeserializer_Deserialize(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	// Test schemas
	stringSchema := `"string"`
	intSchema := `"int"`
	recordSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	// Create test data
	stringData, err := createAvroData(stringSchema, "test string")
	require.NoError(t, err)

	intData, err := createAvroData(intSchema, 42)
	require.NoError(t, err)

	recordData, err := createAvroData(recordSchema, map[string]interface{}{
		"name": "John Doe",
		"age":  30,
	})
	require.NoError(t, err)

	tests := []struct {
		name          string
		data          []byte
		schema        *gsrcore.Schema
		expectError   bool
		errorContains string
		validateResult func(t *testing.T, result interface{})
	}{
		{
			name: "ValidStringData",
			data: stringData,
			schema: &gsrcore.Schema{
				SchemaName: "StringSchema",
				SchemaDefinition: stringSchema,
				DataFormat: "AVRO",
			},
			expectError: false,
			validateResult: func(t *testing.T, result interface{}) {
				assert.Equal(t, "test string", result)
			},
		},
		{
			name: "ValidIntData",
			data: intData,
			schema: &gsrcore.Schema{
				SchemaName: "IntSchema",
				SchemaDefinition: intSchema,
				DataFormat: "AVRO",
			},
			expectError: false,
			validateResult: func(t *testing.T, result interface{}) {
				assert.Equal(t, int(42), result)
			},
		},
		{
			name: "ValidRecordData",
			data: recordData,
			schema: &gsrcore.Schema{
				SchemaName: "UserSchema",
				SchemaDefinition: recordSchema,
				DataFormat: "AVRO",
			},
			expectError: false,
			validateResult: func(t *testing.T, result interface{}) {
				resultMap, ok := result.(map[string]interface{})
				assert.True(t, ok, "Result should be a map")
				assert.Equal(t, "John Doe", resultMap["name"])
				assert.Equal(t, int(30), resultMap["age"])
			},
		},
		{
			name:          "EmptyData",
			data:          []byte{},
			schema:        &gsrcore.Schema{SchemaDefinition: stringSchema},
			expectError:   true,
			errorContains: "cannot deserialize empty data",
		},
		{
			name:          "NilData",
			data:          nil,
			schema:        &gsrcore.Schema{SchemaDefinition: stringSchema},
			expectError:   true,
			errorContains: "cannot deserialize empty data",
		},
		{
			name:          "NilSchema",
			data:          stringData,
			schema:        nil,
			expectError:   true,
			errorContains: "schema cannot be nil",
		},
		{
			name: "EmptySchemaDefinition",
			data: stringData,
			schema: &gsrcore.Schema{
				SchemaDefinition: "",
			},
			expectError:   true,
			errorContains: "schema definition cannot be empty",
		},
		{
			name: "InvalidSchemaDefinition",
			data: stringData,
			schema: &gsrcore.Schema{
				SchemaDefinition: `{"type": "invalid"}`,
			},
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := deserializer.Deserialize(tt.data, tt.schema)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				if tt.validateResult != nil {
					tt.validateResult(t, result)
				}
			}
		})
	}
}

func TestAvroDeserializer_ValidateData(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	stringSchema := `"string"`
	recordSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	// Create test data
	validStringData, err := createAvroData(stringSchema, "test")
	require.NoError(t, err)

	validRecordData, err := createAvroData(recordSchema, map[string]interface{}{
		"name": "John",
		"age":  25,
	})
	require.NoError(t, err)

	tests := []struct {
		name          string
		data          []byte
		schemaString  string
		expectError   bool
		errorContains string
	}{
		{
			name:         "ValidStringData",
			data:         validStringData,
			schemaString: stringSchema,
			expectError:  false,
		},
		{
			name:         "ValidRecordData",
			data:         validRecordData,
			schemaString: recordSchema,
			expectError:  false,
		},
		{
			name:          "EmptyData",
			data:          []byte{},
			schemaString:  stringSchema,
			expectError:   true,
			errorContains: "data cannot be empty",
		},
		{
			name:          "NilData",
			data:          nil,
			schemaString:  stringSchema,
			expectError:   true,
			errorContains: "data cannot be empty",
		},
		{
			name:          "EmptySchema",
			data:          validStringData,
			schemaString:  "",
			expectError:   true,
			errorContains: "schema string cannot be empty",
		},
		{
			name:          "InvalidSchema",
			data:          validStringData,
			schemaString:  `{"type": "invalid"}`,
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := deserializer.ValidateData(tt.data, tt.schemaString)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAvroDeserializer_ValidateSchema(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	tests := []struct {
		name          string
		schemaString  string
		expectError   bool
		errorContains string
	}{
		{
			name:         "ValidStringSchema",
			schemaString: `"string"`,
			expectError:  false,
		},
		{
			name:         "ValidIntSchema",
			schemaString: `"int"`,
			expectError:  false,
		},
		{
			name:         "ValidRecordSchema",
			schemaString: `{
				"type": "record",
				"name": "User",
				"fields": [
					{"name": "name", "type": "string"},
					{"name": "age", "type": "int"}
				]
			}`,
			expectError: false,
		},
		{
			name:         "ValidArraySchema",
			schemaString: `{"type": "array", "items": "string"}`,
			expectError:  false,
		},
		{
			name:         "ValidMapSchema",
			schemaString: `{"type": "map", "values": "int"}`,
			expectError:  false,
		},
		{
			name:         "ValidUnionSchema",
			schemaString: `["null", "string"]`,
			expectError:  false,
		},
		{
			name:          "EmptySchema",
			schemaString:  "",
			expectError:   true,
			errorContains: "schema string cannot be empty",
		},
		{
			name:          "InvalidJsonSchema",
			schemaString:  `{"type": "string", "invalid":}`,
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
		},
		{
			name:          "InvalidAvroSchema",
			schemaString:  `{"type": "invalid"}`,
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
		},
		{
			name:          "InvalidRecordSchema",
			schemaString:  `{"type": "record", "name": "Test"}`, // Missing fields
			expectError:   true,
			errorContains: "failed to parse AVRO schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := deserializer.ValidateSchema(tt.schemaString)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAvroDeserializer_GetConfiguration(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	result := deserializer.GetConfiguration()
	assert.Equal(t, config, result)
}

func TestAvroDeserializer_ComplexScenarios(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	t.Run("NestedRecordSchema", func(t *testing.T) {
		schema := `{
			"type": "record",
			"name": "Order",
			"fields": [
				{"name": "id", "type": "string"},
				{"name": "customer", "type": {
					"type": "record",
					"name": "Customer",
					"fields": [
						{"name": "name", "type": "string"},
						{"name": "email", "type": "string"}
					]
				}},
				{"name": "items", "type": {
					"type": "array",
					"items": {
						"type": "record",
						"name": "Item",
						"fields": [
							{"name": "product", "type": "string"},
							{"name": "quantity", "type": "int"},
							{"name": "price", "type": "double"}
						]
					}
				}}
			]
		}`

		testData := map[string]interface{}{
			"id": "order-123",
			"customer": map[string]interface{}{
				"name":  "John Doe",
				"email": "john@example.com",
			},
			"items": []interface{}{
				map[string]interface{}{
					"product":  "Widget A",
					"quantity": int32(2),
					"price":    19.99,
				},
				map[string]interface{}{
					"product":  "Widget B",
					"quantity": int32(1),
					"price":    29.99,
				},
			},
		}

		avroData, err := createAvroData(schema, testData)
		require.NoError(t, err)

		gsrSchema := &gsrcore.Schema{
			SchemaName: "OrderSchema",
			SchemaDefinition: schema,
			DataFormat: "AVRO",
		}

		result, err := deserializer.Deserialize(avroData, gsrSchema)
		require.NoError(t, err)

		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok, "Result should be a map")

		assert.Equal(t, "order-123", resultMap["id"])

		customer, ok := resultMap["customer"].(map[string]interface{})
		require.True(t, ok, "Customer should be a map")
		assert.Equal(t, "John Doe", customer["name"])
		assert.Equal(t, "john@example.com", customer["email"])

		items, ok := resultMap["items"].([]interface{})
		require.True(t, ok, "Items should be an array")
		assert.Len(t, items, 2)

		item1, ok := items[0].(map[string]interface{})
		require.True(t, ok, "Item should be a map")
		assert.Equal(t, "Widget A", item1["product"])
		assert.Equal(t, int(2), item1["quantity"])
		assert.Equal(t, 19.99, item1["price"])
	})

	t.Run("UnionTypeSchema", func(t *testing.T) {
		schema := `{
			"type": "record",
			"name": "Message",
			"fields": [
				{"name": "content", "type": ["null", "string"]},
				{"name": "priority", "type": ["string", "int"]}
			]
		}`

		testData := map[string]interface{}{
			"content":  map[string]interface{}{"string": "Hello World"},
			"priority": map[string]interface{}{"int": int32(1)},
		}

		avroData, err := createAvroData(schema, testData)
		require.NoError(t, err)

		gsrSchema := &gsrcore.Schema{
			SchemaDefinition: schema,
		}

		result, err := deserializer.Deserialize(avroData, gsrSchema)
		require.NoError(t, err)

		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok, "Result should be a map")

		assert.NotNil(t, resultMap["content"])
		assert.NotNil(t, resultMap["priority"])
	})

	t.Run("MapTypeSchema", func(t *testing.T) {
		schema := `{
			"type": "record",
			"name": "Config",
			"fields": [
				{"name": "settings", "type": {"type": "map", "values": "string"}}
			]
		}`

		testData := map[string]interface{}{
			"settings": map[string]interface{}{
				"theme":    "dark",
				"language": "en",
				"timezone": "UTC",
			},
		}

		avroData, err := createAvroData(schema, testData)
		require.NoError(t, err)

		gsrSchema := &gsrcore.Schema{
			SchemaDefinition: schema,
		}

		result, err := deserializer.Deserialize(avroData, gsrSchema)
		require.NoError(t, err)

		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok, "Result should be a map")

		settings, ok := resultMap["settings"].(map[string]interface{})
		require.True(t, ok, "Settings should be a map")
		assert.Equal(t, "dark", settings["theme"])
		assert.Equal(t, "en", settings["language"])
		assert.Equal(t, "UTC", settings["timezone"])
	})
}

func TestAvroDeserializer_ErrorTypes(t *testing.T) {
	// Test AvroDeserializationError
	baseErr := fmt.Errorf("base error")
	deserErr := &AvroDeserializationError{
		Message: "test deserialization error",
		Cause:   baseErr,
	}

	assert.Contains(t, deserErr.Error(), "test deserialization error", "Error message should contain message")
	assert.Contains(t, deserErr.Error(), "base error", "Error message should contain cause")
	assert.Equal(t, baseErr, deserErr.Unwrap(), "Unwrap should return cause")

	// Test AvroDeserializationError without cause
	deserErrNoCause := &AvroDeserializationError{
		Message: "no cause error",
	}
	assert.Equal(t, "avro deserialization error: no cause error", deserErrNoCause.Error())
	assert.Nil(t, deserErrNoCause.Unwrap(), "Unwrap should return nil when no cause")

	// Test error constants
	assert.NotNil(t, ErrInvalidAvroData)
	assert.NotNil(t, ErrNilData)
	assert.NotNil(t, ErrDeserialization)
	assert.NotNil(t, ErrInvalidSchema)
}

func TestAvroDeserializer_ConcurrentAccess(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	schema := `{
		"type": "record",
		"name": "ConcurrentTest",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "value", "type": "string"}
		]
	}`

	// Test concurrent access to deserializer
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			testData := map[string]interface{}{
				"id":    int32(id),
				"value": fmt.Sprintf("test-%d", id),
			}

			avroData, err := createAvroData(schema, testData)
			if err != nil {
				t.Errorf("Failed to create AVRO data: %v", err)
				return
			}

			gsrSchema := &gsrcore.Schema{
				SchemaDefinition: schema,
			}

			result, err := deserializer.Deserialize(avroData, gsrSchema)
			if err != nil {
				t.Errorf("Failed to deserialize: %v", err)
				return
			}

			resultMap, ok := result.(map[string]interface{})
			if !ok {
				t.Errorf("Expected map result, got %T", result)
				return
			}

			assert.Equal(t, int(id), resultMap["id"])
			assert.Equal(t, fmt.Sprintf("test-%d", id), resultMap["value"])
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

func TestAvroDeserializer_PrimitiveTypes(t *testing.T) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(t,err, "err should be nil")

	tests := []struct {
		name     string
		schema   string
		testData interface{}
		expected interface{}
	}{
		{
			name:     "Boolean",
			schema:   `"boolean"`,
			testData: true,
			expected: true,
		},
		{
			name:     "Int",
			schema:   `"int"`,
			testData: 42,
			expected: int(42),
		},
		{
			name:     "Long",
			schema:   `"long"`,
			testData: int64(1234567890),
			expected: int64(1234567890),
		},
		{
			name:     "Float",
			schema:   `"float"`,
			testData: float32(3.14),
			expected: float32(3.14),
		},
		{
			name:     "Double",
			schema:   `"double"`,
			testData: 3.14159,
			expected: 3.14159,
		},
		{
			name:     "String",
			schema:   `"string"`,
			testData: "hello world",
			expected: "hello world",
		},
		{
			name:     "Bytes",
			schema:   `"bytes"`,
			testData: []byte("binary data"),
			expected: []byte("binary data"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			avroData, err := createAvroData(tt.schema, tt.testData)
			require.NoError(t, err)

			gsrSchema := &gsrcore.Schema{
				SchemaDefinition: tt.schema,
			}

			result, err := deserializer.Deserialize(avroData, gsrSchema)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// avroTestRecordSchema is the Avro schema corresponding to avroTestRecord.
const avroTestRecordSchema = `{
	"type": "record",
	"name": "TestRecord",
	"fields": [
		{"name": "name", "type": "string"}
	]
}`

// TestAvroDeserializer_GenericRecord_Default verifies INV-GENERIC-DEFAULT: when
// no AvroRecordType is configured (zero value = AvroRecordTypeUnknown), the
// deserializer returns the existing interface{} / map[string]interface{} shape.
// Phase 4.14 PBI-02.
func TestAvroDeserializer_GenericRecord_Default(t *testing.T) {
	// Config with no AvroRecordType set — zero value is AvroRecordTypeUnknown,
	// which the dispatch treats identically to AvroRecordTypeGeneric.
	config := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatAvro,
	})
	d, err := NewAvroDeserializer(config)
	require.NoError(t, err)

	avroData, err := createAvroData(avroTestRecordSchema, map[string]interface{}{"name": "alice"})
	require.NoError(t, err)

	result, err := d.Deserialize(avroData, &gsrcore.Schema{SchemaDefinition: avroTestRecordSchema})
	require.NoError(t, err)
	require.NotNil(t, result)

	// Generic path returns map[string]interface{}, not a struct.
	resultMap, ok := result.(map[string]interface{})
	require.True(t, ok, "GENERIC_RECORD path must return map[string]interface{}, got %T", result)
	assert.Equal(t, "alice", resultMap["name"])
}

// TestAvroDeserializer_SpecificRecord_TypedStruct verifies DoD #9: when
// AvroRecordType == AvroRecordTypeSpecific and AvroSpecificType is set, the
// deserializer allocates a new instance of the registered type and populates
// it via hambaavro.Unmarshal, returning a typed pointer (*avroTestRecord).
// Phase 4.14 PBI-02.
func TestAvroDeserializer_SpecificRecord_TypedStruct(t *testing.T) {
	config := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey:    common.DataFormatAvro,
		common.AvroRecordTypeKey:    common.AvroRecordTypeSpecific,
		common.AvroSpecificTypeKey:  reflect.TypeOf(avroTestRecord{}),
	})
	d, err := NewAvroDeserializer(config)
	require.NoError(t, err)

	avroData, err := createAvroData(avroTestRecordSchema, map[string]interface{}{"name": "bob"})
	require.NoError(t, err)

	result, err := d.Deserialize(avroData, &gsrcore.Schema{SchemaDefinition: avroTestRecordSchema})
	require.NoError(t, err)
	require.NotNil(t, result)

	// SPECIFIC_RECORD path must return *avroTestRecord, not map[string]interface{}.
	typed, ok := result.(*avroTestRecord)
	require.True(t, ok, "SPECIFIC_RECORD path must return *avroTestRecord, got %T", result)
	assert.Equal(t, "bob", typed.Name)
}

// TestAvroDeserializer_SpecificRecord_MissingType verifies DoD #10: when
// AvroRecordType == AvroRecordTypeSpecific but AvroSpecificType is nil, the
// deserializer returns an error wrapping ErrMissingAvroSpecificType so that
// errors.Is(err, ErrMissingAvroSpecificType) is true.
// Phase 4.14 PBI-02.
func TestAvroDeserializer_SpecificRecord_MissingType(t *testing.T) {
	// Deliberately omit AvroSpecificTypeKey — AvroSpecificType stays nil.
	config := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatAvro,
		common.AvroRecordTypeKey: common.AvroRecordTypeSpecific,
	})
	d, err := NewAvroDeserializer(config)
	require.NoError(t, err)

	avroData, err := createAvroData(avroTestRecordSchema, map[string]interface{}{"name": "carol"})
	require.NoError(t, err)

	result, err := d.Deserialize(avroData, &gsrcore.Schema{SchemaDefinition: avroTestRecordSchema})
	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrMissingAvroSpecificType)
	assert.Contains(t, err.Error(), "SPECIFIC_RECORD requires AvroSpecificType")
}

// TestAvroDeserializer_SpecificRecord_PointerType verifies the pointer-type guard
// (board MINOR-3 / C1): when a caller registers AvroSpecificType as a pointer type
// (reflect.TypeOf(&avroTestRecord{})) instead of the value type, the deserializer
// must normalize it to the element type before calling reflect.New — producing a
// *avroTestRecord result identical to the value-type case, rather than **avroTestRecord
// which hamba/avro cannot populate.
// Phase 4.14 board-fixes.
func TestAvroDeserializer_SpecificRecord_PointerType(t *testing.T) {
	// Register the POINTER type (common caller mistake) — should produce same
	// result as registering the value type.
	config := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey:   common.DataFormatAvro,
		common.AvroRecordTypeKey:   common.AvroRecordTypeSpecific,
		common.AvroSpecificTypeKey: reflect.TypeOf(&avroTestRecord{}), // pointer type
	})
	d, err := NewAvroDeserializer(config)
	require.NoError(t, err)

	avroData, err := createAvroData(avroTestRecordSchema, map[string]interface{}{"name": "dave"})
	require.NoError(t, err)

	result, err := d.Deserialize(avroData, &gsrcore.Schema{SchemaDefinition: avroTestRecordSchema})
	require.NoError(t, err)
	require.NotNil(t, result)

	// Must return *avroTestRecord — same as if the value type were registered.
	typed, ok := result.(*avroTestRecord)
	require.True(t, ok, "pointer-type registration must yield *avroTestRecord, got %T", result)
	assert.Equal(t, "dave", typed.Name)
}

// ---------------------------------------------------------------------------
// Phase 4.17 — Reader-schema projection tests
// ---------------------------------------------------------------------------

// TestAvroDeserialize_ReaderSchema_AddedField verifies that a v2 reader schema
// with an additional field (email, default null) correctly fills the default
// when decoding v1-encoded bytes (which lack the email field).
func TestAvroDeserialize_ReaderSchema_AddedField(t *testing.T) {
	// Writer schema v1: {id, name}
	writerSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	// Reader schema v2: {id, name, email (default null)}
	readerSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null}
		]
	}`

	// Encode data using the writer schema (v1 shape: only id + name).
	writerData := map[string]interface{}{
		"id":   int32(42),
		"name": "Alice",
	}
	encodedBytes, err := createAvroData(writerSchema, writerData)
	require.NoError(t, err)

	// Configure deserializer with the reader schema.
	config := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey:    common.DataFormatAvro,
		common.AvroReaderSchemaKey:  readerSchema,
	})
	d, err := NewAvroDeserializer(config)
	require.NoError(t, err)

	result, err := d.Deserialize(encodedBytes, &gsrcore.Schema{SchemaDefinition: writerSchema})
	require.NoError(t, err)
	require.NotNil(t, result)

	resultMap, ok := result.(map[string]interface{})
	require.True(t, ok, "expected map[string]interface{}, got %T", result)

	// Original fields preserved.
	assert.Equal(t, int(42), resultMap["id"])
	assert.Equal(t, "Alice", resultMap["name"])

	// Added field filled with default (null branch of the union).
	assert.Contains(t, resultMap, "email", "reader-added field 'email' must be present")
	assert.Nil(t, resultMap["email"], "email default is null")
}

// TestAvroDeserialize_ReaderSchema_RemovedField verifies that a reader schema
// with fewer fields than the writer correctly drops the unknown field during
// decode (field present in writer but absent in reader is ignored).
func TestAvroDeserialize_ReaderSchema_RemovedField(t *testing.T) {
	// Writer schema: {id, name, email}
	writerSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"}
		]
	}`

	// Reader schema: {id, name} — email has been removed.
	readerSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	// Encode data with all 3 fields.
	writerData := map[string]interface{}{
		"id":    int32(7),
		"name":  "Bob",
		"email": "bob@example.com",
	}
	encodedBytes, err := createAvroData(writerSchema, writerData)
	require.NoError(t, err)

	// Configure deserializer with the reader schema.
	config := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey:    common.DataFormatAvro,
		common.AvroReaderSchemaKey:  readerSchema,
	})
	d, err := NewAvroDeserializer(config)
	require.NoError(t, err)

	result, err := d.Deserialize(encodedBytes, &gsrcore.Schema{SchemaDefinition: writerSchema})
	require.NoError(t, err)
	require.NotNil(t, result)

	resultMap, ok := result.(map[string]interface{})
	require.True(t, ok, "expected map[string]interface{}, got %T", result)

	// Only reader fields present.
	assert.Equal(t, int(7), resultMap["id"])
	assert.Equal(t, "Bob", resultMap["name"])

	// email was dropped — not present in the result.
	_, hasEmail := resultMap["email"]
	assert.False(t, hasEmail, "field 'email' must be absent from result (dropped by reader projection)")
}

// TestAvroDeserialize_ReaderSchema_Default verifies that when AvroReaderSchema
// is NOT set (empty string), the deserializer uses writer-only decode —
// regression guard: existing behavior must not break.
func TestAvroDeserialize_ReaderSchema_Default(t *testing.T) {
	writerSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	writerData := map[string]interface{}{
		"id":   int32(99),
		"name": "Carol",
	}
	encodedBytes, err := createAvroData(writerSchema, writerData)
	require.NoError(t, err)

	// No AvroReaderSchema — empty/unset.
	config := common.NewConfiguration(map[string]interface{}{
		common.DataFormatTypeKey: common.DataFormatAvro,
	})
	d, err := NewAvroDeserializer(config)
	require.NoError(t, err)

	result, err := d.Deserialize(encodedBytes, &gsrcore.Schema{SchemaDefinition: writerSchema})
	require.NoError(t, err)
	require.NotNil(t, result)

	resultMap, ok := result.(map[string]interface{})
	require.True(t, ok, "expected map[string]interface{}, got %T", result)

	assert.Equal(t, int(99), resultMap["id"])
	assert.Equal(t, "Carol", resultMap["name"])
}

// BenchmarkAvroDeserializer_Deserialize benchmarks the deserialization performance
func BenchmarkAvroDeserializer_Deserialize(b *testing.B) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(b,err, "err should be nil")

	schema := `{
		"type": "record",
		"name": "BenchmarkRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	testData := map[string]interface{}{
		"id":    int32(123),
		"name":  "John Doe",
		"email": "john.doe@example.com",
		"age":   int32(30),
	}

	avroData, err := createAvroData(schema, testData)
	if err != nil {
		b.Fatalf("Failed to create AVRO data: %v", err)
	}

	gsrSchema := &gsrcore.Schema{
		SchemaDefinition: schema,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := deserializer.Deserialize(avroData, gsrSchema)
		if err != nil {
			b.Fatalf("Deserialization failed: %v", err)
		}
		if result == nil {
			b.Fatal("Result should not be nil")
		}
	}
}

// BenchmarkAvroDeserializer_ValidateData benchmarks data validation performance
func BenchmarkAvroDeserializer_ValidateData(b *testing.B) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(b,err, "err should be nil")

	schema := `{
		"type": "record",
		"name": "ValidationRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "data", "type": "string"}
		]
	}`

	testData := map[string]interface{}{
		"id":   int32(456),
		"data": "validation test data",
	}

	avroData, err := createAvroData(schema, testData)
	if err != nil {
		b.Fatalf("Failed to create AVRO data: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := deserializer.ValidateData(avroData, schema)
		if err != nil {
			b.Fatalf("Validation failed: %v", err)
		}
	}
}

// BenchmarkAvroDeserializer_ValidateSchema benchmarks schema validation performance
func BenchmarkAvroDeserializer_ValidateSchema(b *testing.B) {
	config := createAvroConfig()
			deserializer, err := NewAvroDeserializer(config)
		assert.Nil(b,err, "err should be nil")

	schema := `{
		"type": "record",
		"name": "SchemaValidationRecord",
		"fields": [
			{"name": "field1", "type": "string"},
			{"name": "field2", "type": "int"},
			{"name": "field3", "type": "boolean"}
		]
	}`

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := deserializer.ValidateSchema(schema)
		if err != nil {
			b.Fatalf("Schema validation failed: %v", err)
		}
	}
}
