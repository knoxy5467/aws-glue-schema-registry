package gsrserde

import (
	"testing"
)

func TestHeaderFormat(t *testing.T) {
	// Test wire format constants
	if HeaderVersionByte != 0x03 {
		t.Errorf("Expected HeaderVersionByte 0x03, got 0x%02x", HeaderVersionByte)
	}
	if CompressionByte != 0x00 {
		t.Errorf("Expected CompressionByte 0x00, got 0x%02x", CompressionByte)
	}
}

func TestExtractSchemaNameFromArn(t *testing.T) {
	tests := []struct {
		arn      string
		expected string
	}{
		{"arn:aws:glue:us-east-1:123456789:schema/my-registry/my-schema", "my-schema"},
		{"arn:aws:glue:us-west-2:987654321:schema/test-registry/test-schema-name", "test-schema-name"},
		{"invalid-arn", ""},
		{"", ""},
	}

	for _, test := range tests {
		result := extractSchemaNameFromArn(test.arn)
		if result != test.expected {
			t.Errorf("extractSchemaNameFromArn(%q) = %q, expected %q", test.arn, result, test.expected)
		}
	}
}

func TestCanDecodeBasic(t *testing.T) {
	// Test data too short
	deserializer := &GsrDecoder{closed: false}
	
	canDecode, err := deserializer.CanDecode([]byte{0x01, 0x02})
	if err != nil {
		t.Errorf("CanDecode should not error on short data: %v", err)
	}
	if canDecode {
		t.Error("CanDecode should return false for short data")
	}

	// Test correct version byte
	data := make([]byte, 13)
	data[0] = HeaderVersionByte
	canDecode, err = deserializer.CanDecode(data)
	if err != nil {
		t.Errorf("CanDecode error: %v", err)
	}
	if !canDecode {
		t.Error("CanDecode should return true for correct version")
	}

	// Test wrong version byte
	data[0] = 0x99
	canDecode, err = deserializer.CanDecode(data)
	if err != nil {
		t.Errorf("CanDecode error: %v", err)
	}
	if canDecode {
		t.Error("CanDecode should return false for wrong version")
	}
}
