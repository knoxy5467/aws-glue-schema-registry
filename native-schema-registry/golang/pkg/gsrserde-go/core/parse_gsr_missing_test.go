package gsrserde

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeserializer_ParseGSRData_SchemaIDReadError(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create data with schema ID length but insufficient data for schema ID
	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(0x00) // No compression
	binary.Write(&buf, binary.BigEndian, uint32(10)) // Schema ID length = 10
	buf.WriteString("short") // Only 5 bytes instead of 10
	
	_, _, err := deserializer.parseGSRData(buf.Bytes())
	
	assert.Error(t, err)
	// The error could be either schema ID or schema version read error
	assert.True(t, 
		err.Error() == "deserialization error: failed to read schema ID" ||
		err.Error() == "deserialization error: failed to read schema version")
}

func TestDeserializer_ParseGSRData_SchemaVersionReadError(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create data with schema ID but no schema version
	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(0x00) // No compression
	schemaID := "test"
	binary.Write(&buf, binary.BigEndian, uint32(len(schemaID)))
	buf.WriteString(schemaID)
	// Missing schema version (4 bytes)
	
	_, _, err := deserializer.parseGSRData(buf.Bytes())
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read schema version")
}
