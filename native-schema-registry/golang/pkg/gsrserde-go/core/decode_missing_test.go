package gsrserde

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeserializer_Decode_ParseGSRDataError(t *testing.T) {
	deserializer := &GsrDecoder{}
	
	// Create data that passes CanDecodeData but fails in parseGSRData
	data := []byte{HeaderVersionByte, 0x00, 0x00, 0x00, 0x00, 0x10} // Schema ID length = 16 but no data
	
	_, err := deserializer.Decode(data)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read schema ID")
}
