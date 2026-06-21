package gsrserde

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// Wire-format constants — parity-anchored to Java
// common/src/main/java/com/amazonaws/services/schemaregistry/utils/AWSSchemaRegistryConstants.java.
const (
	// WireFormatVersionByte is the GSR header version. Anchored to
	// AWSSchemaRegistryConstants.HEADER_VERSION_BYTE = (byte) 3.
	WireFormatVersionByte byte = 0x03

	// CompressionByteNone signals an uncompressed payload.
	// AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE = (byte) 0.
	CompressionByteNone byte = 0x00

	// CompressionByteZlib signals a zlib-compressed payload.
	// AWSSchemaRegistryConstants.COMPRESSION_BYTE = (byte) 5.
	CompressionByteZlib byte = 0x05

	// HeaderVersionByteSize is the size of the version byte. Anchored to
	// AWSSchemaRegistryConstants.HEADER_VERSION_BYTE_SIZE = 1.
	HeaderVersionByteSize = 1

	// CompressionByteSize is the size of the compression byte. Anchored to
	// AWSSchemaRegistryConstants.COMPRESSION_BYTE_SIZE = 1.
	CompressionByteSize = 1

	// SchemaVersionIDSize is the size of the 16-byte UUID. Anchored to
	// AWSSchemaRegistryConstants.SCHEMA_VERSION_ID_SIZE = 16.
	SchemaVersionIDSize = 16

	// WireFormatHeaderSize is the total prefix length: 1 + 1 + 16 = 18 bytes.
	WireFormatHeaderSize = HeaderVersionByteSize + CompressionByteSize + SchemaVersionIDSize
)

// ErrIncompatibleData is the sentinel for payloads whose 18-byte prefix is
// missing or malformed. Anchored to Java GlueSchemaRegistryIncompatibleDataException.
// Callers MUST check with errors.Is.
var ErrIncompatibleData = errors.New("payload is not in GSR wire format")

// EncodeWireFormat produces the 18-byte GSR prefix followed by payload bytes.
//
// Wire layout (Java parity: SerializationDataEncoder.write at
// serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/SerializationDataEncoder.java:54-70):
//
//	byte 0:        0x03                                   (version)
//	byte 1:        0x00 | 0x05                            (compression)
//	bytes 2..9:    schemaVersionID MSB, big-endian        (8 bytes)
//	bytes 10..17:  schemaVersionID LSB, big-endian        (8 bytes)
//	bytes 18..:    payload (already-compressed if compressionByte == 0x05)
//
// payload must already be in its final compressed-or-not form. The caller is
// responsible for compressing before calling this, because the protobuf
// message-index prefix has to be applied before compression (mirrors Java).
//
// Returns ErrIncompatibleData wrapped via fmt.Errorf when compressionByte is
// neither CompressionByteNone nor CompressionByteZlib. UUID parse failures
// surface as wrapped uuid.Parse errors.
func EncodeWireFormat(schemaVersionID string, compressionByte byte, payload []byte) ([]byte, error) {
	if compressionByte != CompressionByteNone && compressionByte != CompressionByteZlib {
		return nil, fmt.Errorf("%w: unsupported compression byte %#x", ErrIncompatibleData, compressionByte)
	}

	u, err := uuid.Parse(schemaVersionID)
	if err != nil {
		return nil, fmt.Errorf("schema version id is not a valid UUID: %w", err)
	}

	out := make([]byte, 0, WireFormatHeaderSize+len(payload))
	out = append(out, WireFormatVersionByte)
	out = append(out, compressionByte)
	out = append(out, u[:]...) // uuid.UUID is already [16]byte in MSB→LSB big-endian layout matching Java's putLong(MSB)+putLong(LSB).
	out = append(out, payload...)

	return out, nil
}

// DecodeWireFormat parses the 18-byte GSR prefix and returns
// (schemaVersionID, compressionByte, payload).
//
// Java parity: GlueSchemaRegistryDeserializerDataParser.getSchemaVersionId +
// getPlainData at
// serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/deserializers/GlueSchemaRegistryDeserializerDataParser.java:67-84
// and 128-161.
//
// Returns ErrIncompatibleData wrapped via fmt.Errorf for:
//   - data shorter than 18 bytes
//   - version byte != 0x03
//   - compression byte not in {0x00, 0x05}
//
// payload is left compressed if compressionByte == 0x05 — the caller decides
// whether to decompress. Returning the raw payload + compression byte keeps
// the wire-format module ignorant of zlib (the CompressionHandler interface
// owns that).
func DecodeWireFormat(data []byte) (schemaVersionID string, compressionByte byte, payload []byte, err error) {
	if len(data) < WireFormatHeaderSize {
		return "", 0, nil, fmt.Errorf("%w: size %d below minimum %d", ErrIncompatibleData, len(data), WireFormatHeaderSize)
	}

	if data[0] != WireFormatVersionByte {
		return "", 0, nil, fmt.Errorf("%w: header version byte %#x != 0x03", ErrIncompatibleData, data[0])
	}

	compressionByte = data[1]
	if compressionByte != CompressionByteNone && compressionByte != CompressionByteZlib {
		return "", 0, nil, fmt.Errorf("%w: compression byte %#x not in {0x00, 0x05}", ErrIncompatibleData, compressionByte)
	}

	// Bytes 2..17 are the UUID — Java writes putLong(MSB) then putLong(LSB),
	// both big-endian, which is byte-identical to a UUID's natural network-order
	// representation. uuid.FromBytes accepts that layout directly.
	var u uuid.UUID
	copy(u[:], data[2:WireFormatHeaderSize])
	schemaVersionID = u.String()

	payload = data[WireFormatHeaderSize:]
	return schemaVersionID, compressionByte, payload, nil
}

