package gsrserde

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// Deserializer handles schema registry deserialization
type GsrDecoder struct {
	client       GlueClient
	registryName string
	schemaCache  Cache
	mutex        sync.RWMutex
	closed       bool
}

// NewDeserializerFromMap creates a new deserializer instance from a config map
func NewGsrDecoder(configMap map[string]string) (*GsrDecoder, error) {
	config, err := LoadConfigFromMap(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	client := glue.NewFromConfig(config.AWSConfig)
	
	cache, err := NewCache(config.TimeToLiveMillis)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}
	
	return &GsrDecoder{
		client:       client,
		registryName: config.RegistryName,
		schemaCache:  cache,
	}, nil
}

// Decode deserializes the encoded data
func (d *GsrDecoder) Decode(data []byte) ([]byte, error) {
	if !d.CanDecodeData(data) {
		return nil, NewDeserializationError("invalid GSR format")
	}

	// Skip GSR header and extract payload
	schemaInfo, payload, err := d.parseGSRData(data)
	if err != nil {
		return nil, err
	}

	// For protobuf, strip message index
	schema, err := d.getSchema(schemaInfo.SchemaID, schemaInfo.SchemaVersion)
	if err != nil {
		return nil, err
	}
	
	// For protobuf, strip message index from decompressed payload
	if schema.DataFormat == "PROTOBUF" {
		_, payload, err = stripMessageIndex(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to strip protobuf message index: %w", err)
		}
	}

	return payload, nil
}

// CanDecode checks if the data can be decoded
func (d *GsrDecoder) CanDecode(data []byte) (bool, error) {
	return d.CanDecodeData(data), nil
}

// CanDecodeData checks if data has valid GSR format
func (d *GsrDecoder) CanDecodeData(data []byte) bool {
	if len(data) < 6 {
		return false
	}
	return data[0] == HeaderVersionByte && data[1] == CompressionByte
}

// DecodeSchema extracts the schema from encoded data
func (d *GsrDecoder) DecodeSchema(data []byte) (*Schema, error) {
	if !d.CanDecodeData(data) {
		return nil, NewDeserializationError("invalid GSR format")
	}

	schemaInfo, _, err := d.parseGSRData(data)
	if err != nil {
		return nil, err
	}

	return d.getSchema(schemaInfo.SchemaID, schemaInfo.SchemaVersion)
}

// Close releases all resources
func (d *GsrDecoder) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.schemaCache.Close()
	return nil
}

type schemaInfo struct {
	SchemaID      string
	SchemaVersion uint32
}

func (d *GsrDecoder) parseGSRData(data []byte) (*schemaInfo, []byte, error) {
	if len(data) < 6 {
		return nil, nil, NewDeserializationError("data too short")
	}

	buf := bytes.NewReader(data)
	
	// Skip version byte
	buf.Seek(1, 0)
	
	// Read compression byte
	var compressionByte byte
	if err := binary.Read(buf, binary.BigEndian, &compressionByte); err != nil {
		return nil, nil, NewDeserializationError("failed to read compression byte")
	}
	
	// Read schema ID length
	var schemaIDLen uint32
	if err := binary.Read(buf, binary.BigEndian, &schemaIDLen); err != nil {
		return nil, nil, NewDeserializationError("failed to read schema ID length")
	}
	
	// Read schema ID
	schemaIDBytes := make([]byte, schemaIDLen)
	if _, err := buf.Read(schemaIDBytes); err != nil {
		return nil, nil, NewDeserializationError("failed to read schema ID")
	}
	
	// Read schema version
	var schemaVersion uint32
	if err := binary.Read(buf, binary.BigEndian, &schemaVersion); err != nil {
		return nil, nil, NewDeserializationError("failed to read schema version")
	}
	
	// Read remaining data
	remaining := make([]byte, buf.Len())
	buf.Read(remaining)
	
	// Decompress if needed
	var payload []byte
	if compressionByte == 0x01 { // ZLIB compression
		reader, err := zlib.NewReader(bytes.NewReader(remaining))
		if err != nil {
			return nil, nil, NewDeserializationError("failed to create zlib reader")
		}
		defer reader.Close()
		
		payload, err = io.ReadAll(reader)
		if err != nil {
			return nil, nil, NewDeserializationError("failed to decompress data")
		}
	} else {
		payload = remaining
	}
	
	return &schemaInfo{
		SchemaID:      string(schemaIDBytes),
		SchemaVersion: schemaVersion,
	}, payload, nil
}

func (d *GsrDecoder) getSchema(schemaID string, version uint32) (*Schema, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	cacheKey := fmt.Sprintf("%s:%d", schemaID, version)
	if cached, exists := d.schemaCache.Get(cacheKey); exists {
		return cached.(*Schema), nil
	}

	// Extract schema name from ARN-like schema ID
	schemaName := d.extractSchemaName(schemaID)
	
	versionNumber := int64(version)
	resp, err := d.client.GetSchemaVersion(context.Background(), &glue.GetSchemaVersionInput{
		SchemaId: &types.SchemaId{
			RegistryName: &d.registryName,
			SchemaName:   &schemaName,
		},
		SchemaVersionNumber: &types.SchemaVersionNumber{
			VersionNumber: &versionNumber,
		},
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	schema := &Schema{
		SchemaName:       schemaName,
		SchemaDefinition: *resp.SchemaDefinition,
		DataFormat:       string(resp.DataFormat),
	}

	d.schemaCache.Set(cacheKey, schema)
	return schema, nil
}

func (d *GsrDecoder) extractSchemaName(schemaID string) string {
	// Handle ARN format: arn:aws:glue:region:account:schema/registry/schema-name
	if strings.Contains(schemaID, ":") {
		parts := strings.Split(schemaID, ":")
		if len(parts) > 0 {
			resourcePart := parts[len(parts)-1]
			if strings.Contains(resourcePart, "/") {
				pathParts := strings.Split(resourcePart, "/")
				return pathParts[len(pathParts)-1]
			}
		}
	}
	return schemaID
}

// extractSchemaNameFromArn extracts schema name from ARN format (package level function for tests)
func extractSchemaNameFromArn(arn string) string {
	// Handle ARN format: arn:aws:glue:region:account:schema/registry/schema-name
	if strings.Contains(arn, ":") {
		parts := strings.Split(arn, ":")
		if len(parts) > 0 {
			resourcePart := parts[len(parts)-1]
			if strings.Contains(resourcePart, "/") {
				pathParts := strings.Split(resourcePart, "/")
				if len(pathParts) >= 3 {
					return pathParts[len(pathParts)-1]
				}
			}
		}
	}
	return ""
}
