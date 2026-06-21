package gsrserde

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// GsrDecoder parses GSR-framed payloads.
type GsrDecoder struct {
	client       GlueClient
	registryName string
	schemaCache  Cache
	mutex        sync.RWMutex
	closed       bool
}

// NewGsrDecoder creates a new decoder from a config map.
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

// Decode parses the 18-byte GSR prefix, decompresses if necessary, strips the
// protobuf message-index for PROTOBUF payloads, and returns the original
// payload bytes.
func (d *GsrDecoder) Decode(data []byte) ([]byte, error) {
	schemaVersionID, compressionByte, payload, err := DecodeWireFormat(data)
	if err != nil {
		return nil, err
	}

	handler, err := CompressionFactory{}.HandlerForByte(compressionByte)
	if err != nil {
		return nil, err
	}
	if handler != nil {
		payload, err = handler.Decompress(payload)
		if err != nil {
			return nil, fmt.Errorf("decompress payload: %w", err)
		}
	}

	schema, err := d.getSchemaByVersionID(schemaVersionID)
	if err != nil {
		return nil, err
	}

	if schema.DataFormat == "PROTOBUF" {
		_, payload, err = stripMessageIndex(payload)
		if err != nil {
			return nil, fmt.Errorf("strip protobuf message index: %w", err)
		}
	}

	return payload, nil
}

// CanDecode reports whether data is GSR-framed.
func (d *GsrDecoder) CanDecode(data []byte) (bool, error) {
	return d.CanDecodeData(data), nil
}

// CanDecodeData reports whether data is GSR-framed: at least 18 bytes, version
// byte 0x03, compression byte in {0x00, 0x05}. Matches Java
// GlueSchemaRegistryDeserializerDataParser.isDataCompatible.
func (d *GsrDecoder) CanDecodeData(data []byte) bool {
	if len(data) < WireFormatHeaderSize {
		return false
	}
	if data[0] != WireFormatVersionByte {
		return false
	}
	if data[1] != CompressionByteNone && data[1] != CompressionByteZlib {
		return false
	}
	return true
}

// DecodeSchema returns the Schema referenced by the wire-format prefix without
// returning the underlying payload bytes.
func (d *GsrDecoder) DecodeSchema(data []byte) (*Schema, error) {
	schemaVersionID, _, _, err := DecodeWireFormat(data)
	if err != nil {
		return nil, err
	}
	return d.getSchemaByVersionID(schemaVersionID)
}

// Close releases all resources.
func (d *GsrDecoder) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.schemaCache.Close()
	return nil
}

// getSchemaByVersionID looks up a schema by its Glue schema-version UUID,
// falling through to a `glue.GetSchemaVersion` call (Java parity:
// AWSSchemaRegistryClient.java:168-181 uses GetSchemaVersionRequest.builder()
// .schemaVersionId(uuid) — there is no name-based path).
func (d *GsrDecoder) getSchemaByVersionID(schemaVersionID string) (*Schema, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if cached, exists := d.schemaCache.Get(schemaVersionID); exists {
		return cached.(*Schema), nil
	}

	resp, err := d.client.GetSchemaVersion(context.Background(), &glue.GetSchemaVersionInput{
		SchemaVersionId: &schemaVersionID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	if resp == nil || resp.SchemaDefinition == nil {
		return nil, errors.New("GetSchemaVersion returned no SchemaDefinition")
	}

	// SchemaArn → name extraction is a best-effort label only — the wire-
	// format header doesn't carry the name and the deserialize path doesn't
	// need it for parsing payloads. Keep it so callers that inspect Schema
	// still see something useful.
	schemaName := ""
	if resp.SchemaArn != nil {
		schemaName = extractSchemaNameFromArn(*resp.SchemaArn)
	}

	schema := &Schema{
		SchemaName:       schemaName,
		SchemaDefinition: *resp.SchemaDefinition,
		DataFormat:       string(resp.DataFormat),
		SchemaVersionID:  schemaVersionID,
	}

	d.schemaCache.Set(schemaVersionID, schema)
	return schema, nil
}

// extractSchemaName preserved for tests that exercise the ARN string parsing
// without going through the decoder. Identical algorithm to
// extractSchemaNameFromArn but more permissive (returns the input unchanged
// rather than empty when not an ARN).
func (d *GsrDecoder) extractSchemaName(schemaID string) string {
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

// extractSchemaNameFromArn returns the schema name from an arn:aws:glue:...:
// schema/registry/name ARN, or "" when the input doesn't look like such an ARN.
func extractSchemaNameFromArn(arn string) string {
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
