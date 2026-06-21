package gsrserde

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// Deprecated: HeaderVersionByte / CompressionByte are kept as aliases so the
// scaffolding tests in serde_test.go / edge_cases_test.go / deserializer_test.go
// still compile while they migrate over to WireFormatVersionByte /
// CompressionByteNone (see wire_format.go). New code MUST use those.
const (
	HeaderVersionByte = WireFormatVersionByte
	CompressionByte   = CompressionByteNone
)

type Schema struct {
	SchemaDefinition string
	DataFormat       string
	SchemaName       string
	// SchemaVersionID is the Glue schema-version UUID returned by GetSchemaByDefinition,
	// CreateSchema, or RegisterSchemaVersion. It is the 16-byte UUID that the GSR
	// wire-format header carries after the version + compression bytes; storing it
	// on Schema lets the encoder return the same UUID on the cached path that the
	// live path produced.
	SchemaVersionID string
}

type GsrEncoder struct {
	client                        GlueClient
	registryName                  string
	compatibility                 string
	tags                          map[string]string
	schemaCache                   Cache
	description                   string
	schemaAutoRegistrationEnabled bool
	compressionType               string
	mutex                         sync.RWMutex
}

func NewGsrEncoder(configMap map[string]string) (*GsrEncoder, error) {
	config, err := LoadConfigFromMap(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	client := glue.NewFromConfig(config.AWSConfig)
	
	cache, err := NewCache(config.TimeToLiveMillis)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}
	
	return &GsrEncoder{
		client:                        client,
		registryName:                  config.RegistryName,
		compatibility:                 config.Compatibility,
		tags:                          config.Tags,
		schemaCache:                   cache,
		description:                   config.Description,
		schemaAutoRegistrationEnabled: config.SchemaAutoRegistrationEnabled,
		compressionType:               config.CompressionType,
	}, nil
}

// Encode prepends the 18-byte GSR wire-format prefix to data and returns the
// full payload. The prefix carries the schema-version UUID resolved by
// getSchemaVersionIdByDefinition.
//
// For PROTOBUF schemas the protobuf message-index varint is prepended BEFORE
// compression (mirrors Java
// serializer-deserializer/.../ProtobufWireFormatEncoder.java and
// SerializationDataEncoder.java:62 — compress the schema-format-encoded bytes,
// then write the wire-format header).
func (s *GsrEncoder) Encode(data []byte, transportName string, schema *Schema) ([]byte, error) {
	if data == nil {
		return nil, NewSerializationError("data cannot be nil")
	}
	if schema == nil {
		return nil, NewSerializationError("schema cannot be nil")
	}

	schemaVersionID, _, err := s.getSchemaVersionIdByDefinition(schema.SchemaDefinition, schema.SchemaName, schema.DataFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	payload := data

	// Protobuf: prepend the message-index varint BEFORE compression.
	if schema.DataFormat == "PROTOBUF" {
		payload, err = prefixMessageIndexToBytes(payload, schema.SchemaDefinition, schema.SchemaName)
		if err != nil {
			return nil, fmt.Errorf("failed to prefix protobuf message index: %w", err)
		}
	}

	handler, err := CompressionFactory{}.HandlerForType(CompressionType(s.compressionType))
	if err != nil {
		return nil, fmt.Errorf("compression handler: %w", err)
	}

	compressionByte := CompressionByteNone
	if handler != nil {
		compressionByte = handler.CompressionByte()
		payload, err = handler.Compress(payload)
		if err != nil {
			return nil, fmt.Errorf("compress payload: %w", err)
		}
	}

	out, err := EncodeWireFormat(schemaVersionID, compressionByte, payload)
	if err != nil {
		return nil, fmt.Errorf("encode wire format: %w", err)
	}
	return out, nil
}

func (s *GsrEncoder) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.schemaCache.Close()
	return nil
}

func (s *GsrEncoder) getSchemaVersionIdByDefinition(schemaDefinition, schemaName, dataFormat string) (string, uint32, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Schema definition is already in correct format (.proto text for protobuf)
	processedDefinition := schemaDefinition

	cacheKey := fmt.Sprintf("%s:%s", schemaName, dataFormat)
	if cached, exists := s.schemaCache.Get(cacheKey); exists {
		schema := cached.(*Schema)
		// Plan §2.2 divergence (b): cached path must return the Glue schema-version
		// UUID, not the schema name. The wire-format header carries the UUID, not
		// the name (16 bytes after version + compression byte). Aligned with the
		// live-API path below at encoder.go's GetSchemaByDefinition branch.
		return schema.SchemaVersionID, 1, nil
	}

	getResp, err := s.client.GetSchemaByDefinition(context.Background(), &glue.GetSchemaByDefinitionInput{
		SchemaId: &types.SchemaId{
			RegistryName: &s.registryName,
			SchemaName:   &schemaName,
		},
		SchemaDefinition: &processedDefinition,
	})

	if err == nil && getResp.SchemaVersionId != nil && getResp.Status == types.SchemaVersionStatusAvailable {
		schema := &Schema{
			SchemaName:       schemaName,
			SchemaDefinition: schemaDefinition,
			DataFormat:       dataFormat,
			SchemaVersionID:  *getResp.SchemaVersionId,
		}
		s.schemaCache.Set(cacheKey, schema)
		return *getResp.SchemaVersionId, 1, nil
	}

	schemaVersionId, version, err := s.createSchema(schemaName, dataFormat, schemaDefinition)
	if err != nil {
		// If schema already exists, try to register a new version
		if strings.Contains(err.Error(), "AlreadyExistsException") || strings.Contains(err.Error(), "already exists") {
			return s.registerSchemaVersion(schemaDefinition, schemaName, dataFormat)
		}
		return "", 0, err
	}

	schema := &Schema{
		SchemaName:       schemaName,
		SchemaDefinition: schemaDefinition,
		DataFormat:       dataFormat,
		SchemaVersionID:  schemaVersionId,
	}
	s.schemaCache.Set(cacheKey, schema)
	return schemaVersionId, version, nil
}

func (s *GsrEncoder) registerSchemaVersion(schemaDefinition, schemaName, dataFormat string) (string, uint32, error) {
	resp, err := s.client.RegisterSchemaVersion(context.Background(), &glue.RegisterSchemaVersionInput{
		SchemaId: &types.SchemaId{
			RegistryName: &s.registryName,
			SchemaName:   &schemaName,
		},
		SchemaDefinition: &schemaDefinition,
	})
	
	if err != nil {
		return "", 0, fmt.Errorf("failed to register schema version: %w", err)
	}
	
	if resp.SchemaVersionId == nil {
		return "", 0, fmt.Errorf("no schema version ID returned")
	}
	
	// Cache the schema
	schema := &Schema{
		SchemaName:       schemaName,
		SchemaDefinition: schemaDefinition,
		DataFormat:       dataFormat,
	}
	cacheKey := fmt.Sprintf("%s:%s:%s", schemaDefinition, schemaName, dataFormat)
	s.schemaCache.Set(cacheKey, schema)
	
	version := uint32(1) // Default fallback
	if resp.VersionNumber != nil {
		version = uint32(*resp.VersionNumber)
	}
	
	return *resp.SchemaVersionId, version, nil
}

// createSchema mirrors Java AWSSchemaRegistryClient.java:242 — returns the
// Glue schema-version UUID (createSchemaResponse.schemaVersionId()), NOT the
// LatestSchemaVersion integer (which is the version *number*, not the version
// *UUID*). Java's signature is `public UUID createSchema(...)`.
//
// We also return the version number for the caller that propagates it through
// the encoder's (versionID, versionNumber, error) return — the wire-format
// header carries the UUID; the version number is informational.
func (s *GsrEncoder) createSchema(schemaName, dataFormat, schemaDefinition string) (string, uint32, error) {
	// Convert tags map to AWS SDK format
	var tags map[string]string
	if len(s.tags) > 0 {
		tags = s.tags
	}

	createResp, err := s.client.CreateSchema(context.Background(), &glue.CreateSchemaInput{
		RegistryId: &types.RegistryId{
			RegistryName: &s.registryName,
		},
		SchemaName:       &schemaName,
		DataFormat:       types.DataFormat(dataFormat),
		SchemaDefinition: &schemaDefinition,
		Compatibility:    types.Compatibility(s.compatibility),
		Description:      &s.description,
		Tags:             tags,
	})

	if err != nil {
		return "", 0, fmt.Errorf("failed to create schema: %w", err)
	}

	if createResp.SchemaVersionId == nil {
		return "", 0, fmt.Errorf("CreateSchema returned no SchemaVersionId")
	}

	version := uint32(1)
	if createResp.LatestSchemaVersion != nil {
		version = uint32(*createResp.LatestSchemaVersion)
	}

	return *createResp.SchemaVersionId, version, nil
}
