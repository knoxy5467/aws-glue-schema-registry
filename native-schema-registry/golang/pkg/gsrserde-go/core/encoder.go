package gsrserde

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

const (
	HeaderVersionByte = 0x03
	CompressionByte   = 0x00
)

type Schema struct {
	SchemaDefinition string
	DataFormat       string
	SchemaName       string
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

func (s *GsrEncoder) Encode(data []byte, transportName string, schema *Schema) ([]byte, error) {
	if data == nil {
		return nil, NewSerializationError("data cannot be nil")
	}
	if schema == nil {
		return nil, NewSerializationError("schema cannot be nil")
	}

	schemaID, schemaVersion, err := s.getSchemaVersionIdByDefinition(schema.SchemaDefinition, schema.SchemaName, schema.DataFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Apply compression if configured
	compressedData := data
	compressionByte := byte(0x00) // No compression
	
	// For protobuf, add message index prefix BEFORE compression (matches Java implementation)
	if schema.DataFormat == "PROTOBUF" {
		compressedData = prefixMessageIndexToBytes(compressedData, schema.SchemaDefinition, schema.SchemaName)
	}
	
	if s.compressionType == "ZLIB" {
		var buf bytes.Buffer
		writer := zlib.NewWriter(&buf)
		writer.Write(compressedData)
		writer.Close()
		compressedData = buf.Bytes()
		compressionByte = byte(0x01) // ZLIB compression
	}

	var buf bytes.Buffer
	buf.WriteByte(HeaderVersionByte)
	buf.WriteByte(compressionByte)
	
	schemaIDBytes := []byte(schemaID)
	binary.Write(&buf, binary.BigEndian, uint32(len(schemaIDBytes)))
	buf.Write(schemaIDBytes)
	binary.Write(&buf, binary.BigEndian, schemaVersion)
	buf.Write(compressedData)

	return buf.Bytes(), nil
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
		return schema.SchemaName, 1, nil
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
		}
		s.schemaCache.Set(cacheKey, schema)
		return *getResp.SchemaVersionId, 1, nil
	}

	schemaVersionId, err := s.createSchema(schemaName, dataFormat, schemaDefinition)
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
	}
	s.schemaCache.Set(cacheKey, schema)
	return schemaName, schemaVersionId, nil
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

func (s *GsrEncoder) createSchema(schemaName, dataFormat, schemaDefinition string) (uint32, error) {
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
		return 0, fmt.Errorf("failed to create schema: %w", err)
	}

	return uint32(*createResp.LatestSchemaVersion), nil
}
