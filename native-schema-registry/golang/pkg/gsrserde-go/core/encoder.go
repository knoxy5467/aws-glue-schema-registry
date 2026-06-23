package gsrserde

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"golang.org/x/sync/singleflight"
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
	// AdditionalInfo is opaque format-layer metadata that the orchestrator and
	// format-specific serializers attach to a Schema for downstream consumption
	// (e.g. the protobuf full-message-name for dynamic-message dispatch). Core
	// reads/writes nothing here; it round-trips the field through the cache.
	AdditionalInfo string
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

	// versionIDGroup dedups concurrent first-encode of the same schema. Java
	// achieves this via Caffeine's LoadingCache + AsyncCacheLoader; Go's
	// idiomatic equivalent is singleflight per cache key. Without this,
	// N concurrent encoders calling Encode on a brand-new schema would all
	// fire GetSchemaByDefinition (and possibly CreateSchema), wasting Glue
	// quota and risking AlreadyExistsException races.
	versionIDGroup singleflight.Group

	mutex sync.RWMutex
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
	// The message-index is computed from the protobuf message FULL NAME
	// (e.g. "test.TestMessage"), NOT the Glue schema name. The format
	// layer sets schema.AdditionalInfo to the proto fully-qualified
	// message name via SetAdditionalSchemaInfo; that's what we look up.
	// Fall back to SchemaName for backwards compatibility with callers
	// that haven't populated AdditionalInfo, but a real protobuf flow
	// must populate it or the index lookup will fail.
	if schema.DataFormat == "PROTOBUF" {
		messageType := schema.AdditionalInfo
		if messageType == "" {
			messageType = schema.SchemaName
		}
		payload, err = prefixMessageIndexToBytes(payload, schema.SchemaDefinition, messageType)
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

// versionIDLookupResult is the singleflight payload — singleflight.Do can only
// return one value plus an error, so pack the (id, version) tuple in a struct.
type versionIDLookupResult struct {
	SchemaVersionID string
	Version         uint32
}

func (s *GsrEncoder) getSchemaVersionIdByDefinition(schemaDefinition, schemaName, dataFormat string) (string, uint32, error) {
	cacheKey := fmt.Sprintf("%s:%s", schemaName, dataFormat)

	// Fast path: cache hit. Read-lock so concurrent encoders don't serialize
	// on the cached path.
	s.mutex.RLock()
	cached, exists := s.schemaCache.Get(cacheKey)
	s.mutex.RUnlock()
	if exists {
		schema := cached.(*Schema)
		// Plan §2.2 divergence (b): cached path returns the Glue schema-version
		// UUID, not the schema name. Aligned with the live-API path below.
		return schema.SchemaVersionID, 1, nil
	}

	// Slow path: singleflight dedup. N concurrent first-encodes for the same
	// cacheKey collapse into ONE Glue call. Subsequent calls hit the cache.
	resAny, err, _ := s.versionIDGroup.Do(cacheKey, func() (interface{}, error) {
		// Re-check the cache under lock — a sibling call may have populated
		// it between the RLock release above and the singleflight entry here.
		s.mutex.Lock()
		if cached, exists := s.schemaCache.Get(cacheKey); exists {
			s.mutex.Unlock()
			schema := cached.(*Schema)
			return &versionIDLookupResult{SchemaVersionID: schema.SchemaVersionID, Version: 1}, nil
		}
		s.mutex.Unlock()

		return s.fetchSchemaVersionID(schemaDefinition, schemaName, dataFormat, cacheKey)
	})
	if err != nil {
		return "", 0, err
	}
	res := resAny.(*versionIDLookupResult)
	return res.SchemaVersionID, res.Version, nil
}

// fetchSchemaVersionID is the part of getSchemaVersionIdByDefinition that
// actually talks to Glue. Extracted from the singleflight callback for
// readability — it must NOT be called outside the singleflight wrapper
// (parallel callers without dedup would defeat the whole point).
func (s *GsrEncoder) fetchSchemaVersionID(schemaDefinition, schemaName, dataFormat, cacheKey string) (*versionIDLookupResult, error) {
	processedDefinition := schemaDefinition

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
		s.mutex.Lock()
		s.schemaCache.Set(cacheKey, schema)
		s.mutex.Unlock()
		return &versionIDLookupResult{SchemaVersionID: *getResp.SchemaVersionId, Version: 1}, nil
	}

	// Phase 4.5 bug 2 fix: only fall through to CreateSchema on the
	// documented auto-register trigger (EntityNotFoundException). Any
	// other typed error from GetSchemaByDefinition — AccessDenied,
	// Throttling, InvalidInput, network — must propagate directly.
	// Java parity: AWSSchemaRegistryClient.java:151 only catches
	// EntityNotFoundException and re-raises everything else. The
	// previous code's blanket fall-through was write-amplifying
	// (turned read-denied into write-attempt) and could mask the
	// originating typed error behind a CreateSchema failure.
	//
	// `err == nil && getResp == nil` (or success-shape getResp with
	// SchemaVersionId == nil) is also treated as "not present" — Glue
	// returns 200 with empty body in some edge cases.
	if err != nil {
		var notFound *types.EntityNotFoundException
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("get schema by definition: %w", err)
		}
	}

	// Phase 4.5 bug 1 fix: honor SchemaAutoRegistrationEnabled before
	// attempting the auto-register write. Previously this flag was
	// stored on GsrEncoder but never read — producers explicitly
	// opting OUT of registry mutation (e.g. compliance / approval-gate
	// pipelines) silently got the opposite behavior. Returning
	// ErrSchemaAutoRegistrationDisabled (already declared in errors.go)
	// preserves the typed-error contract via errors.Is.
	if !s.schemaAutoRegistrationEnabled {
		return nil, fmt.Errorf("%w: schema %q not registered", ErrSchemaAutoRegistrationDisabled, schemaName)
	}

	schemaVersionId, version, err := s.createSchema(schemaName, dataFormat, schemaDefinition)
	if err != nil {
		// If schema already exists (race with another producer), register a new version.
		// Phase 4.5 bug 3 fix: match the typed SDK error rather than its
		// string form. strings.Contains was brittle to SDK formatting changes
		// (ErrorCodeOverride, middleware wrapping, localized messages) and
		// could mis-trigger on unrelated errors whose message happened to
		// contain "already exists" (e.g. validation messages from
		// *types.InvalidInputException). errors.As walks the wrap chain and
		// matches only the genuine typed Glue error.
		var alreadyExists *types.AlreadyExistsException
		if errors.As(err, &alreadyExists) {
			id, ver, regErr := s.registerSchemaVersion(schemaDefinition, schemaName, dataFormat)
			if regErr != nil {
				return nil, regErr
			}
			return &versionIDLookupResult{SchemaVersionID: id, Version: ver}, nil
		}
		return nil, err
	}

	schema := &Schema{
		SchemaName:       schemaName,
		SchemaDefinition: schemaDefinition,
		DataFormat:       dataFormat,
		SchemaVersionID:  schemaVersionId,
	}
	s.mutex.Lock()
	s.schemaCache.Set(cacheKey, schema)
	s.mutex.Unlock()
	return &versionIDLookupResult{SchemaVersionID: schemaVersionId, Version: version}, nil
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
