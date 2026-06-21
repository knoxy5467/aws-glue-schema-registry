//go:build integration

// Package fakeglue provides a minimal in-memory fake of the
// gsrcore.GlueClient interface. The integration-tests module uses it
// to drive the Phase 4 lifecycle / cache / error scenarios (§5.3
// items 13-29) without billing real Glue.
//
// fakeglue is NOT a replacement for the testify/mock fake under
// pkg/gsrserde-go/core/ (which is _test.go-only). Reach for testify/
// mock when a test needs call-order or argument-matcher assertions;
// reach for fakeglue when the test wants stateful schema-by-
// definition / schema-by-version-id lookups that mimic the real Glue
// API at a behavioral level.
package fakeglue

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/google/uuid"
)

// Fake is an in-memory GlueClient. It implements the schema-by-
// definition / schema-by-version-id lookup behaviors that the core
// encoder/decoder rely on, plus enough of the metadata + tag APIs to
// satisfy the GlueClient interface.
//
// Concurrency: methods are goroutine-safe; tests can drive Encode
// from N goroutines without external locking.
type Fake struct {
	mu sync.Mutex

	// definitions maps (registryName + "/" + schemaName + "/" +
	// schemaDefinition) -> schemaVersionId, mimicking the
	// GetSchemaByDefinition lookup key.
	definitions map[string]string

	// versions maps schemaVersionId -> stored schema.
	versions map[string]*storedSchema

	// CallCounts is a public counter map keyed by Glue API method
	// name. Tests assert on these for the §5.3 cache / singleflight
	// scenarios ("only one CreateSchema for N concurrent encodes").
	CallCounts map[string]int

	// ForceCreateError, when non-nil, makes CreateSchema return this
	// error verbatim. Used by negative scenarios (§5.3 items 23-25).
	ForceCreateError error

	// ForceGetSchemaError, when non-nil, makes GetSchemaByDefinition
	// return this error verbatim. Used to simulate IAM denied / Throttling.
	ForceGetSchemaError error

	// ForceGetVersionError, when non-nil, makes GetSchemaVersion
	// return this error verbatim.
	ForceGetVersionError error
}

type storedSchema struct {
	registryName     string
	schemaName       string
	schemaDefinition string
	dataFormat       types.DataFormat
}

// New returns an empty Fake.
func New() *Fake {
	return &Fake{
		definitions: make(map[string]string),
		versions:    make(map[string]*storedSchema),
		CallCounts:  make(map[string]int),
	}
}

func (f *Fake) bump(name string) {
	f.CallCounts[name]++
}

// definitionKey returns the in-memory lookup key for
// (registry, schema-name, schema-definition). registryName may be ""
// (the Java default) which deliberately maps to a distinct key from
// "default-registry" so callers see whatever the encoder actually
// sent over.
func definitionKey(registry, name, def string) string {
	return fmt.Sprintf("%s/%s/%s", registry, name, def)
}

// GetSchemaByDefinition implements gsrcore.GlueClient.
func (f *Fake) GetSchemaByDefinition(ctx context.Context, in *glue.GetSchemaByDefinitionInput, _ ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bump("GetSchemaByDefinition")

	if f.ForceGetSchemaError != nil {
		return nil, f.ForceGetSchemaError
	}

	registry := ""
	name := ""
	if in.SchemaId != nil {
		if in.SchemaId.RegistryName != nil {
			registry = *in.SchemaId.RegistryName
		}
		if in.SchemaId.SchemaName != nil {
			name = *in.SchemaId.SchemaName
		}
	}
	def := ""
	if in.SchemaDefinition != nil {
		def = *in.SchemaDefinition
	}

	versionID, ok := f.definitions[definitionKey(registry, name, def)]
	if !ok {
		return nil, &types.EntityNotFoundException{Message: aws.String("schema not found")}
	}
	return &glue.GetSchemaByDefinitionOutput{
		SchemaVersionId: aws.String(versionID),
		Status:          types.SchemaVersionStatusAvailable,
	}, nil
}

// GetSchemaVersion implements gsrcore.GlueClient.
func (f *Fake) GetSchemaVersion(ctx context.Context, in *glue.GetSchemaVersionInput, _ ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bump("GetSchemaVersion")

	if f.ForceGetVersionError != nil {
		return nil, f.ForceGetVersionError
	}

	id := ""
	if in.SchemaVersionId != nil {
		id = *in.SchemaVersionId
	}
	stored, ok := f.versions[id]
	if !ok {
		return nil, &types.EntityNotFoundException{Message: aws.String("schema version not found")}
	}
	arn := fmt.Sprintf("arn:aws:glue:us-east-1:0:schema/%s/%s", stored.registryName, stored.schemaName)
	return &glue.GetSchemaVersionOutput{
		SchemaVersionId:  aws.String(id),
		SchemaDefinition: aws.String(stored.schemaDefinition),
		DataFormat:       stored.dataFormat,
		SchemaArn:        aws.String(arn),
	}, nil
}

// CreateSchema implements gsrcore.GlueClient.
func (f *Fake) CreateSchema(ctx context.Context, in *glue.CreateSchemaInput, _ ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bump("CreateSchema")

	if f.ForceCreateError != nil {
		return nil, f.ForceCreateError
	}

	registry := ""
	if in.RegistryId != nil && in.RegistryId.RegistryName != nil {
		registry = *in.RegistryId.RegistryName
	}
	name := ""
	if in.SchemaName != nil {
		name = *in.SchemaName
	}
	def := ""
	if in.SchemaDefinition != nil {
		def = *in.SchemaDefinition
	}

	versionID := uuid.NewString()
	f.definitions[definitionKey(registry, name, def)] = versionID
	f.versions[versionID] = &storedSchema{
		registryName:     registry,
		schemaName:       name,
		schemaDefinition: def,
		dataFormat:       in.DataFormat,
	}

	v := int64(1)
	return &glue.CreateSchemaOutput{
		SchemaVersionId:     aws.String(versionID),
		LatestSchemaVersion: &v,
	}, nil
}

// RegisterSchemaVersion implements gsrcore.GlueClient.
func (f *Fake) RegisterSchemaVersion(ctx context.Context, in *glue.RegisterSchemaVersionInput, _ ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bump("RegisterSchemaVersion")

	versionID := uuid.NewString()
	registry := ""
	name := ""
	if in.SchemaId != nil {
		if in.SchemaId.RegistryName != nil {
			registry = *in.SchemaId.RegistryName
		}
		if in.SchemaId.SchemaName != nil {
			name = *in.SchemaId.SchemaName
		}
	}
	def := ""
	if in.SchemaDefinition != nil {
		def = *in.SchemaDefinition
	}
	f.definitions[definitionKey(registry, name, def)] = versionID
	// We don't have a DataFormat in RegisterSchemaVersionInput — Glue
	// derives it from the schema. Tests that care should call
	// CreateSchema first.
	f.versions[versionID] = &storedSchema{registryName: registry, schemaName: name, schemaDefinition: def}
	v := int64(2)
	return &glue.RegisterSchemaVersionOutput{
		SchemaVersionId: aws.String(versionID),
		VersionNumber:   &v,
	}, nil
}

// PutSchemaVersionMetadata implements gsrcore.GlueClient.
func (f *Fake) PutSchemaVersionMetadata(ctx context.Context, in *glue.PutSchemaVersionMetadataInput, _ ...func(*glue.Options)) (*glue.PutSchemaVersionMetadataOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bump("PutSchemaVersionMetadata")
	return &glue.PutSchemaVersionMetadataOutput{}, nil
}

// QuerySchemaVersionMetadata implements gsrcore.GlueClient.
func (f *Fake) QuerySchemaVersionMetadata(ctx context.Context, in *glue.QuerySchemaVersionMetadataInput, _ ...func(*glue.Options)) (*glue.QuerySchemaVersionMetadataOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bump("QuerySchemaVersionMetadata")
	return &glue.QuerySchemaVersionMetadataOutput{}, nil
}

// GetTags implements gsrcore.GlueClient.
func (f *Fake) GetTags(ctx context.Context, in *glue.GetTagsInput, _ ...func(*glue.Options)) (*glue.GetTagsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bump("GetTags")
	return &glue.GetTagsOutput{}, nil
}
