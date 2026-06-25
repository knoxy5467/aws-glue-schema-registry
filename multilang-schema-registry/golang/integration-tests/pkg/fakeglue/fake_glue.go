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

	// CallCounts is a counter map keyed by Glue API method name.
	// Prefer Count(name) / Snapshot() over reading this map directly:
	// writes go under f.mu, so a direct mid-flight read would race
	// (the field is exported only so legacy tests that read it after
	// wg.Wait() keep compiling — those reads are safe via the
	// happens-before edge wg.Wait gives, but the raw map invites
	// future misuse).
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

	// ForceRegisterPending, when true, makes RegisterSchemaVersion return
	// Status=PENDING instead of AVAILABLE. Pair with ForcePendingCount to
	// script a "N PENDING responses then AVAILABLE" sequence via the
	// GetSchemaVersion poll loop — mirroring the Tier-1 mock approach in
	// poll_test.go for integration-level Tier-2 scenarios (spec §5.2 PBI-11).
	ForceRegisterPending bool

	// ForcePendingCount is the number of GetSchemaVersion calls that will
	// return Status=PENDING before switching to AVAILABLE. Decremented on
	// each call. When it reaches zero, subsequent GetSchemaVersion calls
	// return the normal AVAILABLE response (unless ForceGetVersionError is
	// set). Only meaningful when ForceRegisterPending=true triggers the poll.
	ForcePendingCount int
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

// Count returns the call count for the given Glue API method under
// the mutex. Use this from tests that may read mid-flight; direct
// access to f.CallCounts is safe only after a happens-before edge
// (e.g. wg.Wait).
func (f *Fake) Count(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.CallCounts[name]
}

// Snapshot returns a copy of CallCounts taken under the mutex.
func (f *Fake) Snapshot() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.CallCounts))
	for k, v := range f.CallCounts {
		out[k] = v
	}
	return out
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

	// Script "N PENDING then AVAILABLE" for poll-path Tier-2 tests (spec §5.2
	// PBI-11). The counter is decremented under the mutex so concurrent callers
	// each see the correct sequence without races.
	if f.ForcePendingCount > 0 {
		f.ForcePendingCount--
		id := ""
		if in.SchemaVersionId != nil {
			id = *in.SchemaVersionId
		}
		return &glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(id),
			Status:          types.SchemaVersionStatusPending,
		}, nil
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
		Status:           types.SchemaVersionStatusAvailable,
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
//
// Real Glue derives DataFormat from the parent schema (registered via
// CreateSchema); Register doesn't carry it on the input. To match that
// behavior the fake inherits DataFormat from any prior storedSchema
// under (registry, name). Without this inheritance, the decoder side
// would receive DataFormat="" on the AlreadyExists fall-through path
// and silently skip protobuf message-index stripping (decoder.go:75),
// corrupting the decoded bytes.
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

	// Inherit DataFormat from any prior storedSchema for this
	// (registry, name) — mirrors real Glue. If no prior schema exists
	// the format stays zero-value, matching real Glue's behavior of
	// rejecting RegisterSchemaVersion against an unknown schema name.
	var inheritedFormat types.DataFormat
	for _, prior := range f.versions {
		if prior.registryName == registry && prior.schemaName == name && prior.dataFormat != "" {
			inheritedFormat = prior.dataFormat
			break
		}
	}
	f.versions[versionID] = &storedSchema{
		registryName:     registry,
		schemaName:       name,
		schemaDefinition: def,
		dataFormat:       inheritedFormat,
	}

	v := int64(2)
	status := types.SchemaVersionStatusAvailable
	if f.ForceRegisterPending {
		status = types.SchemaVersionStatusPending
	}
	return &glue.RegisterSchemaVersionOutput{
		SchemaVersionId: aws.String(versionID),
		VersionNumber:   &v,
		Status:          status,
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
