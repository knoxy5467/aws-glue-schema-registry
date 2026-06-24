package gsrserde

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
)

// MockGlueClient is a testify/mock-based fake for the GlueClient interface.
// Test files set up expectations with .On("MethodName", ...).Return(...).
//
// The set of methods MUST stay in lock-step with client_interface.go's
// GlueClient interface — a missing method here surfaces as "MockGlueClient
// does not implement GlueClient" at test-compile time. That's intentional:
// growing the interface forces a corresponding growth of the fake.
//
// PutSchemaVersionMetadataPairs is an out-of-band recorder for the
// (MetadataKey, MetadataValue) pair captured on every
// PutSchemaVersionMetadata call. The encoder's metadata-batch helper
// iterates sequentially under a single mutex (INV-9), but tests that
// exercise it from multiple goroutines (or that assert no-call from the
// cache-hit fast path) still need a race-free recorder — hence the
// dedicated mutex. Set-equality assertions on the recorded slice cover
// the AC-4 / AC-5 / AC-9 / AC-9b cases without coupling to call order
// (spec §3.4(d) — the sequential-for-loop choice keeps order stable in
// practice but ordering is NOT part of the contract).
//
// Compile-time check that MockGlueClient satisfies GlueClient.
var _ GlueClient = (*MockGlueClient)(nil)

type MetadataPair struct {
	Key   string
	Value string
}

type MockGlueClient struct {
	mock.Mock

	metadataMu                    sync.Mutex
	PutSchemaVersionMetadataPairs []MetadataPair

	// getSchemaVersionMu guards GetSchemaVersionCallCount against concurrent
	// test goroutines. The poll_test.go Tier-1 tests inject a no-op sleepFn
	// so the loop runs synchronously; the mutex is a safety net for any future
	// parallel-encode scenario.
	getSchemaVersionMu    sync.Mutex
	GetSchemaVersionCalls int
}

func (m *MockGlueClient) GetSchemaByDefinition(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaByDefinitionOutput), args.Error(1)
}

// GetSchemaVersion records the call in GetSchemaVersionCalls then dispatches
// to the configured testify expectation (if any). When NO expectation is
// configured the method returns a default AVAILABLE response so that tests
// which exercise paths NOT related to the poll loop (e.g.
// already_exists_recovery_test.go, metadata_test.go) do not need to add
// boilerplate stubs — absence-of-expectation here means "schema is
// immediately available". Tests that DO exercise the poll loop set explicit
// expectations via .On("GetSchemaVersion", ...).Return(...); those
// expectations are honored normally.
func (m *MockGlueClient) GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	m.getSchemaVersionMu.Lock()
	m.GetSchemaVersionCalls++
	m.getSchemaVersionMu.Unlock()

	if !m.hasExpectationFor("GetSchemaVersion") {
		id := ""
		if params != nil && params.SchemaVersionId != nil {
			id = *params.SchemaVersionId
		}
		return &glue.GetSchemaVersionOutput{
			SchemaVersionId: aws.String(id),
			Status:          types.SchemaVersionStatusAvailable,
		}, nil
	}
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaVersionOutput), args.Error(1)
}

func (m *MockGlueClient) CreateSchema(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.CreateSchemaOutput), args.Error(1)
}

func (m *MockGlueClient) RegisterSchemaVersion(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.RegisterSchemaVersionOutput), args.Error(1)
}

func (m *MockGlueClient) PutSchemaVersionMetadata(ctx context.Context, params *glue.PutSchemaVersionMetadataInput, optFns ...func(*glue.Options)) (*glue.PutSchemaVersionMetadataOutput, error) {
	// Capture the (MetadataKey, MetadataValue) pair into the order-independent
	// recorder BEFORE consulting the testify expectation. Tests assert on the
	// recorder slice via set equality (AC-4 / AC-5 / AC-9 / AC-9b); the testify
	// `.On("PutSchemaVersionMetadata", ...).Return(...)` configuration is what
	// decides success / error per call. The two channels are independent: the
	// recorder captures every invocation regardless of the configured return.
	if params != nil && params.MetadataKeyValue != nil {
		var k, v string
		if params.MetadataKeyValue.MetadataKey != nil {
			k = *params.MetadataKeyValue.MetadataKey
		}
		if params.MetadataKeyValue.MetadataValue != nil {
			v = *params.MetadataKeyValue.MetadataValue
		}
		m.metadataMu.Lock()
		m.PutSchemaVersionMetadataPairs = append(m.PutSchemaVersionMetadataPairs, MetadataPair{Key: k, Value: v})
		m.metadataMu.Unlock()
	}
	// Default to success when no explicit expectation is configured. The
	// metadata flush is INV-6 / C-13 fire-and-forget for the encoder; many
	// pre-existing Tier-1 tests trigger CreateSchema / RegisterSchemaVersion
	// success but do NOT care about the metadata flush. Forcing each of them
	// to add a `.On("PutSchemaVersionMetadata", ...)` stub would either
	// require renames (forbidden by INV-11) or scatter unrelated wiring into
	// every test fixture. Tests that DO assert metadata behavior set
	// explicit expectations the way they already do for other GlueClient
	// methods; absence-of-expectation here means "no-op success".
	if !m.hasExpectationFor("PutSchemaVersionMetadata") {
		return &glue.PutSchemaVersionMetadataOutput{}, nil
	}
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.PutSchemaVersionMetadataOutput), args.Error(1)
}

// hasExpectationFor reports whether any `.On(method, ...)` expectation was
// configured for the named mock method. Used by PutSchemaVersionMetadata to
// distinguish "test stubbed this; honor the stub" from "test ignored this;
// return success".
func (m *MockGlueClient) hasExpectationFor(method string) bool {
	for i := range m.ExpectedCalls {
		if m.ExpectedCalls[i].Method == method {
			return true
		}
	}
	return false
}

// MetadataCallCount returns the number of recorded PutSchemaVersionMetadata
// invocations regardless of success / failure outcome. Tests use this to
// assert call counts without relying on testify's call-history (which
// records the call args, not the captured pair).
func (m *MockGlueClient) MetadataCallCount() int {
	m.metadataMu.Lock()
	defer m.metadataMu.Unlock()
	return len(m.PutSchemaVersionMetadataPairs)
}

// MetadataPairs returns a copy of the recorded pairs in invocation order.
// The set-equality assertions in metadata_test.go convert this to a set
// before comparing — the spec does NOT pin call order (§3.4(d)).
func (m *MockGlueClient) MetadataPairs() []MetadataPair {
	m.metadataMu.Lock()
	defer m.metadataMu.Unlock()
	out := make([]MetadataPair, len(m.PutSchemaVersionMetadataPairs))
	copy(out, m.PutSchemaVersionMetadataPairs)
	return out
}

func (m *MockGlueClient) QuerySchemaVersionMetadata(ctx context.Context, params *glue.QuerySchemaVersionMetadataInput, optFns ...func(*glue.Options)) (*glue.QuerySchemaVersionMetadataOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.QuerySchemaVersionMetadataOutput), args.Error(1)
}

func (m *MockGlueClient) GetTags(ctx context.Context, params *glue.GetTagsInput, optFns ...func(*glue.Options)) (*glue.GetTagsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetTagsOutput), args.Error(1)
}
