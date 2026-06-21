package gsrserde

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/glue"
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
// Compile-time check that MockGlueClient satisfies GlueClient.
var _ GlueClient = (*MockGlueClient)(nil)

type MockGlueClient struct {
	mock.Mock
}

func (m *MockGlueClient) GetSchemaByDefinition(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaByDefinitionOutput), args.Error(1)
}

func (m *MockGlueClient) GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
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
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.PutSchemaVersionMetadataOutput), args.Error(1)
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
