// Tier-1 tests for gsr_serializer.Serializer against the core-backed
// implementation. These tests deliberately never reach AWS; they inject a
// fake core.GlueClient under a hand-built core.GsrEncoder via the
// NewSerializerWithEncoder test seam.
//
// Plan §5.1: Tier-1 unit tests must not require Docker, AWS, or Kafka.

package serializer

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	gsrjson "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/json"
)

// fakeGlueClient is a minimal testify/mock fake for the core.GlueClient
// interface. Reusing core's MockGlueClient would require exporting it from
// the test package; this fake is the equivalent shape for the calls the
// serializer path actually triggers.
type fakeGlueClient struct{ mock.Mock }

func (f *fakeGlueClient) GetSchemaByDefinition(ctx context.Context, in *glue.GetSchemaByDefinitionInput, _ ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
	args := f.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaByDefinitionOutput), args.Error(1)
}

func (f *fakeGlueClient) GetSchemaVersion(ctx context.Context, in *glue.GetSchemaVersionInput, _ ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	args := f.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaVersionOutput), args.Error(1)
}

func (f *fakeGlueClient) CreateSchema(ctx context.Context, in *glue.CreateSchemaInput, _ ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
	args := f.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.CreateSchemaOutput), args.Error(1)
}

func (f *fakeGlueClient) RegisterSchemaVersion(ctx context.Context, in *glue.RegisterSchemaVersionInput, _ ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
	args := f.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.RegisterSchemaVersionOutput), args.Error(1)
}

func (f *fakeGlueClient) PutSchemaVersionMetadata(ctx context.Context, in *glue.PutSchemaVersionMetadataInput, _ ...func(*glue.Options)) (*glue.PutSchemaVersionMetadataOutput, error) {
	return nil, nil
}

func (f *fakeGlueClient) QuerySchemaVersionMetadata(ctx context.Context, in *glue.QuerySchemaVersionMetadataInput, _ ...func(*glue.Options)) (*glue.QuerySchemaVersionMetadataOutput, error) {
	return nil, nil
}

func (f *fakeGlueClient) GetTags(ctx context.Context, in *glue.GetTagsInput, _ ...func(*glue.Options)) (*glue.GetTagsOutput, error) {
	return nil, nil
}

const knownAvroVersionID = "11111111-1111-1111-1111-111111111111"

// onGetSchemaByDefinitionReturning wires the fake to short-circuit
// GetSchemaByDefinition with an "AVAILABLE" version. The serializer's first
// encode path resolves the version UUID through this call when the schema
// already exists.
func onGetSchemaByDefinitionReturning(fake *fakeGlueClient, versionID string) {
	id := versionID
	fake.On("GetSchemaByDefinition", mock.Anything, mock.Anything).Return(&glue.GetSchemaByDefinitionOutput{
		SchemaVersionId: &id,
		Status:          types.SchemaVersionStatusAvailable,
	}, nil)
}

// newCoreEncoder constructs a core.GsrEncoder wired with the fake Glue
// client and a real in-process cache. It mirrors the production constructor
// minus the AWS config wiring — that wiring is exercised separately by
// core/config_test.go.
func newCoreEncoder(t *testing.T, fake *fakeGlueClient, opts ...func(*gsrcore.GsrEncoderOptions)) *gsrcore.GsrEncoder {
	t.Helper()
	options := gsrcore.GsrEncoderOptions{
		RegistryName:    "test-registry",
		Compatibility:   "BACKWARD",
		CompressionType: "NONE",
	}
	for _, opt := range opts {
		opt(&options)
	}
	enc, err := gsrcore.NewGsrEncoderForTest(fake, options)
	require.NoError(t, err)
	return enc
}

// avroTestRecord returns a valid *avro.AvroRecord and the matching schema
// definition for the topic.
func avroTestRecord() (*avro.AvroRecord, string) {
	schema := `{"type":"record","name":"TestRecord","fields":[{"name":"message","type":"string"}]}`
	return avro.NewAvroRecord(schema, map[string]any{"message": "hello"}), schema
}

func avroConfig() *common.Configuration {
	return common.NewConfiguration(map[string]any{
		common.DataFormatTypeKey: common.DataFormatAvro,
	})
}

func jsonConfig() *common.Configuration {
	return common.NewConfiguration(map[string]any{
		common.DataFormatTypeKey: common.DataFormatJSON,
	})
}

// TestSerialize_AvroFormat_GoesThroughCore proves the orchestrator delegates
// to core for the wire-format prefix when the format is Avro. The encoded
// output must carry the 18-byte GSR header (version 0x03, compression byte
// 0x00 for NONE, 16-byte UUID).
func TestSerialize_AvroFormat_GoesThroughCore(t *testing.T) {
	fake := &fakeGlueClient{}
	onGetSchemaByDefinitionReturning(fake, knownAvroVersionID)

	enc := newCoreEncoder(t, fake)
	s, err := NewSerializerWithEncoder(avroConfig(), enc)
	require.NoError(t, err)

	record, _ := avroTestRecord()
	out, err := s.Serialize("my-topic", record)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(out), gsrcore.WireFormatHeaderSize, "output must carry GSR header")
	require.Equal(t, byte(gsrcore.WireFormatVersionByte), out[0])
	require.Equal(t, byte(gsrcore.CompressionByteNone), out[1])
}

// TestSerialize_JsonFormat_GoesThroughCore mirrors the Avro test for JSON.
func TestSerialize_JsonFormat_GoesThroughCore(t *testing.T) {
	fake := &fakeGlueClient{}
	onGetSchemaByDefinitionReturning(fake, "22222222-2222-2222-2222-222222222222")

	enc := newCoreEncoder(t, fake)
	s, err := NewSerializerWithEncoder(jsonConfig(), enc)
	require.NoError(t, err)

	wrapper, err := gsrjson.NewJsonDataWithSchema(
		`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`,
		`{"name":"alice"}`,
	)
	require.NoError(t, err)

	out, err := s.Serialize("my-json-topic", wrapper)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(out), gsrcore.WireFormatHeaderSize)
	require.Equal(t, byte(gsrcore.WireFormatVersionByte), out[0])
	require.Equal(t, byte(gsrcore.CompressionByteNone), out[1])
}

// TestSerialize_RoutesSchemaNameThroughStrategy is the Phase 2 acceptance
// for replacing the hard-coded "topic + \"-value\"" naming with an injected
// SchemaNameStrategy. The fake captures the SchemaName the orchestrator
// hands to core and the test asserts it matches the strategy.
func TestSerialize_RoutesSchemaNameThroughStrategy(t *testing.T) {
	fake := &fakeGlueClient{}
	var capturedSchemaName string
	id := knownAvroVersionID
	fake.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			in := args.Get(1).(*glue.GetSchemaByDefinitionInput)
			capturedSchemaName = aws.ToString(in.SchemaId.SchemaName)
		}).
		Return(&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &id,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)

	enc := newCoreEncoder(t, fake)

	// Inject a non-default strategy and assert it lands in the Glue call.
	strategy := suffixStrategy{suffix: "-custom"}
	s, err := NewSerializerWithEncoderAndStrategy(avroConfig(), enc, strategy)
	require.NoError(t, err)

	record, _ := avroTestRecord()
	_, err = s.Serialize("orders", record)
	require.NoError(t, err)
	require.Equal(t, "orders-custom", capturedSchemaName,
		"SchemaName must flow through injected SchemaNameStrategy")
}

// TestSerialize_DefaultStrategy_PassesTransportNameVerbatim locks in the
// post-Phase-2 default: DefaultSchemaNameStrategy returns the transport
// name unchanged. The pre-Phase-2 code hard-coded `topic+"-value"`; this
// test would fail against that.
func TestSerialize_DefaultStrategy_PassesTransportNameVerbatim(t *testing.T) {
	fake := &fakeGlueClient{}
	var capturedSchemaName string
	id := knownAvroVersionID
	fake.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			in := args.Get(1).(*glue.GetSchemaByDefinitionInput)
			capturedSchemaName = aws.ToString(in.SchemaId.SchemaName)
		}).
		Return(&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &id,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil)

	enc := newCoreEncoder(t, fake)
	s, err := NewSerializerWithEncoder(avroConfig(), enc) // default strategy
	require.NoError(t, err)

	record, _ := avroTestRecord()
	_, err = s.Serialize("orders", record)
	require.NoError(t, err)
	require.Equal(t, "orders", capturedSchemaName,
		"default strategy must return transport name verbatim, NOT 'topic-value'")
	require.False(t, strings.HasSuffix(capturedSchemaName, "-value"),
		"Phase 2 removed the hard-coded '-value' suffix")
}

// TestSerialize_NilData_ReturnsNil locks in the legacy behavior that nil
// payload is a no-op (mirrors Java's Optional/null-tolerant input).
func TestSerialize_NilData_ReturnsNil(t *testing.T) {
	fake := &fakeGlueClient{}
	enc := newCoreEncoder(t, fake)
	s, err := NewSerializerWithEncoder(avroConfig(), enc)
	require.NoError(t, err)

	out, err := s.Serialize("any-topic", nil)
	require.NoError(t, err)
	require.Nil(t, out)
}

// TestSerialize_ClosedSerializer_ReturnsError locks in that Close() is
// terminal.
func TestSerialize_ClosedSerializer_ReturnsError(t *testing.T) {
	fake := &fakeGlueClient{}
	enc := newCoreEncoder(t, fake)
	s, err := NewSerializerWithEncoder(avroConfig(), enc)
	require.NoError(t, err)

	require.NoError(t, s.Close())

	record, _ := avroTestRecord()
	_, err = s.Serialize("any-topic", record)
	require.Error(t, err)
}

// suffixStrategy is a tiny SchemaNameStrategy implementation that lets the
// strategy-injection tests prove the strategy field actually wires through.
type suffixStrategy struct{ suffix string }

func (s suffixStrategy) SchemaName(transportName string) string {
	return transportName + s.suffix
}
func (s suffixStrategy) SchemaNameForData(transportName string, _ []byte) string {
	return s.SchemaName(transportName)
}
func (s suffixStrategy) SchemaNameForKey(transportName string, _ []byte, _ bool) string {
	return s.SchemaName(transportName)
}
