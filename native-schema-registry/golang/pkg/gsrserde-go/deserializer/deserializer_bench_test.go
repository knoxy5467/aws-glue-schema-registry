// Phase 6 — orchestrator-level decode benchmarks. Symmetric to the
// Serializer benches: drives *Deserializer.Deserialize across
// (format × compression × payload × cacheState).
//
// The wire-format bytes consumed by each iteration are produced once at
// b.Run setup time via a Serializer with the matching configuration so
// the bench measures decode cost end-to-end (header parse, decompress,
// strip protobuf message-index, format-layer unmarshal).

package deserializer

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	hambaavro "github.com/hamba/avro/v2"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	gsrjson "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/json"
)

const benchDesVersionID = "11111111-1111-1111-1111-111111111111"

var benchDesPayloadSizes = []struct {
	name string
	size int
}{
	{"small_100B", 100},
	{"medium_10KB", 10 * 1024},
	{"large_1MB", 1 * 1024 * 1024},
}

var benchDesCompressionModes = []struct {
	name string
	typ  string
}{
	{"NONE", "NONE"},
	{"ZLIB", "ZLIB"},
}

func benchDesPayloadBytes(size int) []byte {
	out := make([]byte, size)
	if _, err := rand.Read(out); err != nil {
		panic(err)
	}
	return out
}

// benchDesProtoFD builds the same single-message FileDescriptor the
// serializer bench uses (perf.Payload { bytes blob = 1 }).
var benchDesProtoFD protoreflect.FileDescriptor

func init() {
	fdProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("perf.proto"),
		Package: proto.String("perf"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Payload"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:     proto.String("blob"),
						Number:   proto.Int32(1),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
						JsonName: proto.String("blob"),
					},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdProto, nil)
	if err != nil {
		panic(fmt.Errorf("build proto file descriptor: %w", err))
	}
	benchDesProtoFD = fd
}

// benchFakeGlueClient mirrors the one in the serializer package — we duplicate
// rather than import to avoid coupling these benchmarks to the
// serializer package's _test.go files.
type benchFakeGlueClient struct{ mock.Mock }

func (f *benchFakeGlueClient) GetSchemaByDefinition(ctx context.Context, in *glue.GetSchemaByDefinitionInput, _ ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
	args := f.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaByDefinitionOutput), args.Error(1)
}
func (f *benchFakeGlueClient) GetSchemaVersion(ctx context.Context, in *glue.GetSchemaVersionInput, _ ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	args := f.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaVersionOutput), args.Error(1)
}
func (f *benchFakeGlueClient) CreateSchema(ctx context.Context, in *glue.CreateSchemaInput, _ ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
	args := f.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.CreateSchemaOutput), args.Error(1)
}
func (f *benchFakeGlueClient) RegisterSchemaVersion(ctx context.Context, in *glue.RegisterSchemaVersionInput, _ ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
	args := f.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.RegisterSchemaVersionOutput), args.Error(1)
}
func (f *benchFakeGlueClient) PutSchemaVersionMetadata(context.Context, *glue.PutSchemaVersionMetadataInput, ...func(*glue.Options)) (*glue.PutSchemaVersionMetadataOutput, error) {
	return nil, nil
}
func (f *benchFakeGlueClient) QuerySchemaVersionMetadata(context.Context, *glue.QuerySchemaVersionMetadataInput, ...func(*glue.Options)) (*glue.QuerySchemaVersionMetadataOutput, error) {
	return nil, nil
}
func (f *benchFakeGlueClient) GetTags(context.Context, *glue.GetTagsInput, ...func(*glue.Options)) (*glue.GetTagsOutput, error) {
	return nil, nil
}

// newBenchDeserializer builds a *Deserializer wired with a fake Glue client
// primed to return the bench's seed schema for any GetSchemaVersion call.
func newBenchDeserializer(b *testing.B, format common.DataFormat, schemaDefinition, schemaName string) *Deserializer {
	b.Helper()

	fake := &benchFakeGlueClient{}
	definition := schemaDefinition
	dataFormat := types.DataFormat(format.String())
	arn := "arn:aws:glue:us-east-2:000000000000:schema/default-registry/" + schemaName
	fake.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaDefinition: &definition,
			DataFormat:       dataFormat,
			SchemaArn:        &arn,
		}, nil).Maybe()

	dec, err := gsrcore.NewGsrDecoderForTest(fake, gsrcore.GsrDecoderOptions{
		RegistryName: "perf-bench-registry",
	})
	if err != nil {
		b.Fatalf("NewGsrDecoderForTest: %v", err)
	}

	cfgMap := map[string]any{common.DataFormatTypeKey: format}
	if format == common.DataFormatProtobuf {
		cfgMap[common.ProtobufMessageDescriptorKey] = benchDesProtoFD.Messages().ByName("Payload")
	}
	if format == common.DataFormatJSON {
		// JSON deserializer returns the validated payload as a string
		// wrapper; jsonObjectType is intentionally left unset (the
		// existing format adapter accepts that).
		_ = reflect.TypeOf(map[string]any{})
	}
	cfg := common.NewConfiguration(cfgMap)

	d, err := NewDeserializerWithDecoder(cfg, dec)
	if err != nil {
		b.Fatalf("NewDeserializerWithDecoder: %v", err)
	}
	return d
}

// primeDeserializerCache seeds the underlying decoder's version-id cache.
// Warm-path Deserialize calls then skip GetSchemaVersion.
func primeDeserializerCache(d *Deserializer, format, schemaDefinition, schemaName string) {
	gsrcore.PrimeSchemaCache(d.coreDecoder, benchDesVersionID, &gsrcore.Schema{
		SchemaName:       schemaName,
		SchemaDefinition: schemaDefinition,
		DataFormat:       format,
		SchemaVersionID:  benchDesVersionID,
	})
}

// buildSerializedPayload produces wire-formatted bytes the Deserializer
// will consume. We construct them by hand rather than via Serializer to
// avoid pulling the serializer package as a test dep — the wire-format
// constants live in core.
func buildSerializedPayload(b *testing.B, format string, compressionType string, payload []byte) (wire []byte, schemaDefinition, schemaName string) {
	b.Helper()
	switch format {
	case "AVRO":
		schemaName = "perf-topic"
		schemaDefinition = `{"type":"record","name":"PerfRecord","namespace":"perf","fields":[{"name":"blob","type":"bytes"}]}`
		body := mustMarshalAvro(b, schemaDefinition, payload)
		wire = mustEncodeWire(b, schemaDefinition, body, compressionType, "AVRO", schemaName)
	case "JSON":
		schemaName = "perf-topic"
		schemaDefinition = `{"type":"object","properties":{"blob":{"type":"string"}},"required":["blob"]}`
		encoded, _ := json.Marshal(map[string]string{"blob": string(payload)})
		wire = mustEncodeWire(b, schemaDefinition, encoded, compressionType, "JSON", schemaName)
	case "PROTOBUF":
		schemaName = "perf.Payload"
		// Build the canonical .proto text the encoder would write. We
		// don't actually need the text equality here, just a schema the
		// deserializer's cache can store (the decoder doesn't re-parse
		// it; the deserializer's protobuf format adapter uses the
		// pre-injected ProtobufMessageDescriptor).
		schemaDefinition = `syntax = "proto3"; package perf; message Payload { bytes blob = 1; }`
		msg := dynamicpb.NewMessage(benchDesProtoFD.Messages().ByName("Payload"))
		msg.Set(benchDesProtoFD.Messages().ByName("Payload").Fields().ByName("blob"), protoreflect.ValueOfBytes(payload))
		body, err := proto.Marshal(msg)
		if err != nil {
			b.Fatalf("proto.Marshal: %v", err)
		}
		// PROTOBUF gets the message-index varint prefix BEFORE
		// compression. For perf.Payload, sole top-level message → index 0
		// → single byte 0x00.
		body = append([]byte{0x00}, body...)
		wire = mustEncodeWire(b, schemaDefinition, body, compressionType, "PROTOBUF", schemaName)
	default:
		b.Fatalf("unsupported format %s", format)
	}
	return wire, schemaDefinition, schemaName
}

// mustMarshalAvro produces a hamba-avro-encoded payload using the bench schema.
func mustMarshalAvro(b *testing.B, schemaJSON string, blob []byte) []byte {
	b.Helper()
	// Use the format adapter to keep the wire bytes consistent with the
	// production encoder path; round-tripping via avroDataForBench would
	// pull the serializer pkg as a circular dep, so call hamba directly.
	rec := &avro.AvroRecord{Schema: schemaJSON, Data: map[string]any{"blob": blob}}
	body, err := marshalAvroForBench(rec)
	if err != nil {
		b.Fatalf("marshal avro: %v", err)
	}
	return body
}

// marshalAvroForBench is the minimal hamba-driven marshal we need for the
// bench. Mirrors what serializer/avro.AvroSerializer.Serialize does.
func marshalAvroForBench(record *avro.AvroRecord) ([]byte, error) {
	parsed, err := hambaavro.Parse(record.Schema)
	if err != nil {
		return nil, err
	}
	return hambaavro.Marshal(parsed, record.Data)
}

func mustEncodeWire(b *testing.B, schemaDefinition string, body []byte, compressionType, format, schemaName string) []byte {
	b.Helper()
	compressionByte := gsrcore.CompressionByteNone
	out := body
	if compressionType == "ZLIB" {
		zh := gsrcore.ZlibCompressionHandler{}
		compressed, err := zh.Compress(body)
		if err != nil {
			b.Fatalf("zlib compress: %v", err)
		}
		out = compressed
		compressionByte = gsrcore.CompressionByteZlib
	}
	wire, err := gsrcore.EncodeWireFormat(benchDesVersionID, compressionByte, out)
	if err != nil {
		b.Fatalf("EncodeWireFormat: %v", err)
	}
	// schemaDefinition / format / schemaName are used by the caller to
	// prime the deserializer cache so the decoder skips GetSchemaVersion.
	_ = schemaDefinition
	_ = format
	_ = schemaName
	return wire
}

// gsrjson is imported only to keep the deserializer package's compile graph
// honest — the package's format adapter sometimes returns
// *gsrjson.JsonDataWithSchema, and silencing the unused-import linter via
// an explicit reference reads more clearly than an `_ = gsrjson.X`.
var _ = gsrjson.NewJsonDataWithSchema

// BenchmarkDeserializerDeserialize exercises Deserializer.Deserialize across
// the format × compression × size × cacheState matrix.
func BenchmarkDeserializerDeserialize(b *testing.B) {
	for _, format := range []string{"AVRO", "JSON", "PROTOBUF"} {
		var dataFormat common.DataFormat
		switch format {
		case "AVRO":
			dataFormat = common.DataFormatAvro
		case "JSON":
			dataFormat = common.DataFormatJSON
		case "PROTOBUF":
			dataFormat = common.DataFormatProtobuf
		}
		for _, comp := range benchDesCompressionModes {
			for _, sz := range benchDesPayloadSizes {
				payload := benchDesPayloadBytes(sz.size)
				wire, schemaDef, schemaName := buildSerializedPayload(b, format, comp.typ, payload)

				b.Run(fmt.Sprintf("%s/%s/%s/warm", format, comp.name, sz.name), func(b *testing.B) {
					d := newBenchDeserializer(b, dataFormat, schemaDef, schemaName)
					primeDeserializerCache(d, format, schemaDef, schemaName)
					b.SetBytes(int64(len(payload)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := d.Deserialize("perf-topic", wire); err != nil {
							b.Fatalf("Deserialize: %v", err)
						}
					}
				})

				b.Run(fmt.Sprintf("%s/%s/%s/cold", format, comp.name, sz.name), func(b *testing.B) {
					b.SetBytes(int64(len(payload)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						d := newBenchDeserializer(b, dataFormat, schemaDef, schemaName)
						b.StartTimer()
						if _, err := d.Deserialize("perf-topic", wire); err != nil {
							b.Fatalf("Deserialize: %v", err)
						}
					}
				})
			}
		}
	}
}
