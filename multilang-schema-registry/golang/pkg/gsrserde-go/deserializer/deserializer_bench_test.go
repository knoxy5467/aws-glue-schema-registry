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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	hambaavro "github.com/hamba/avro/v2"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/test_helpers"
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

// benchDesPayloadBytes delegates to the shared test_helpers fixture —
// printable-ASCII, so json.Marshal stays lossless when the JSON bench
// embeds the payload in a string field (Phase 6.1 review finding 4).
func benchDesPayloadBytes(size int) []byte {
	return test_helpers.PerfPayload(size)
}

// newBenchDeserializer builds a *Deserializer wired with a fake Glue client
// primed to return the bench's seed schema for any GetSchemaVersion call.
// Reuses the package-local fakeGlueClient from gsr_deserializer_core_test.go
// rather than declaring a benchmark-local duplicate (Phase 6.1 review
// finding 8). The unit-test fake stubs only GetSchemaVersion via
// testify/mock — every other method short-circuits to (nil, nil), which
// is fine here because the bench never invokes them.
//
// DANGER (Phase 6.2 review finding 6): if a future regression bypasses
// PrimeSchemaCache and the decoder takes the slow path, it will call
// GetSchemaByDefinition (or another method that returns nil, nil from
// the unit-test fake) and proceed with a zero-value schema. The bench
// will then report successful but meaningless throughput. If you change
// the priming flow, switch this fake to one whose un-stubbed methods
// b.Fatalf instead of returning nil.
func newBenchDeserializer(b *testing.B, format common.DataFormat, schemaDefinition, schemaName string) *Deserializer {
	b.Helper()

	fake := &fakeGlueClient{}
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
		cfgMap[common.ProtobufMessageDescriptorKey] = test_helpers.PerfPayloadDescriptor().Messages().ByName("Payload")
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
//
// Schemas come from test_helpers (Phase 6.1 review finding 9 dedup); the
// AVRO schema uses a `string` field rather than `bytes` so payload
// (printable ASCII) round-trips losslessly.
func buildSerializedPayload(b *testing.B, format string, compressionType string, payload []byte) (wire []byte, schemaDefinition, schemaName string) {
	b.Helper()
	switch format {
	case "AVRO":
		schemaName = "perf-topic"
		schemaDefinition = test_helpers.PerfAvroSchema
		parsed, err := hambaavro.Parse(schemaDefinition)
		if err != nil {
			b.Fatalf("parse avro schema: %v", err)
		}
		body, err := hambaavro.Marshal(parsed, map[string]any{"blob": string(payload)})
		if err != nil {
			b.Fatalf("marshal avro: %v", err)
		}
		wire = mustEncodeWire(b, body, compressionType)
	case "JSON":
		schemaName = "perf-topic"
		schemaDefinition = test_helpers.PerfJSONSchema
		encoded, err := json.Marshal(map[string]string{"blob": string(payload)})
		if err != nil {
			b.Fatalf("marshal json: %v", err)
		}
		wire = mustEncodeWire(b, encoded, compressionType)
	case "PROTOBUF":
		schemaName = test_helpers.PerfPayloadMessageName
		// The decoder doesn't re-parse this; it's stashed in the cache
		// so the bench skips GetSchemaVersion. The format adapter
		// dispatches via the injected ProtobufMessageDescriptor.
		schemaDefinition = test_helpers.PerfProtoTextSchema
		fd := test_helpers.PerfPayloadDescriptor()
		md := fd.Messages().ByName("Payload")
		msg := dynamicpb.NewMessage(md)
		msg.Set(md.Fields().ByName("blob"), protoreflect.ValueOfBytes(payload))
		body, err := proto.Marshal(msg)
		if err != nil {
			b.Fatalf("proto.Marshal: %v", err)
		}
		// PROTOBUF gets the message-index varint prefix BEFORE
		// compression. For perf.Payload, sole top-level message → index 0
		// → single byte 0x00.
		body = append([]byte{0x00}, body...)
		wire = mustEncodeWire(b, body, compressionType)
	default:
		b.Fatalf("unsupported format %s", format)
	}
	return wire, schemaDefinition, schemaName
}

// mustEncodeWire compresses body (if ZLIB) and wraps the result in the
// 18-byte GSR header. Phase 6.1 review finding 12 trimmed the previous
// signature (had schemaDefinition / format / schemaName that the body
// never read).
func mustEncodeWire(b *testing.B, body []byte, compressionType string) []byte {
	b.Helper()
	compressionByte := gsrcore.CompressionByteNone
	out := body
	if compressionType == "ZLIB" {
		compressed, err := gsrcore.ZlibCompressionHandler{}.Compress(body)
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
	return wire
}

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
