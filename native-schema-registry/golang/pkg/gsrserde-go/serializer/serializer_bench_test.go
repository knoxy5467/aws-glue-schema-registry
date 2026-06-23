// Phase 6 — orchestrator-level encode benchmarks. Exercises
// *Serializer.Serialize end-to-end: the format-layer adapter
// (avro/json/protobuf) + the core encoder. Glue is mocked via the
// hand-rolled fakeGlueClient already used by gsr_serializer_core_test.go.
//
// Dimensions match the core benches (format × compression × payload × cache).
// b.SetBytes(len(payload)) where "payload" is the source bytes the caller
// hands to Serialize (Avro Go data → marshal-then-encode; JSON wrapper →
// already-JSON payload; protobuf message → marshal-then-encode), so the
// reported MB/s metric is throughput of *user data*, not wire bytes.

package serializer

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
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

// benchProtoFileDescriptor lazy-builds a FileDescriptor for a single-message
// schema (package=perf, message=Payload { bytes blob = 1 }). dynamicpb wraps
// any descriptor so the bench can produce a proto.Message without depending
// on an integration-tests-only generated .pb.go file.
//
// Built once per process; benchmarks share the descriptor via newBenchProtoMessage.
var benchProtoFileDescriptor protoreflect.FileDescriptor

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
	benchProtoFileDescriptor = fd
}

const benchSerVersionID = "11111111-1111-1111-1111-111111111111"

// Bench payload sizes mirror the core file. Repeated here rather than imported
// so the inner/outer module split stays honest (the outer module has no
// dependency on core's private bench helpers).
var benchSerPayloadSizes = []struct {
	name string
	size int
}{
	{"small_100B", 100},
	{"medium_10KB", 10 * 1024},
	{"large_1MB", 1 * 1024 * 1024},
}

var benchSerCompressionModes = []struct {
	name string
	typ  string
}{
	{"NONE", "NONE"},
	{"ZLIB", "ZLIB"},
}

func benchSerPayloadBytes(size int) []byte {
	out := make([]byte, size)
	if _, err := rand.Read(out); err != nil {
		panic(err)
	}
	return out
}

// avroDataForBench wraps payload bytes in an *avro.AvroRecord whose schema
// declares a single `bytes` field. hamba/avro will length-prefix and copy
// the bytes; the throughput number tracks the input size including the
// length prefix overhead.
func avroDataForBench(payload []byte) *avro.AvroRecord {
	schema := `{"type":"record","name":"PerfRecord","namespace":"perf","fields":[{"name":"blob","type":"bytes"}]}`
	return avro.NewAvroRecord(schema, map[string]any{"blob": payload})
}

// jsonDataForBench produces a JsonDataWithSchema wrapper carrying payload
// bytes as a base64-encoded string. Plain `bytes` isn't a JSON schema
// primitive, so the wrapper uses `type:string` and the caller is responsible
// for the encode. base64 inflates by ~33%; benchmarks normalize by the
// post-encode payload size for SetBytes so MB/s stays interpretable.
func jsonDataForBench(payload []byte) *gsrjson.JsonDataWithSchema {
	schema := `{"type":"object","properties":{"blob":{"type":"string"}},"required":["blob"]}`
	encoded, _ := json.Marshal(map[string]string{"blob": string(payload)})
	// Re-quote the bytes via json.Marshal so any non-printable bytes are
	// properly escaped; the resulting JSON is what the bench measures.
	wrapper, err := gsrjson.NewJsonDataWithSchema(schema, string(encoded))
	if err != nil {
		panic(err)
	}
	return wrapper
}

// protoDataForBench builds a dynamic perf.Payload carrying the payload in
// its single `blob` bytes field. dynamicpb gives us a proto.Message without
// dragging the integration-tests module into the outer module's deps.
func protoDataForBench(payload []byte) proto.Message {
	md := benchProtoFileDescriptor.Messages().ByName("Payload")
	if md == nil {
		panic("perf.Payload descriptor missing")
	}
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("blob"), protoreflect.ValueOfBytes(payload))
	return msg
}

// fixedSchemaNameStrategy returns the same schemaName for every call. The
// protobuf path needs schema.SchemaName == proto message full name (the
// encoder uses it to compute the message-index varint); the
// DefaultSchemaNameStrategy returns the transport name verbatim, which
// only works for protobuf when the test arranges topic == message-full-name.
// For Phase 6 benches we use the strategy seam to decouple topic from
// message-type name. This is a *bench* workaround — the underlying
// orchestrator-level protobuf-naming asymmetry is tracked elsewhere.
type fixedSchemaNameStrategy struct{ name string }

func (f fixedSchemaNameStrategy) SchemaName(string) string { return f.name }
func (f fixedSchemaNameStrategy) SchemaNameForData(string, []byte) string {
	return f.name
}
func (f fixedSchemaNameStrategy) SchemaNameForKey(string, []byte, bool) string {
	return f.name
}

// newBenchSerializer builds a *Serializer wired with a fake Glue client.
// The fake is primed to short-circuit GetSchemaByDefinition; cold-cache
// iterations hit the fake on the fast path, warm-cache iterations are
// pre-seeded via gsrcore.PrimeEncoderCache.
//
// schemaNameOverride is non-empty when the caller wants to bypass the
// DefaultSchemaNameStrategy — protobuf benches set it to the proto
// message full name (perf.Payload).
func newBenchSerializer(b *testing.B, format common.DataFormat, compressionType, schemaNameOverride string) *Serializer {
	b.Helper()
	fake := &fakeGlueClient{}
	id := benchSerVersionID
	fake.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &id,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil).Maybe()
	enc, err := gsrcore.NewGsrEncoderForTest(fake, gsrcore.GsrEncoderOptions{
		RegistryName:                  "perf-bench-registry",
		Compatibility:                 "BACKWARD",
		CompressionType:               compressionType,
		SchemaAutoRegistrationEnabled: true,
	})
	if err != nil {
		b.Fatalf("NewGsrEncoderForTest: %v", err)
	}
	cfg := common.NewConfiguration(map[string]any{
		common.DataFormatTypeKey: format,
	})
	var s *Serializer
	if schemaNameOverride != "" {
		s, err = NewSerializerWithEncoderAndStrategy(cfg, enc, fixedSchemaNameStrategy{name: schemaNameOverride})
	} else {
		s, err = NewSerializerWithEncoder(cfg, enc)
	}
	if err != nil {
		b.Fatalf("NewSerializerWithEncoder: %v", err)
	}
	return s
}

// primeSerializerCache seeds the underlying encoder's cache so warm-path
// Serialize calls skip GetSchemaByDefinition. We can't reach the encoder
// directly through the Serializer struct, so we walk the unexported
// coreEncoder field via the exported test seam.
//
// The cache key core uses is "<schemaName>:<dataFormat>" where schemaName
// is whatever the configured SchemaNameStrategy returns. Callers pass
// `cacheKeyName` matching the strategy output so PrimeEncoderCache writes
// under the same key the live path will read.
func primeSerializerCache(s *Serializer, cacheKeyName, format, definition string) {
	gsrcore.PrimeEncoderCache(s.coreEncoder, cacheKeyName, format, &gsrcore.Schema{
		SchemaName:       cacheKeyName,
		SchemaDefinition: definition,
		DataFormat:       format,
		SchemaVersionID:  benchSerVersionID,
	})
}

// BenchmarkSerializerSerialize exercises Serializer.Serialize across the
// format × compression × size × cacheState matrix. The format-layer
// marshal cost is in scope; that's the whole point of measuring at the
// orchestrator level rather than at the core wire-format level.
func BenchmarkSerializerSerialize(b *testing.B) {
	type formatEntry struct {
		name       string
		dataFormat common.DataFormat
		// dataFor returns the input the bench will pass to Serialize for
		// a given payload size, plus the schema definition that ends up
		// in the encoder cache for warm-path priming.
		dataFor func(payload []byte) (any, string)
		// schemaNameOverride is non-empty when the bench must inject a
		// non-default SchemaNameStrategy. Protobuf needs this — the
		// encoder uses Schema.SchemaName as the message-type name when
		// computing the message-index varint, which must be the proto
		// full name (`perf.Payload`), not the transport name.
		schemaNameOverride string
	}

	formats := []formatEntry{
		{
			name:       "AVRO",
			dataFormat: common.DataFormatAvro,
			dataFor: func(p []byte) (any, string) {
				r := avroDataForBench(p)
				return r, r.Schema
			},
		},
		{
			name:       "JSON",
			dataFormat: common.DataFormatJSON,
			dataFor: func(p []byte) (any, string) {
				w := jsonDataForBench(p)
				return w, w.GetSchema()
			},
		},
		{
			name:       "PROTOBUF",
			dataFormat: common.DataFormatProtobuf,
			dataFor: func(p []byte) (any, string) {
				m := protoDataForBench(p)
				return m, ""
			},
			schemaNameOverride: "perf.Payload",
		},
	}

	const topic = "perf-topic"

	for _, f := range formats {
		for _, comp := range benchSerCompressionModes {
			for _, sz := range benchSerPayloadSizes {
				payload := benchSerPayloadBytes(sz.size)
				data, schemaDef := f.dataFor(payload)

				// The encoder cache key is derived from
				// strategy.SchemaName(topic). For Avro/JSON that's the
				// topic; for protobuf the override puts the message-full-name
				// there.
				cacheKey := topic
				if f.schemaNameOverride != "" {
					cacheKey = f.schemaNameOverride
				}

				b.Run(fmt.Sprintf("%s/%s/%s/warm", f.name, comp.name, sz.name), func(b *testing.B) {
					s := newBenchSerializer(b, f.dataFormat, comp.typ, f.schemaNameOverride)
					if schemaDef != "" {
						primeSerializerCache(s, cacheKey, f.name, schemaDef)
					} else {
						// Protobuf: the schema definition isn't known
						// statically (it's derived from the dynamic
						// descriptor at Serialize time). Warm the cache
						// via a single throwaway Serialize.
						if _, err := s.Serialize(topic, data); err != nil {
							b.Fatalf("warm-up Serialize: %v", err)
						}
					}
					b.SetBytes(int64(len(payload)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := s.Serialize(topic, data); err != nil {
							b.Fatalf("Serialize: %v", err)
						}
					}
				})

				b.Run(fmt.Sprintf("%s/%s/%s/cold", f.name, comp.name, sz.name), func(b *testing.B) {
					b.SetBytes(int64(len(payload)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						s := newBenchSerializer(b, f.dataFormat, comp.typ, f.schemaNameOverride)
						b.StartTimer()
						if _, err := s.Serialize(topic, data); err != nil {
							b.Fatalf("Serialize: %v", err)
						}
					}
				})
			}
		}
	}
}
