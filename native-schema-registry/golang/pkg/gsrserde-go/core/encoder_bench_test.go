// Phase 6 — Core encoder benchmarks. Pure CPU-bound wire-format + compression
// + protobuf message-index work; the Glue client is the fake from
// mock_glue_client_test.go primed to short-circuit GetSchemaByDefinition so
// the bench measures encode cost, not RPC cost.
//
// Plan §7 Phase 6 cells exercised:
//   format     ∈ {AVRO, JSON, PROTOBUF}
//   payload    ∈ {small ~100B, medium ~10KB, large ~1MB}
//   compress   ∈ {NONE, ZLIB}
//   cacheState ∈ {warm, cold}
//
// Cold-cache means a fresh *GsrEncoder per b.N iteration so the
// GetSchemaByDefinition fast path fires. Warm-cache means the version-id
// cache is pre-seeded via PrimeEncoderCache so the iteration measures only
// the encode pipeline.
//
// Throughput is reported in MB/s via b.SetBytes(len(payload)). The "payload"
// metric is the pre-compression source bytes, NOT the post-compression wire
// bytes — comparing the two columns across compression=NONE vs ZLIB shows
// the compression overhead directly.

package gsrserde

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
)

const (
	benchSchemaVersionID = "11111111-1111-1111-1111-111111111111"
	benchRegistryName    = "perf-bench-registry"
)

// benchPayloadSizes is the (label, bytes) matrix from the user prompt. The
// "large" 1 MiB bucket is in the same order of magnitude as a Kafka max-message
// default (1 MB), so the numbers reflect a realistic tail.
var benchPayloadSizes = []struct {
	name string
	size int
}{
	{"small_100B", 100},
	{"medium_10KB", 10 * 1024},
	{"large_1MB", 1 * 1024 * 1024},
}

var benchCompressionModes = []struct {
	name string
	typ  string
}{
	{"NONE", "NONE"},
	{"ZLIB", "ZLIB"},
}

// benchProtoSchema is a single-message proto2 schema usable by
// prefixMessageIndexToBytes. The simplest possible schema keeps the
// message-index varint at a single byte (index 0) so the benchmark
// measures the bookkeeping cost, not protoparse cost — parseSchemaDefinition
// is the same per iteration and lifts the protoparse cost out of the
// inner loop via the version-id cache.
const benchProtoSchema = `syntax = "proto3";
package perf;
message Payload { bytes blob = 1; }`

const benchAvroSchema = `{"type":"record","name":"Payload","namespace":"perf","fields":[{"name":"blob","type":"bytes"}]}`

const benchJSONSchema = `{"type":"object","properties":{"blob":{"type":"string"}},"required":["blob"]}`

// benchPayload allocates a deterministic-but-non-zero payload of the
// requested size. zlib on all-zeros would over-state compression gains.
func benchPayload(size int) []byte {
	out := make([]byte, size)
	if _, err := rand.Read(out); err != nil {
		panic(fmt.Errorf("rand.Read: %w", err))
	}
	return out
}

// benchSchemaFor returns the format-specific schema definition the bench
// will store in *Schema. AVRO and JSON tolerate any string, but PROTOBUF
// must parse via protoparse — getMessageIndexFromProtoDefinition fails
// otherwise.
func benchSchemaFor(format string) string {
	switch format {
	case "PROTOBUF":
		return benchProtoSchema
	case "AVRO":
		return benchAvroSchema
	case "JSON":
		return benchJSONSchema
	default:
		return ""
	}
}

// benchSchemaName returns the protobuf message-type name the encoder will
// resolve to an index. Only used when format=PROTOBUF; AVRO/JSON ignore it.
func benchSchemaName(format string) string {
	if format == "PROTOBUF" {
		return "perf.Payload"
	}
	return fmt.Sprintf("bench-%s-schema", format)
}

// newBenchEncoder builds a *GsrEncoder wired with a mock GlueClient. The
// mock is primed to return SchemaVersionId=benchSchemaVersionID for any
// GetSchemaByDefinition call so cold-cache iterations exercise the
// fast-path (cache miss → mock hit) and warm-cache iterations exercise
// the cached-path (no mock call).
//
// The encoder's mutex/singleflight/cache are all initialized; the only
// thing the bench skips is the AWS Config load.
func newBenchEncoder(b *testing.B, compressionType string) *GsrEncoder {
	b.Helper()
	mockClient := &MockGlueClient{}
	id := benchSchemaVersionID
	mockClient.On("GetSchemaByDefinition", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaByDefinitionOutput{
			SchemaVersionId: &id,
			Status:          types.SchemaVersionStatusAvailable,
		}, nil).Maybe()

	enc, err := NewGsrEncoderForTest(mockClient, GsrEncoderOptions{
		RegistryName:                  benchRegistryName,
		Compatibility:                 "BACKWARD",
		CompressionType:               compressionType,
		SchemaAutoRegistrationEnabled: true,
	})
	if err != nil {
		b.Fatalf("NewGsrEncoderForTest: %v", err)
	}
	return enc
}

// primeEncoderForBench seeds the encoder's cache with a Schema whose
// SchemaVersionID is benchSchemaVersionID for the (format, schemaName) pair
// the bench will encode under. After priming, Encode follows the cached
// fast path and never touches the mock Glue client.
func primeEncoderForBench(enc *GsrEncoder, format string) {
	schemaName := benchSchemaName(format)
	schema := &Schema{
		SchemaName:       schemaName,
		SchemaDefinition: benchSchemaFor(format),
		DataFormat:       format,
		SchemaVersionID:  benchSchemaVersionID,
		// AdditionalInfo carries the proto fully-qualified message name
		// for PROTOBUF; the encoder requires it to be non-empty when
		// DataFormat=PROTOBUF. AVRO/JSON ignore it.
		AdditionalInfo: benchSchemaName(format),
	}
	PrimeEncoderCache(enc, schemaName, format, schema)
}

// BenchmarkEncodeWireFormat exercises *GsrEncoder.Encode across the full
// (format × compression × payload × cacheState) matrix. Throughput is
// reported in MB/s.
//
// Each sub-benchmark name is of the form
//
//	BenchmarkEncodeWireFormat/<format>/<compression>/<size>/<cache>
//
// which is the canonical benchstat dimension layout for a Phase 5+ CI
// regression gate.
func BenchmarkEncodeWireFormat(b *testing.B) {
	for _, format := range []string{"AVRO", "JSON", "PROTOBUF"} {
		for _, comp := range benchCompressionModes {
			for _, sz := range benchPayloadSizes {
				payload := benchPayload(sz.size)
				schema := &Schema{
					SchemaName:       benchSchemaName(format),
					SchemaDefinition: benchSchemaFor(format),
					DataFormat:       format,
					SchemaVersionID:  benchSchemaVersionID,
					AdditionalInfo:   benchSchemaName(format),
				}

				b.Run(fmt.Sprintf("%s/%s/%s/warm", format, comp.name, sz.name), func(b *testing.B) {
					enc := newBenchEncoder(b, comp.typ)
					primeEncoderForBench(enc, format)
					b.SetBytes(int64(len(payload)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := enc.Encode(payload, "perf-topic", schema); err != nil {
							b.Fatalf("Encode: %v", err)
						}
					}
				})

				b.Run(fmt.Sprintf("%s/%s/%s/cold", format, comp.name, sz.name), func(b *testing.B) {
					// Cold means the schema cache is empty on every
					// iteration, so the encoder takes the
					// GetSchemaByDefinition fast path. The mock is
					// always-on, so the cost measured is
					// fetch-via-mock + cache-insert + encode.
					b.SetBytes(int64(len(payload)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						enc := newBenchEncoder(b, comp.typ)
						b.StartTimer()
						if _, err := enc.Encode(payload, "perf-topic", schema); err != nil {
							b.Fatalf("Encode: %v", err)
						}
					}
				})
			}
		}
	}
}

// BenchmarkEncodeWireFormatRaw exercises EncodeWireFormat — the pure
// header-construction path — without the surrounding orchestration
// (no compression, no schema lookup, no protobuf prefix). This is the
// minimal-cost floor that BenchmarkEncodeWireFormat is allowed to be
// slower than. Useful for benchstat regression triage: if both
// benchmarks slow down together, the regression is in EncodeWireFormat
// itself; if only the orchestrated bench slows down, blame the
// surrounding pipeline.
//
// Phase 6.1 review (finding 4) removed the ZLIB sub-benchmark from this
// floor bench: EncodeWireFormat does not compress, so a "ZLIB" sub-bench
// with pre-compressed input would measure header construction over a
// shorter byte slice while SetBytes() reported throughput against the
// uncompressed source — making ZLIB look spuriously fast. ZLIB cost is
// covered by BenchmarkEncodeWireFormat above, which exercises the full
// GsrEncoder.Encode path including compression.
func BenchmarkEncodeWireFormatRaw(b *testing.B) {
	for _, sz := range benchPayloadSizes {
		payload := benchPayload(sz.size)
		b.Run(sz.name, func(b *testing.B) {
			b.SetBytes(int64(len(payload)))
			for i := 0; i < b.N; i++ {
				if _, err := EncodeWireFormat(benchSchemaVersionID, CompressionByteNone, payload); err != nil {
					b.Fatalf("EncodeWireFormat: %v", err)
				}
			}
		})
	}
}

// BenchmarkSmoke_* is the Phase 6 deliverable 5 regression-guard set:
// three quick benchmarks (one per format, medium-only, no compression,
// warm cache). Today's commit is the baseline.
//
// TODO(phase 5+ CI): gate `bench-go` regressions ≥5% relative to baseline
// on these three benchmarks. The intent is that a single benchstat run
// against the committed baseline detects any encode regression bigger
// than benchmark noise. Lift to the full matrix once perf data
// stabilizes.
func BenchmarkSmoke_Avro_Encode(b *testing.B) {
	runSmokeEncode(b, "AVRO")
}

func BenchmarkSmoke_JSON_Encode(b *testing.B) {
	runSmokeEncode(b, "JSON")
}

func BenchmarkSmoke_Protobuf_Encode(b *testing.B) {
	runSmokeEncode(b, "PROTOBUF")
}

func runSmokeEncode(b *testing.B, format string) {
	payload := benchPayload(10 * 1024)
	schema := &Schema{
		SchemaName:       benchSchemaName(format),
		SchemaDefinition: benchSchemaFor(format),
		DataFormat:       format,
		SchemaVersionID:  benchSchemaVersionID,
		AdditionalInfo:   benchSchemaName(format),
	}
	enc := newBenchEncoder(b, "NONE")
	primeEncoderForBench(enc, format)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := enc.Encode(payload, "perf-topic", schema); err != nil {
			b.Fatalf("Encode: %v", err)
		}
	}
}

