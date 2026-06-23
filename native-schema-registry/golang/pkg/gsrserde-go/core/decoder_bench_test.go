// Phase 6 — Core decoder benchmarks. Symmetric to encoder_bench_test.go;
// drives *GsrDecoder.Decode across the same matrix.
//
// Cold-cache means the version-id → Schema cache is empty so the decoder
// dispatches GetSchemaVersion via the mock; warm-cache means PrimeSchemaCache
// was called so the iteration measures only wire-format parse + decompress +
// (PROTOBUF) strip-message-index.

package gsrserde

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
)

// newBenchDecoder builds a *GsrDecoder wired with a mock GlueClient primed to
// return the same Schema the encoder side wrote. The mock is marked .Maybe()
// because warm-cache iterations never invoke it.
func newBenchDecoder(b *testing.B, format string) *GsrDecoder {
	b.Helper()
	mockClient := &MockGlueClient{}
	definition := benchSchemaFor(format)
	dataFormat := types.DataFormat(format)
	arn := "arn:aws:glue:us-east-2:000000000000:schema/default-registry/" + benchSchemaName(format)
	mockClient.On("GetSchemaVersion", mock.Anything, mock.Anything).
		Return(&glue.GetSchemaVersionOutput{
			SchemaDefinition: &definition,
			DataFormat:       dataFormat,
			SchemaArn:        &arn,
		}, nil).Maybe()

	dec, err := NewGsrDecoderForTest(mockClient, GsrDecoderOptions{
		RegistryName: benchRegistryName,
	})
	if err != nil {
		b.Fatalf("NewGsrDecoderForTest: %v", err)
	}
	return dec
}

// primeDecoderForBench seeds the decoder's version-id cache so warm
// iterations skip GetSchemaVersion.
func primeDecoderForBench(dec *GsrDecoder, format string) {
	PrimeSchemaCache(dec, benchSchemaVersionID, &Schema{
		SchemaName:       benchSchemaName(format),
		SchemaDefinition: benchSchemaFor(format),
		DataFormat:       format,
		SchemaVersionID:  benchSchemaVersionID,
	})
}

// buildWireFormatPayload builds the byte stream the decoder will consume.
// Mirrors what *GsrEncoder.Encode would produce for the same inputs — the
// bench can't reuse the encoder directly because the warm-cache decoder
// path must measure decode-only cost, so we pre-construct the wire bytes
// once per sub-benchmark.
func buildWireFormatPayload(b *testing.B, format, compressionType string, payload []byte) []byte {
	b.Helper()
	// PROTOBUF gets the message-index varint prefix BEFORE compression.
	body := payload
	if format == "PROTOBUF" {
		prefixed, err := prefixMessageIndexToBytes(body, benchSchemaFor(format), benchSchemaName(format))
		if err != nil {
			b.Fatalf("prefixMessageIndexToBytes: %v", err)
		}
		body = prefixed
	}

	compressionByte := CompressionByteNone
	if compressionType == "ZLIB" {
		compressed, err := ZlibCompressionHandler{}.Compress(body)
		if err != nil {
			b.Fatalf("zlib compress: %v", err)
		}
		body = compressed
		compressionByte = CompressionByteZlib
	}

	wire, err := EncodeWireFormat(benchSchemaVersionID, compressionByte, body)
	if err != nil {
		b.Fatalf("EncodeWireFormat: %v", err)
	}
	return wire
}

// BenchmarkDecodeWireFormat is the symmetric matrix to
// BenchmarkEncodeWireFormat. b.SetBytes(len(payload)) reports throughput
// on the SOURCE payload size — comparing ZLIB vs NONE here shows the
// decompression overhead per byte of decoded data.
func BenchmarkDecodeWireFormat(b *testing.B) {
	for _, format := range []string{"AVRO", "JSON", "PROTOBUF"} {
		for _, comp := range benchCompressionModes {
			for _, sz := range benchPayloadSizes {
				payload := benchPayload(sz.size)
				wire := buildWireFormatPayload(b, format, comp.typ, payload)

				b.Run(fmt.Sprintf("%s/%s/%s/warm", format, comp.name, sz.name), func(b *testing.B) {
					dec := newBenchDecoder(b, format)
					primeDecoderForBench(dec, format)
					b.SetBytes(int64(len(payload)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := dec.Decode(wire); err != nil {
							b.Fatalf("Decode: %v", err)
						}
					}
				})

				b.Run(fmt.Sprintf("%s/%s/%s/cold", format, comp.name, sz.name), func(b *testing.B) {
					b.SetBytes(int64(len(payload)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						dec := newBenchDecoder(b, format)
						b.StartTimer()
						if _, err := dec.Decode(wire); err != nil {
							b.Fatalf("Decode: %v", err)
						}
					}
				})
			}
		}
	}
}

// BenchmarkDecodeWireFormatRaw measures only DecodeWireFormat (header parse).
// Equivalent floor to BenchmarkEncodeWireFormatRaw on the decode side.
func BenchmarkDecodeWireFormatRaw(b *testing.B) {
	for _, sz := range benchPayloadSizes {
		payload := benchPayload(sz.size)
		wire, err := EncodeWireFormat(benchSchemaVersionID, CompressionByteNone, payload)
		if err != nil {
			b.Fatalf("setup encode: %v", err)
		}
		b.Run(fmt.Sprintf("NONE/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(len(payload)))
			for i := 0; i < b.N; i++ {
				if _, _, _, err := DecodeWireFormat(wire); err != nil {
					b.Fatalf("DecodeWireFormat: %v", err)
				}
			}
		})
	}
}

// Smoke decode benchmarks: regression-guard trio symmetric to
// BenchmarkSmoke_*_Encode.
func BenchmarkSmoke_Avro_Decode(b *testing.B) {
	runSmokeDecode(b, "AVRO")
}

func BenchmarkSmoke_JSON_Decode(b *testing.B) {
	runSmokeDecode(b, "JSON")
}

func BenchmarkSmoke_Protobuf_Decode(b *testing.B) {
	runSmokeDecode(b, "PROTOBUF")
}

func runSmokeDecode(b *testing.B, format string) {
	payload := benchPayload(10 * 1024)
	wire := buildWireFormatPayload(b, format, "NONE", payload)
	dec := newBenchDecoder(b, format)
	primeDecoderForBench(dec, format)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := dec.Decode(wire); err != nil {
			b.Fatalf("Decode: %v", err)
		}
	}
}
