// Phase 6.3 — Cache hit/miss microbench + concurrent throughput benchmarks.
//
// BenchmarkEncoderCacheHit:  pre-populated cache → warm fast path.
// BenchmarkEncoderCacheMiss: fresh encoder per-iter → cold path (mock Glue call).
//
// BenchmarkEncodeWireFormat_Parallel: b.RunParallel wrapper over the warm
// encode path, measuring concurrent throughput scaling.
// BenchmarkDecodeWireFormat_Parallel: symmetric for decode.
//
// These supplement the sequential matrix in encoder_bench_test.go and
// decoder_bench_test.go.

package gsrserde

import (
	"fmt"
	"testing"
)

// ---- Cache hit / miss microbenchmarks ----

// BenchmarkEncoderCacheHit measures the warm-cache encode path. The encoder's
// schema cache is pre-populated so Encode never touches the mock Glue client.
// This isolates pure encode cost: header construction + compression +
// (PROTOBUF) message-index varint.
func BenchmarkEncoderCacheHit(b *testing.B) {
	for _, format := range []string{"AVRO", "JSON", "PROTOBUF"} {
		for _, sz := range benchPayloadSizes {
			payload := benchPayload(sz.size)
			schema := &Schema{
				SchemaName:       benchSchemaName(format),
				SchemaDefinition: benchSchemaFor(format),
				DataFormat:       format,
				SchemaVersionID:  benchSchemaVersionID,
				AdditionalInfo:   benchSchemaName(format),
			}

			b.Run(fmt.Sprintf("%s/%s", format, sz.name), func(b *testing.B) {
				enc := newBenchEncoder(b, "NONE")
				primeEncoderForBench(enc, format)
				b.SetBytes(int64(len(payload)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := enc.Encode(payload, "perf-topic", schema); err != nil {
						b.Fatalf("Encode: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkEncoderCacheMiss measures the cold-cache encode path. A fresh
// encoder is created per iteration (cache empty), so every Encode call
// takes the GetSchemaByDefinition → mock → cache-insert path. The mock
// returns instantly, so the measured delta vs CacheHit is the per-call
// overhead of cache-miss handling (mutex acquisition, singleflight,
// mock dispatch, cache write).
func BenchmarkEncoderCacheMiss(b *testing.B) {
	for _, format := range []string{"AVRO", "JSON", "PROTOBUF"} {
		for _, sz := range benchPayloadSizes {
			payload := benchPayload(sz.size)
			schema := &Schema{
				SchemaName:       benchSchemaName(format),
				SchemaDefinition: benchSchemaFor(format),
				DataFormat:       format,
				SchemaVersionID:  benchSchemaVersionID,
				AdditionalInfo:   benchSchemaName(format),
			}

			b.Run(fmt.Sprintf("%s/%s", format, sz.name), func(b *testing.B) {
				b.SetBytes(int64(len(payload)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					enc := newBenchEncoder(b, "NONE")
					b.StartTimer()
					if _, err := enc.Encode(payload, "perf-topic", schema); err != nil {
						b.Fatalf("Encode: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkDecoderCacheHit measures the warm-cache decode path.
func BenchmarkDecoderCacheHit(b *testing.B) {
	for _, format := range []string{"AVRO", "JSON", "PROTOBUF"} {
		for _, sz := range benchPayloadSizes {
			payload := benchPayload(sz.size)
			wire := buildWireFormatPayload(b, format, "NONE", payload)

			b.Run(fmt.Sprintf("%s/%s", format, sz.name), func(b *testing.B) {
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
		}
	}
}

// BenchmarkDecoderCacheMiss measures the cold-cache decode path.
func BenchmarkDecoderCacheMiss(b *testing.B) {
	for _, format := range []string{"AVRO", "JSON", "PROTOBUF"} {
		for _, sz := range benchPayloadSizes {
			payload := benchPayload(sz.size)
			wire := buildWireFormatPayload(b, format, "NONE", payload)

			b.Run(fmt.Sprintf("%s/%s", format, sz.name), func(b *testing.B) {
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

// ---- Concurrent throughput benchmarks (RunParallel) ----

// BenchmarkEncodeWireFormat_Parallel exercises the warm-cache encode path
// concurrently via b.RunParallel. The encoder is shared across goroutines —
// this measures the real concurrent throughput including mutex contention
// on the schema cache. Comparable to Java's OrchestratorBench @Threads(N).
func BenchmarkEncodeWireFormat_Parallel(b *testing.B) {
	for _, format := range []string{"AVRO", "JSON", "PROTOBUF"} {
		for _, sz := range benchPayloadSizes {
			payload := benchPayload(sz.size)
			schema := &Schema{
				SchemaName:       benchSchemaName(format),
				SchemaDefinition: benchSchemaFor(format),
				DataFormat:       format,
				SchemaVersionID:  benchSchemaVersionID,
				AdditionalInfo:   benchSchemaName(format),
			}

			for _, parallelism := range []int{4, 16} {
				b.Run(fmt.Sprintf("%s/%s/P%d", format, sz.name, parallelism), func(b *testing.B) {
					enc := newBenchEncoder(b, "NONE")
					primeEncoderForBench(enc, format)
					b.SetBytes(int64(len(payload)))
					b.SetParallelism(parallelism)
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if _, err := enc.Encode(payload, "perf-topic", schema); err != nil {
								b.Errorf("Encode: %v", err)
								return
							}
						}
					})
				})
			}
		}
	}
}

// BenchmarkDecodeWireFormat_Parallel exercises the warm-cache decode path
// concurrently. The decoder is shared across goroutines.
func BenchmarkDecodeWireFormat_Parallel(b *testing.B) {
	for _, format := range []string{"AVRO", "JSON", "PROTOBUF"} {
		for _, sz := range benchPayloadSizes {
			payload := benchPayload(sz.size)
			wire := buildWireFormatPayload(b, format, "NONE", payload)

			for _, parallelism := range []int{4, 16} {
				b.Run(fmt.Sprintf("%s/%s/P%d", format, sz.name, parallelism), func(b *testing.B) {
					dec := newBenchDecoder(b, format)
					primeDecoderForBench(dec, format)
					b.SetBytes(int64(len(payload)))
					b.SetParallelism(parallelism)
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if _, err := dec.Decode(wire); err != nil {
								b.Errorf("Decode: %v", err)
								return
							}
						}
					})
				})
			}
		}
	}
}

