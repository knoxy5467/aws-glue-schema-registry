// Phase 6.3 — Orchestrator-level parallel serialization benchmarks.
// Wraps BenchmarkSerializerSerialize's warm path in b.RunParallel to
// measure concurrent throughput. Comparable to Java's OrchestratorBench
// @Threads(4) and @Threads(16).

package serializer

import (
	"fmt"
	"testing"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// BenchmarkSerializerSerialize_Parallel exercises Serializer.Serialize
// concurrently using b.RunParallel. Only warm-cache path; cold-cache
// parallel benches don't make sense (each iteration creates a fresh
// serializer, which defeats sharing). Parallelism=4 and 16.
func BenchmarkSerializerSerialize_Parallel(b *testing.B) {
	type formatEntry struct {
		name               string
		dataFormat         common.DataFormat
		dataFor            func(payload []byte) (any, string)
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
			name:               "PROTOBUF",
			dataFormat:         common.DataFormatProtobuf,
			dataFor: func(p []byte) (any, string) {
				m := protoDataForBench(p)
				return m, ""
			},
			schemaNameOverride: "perf.Payload",
		},
	}

	const topic = "perf-topic"

	for _, f := range formats {
		for _, sz := range benchSerPayloadSizes {
			payload := benchSerPayloadBytes(sz.size)
			data, schemaDef := f.dataFor(payload)

			cacheKey := topic
			if f.schemaNameOverride != "" {
				cacheKey = f.schemaNameOverride
			}

			for _, parallelism := range []int{4, 16} {
				b.Run(fmt.Sprintf("%s/%s/P%d", f.name, sz.name, parallelism), func(b *testing.B) {
					s := newBenchSerializer(b, f.dataFormat, "NONE", f.schemaNameOverride)
					if schemaDef != "" {
						primeSerializerCache(s, cacheKey, f.name, schemaDef)
					} else {
						// Protobuf: warm via a throwaway call
						if _, err := s.Serialize(topic, data); err != nil {
							b.Fatalf("warm-up Serialize: %v", err)
						}
					}
					b.SetBytes(int64(len(payload)))
					b.SetParallelism(parallelism)
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if _, err := s.Serialize(topic, data); err != nil {
								b.Errorf("Serialize: %v", err)
								return
							}
						}
					})
				})
			}
		}
	}
}
