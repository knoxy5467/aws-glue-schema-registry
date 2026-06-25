// Phase 6.3 — Orchestrator-level parallel deserialization benchmarks.
// Wraps BenchmarkDeserializerDeserialize's warm path in b.RunParallel to
// measure concurrent throughput. Comparable to Java's OrchestratorBench
// @Threads(4) and @Threads(16).

package deserializer

import (
	"fmt"
	"testing"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
)

// BenchmarkDeserializerDeserialize_Parallel exercises Deserializer.Deserialize
// concurrently. Only warm-cache (the decoder's schema cache is pre-primed).
func BenchmarkDeserializerDeserialize_Parallel(b *testing.B) {
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
		for _, sz := range benchDesPayloadSizes {
			payload := benchDesPayloadBytes(sz.size)
			wire, schemaDef, schemaName := buildSerializedPayload(b, format, "NONE", payload)

			for _, parallelism := range []int{4, 16} {
				b.Run(fmt.Sprintf("%s/%s/P%d", format, sz.name, parallelism), func(b *testing.B) {
					d := newBenchDeserializer(b, dataFormat, schemaDef, schemaName)
					primeDeserializerCache(d, format, schemaDef, schemaName)
					b.SetBytes(int64(len(payload)))
					b.SetParallelism(parallelism)
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if _, err := d.Deserialize("perf-topic", wire); err != nil {
								b.Errorf("Deserialize: %v", err)
								return
							}
						}
					})
				})
			}
		}
	}
}

