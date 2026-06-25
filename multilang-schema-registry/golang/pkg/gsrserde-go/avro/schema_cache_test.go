// Copyright 2020 Amazon.com, Inc. or its affiliates.
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package avro

import (
	"testing"

	hambaavro "github.com/hamba/avro/v2"
)

const cacheTestSchema = `{
  "type": "record",
  "name": "Person",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age",  "type": "int"}
  ]
}`

// TestParseSchemaCached_HitReturnsIdenticalPointer asserts that two
// successive parses of the same schema text return the same hambaavro.Schema
// value (identity check, not equality). This is the contract the
// serializer/deserializer rely on for the speedup — if the cache returned a
// fresh parse on every call, the speedup would disappear.
func TestParseSchemaCached_HitReturnsIdenticalPointer(t *testing.T) {
	schemaCacheClearForTest()

	first, err := ParseSchemaCached(cacheTestSchema)
	if err != nil {
		t.Fatalf("first parse failed: %v", err)
	}
	second, err := ParseSchemaCached(cacheTestSchema)
	if err != nil {
		t.Fatalf("second parse failed: %v", err)
	}

	// hambaavro.Schema is an interface; comparing values compares the underlying
	// pointer for pointer-typed implementations, which is what we want.
	if first != second {
		t.Errorf("cache miss: expected identical schema value on second call, got distinct values (%p vs %p)", first, second)
	}
}

// TestParseSchemaCached_DifferentTextDifferentEntries asserts the cache keys
// by raw text — semantically-equivalent schemas with different whitespace
// produce different cache entries. This is the documented trade-off.
func TestParseSchemaCached_DifferentTextDifferentEntries(t *testing.T) {
	schemaCacheClearForTest()

	compact := `{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`
	pretty := `{
  "type": "record",
  "name": "R",
  "fields": [
    {"name": "f", "type": "int"}
  ]
}`

	a, err := ParseSchemaCached(compact)
	if err != nil {
		t.Fatalf("compact parse failed: %v", err)
	}
	b, err := ParseSchemaCached(pretty)
	if err != nil {
		t.Fatalf("pretty parse failed: %v", err)
	}

	// Both should succeed and both should produce semantically-equivalent
	// schemas, but they are distinct cache entries.
	if a == b {
		t.Errorf("expected distinct cache entries for different schema text, got identical")
	}
}

// TestParseSchemaCached_MalformedSchemaReturnsError asserts malformed input
// surfaces the hamba/avro parse error rather than caching nil.
func TestParseSchemaCached_MalformedSchemaReturnsError(t *testing.T) {
	schemaCacheClearForTest()

	_, err := ParseSchemaCached(`{"type":"recordWithTypo","name":"X"}`)
	if err == nil {
		t.Fatalf("expected parse error for malformed schema, got nil")
	}
}

// BenchmarkParseSchemaCached measures the cached-parse cost. The first
// iteration parses cold; subsequent iterations are cache hits and should be
// dominated by sync.Map.Load, not by Avro schema parsing.
func BenchmarkParseSchemaCached(b *testing.B) {
	schemaCacheClearForTest()
	// Warm the cache so the b.N loop measures hit cost.
	if _, err := ParseSchemaCached(cacheTestSchema); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ParseSchemaCached(cacheTestSchema); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseSchemaUncached is the baseline — calling hambaavro.Parse
// directly on every iteration. The ratio with BenchmarkParseSchemaCached
// shows the cache win.
func BenchmarkParseSchemaUncached(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := hambaavro.Parse(cacheTestSchema); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseSchemaCachedParallel exercises the concurrent-Load path.
// The sync.Map design relies on this being a near-lock-free fast path; if a
// future change regresses concurrent throughput, this bench surfaces it.
func BenchmarkParseSchemaCachedParallel(b *testing.B) {
	schemaCacheClearForTest()
	if _, err := ParseSchemaCached(cacheTestSchema); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := ParseSchemaCached(cacheTestSchema); err != nil {
				b.Fatal(err)
			}
		}
	})
}
