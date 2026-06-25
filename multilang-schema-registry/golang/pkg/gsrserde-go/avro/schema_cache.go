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
	"sync"

	hambaavro "github.com/hamba/avro/v2"
)

// schemaParseCache memoizes hambaavro.Parse results keyed by the raw schema
// text. Parsing an Avro schema runs a recursive descent over the JSON
// definition; for a typical 1 KB schema this is the dominant cost of a
// small-payload Serialize/Deserialize call. The parsed hambaavro.Schema is
// immutable and safe to share across goroutines, so a single shared cache
// suffices.
//
// Cache key trade-off (deliberate):
//
// We key on the raw schema TEXT, not the canonical form. Two byte-identical
// schemas hit the cache; a customer who normalizes whitespace differently
// between calls pays the parse cost once per normalization. Keying on the
// canonical form would dedup those, but computing the canonical form
// requires a parse — which is exactly what we're trying to avoid. The vast
// majority of real workloads register a schema once and re-use the same
// schema string, so raw-text keying captures the win without the overhead.
//
// Eviction: sync.Map grows unbounded. In practice a single Avro client
// touches a handful of schemas over its lifetime (one per Glue schema-name
// the application uses); growth is bounded by the client's deployment, not
// by traffic. A bounded LRU would be a future enhancement if a workload
// rotates through thousands of distinct schemas, but for v1 the simplicity
// of sync.Map outweighs the marginal memory savings.
//
// Concurrent safety: sync.Map is safe for concurrent Load/Store/LoadOrStore
// from multiple goroutines, which is the access pattern under benchmarks
// (b.RunParallel) and real-world concurrent serializers/deserializers.
var schemaParseCache sync.Map // map[string]hambaavro.Schema

// ParseSchemaCached parses an Avro schema definition, returning a cached
// result for previously-seen schema text. The returned hambaavro.Schema is
// the same value across calls for the same input text — callers must not
// mutate it (hambaavro.Schema is a read-only interface in practice, so this
// is enforced by the type system).
//
// Errors from hambaavro.Parse are NOT cached; a malformed schema is
// re-parsed every call. This is deliberate: a transient parse failure is
// uncommon, and caching errors risks a poison-pill scenario if hamba/avro
// behavior changes across versions.
func ParseSchemaCached(schemaText string) (hambaavro.Schema, error) {
	if cached, ok := schemaParseCache.Load(schemaText); ok {
		return cached.(hambaavro.Schema), nil
	}
	parsed, err := hambaavro.Parse(schemaText)
	if err != nil {
		return nil, err
	}
	// LoadOrStore handles the race where two goroutines parse the same
	// schema concurrently — one's value wins, both return the winner.
	actual, _ := schemaParseCache.LoadOrStore(schemaText, parsed)
	return actual.(hambaavro.Schema), nil
}

// schemaCacheClearForTest is exposed via avro_schema_cache_test.go for tests
// that need to verify cache behavior with a clean slate. It is NOT part of
// the public API and intentionally lower-cased.
func schemaCacheClearForTest() {
	schemaParseCache.Range(func(key, _ interface{}) bool {
		schemaParseCache.Delete(key)
		return true
	})
}
