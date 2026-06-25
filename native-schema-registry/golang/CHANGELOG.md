# Go Client Changelog

Changes to the GSR Go client live here. The top-level repository
`CHANGELOG.md` tracks Java releases.

## Unreleased

<<<<<<< HEAD
### Phase 4.16 fixture coverage

Added shared multilang fixture test coverage for the Go GSR client, organized
in three layers:

- **Layer A** (`pkg/gsrserde-go/fixtures_test.go`): parse sweep of all 33
  top-level .proto files via `bufbuild/protocompile` and all 23 .avsc files via
  `hamba/avro/v2`. Asserts round-trip structural equality for proto descriptors
  and fingerprint stability for Avro schemas. Skips
  `TestOrderingSyntax3Options.proto` (Java parity gap — see entry below on
  deprecated `*_generic_services` options).

- **Layer B** (`integration-tests/tests/fixture_avro_evolution_test.go`):
  real-AWS integration test registering all .avsc fixtures in
  `shared/test/avro/{backward,forward,full,disabled,none}/` under the
  appropriate Glue compatibility mode. Encodes + decodes a sample record at
  each schema version. Negative-evolution test asserts that incompatible
  schema versions are rejected by Glue.

- **Layer C** (`integration-tests/tests/fixture_avro_interop_test.go` +
  `fixture_proto_interop_test.go`): Java↔Go cross-language interop via the
  Phase 4.13 Java sidecar + real Kafka + real Glue. Avro fixtures exercise
  same-version round-trip in both directions (Java produce → Go consume, Go
  produce → Java consume) for backward/forward/full modes; multi-version
  registration sets the stage for future cross-version cells but
  cross-version round-trip itself is deferred to Phase 5. Protobuf fixtures
  exercise 5 representative .proto files (proto2 baseline, proto3 baseline,
  oneOf, complex nesting, all scalar types) in both directions with
  same-version round-trip.

**Out-of-scope .proto fixtures** (not exercised in any layer):
- `◉◉◉unicode⏩.proto`
- `.protodevelasl.proto.proto.protodevel$---$$.bar.3.proto`
- `hyphen-ated-proto_file-.proto`
- `foo$$$1.proto`
- `NestedConflicting#ClassName.proto`
- `ConflictingName.proto`
- `snake_case_file.proto`

Rationale: AWS Glue Schema Registry rejects schema names containing characters
outside `[A-Za-z0-9_.-]`. These fixtures exist to test the Apicurio protobuf
parser's handling of exotic file names, not the GSR client's wire-format or
registration logic. They parse cleanly in Layer A's sweep but cannot be
registered in Glue without schema-name sanitization (a customer-facing concern
outside the scope of the Go client library).

### Known parity gap: deprecated `*_generic_services` proto options
Schemas with `option php_generic_services = true;` (or the related deprecated
`py_generic_services`, `cc_generic_services`, `java_generic_services` options
on `google.protobuf.FileOptions`) parse successfully on the Java GSR client
(pinned at protobuf-java 3.25.5) but fail to parse on the Go GSR client.

Root cause: modern `google.golang.org/protobuf` (v1.36+, used by `bufbuild/protocompile`)
treats `php_generic_services` as a *removed* field on `FileOptions`. Java's
pinned protobuf-java 3.25.5 still recognizes it. Upstream protobuf removed
these options across all language runtimes in protobuf v27 (2024-06); when
Java GSR upgrades to protobuf-java v27+ the gap closes naturally.

Customer impact: customers with `.proto` schemas using these options must
strip them before registering with Glue (or normalize the schema at
registration time). Documented in `pkg/gsrserde-go/fixtures_test.go` via the
`TestOrderingSyntax3Options.proto` skipList entry. Phase 5 may add a
pre-process step to the Go parser for full parity; deferred for now.

### Phase 4.15 demo binary
- Added `cmd/demo-interop` binary that narrates a full cross-language
  cross-version interop demo across 12 cells (3 formats × 2 directions × 2
  compressions {NONE, ZLIB}). Output uses verbatim lowercase tags per spec:
  `[java-producer]`, `[java-consumer]`, `[go-producer]`, `[go-consumer]`,
  `[glue]`, `[cache]`, `[kafka]`, `[schema-evolution]`, `[wire-format]`,
  `[demo]`, `[verdict]`.
- Deferred to Phase 5: cache-behavior scenarios, auto-register-fall-through
  scenarios, same-version-baseline scenarios.

### Configuration
- `LoadConfigFromMap` now synthesizes a default value for `Config.Description`
  of `DEFAULT-DESCRIPTION-<region>-<registryName>` when the `description` key
  is absent or empty. Previously the field stayed blank. Mirrors Java
  `GlueSchemaRegistryConfiguration.java:343-352` and matches the description
  string the Glue console surfaces for Java callers. Empty region collapses
  the region segment to two consecutive dashes
  (`DEFAULT-DESCRIPTION--<registryName>`), matching Java. Callers that pass
  an explicit non-empty `description` are unaffected. (PBI-4.10-2)

### Cache TTL boundary fix
TTL eviction now uses strict `After` comparison (Java Caffeine parity): an entry
expires only once `clock.Now()` strictly exceeds `expiresAt`, not when it equals
it. The previous `!Before` check evicted at the exact boundary (one tick early).

### Cache size default
The legacy `NewCache(ttlMillis)` constructor now backs a cache with default
`CacheSize=200`, matching Java GSR's Caffeine `maximumSize` default at
`common/src/main/java/com/amazonaws/services/schemaregistry/common/configs/GlueSchemaRegistryConfiguration.java:50`
(`private int cacheSize = 200`). Callers needing a different size should use
`NewCacheWithOptions(CacheOptions{Size: N, TTLMillis: ms, Clock: ...})`.

### JSON Schema draft default pinned to Draft-07
The JSON serializer and deserializer now explicitly pin the
`santhosh-tekuri/jsonschema/v6` compiler to Draft-07 as the default for schemas
that do not include an explicit `$schema` field. This preserves
`xeipuuv/gojsonschema` legacy behavior and prevents a silent regression to
v6's out-of-the-box default of Draft 2020-12.

### Clock seam, LRU cache, singleflight
See individual PBI commit messages (PBI-4.11-1 through PBI-4.11-7) for the full
feature list.

### secondaryDeserializer removed
The `secondaryDeserializer` config key and `Config.SecondaryDeserializer` field
have been removed. Java's fallback-deserializer chain is intentionally not
implemented in Go. Callers that receive non-GSR-framed data get a typed
`ErrIncompatibleData` (via `errors.Is(err, gsrcore.ErrIncompatibleData)`) and
may route to their own pre-existing decoder. See package docs on
`pkg/gsrserde-go/deserializer` for rationale.

### User-Agent identity
The Go client identifies itself as `glue-schema-registry-go/<version>` in the
HTTP User-Agent header (distinct from Java's
`aws-glue-schema-registry-java/<app>`). The `userAgentApp` config key sets the
`<version>` segment at construction time; the default is `"default"`. Runtime
mutation of the user-agent string (Java's `OverrideUserAgentApp`) is
intentionally not implemented. The construction-time config knob provides
sufficient flexibility for all known use cases.

### Out of scope (Phase 4.14)
The following items from the validation matrix (section 5.3) are documented as
NOT requiring real-AWS companion tests:

- Item 23 (IAM denied): Tier-1 + Tier-2 fake-backend coverage only. A real-AWS
  IAM-denied scenario would require a separate restricted role; deferred to
  canary infrastructure (Phase 5).
- Item 24 (Throttling): Tier-1 + Tier-2 fake-backend coverage only. Real Glue
  cannot reliably produce ThrottlingException in a deterministic test; deferred
  to canary harness (Phase 5).
- Items 26-29 (Malformed Avro/JSON/Protobuf, non-UTF-8 JSON, truncated
  payload, corrupt UUID): Kept at Tier-1 unit + Tier-2 fake-backend only.
  These are pure client-side decode paths where a real-AWS gate adds zero
  signal (the Glue service is not exercised in the failure path).
