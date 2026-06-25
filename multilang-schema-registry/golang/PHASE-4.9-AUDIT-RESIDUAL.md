# Phase 4.9 Audit — Residual Reconciliation

## Front matter
- Date: 2026-06-24
- Reconciled against branch: golang-mrknox-4x-integrated @ 205758a
- Audit base: 0a95eae (frozen)
- Reconciler: swarm-implementor (residual reconciliation pass)

## Summary
- Originally Stubbed (17) → Closed: 14 · Partial: 1 · Still Open: 2
- Originally Missing (5) → Closed: 0 · Partial: 0 · Still Open: 5
- §2 format-layer (1 row) → Closed by Phase 4.11 (PBI-4.11-5 / f6496fe)
- §3 Tier-1 N items (5 rows) → Closed: 4 · Still Open: 1 (item 30 — Tier-1 explicit shared-instance round-trip absent; singleflight tests cover it indirectly)
- Triage: Blocker = 0 · Important = 2 · Optional = 7

## §1 Reconciliation

| Java file:line | Original Status | Current Status | Closed-by | Triage (if not CLOSED) | Evidence (Go file:line on integrated tree) |
|---|---|---|---|---|---|
| `GlueSchemaRegistryConfiguration.java:49` (timeToLiveMillis) | Stubbed | CLOSED | PBI-4.10-1 / 908d5f4 | — | `pkg/gsrserde-go/core/config.go:234-240` — parse error now returns `%w: ErrInvalidCacheTTL` rather than being swallowed; sentinel declared at `errors.go:80`. |
| `GlueSchemaRegistryConfiguration.java:50` (cacheSize) | Stubbed | PARTIAL | PBI-4.10-1 / 908d5f4 (parse fix); cache-size wiring still missing | Important | `pkg/gsrserde-go/core/config.go:243-249` — parse error now returns `%w: ErrInvalidCacheSize`. However, `pkg/gsrserde-go/core/encoder.go:93` and `decoder.go:38` both call `NewCache(config.TimeToLiveMillis)` (no `Size` argument), so the configured `CacheSize` is parsed but never reaches the LRU. The cache caps at `DefaultCacheSize=200` regardless of configMap. |
| `GlueSchemaRegistryConfiguration.java:51` (avroRecordType) | Stubbed | STILL OPEN | — | Optional | `pkg/gsrserde-go/core/config.go:276` parses into `Config.AvroRecordType` but no Go deserializer consumes it. Grep for `AvroRecordType` across `pkg/gsrserde-go/deserializer/avro/` returns nothing. No SPECIFIC_RECORD vs GENERIC_RECORD enum validation. Java SPECIFIC_RECORD relies on Avro reflection-based codegen; Go's `goavro` is fundamentally generic — translating this enum is a non-trivial format-layer decision deferred indefinitely. |
| `GlueSchemaRegistryConfiguration.java:52` (protobufMessageType) | Stubbed | STILL OPEN | — | Optional | `pkg/gsrserde-go/core/config.go:277` parses into `Config.ProtobufMessageType` but no Go deserializer consumes it. Go dispatches via reflected `ProtoMessage()` interface and the descriptor-tree BFS walk, not via a POJO/DYNAMIC_MESSAGE enum. No enum validation. |
| `GlueSchemaRegistryConfiguration.java:54` (compatibility) | Stubbed | CLOSED | PBI-4.10-1 / 908d5f4 | — | `pkg/gsrserde-go/core/config.go:200-206` reads the key and calls `validateCompatibility` (`config.go:341-350`) which rejects values outside the case-exact Java enum set `{NONE, DISABLED, BACKWARD, BACKWARD_ALL, FORWARD, FORWARD_ALL, FULL, FULL_ALL}` with `ErrInvalidCompatibility` (`errors.go:76`). |
| `GlueSchemaRegistryConfiguration.java:55` (description) | Stubbed | CLOSED | PBI-4.10-2 / 32482a0 | — | `pkg/gsrserde-go/core/config.go:258-261` synthesizes `fmt.Sprintf("DEFAULT-DESCRIPTION-%s-%s", region, registryName)` when description is absent; mirrors Java `GlueSchemaRegistryConfiguration.java:343-352`. |
| `GlueSchemaRegistryConfiguration.java:58` (metadata) | Stubbed | CLOSED | PBI-4.10-4 / a891119 | — | `pkg/gsrserde-go/core/metadata.go:55-94` (`putSchemaVersionMetadataBatch`) merges configured `Config.Metadata` with the `(TransportMetadataKey, transportName)` entry and invokes `PutSchemaVersionMetadata` once per entry; invoked from `encoder.go:484` after `CreateSchema` success and `encoder.go:393` after `RegisterSchemaVersion` success. Java parity AWSSchemaRegistryClient.java:264/281. |
| `GlueSchemaRegistryConfiguration.java:59` (secondaryDeserializer) | Stubbed | STILL OPEN | PBI-4.10-7 / b795ca3 (explicit deferral) | Optional | `pkg/gsrserde-go/core/config.go:278` still parses the value into `Config.SecondaryDeserializer` without a fallback decoder chain. PBI-4.10-7 commit body explicitly defers fallback-chain implementation per spec §3.10 and the user's explicit-exclusion directive; the deferral comment lives above `TestConfig_SecondaryDeserializer` (`config_test.go:220`). |
| `GlueSchemaRegistryConfiguration.java:66` (userAgentApp) | Stubbed | CLOSED | PBI-4.10-3 / e0f7ad4 | — | `pkg/gsrserde-go/core/config.go:150-156` always installs the User-Agent middleware via `AddUserAgentKeyValue("glue-schema-registry-go", effectiveUserAgent)` where `effectiveUserAgent` falls back to `DefaultUserAgentApp = "default"` (`config.go:66`) when the configMap key is absent. `EffectiveUserAgentApp` field added at `config.go:101` records the resolved value; raw `UserAgentApp` preserved per INV-3. |
| `AWSSchemaRegistryClient.java:242-267` (createSchema → metadata follow-up) | Stubbed | CLOSED | PBI-4.10-4 / a891119 | — | `pkg/gsrserde-go/core/encoder.go:481-484` invokes `putSchemaVersionMetadataBatch` immediately after a successful `CreateSchema`, propagating `Config.Metadata` plus the always-on transport entry to Glue. Errors are logged-only per Java parity AWSSchemaRegistryClient.java:425 (INV-6 / C-13). |
| `AWSSchemaRegistryClient.java:278-321` (registerSchemaVersion → poll + metadata follow-up) | Stubbed | CLOSED | PBI-4.10-5 / 7e8c6b9 (poll) + PBI-4.10-4 / a891119 (metadata flush) | — | `pkg/gsrserde-go/core/encoder.go:344-401` adds `waitForSchemaEvolutionCheck` polling (`encoder.go:412-444`) with constants `schemaEvolutionMaxAttempts=10` and `schemaEvolutionMaxWaitInterval=3*time.Second` (`encoder.go:18, 23`). Metadata flush fires after poll success at `encoder.go:393`. Sleep is injected via `s.sleepFn` (`encoder.go:72`) for tests. |
| `AWSSchemaRegistryClient.java:418-451` (putSchemaVersionMetadata) | Stubbed | CLOSED | PBI-4.10-4 / a891119 | — | `pkg/gsrserde-go/core/metadata.go:55-94` (`putSchemaVersionMetadataBatch`) is the only non-test caller; invoked from both success branches in `encoder.go`. Sequential for-loop per spec §3.4(d) / INV-9. Stdlib `log.Printf` with `gsr: ` prefix on per-entry failures (INV-8 / C-21). |
| `AWSSchemaRegistryClient.java:476-487` (querySchemaVersionMetadata) | Stubbed | CLOSED | PBI-4.10-6 / 37b80ac | — | `pkg/gsrserde-go/core/metadata_query.go:17-35` declares `GsrEncoder.QuerySchemaVersionMetadata(ctx, schemaVersionId) (map[string]string, error)`, flattening `MetadataInfoMap` into a string map. |
| `AWSSchemaRegistryClient.java:502-517` (querySchemaTags) | Stubbed | CLOSED | PBI-4.10-6 / 37b80ac | — | `pkg/gsrserde-go/core/metadata_query.go:42-64` declares `GsrEncoder.QuerySchemaTags(ctx, schemaDefinition, schemaName)` which calls `GetSchemaByDefinition` → reads `SchemaArn` → calls `GetTags`. Mirrors Java AWSSchemaRegistryClient.java:502-517. |
| `GlueSchemaRegistrySerializationFacade.java:76-89` (getOrRegisterSchemaVersion → x-amz-meta-transport injection) | Stubbed | CLOSED | PBI-4.10-4 / a891119 | — | `pkg/gsrserde-go/core/metadata.go:54-57` always merges `TransportMetadataKey → transportName` (`config.go:52`: `"x-amz-meta-transport"`). The `Encode` signature is `(data []byte, transportName string, schema *Schema)` (`encoder.go:121`) so callers thread the per-call value through to the metadata flush. Customer-configured `metadata.x-amz-meta-transport` wins on collision (AC-4-collision). |
| `GlueSchemaRegistrySerializationFacade.java:135-141` (getSchemaDefinition(DataFormat, Object)) | Stubbed | STILL OPEN | — | Optional | `pkg/gsrserde-go/serializer/gsr_serializer.go:147-177` still keeps `schemaFromData` private. No public `Serializer.GetSchemaDefinition(data interface{}) (string, error)` shorthand. Java callers can ask for the schema definition without performing a full encode; Go callers cannot. |
| `GlueSchemaRegistryDeserializationFacade.java:127-155` (getSchemaDefinition(byte[]) shorthand) | Stubbed | STILL OPEN | — | Optional | `pkg/gsrserde-go/deserializer/gsr_deserializer.go:116-124` exposes `Deserializer.GetSchema(data []byte) (*gsrcore.Schema, error)` only. No public `GetSchemaDefinition(data []byte) (string, error)` shorthand; callers must read `.SchemaDefinition` themselves. |

### Originally Missing rows

| Java file:line | Original Status | Current Status | Triage | Evidence / Notes |
|---|---|---|---|---|
| `GlueSchemaRegistryConfiguration.java:68` (jacksonSerializationFeatures) | Missing | STILL OPEN | Optional | Java-only. Go uses `encoding/json` + santhosh-tekuri/jsonschema; no Jackson surface to forward feature flags to. Will never close — listed for completeness. |
| `GlueSchemaRegistryConfiguration.java:69` (jacksonDeserializationFeatures) | Missing | STILL OPEN | Optional | Same divergence — Go has no Jackson layer. Will never close. |
| `GlueSchemaRegistrySerializationFacade.java:112-133` (encode(String, Schema, byte[]) — pre-serialized bytes path) | Missing | STILL OPEN | Optional | `pkg/gsrserde-go/serializer/gsr_serializer.go:100` `Serialize(topic, data interface{}) ([]byte, error)` still accepts only typed objects. No public path for callers who already hold serialized bytes plus a `Schema`. |
| `GlueSchemaRegistryDeserializationFacade.java:115-117` (overrideUserAgentApp) | Missing | STILL OPEN | Optional | `grep -rn "OverrideUserAgent" pkg/gsrserde-go/` returns no results. No post-construction user-agent mutation surface. Phase 4.10 made the User-Agent middleware always-on at construction (`config.go:138-156`) but there is no override seam after the SDK client is built. |
| `GlueSchemaRegistryDeserializationFacade.java:133-136` (getActualData) | Missing | STILL OPEN | Important | `grep -rn "GetActualData" pkg/gsrserde-go/` returns no results. The plain-bytes extraction still happens inside `core/decoder.go:53-83` (`Decode`) coupled to the full decode flow. External callers cannot reach the raw post-prefix bytes without performing a format-layer decode. |

## §2 Reconciliation

The §2 row "JSON-Schema library choice" — `xeipuuv/gojsonschema` → `santhosh-tekuri/jsonschema/v6` — is CLOSED by Phase 4.11 commit `f6496fe` (PBI-4.11-5). Evidence: `multilang-schema-registry/golang/go.mod:11` now declares `github.com/santhosh-tekuri/jsonschema/v6 v6.0.2` as a direct dependency; `pkg/gsrserde-go/serializer/json/json_serializer.go:9` and `pkg/gsrserde-go/deserializer/json/json_deserializer.go:10` both import `santhosh-tekuri/jsonschema/v6`. A `grep -n "xeipuuv/gojsonschema" go.mod go.sum` over the main Go module returns no results — the legacy import is fully gone (only an indirect transitive in `integration-tests/go.sum` remains, irrelevant for the main module). The Phase-3 §2 "Still stub" row's "1 multi-phase carry-over" is now resolved.

(§2 rows 1 and 2 — protobuf Validate and protobuf deserializer nil-descriptor — were already "Done" in the audit and remain unchanged on the integrated tree.)

## §3 Reconciliation

| Item # | Item description | Original Tier-1 | Current Tier-1 | Closed-by | Triage (if not CLOSED) | Evidence |
|---|---|---|---|---|---|---|
| 18 | BACKWARD evolution v1→v2 — wire-flow | N | CLOSED | PBI-4.12-7 / ed9ef40 | — | `pkg/gsrserde-go/core/evolution_test.go:183` `TestEvolution_BackwardV1ToV2_WireFlow`. |
| 19 | BACKWARD_ALL across three versions | N | CLOSED | PBI-4.12-7 / ed9ef40 | — | `pkg/gsrserde-go/core/evolution_test.go:214` `TestEvolution_BackwardAll_ThreeVersions_WireFlow`. |
| 20 | FORWARD evolution v2→v1 | N | CLOSED | PBI-4.12-7 / ed9ef40 | — | `pkg/gsrserde-go/core/evolution_test.go:247` `TestEvolution_ForwardV2ToV1_WireFlow`. |
| 21 | FULL evolution both directions | N | CLOSED | PBI-4.12-7 / ed9ef40 | — | `pkg/gsrserde-go/core/evolution_test.go:273` `TestEvolution_FullBothDirections_WireFlow`. |
| 30 | Multithreaded produce/consume with shared serializer/deserializer instances | N | PARTIAL | PBI-4.11-7 / 40e87f8 (Tier-2 only); singleflight Tier-1 tests provide indirect coverage | Important | `integration-tests/tests/multithreaded_shared_instance_test.go` (Tier-2 added by PBI-4.11-7) targets the real-AWS shared-instance contract. No Tier-1 unit test explicitly drives N goroutines through `Serializer.Serialize` + `Deserializer.Deserialize` on a single shared instance; `pkg/gsrserde-go/core/singleflight_test.go:24/113/178` cover concurrent first-encode/decode at the `*GsrEncoder`/`*GsrDecoder` layer via `sync.WaitGroup` but stop short of the facade surface used by customers. |

## Blocker list (must close before customer use)

None. Every originally-Stubbed Glue-call gap (metadata flush, RegisterSchemaVersion poll, QuerySchemaVersionMetadata, QuerySchemaTags, x-amz-meta-transport injection) has been closed by Phase 4.10. Configuration validators (compatibility enum, cacheSize/TTL parse errors, userAgentApp default, description default) are in place. The JSON-Schema lib swap is done. No remaining gap is a correctness, safety, or auth blocker that would prevent external customer encode/decode round-trips.

## Important list

- §1.1 cacheSize wiring (PARTIAL) — parse error is now typed (good) but `pkg/gsrserde-go/core/encoder.go:93` and `decoder.go:38` call `NewCache(config.TimeToLiveMillis)` ignoring `config.CacheSize`. Customers who configure `cacheSize=1000` will silently get the 200-entry default cap. Fix is a one-line switch to `NewCacheWithOptions(CacheOptions{TTLMillis: cfg.TimeToLiveMillis, Size: cfg.CacheSize})` at both call sites. Should close before customer-tunable cache claims are made.
- §1.4 row 4 GetActualData (STILL OPEN, Missing) — no public extract-raw-bytes-after-prefix surface on the deserializer facade. Java exposes this for Kafka stream-processor inter-op (logging the post-prefix payload without performing format decode). Not blocking for the encode/decode happy path, but a documented Java surface customers may reach for; promote from Optional to Important on that basis.
- §3 item 30 Tier-1 shared-instance (STILL OPEN/PARTIAL) — Tier-2 covers it against real Glue, but a Tier-1 unit test on the public `*Serializer`/`*Deserializer` facade would catch concurrency regressions without needing the integration-tests harness. Singleflight tests in core cover the encoder/decoder internals only.

## Optional list

- §1.1 avroRecordType / protobufMessageType enums (STILL OPEN) — Go format layer fundamentally diverges from Java's SPECIFIC_RECORD / POJO / DYNAMIC_MESSAGE distinction; the configMap values are parsed and held but have no Go-side consumer. Adding enum validation would catch typos but offers no behavioral parity gain because the consumer side is permanently missing.
- §1.1 secondaryDeserializer (STILL OPEN) — explicitly deferred by spec §3.10 / PBI-4.10-7 / user directive. No Java fallback chain. Keep as-is.
- §1.3 / §1.4 facade convenience shortcuts (`getSchemaDefinition(data)`, `getSchemaDefinition(bytes)`, pre-serialized-bytes `encode`) — divergence from Java's builder/overload surface, not a correctness gap. Optional unless a customer specifically asks.
- §1.4 overrideUserAgentApp (STILL OPEN) — Java-only mutation hook; Go's always-on User-Agent middleware is installed at construction time. Adding post-construction mutation would require rebuilding the SDK client. Not blocking and unidiomatic in Go; keep as Optional.
- §1.1 jacksonSerializationFeatures / jacksonDeserializationFeatures (STILL OPEN, Missing) — Java-only, will never close. Listed for record-keeping.
