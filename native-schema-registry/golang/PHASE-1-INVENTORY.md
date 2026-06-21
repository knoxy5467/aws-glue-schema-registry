# Phase 1 — `core/` Scaffolding Inventory

**Purpose:** input for Phase 1 items 2-10, not bookkeeping. For every existing
`*_test.go` in `pkg/gsrserde-go/core/` this lists tests, current status
(green/red, observed 2026-06-21 with `go test -short -count=1 ./...`), the
behavior the test purports to cover, and a Phase-1 disposition (KEEP /
REWRITE / DROP).

Production-file disposition is summarized in the second table.

**Run summary:** 67 distinct `func Test...` over 18 files. **62 green, 5 red.**
The 5 reds are the stub-tracking failures already flagged in plan §2.2 and
the parity-audit doc comments (commit `d35843f`).

---

## Test inventory (67 rows)

Status reflects what `go test -short ./...` does today on the post-Phase-0
`golang-mrknox` tip (`409aa16`).

| # | Test | File | Status | Covered behavior | Phase 1 disposition |
|---|---|---|:-:|---|---|
| 1 | `TestCacheTTL` | `cache_test.go` | ✅ | `patrickmn/go-cache` TTL eviction wrapper | KEEP (Phase 1 item 6 expands TTL coverage) |
| 2 | `TestCacheInterface` | `cache_test.go` | ✅ | cache satisfies a public interface | KEEP |
| 3 | `TestCacheSize` | `cache_test.go` | ✅ | size-based eviction | KEEP (expand: singleflight first-encode) |
| 4 | `TestLoadConfigFromMap_Basic` | `config_enhanced_test.go` | ✅ | parses *some* config keys from `map[string]any` | REWRITE — must cover every Java key (item 5) |
| 5 | `TestLoadConfigFromMapDefaults` | `config_test.go` | ✅ | default values for unspecified keys | REWRITE — defaults must match Java reference |
| 6 | `TestNewGsrDecoder_Success` | `constructor_test.go` | ✅ | decoder constructor runs | KEEP, expand to assert injected deps |
| 7 | `TestNewGsrEncoder_Success` | `constructor_test.go` | ✅ | encoder constructor runs | KEEP, expand to assert injected deps |
| 8 | `TestNewGsrDecoder_Coverage` | `coverage_test.go` | ✅ | coverage-chasing duplicate of #6 | DROP — duplicates #6, coverage-bait |
| 9 | `TestNewGsrEncoder_Coverage` | `coverage_test.go` | ✅ | coverage-chasing duplicate of #7 | DROP — duplicates #7 |
| 10 | `TestGsrDecoder_Decode_Coverage` | `coverage_test.go` | ✅ | drives `Decode` paths for coverage | DROP — replaced by spec-anchored cases below |
| 11 | `TestGsrDecoder_parseGSRData_Coverage` | `coverage_test.go` | ✅ | exercises private `parseGSRData` | DROP — internal-detail test |
| 12 | `TestGsrDecoder_Close_Coverage` | `coverage_test.go` | ✅ | `Close()` runs | DROP — meaningless smoke (no resource to close in pure-Go core) |
| 13 | `TestGsrEncoder_Close_Coverage` | `coverage_test.go` | ✅ | `Close()` runs | DROP — same as #12 |
| 14 | `TestDeserializer_Decode_ParseGSRDataError` | `decode_missing_test.go` | ✅ | bad header byte ⇒ error | KEEP, retitle, move into `deserializer_test.go` |
| 15 | `TestDeserializer_Decode_GetSchemaError` | `deserializer_missing_coverage_test.go` | ✅ | Glue client error propagation | KEEP — but rewrite against `GlueClient` mock (item 2) |
| 16 | `TestDeserializer_DecodeSchema_GetSchemaError` | `deserializer_missing_coverage_test.go` | ✅ | same as #15 for schema-only path | KEEP, see #15 |
| 17 | `TestDeserializer_DecodeSchema_ParseError` | `deserializer_missing_coverage_test.go` | ✅ | malformed payload ⇒ typed error | KEEP, REWRITE to assert the new error surface (item 9) |
| 18 | `TestDeserializer_ParseGSRData_ZlibDecompressionError` | `deserializer_missing_coverage_test.go` | ✅ | zlib corruption ⇒ typed error | KEEP — feeds item 8 + 9 |
| 19 | `TestDeserializer_ParseGSRData_ValidZlibDecompression` | `deserializer_missing_coverage_test.go` | ✅ | zlib round-trip | KEEP — feeds item 8 |
| 20 | `TestDeserializer_ParseGSRData_InvalidZlibReader` | `deserializer_missing_coverage_test.go` | ✅ | nil/invalid reader | DROP — internal-detail; covered by #18 effectively |
| 21 | `TestDeserializer_ParseGSRData_ZlibReadAllError` | `deserializer_missing_coverage_test.go` | ✅ | io.ReadAll error path | DROP — internal-detail |
| 22 | `TestDeserializer_Decode_Success` | `deserializer_test.go` | ✅ | happy-path decode | KEEP — golden anchor |
| 23 | `TestDeserializer_Decode_ProtobufFormat` | `deserializer_test.go` | ❌ | protobuf decode w/ message-index | REWRITE — stub-tracking red, ties to item 10 |
| 24 | `TestDeserializer_Decode_WithCompression` | `deserializer_test.go` | ✅ | decode with zlib compression byte set | KEEP |
| 25 | `TestDeserializer_DecodeSchema_Success` | `deserializer_test.go` | ✅ | schema-only decode | KEEP |
| 26 | `TestDeserializer_GetSchema_Cached` | `deserializer_test.go` | ✅ | second decode hits cache | KEEP — feeds item 6 |
| 27 | `TestDeserializer_GetSchema_Error` | `deserializer_test.go` | ✅ | Glue error propagation | KEEP, rewrite vs mock (item 2) |
| 28 | `TestDeserializer_ExtractSchemaName` | `deserializer_test.go` | ✅ | strips ARN to schema name | KEEP |
| 29 | `TestDeserializer_ExtractSchemaNameFromArn` | `deserializer_test.go` | ✅ | ARN-format input | KEEP |
| 30 | `TestDeserializer_ParseGSRData_CompressionError` | `deserializer_test.go` | ✅ | unknown compression byte ⇒ error | KEEP |
| 31 | `TestSerializer_GetSchemaVersionIdByDefinition_CreateSchemaPath` | `edge_cases_test.go` | ✅ | auto-register path | KEEP, rewrite vs mock |
| 32 | `TestSerializer_GetSchemaVersionIdByDefinition_GetSchemaSuccess` | `edge_cases_test.go` | ❌ | cached + live path return same UUID | REWRITE — Phase 1 item 4(b) lands the fix that makes this green |
| 33 | `TestSerializer_GetSchemaVersionIdByDefinition_GetSchemaUnavailable` | `edge_cases_test.go` | ✅ | Glue 404 ⇒ documented error | KEEP, rewrite to use typed sentinel (item 9) |
| 34 | `TestDeserializer_ParseGSRData_InvalidCompressionByte` | `edge_cases_test.go` | ✅ | duplicate of #30 | DROP — overlaps #30 |
| 35 | `TestDeserializer_ParseGSRData_ReadErrors` | `edge_cases_test.go` | ✅ | truncated payload subtests | KEEP — feeds item 9 (sentinel error coverage) |
| 36 | `TestDeserializer_CanDecodeData_EdgeCases` | `edge_cases_test.go` | ✅ | header recognition matrix | KEEP — anchors wire-format work in item 3 |
| 37 | `TestDeserializer_CanDecode_Wrapper` | `edge_cases_test.go` | ✅ | public wrapper delegates | KEEP — thin |
| 38 | `TestCache_EdgeCases` | `edge_cases_test.go` | ✅ | nil/empty key | KEEP, fold into `cache_test.go` |
| 39 | `TestSerializer_Encode_GetSchemaError` | `encode_missing_test.go` | ✅ | encode propagates Glue error | KEEP, rewrite vs mock |
| 40 | `TestSerializationError` | `errors_test.go` | ✅ | error-type marshaling | REWRITE — new error surface (item 9) replaces this shape |
| 41 | `TestDeserializationError` | `errors_test.go` | ✅ | error-type marshaling | REWRITE — see #40 |
| 42 | `TestErrorTypes` | `errors_test.go` | ✅ | type-equality on errors | REWRITE — `errors.Is`/`errors.As` form (item 9) |
| 43 | `TestDeserializer_ParseGSRData_SchemaIDReadError` | `parse_gsr_missing_test.go` | ✅ | truncated schema ID | KEEP, fold into `parse_gsr_test.go` |
| 44 | `TestDeserializer_ParseGSRData_SchemaVersionReadError` | `parse_gsr_missing_test.go` | ✅ | truncated schema version | KEEP, fold into `parse_gsr_test.go` |
| 45 | `TestPrefixMessageIndexToBytes_ErrorCases` | `protobuf_utils_enhanced_test.go` | ✅ | invalid/empty proto + missing message — passes today (silent 0 return) | REWRITE — once item 4(a) returns typed error, `Message not found` subtest must assert the error, not a 0 |
| 46 | `TestStripMessageIndex_EdgeCases` | `protobuf_utils_enhanced_test.go` | ❌ | strip behaviour on small inputs | REWRITE — stub-tracking, replace with spec-anchored cases (item 10) |
| 47 | `TestGetMessageIndexFromProtoDefinition_ComplexCases` | `protobuf_utils_enhanced_test.go` | ✅ | BFS+lex-sort across nested/multiple messages | KEEP — but needs an `unknown message → ErrMessageTypeNotFound` case after item 4(a) |
| 48 | `TestConvertBase64SchemaToStringSchema_InvalidBase64` | `protobuf_utils_missing_test.go` | ✅ | bad base64 ⇒ error | KEEP, fold |
| 49 | `TestConvertBase64SchemaToStringSchema_InvalidProto` | `protobuf_utils_missing_test.go` | ✅ | bad proto bytes ⇒ error | KEEP, fold |
| 50 | `TestConvertBase64SchemaToStringSchema_InvalidFileDescriptor` | `protobuf_utils_missing_test.go` | ✅ | bad FileDescriptor ⇒ error | KEEP, fold |
| 51 | `TestConvertBase64SchemaToStringSchema` | `protobuf_utils_test.go` | ✅ | happy-path | KEEP |
| 52 | `TestPrefixMessageIndexToBytes` | `protobuf_utils_test.go` | ❌ | stub-tracking | REWRITE — Phase 1 item 10, spec-anchored varint cases |
| 53 | `TestStripMessageIndex` | `protobuf_utils_test.go` | ❌ | stub-tracking | REWRITE — Phase 1 item 10 |
| 54 | `TestHeaderFormat` | `serde_test.go` | ✅ | 1-line `cap(prefix)==18` check | REWRITE — Phase 1 item 3, replace with byte-level assertions (`0x03`, comp byte, UUID layout) |
| 55 | `TestExtractSchemaNameFromArn` | `serde_test.go` | ✅ | ARN tail | DROP — overlaps #29 |
| 56 | `TestCanDecodeBasic` | `serde_test.go` | ✅ | `CanDecode` happy/sad | DROP — overlaps #36 |
| 57 | `TestSerializer_Encode_NilData` | `serializer_test.go` | ✅ | nil payload ⇒ error | KEEP |
| 58 | `TestSerializer_Encode_NilSchema` | `serializer_test.go` | ✅ | nil schema ⇒ error | KEEP |
| 59 | `TestSerializer_Encode_Success` | `serializer_test.go` | ✅ | happy-path encode | KEEP — golden anchor; tie into golden-byte fixture (§5.5) |
| 60 | `TestSerializer_Encode_WithZlibCompression` | `serializer_test.go` | ✅ | encode with zlib | KEEP — feeds item 8 |
| 61 | `TestSerializer_Encode_ProtobufFormat` | `serializer_test.go` | ✅ | proto encode wraps payload w/ varint(0) | KEEP — verify against #52/#53 after their rewrite |
| 62 | `TestSerializer_CreateSchema_Success` | `serializer_test.go` | ✅ | auto-register happy-path | KEEP, rewrite vs mock |
| 63 | `TestSerializer_CreateSchema_Error` | `serializer_test.go` | ✅ | auto-register error path | KEEP, rewrite vs mock |
| 64 | `TestSerializer_GetSchemaVersionIdByDefinition_Cached` | `serializer_test.go` | ✅ | cached path returns *something* | REWRITE — paired with #32; assert UUID, not `SchemaName` |
| 65 | `TestDeserializer_extractSchemaName_Coverage` | `simple_test.go` | ✅ | duplicate of #28 | DROP — overlaps #28 |
| 66 | `TestConvertBase64SchemaToStringSchema_Coverage` | `simple_test.go` | ✅ | duplicate of #51 | DROP — overlaps #51 |
| 67 | `TestDeserializer_DecodeSchema_InvalidFormat_Coverage` | `simple_test.go` | ✅ | bad header on schema-only decode | KEEP, fold into deserializer_test.go |

### Disposition rollup

- **KEEP** (anchor of behavior we keep): 32
- **REWRITE** (behavior is right, assertions need updating to match Java parity / new error surface / new mock interface): 18
- **DROP** (coverage-bait, duplicates, internal-detail probes): 17
- **Total:** 67

### Files to consolidate

Phase 1 will collapse these clusters into fewer, behavior-named files:

- `coverage_test.go`, `*_missing_test.go`, `simple_test.go` — 17 of their tests are DROP/duplicate; the survivors fold back into the canonical file (`deserializer_test.go`, `serializer_test.go`, `cache_test.go`, `protobuf_utils_test.go`).
- `edge_cases_test.go` splits: serializer cases → `serializer_test.go`; deserializer cases → `deserializer_test.go`; cache cases → `cache_test.go`.

After this collapse the test-file count drops from 18 to roughly 6:
`cache_test.go`, `config_test.go`, `deserializer_test.go`, `serializer_test.go`,
`protobuf_utils_test.go`, `errors_test.go` (plus new `wire_format_test.go`,
`compression_test.go`, `schema_name_strategy_test.go` added by Phase 1
items 3, 8, 7 respectively).

---

## Production-file disposition

Per plan §2.2 / §7 Phase 1: the scaffolding is *starting reference*, not the
implementation.

| File | Lines | Phase 1 disposition |
|---|---:|---|
| `cache.go` | thin | KEEP — `patrickmn/go-cache` wrapper is the design intent (item 6); expand for two caches + singleflight |
| `client_interface.go` | small | REWRITE — tighten against Java reference, every Glue v2 call the library makes must be on the interface (item 2) |
| `config.go` | partial | EXTEND — audit every Java key in `AWSSchemaRegistryConstants.java` + `GlueSchemaRegistryConfiguration.java` (item 5) |
| `decoder.go` | partial | KEEP shape; behavior driven by new wire-format Tier-1 tests (item 3) and error surface (item 9) |
| `encoder.go` | partial | FIX `getSchemaVersionIdByDefinition` cached path (item 4b); rebuild rest via TDD (item 3, 7, 8) |
| `errors.go` | small | REWRITE — define typed/sentinel/wrapped error surface for `errors.Is`/`errors.As` (item 9) |
| `protobuf_utils.go` | doc-commented for Java parity in commit `d35843f` | FIX `getMessageIndexFromProtoDefinition` return type to `(uint32, error)` + typed `ErrMessageTypeNotFound` (item 4a). Encode/decode varint code itself is already spec-anchored. |

No new top-level files added by this inventory step. New files appear when
items 3, 6, 7, 8 land.
