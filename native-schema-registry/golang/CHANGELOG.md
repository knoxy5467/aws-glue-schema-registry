# Go Client Changelog

Changes to the GSR Go client live here. The top-level repository
`CHANGELOG.md` tracks Java releases.

## Unreleased

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
