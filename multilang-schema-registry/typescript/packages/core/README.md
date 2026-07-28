# @gsr/core

Core primitives for the AWS Glue Schema Registry (GSR) TypeScript client: the on-the-wire message format, compression, configuration parsing, an in-memory schema cache, the exported error taxonomy, and the seam over `@aws-sdk/client-glue` used by the higher-level `@gsr/serde` package.

This is the reference package. If you are looking to serialize or deserialize records, install `@gsr/serde` — it re-exports the parts of this package you need for a typical caller.

## Install

```
npm install @gsr/core
```

The publish paths this repository supports (private-registry and
public-npm-registry) are documented in
[`../PUBLISHING.md`](../PUBLISHING.md). If you are consuming from a
private registry, configure your `npm` client to point at that
registry (via `~/.npmrc` or `--registry`) before running the
`npm install` above.

**Prerequisite:** Node.js 20 or newer (`engines.node` is `>=20`). The
package ships a dual build — CommonJS (`dist/index.cjs`) and ESM
(`dist/index.mjs`) — plus matching `.d.ts` / `.d.cts` type declarations.
Node picks the right entry via the package `exports` map, so both styles
work out of the box:

```ts
// ESM (TypeScript, or Node with `"type": "module"` / .mjs files)
import { parseConfig, GsrConfig, encodeMessage, decodeMessage } from "@gsr/core";
```

```js
// CommonJS (Node with `"type": "commonjs"` / .cjs / .js files)
const { parseConfig, encodeMessage, decodeMessage } = require("@gsr/core");
```

Most callers use `@gsr/serde` instead — it re-exports the parts of this
package a typical caller needs and layers the per-format encode/decode on
top. See the [`@gsr/serde` README](../serde/README.md) for the customer-
facing quick-start.

## Configuration

Configuration is a flat string-to-string map (`Record<string, string>`), parsed with `parseConfig` into a typed `GsrConfig`. The map shape mirrors the Java canonical client so a caller can port a Java config verbatim.

```ts
import { parseConfig, GsrConfig } from "@gsr/core";

const cfg: GsrConfig = parseConfig({
  region: "us-west-2",
  "registry.name": "my-registry",
  compatibility: "BACKWARD",
  compression: "ZLIB",
  timeToLiveMillis: "3600000",
  cacheSize: "500",
  "tags.env": "prod",
});
```

`parseConfig` is pure — it performs no SDK call and no network I/O. It applies defaults, validates enum and integer fields, and throws a typed error when a value is out of set (see [Errors](#errors) below). An absent key falls back to the documented default; an explicit empty string is treated as absent for optional keys.

### Configuration reference

The full set of input keys `parseConfig` reads. Every key documented here exists in `parseConfig`; every key `parseConfig` reads is documented here.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `region` | string | `us-east-2` | AWS region. An explicit default is applied when the key is absent or empty. |
| `endpoint` | string | *(unset)* | Custom Glue endpoint override. Applied regardless of `region` when set. |
| `proxyUrl` | string | *(accept-and-ignore)* | Accepted for forward compatibility. Produces no observable effect (proxy wiring is deferred to a later transport layer). |
| `registry.name` | string | `default-registry` | Glue registry name. |
| `compatibility` | enum string | `BACKWARD` | Compatibility mode for schema evolution. One of `NONE`, `DISABLED`, `BACKWARD`, `BACKWARD_ALL`, `FORWARD`, `FORWARD_ALL`, `FULL`, `FULL_ALL` (case-exact). Out-of-set values throw `GsrInvalidCompatibilityError`. |
| `description` | string | synthesized `DEFAULT-DESCRIPTION-<region>-<registryName>` | Registry description. When absent, synthesized after defaults resolve. |
| `schemaAutoRegistrationEnabled` | bool | `false` | When `true`, an unknown schema is auto-registered on first serialize. Lenient parse: only the literal `"true"` (case-insensitive) is truthy; every other value is `false`. |
| `compression` | enum string | `NONE` | Wire-payload compression algorithm. One of `NONE`, `ZLIB` (case-insensitive validation). An explicit out-of-set value throws `GsrInvalidCompressionTypeError`. |
| `compressionType` | enum string | *(legacy alias of `compression`)* | Legacy Java-canonical key retained for compatibility. If both `compression` and `compressionType` are set, `compression` wins. |
| `timeToLiveMillis` | integer | `86400000` (24 hours) | Cache entry TTL in milliseconds. Non-integer input throws `GsrInvalidCacheTtlError`. |
| `cacheSize` | integer | `200` | Cache entry count (LRU bound). Non-integer input throws `GsrInvalidCacheSizeError`. |
| `avroRecordType` | enum string | *(unset)* | Avro record shape hint. `GENERIC_RECORD` or `SPECIFIC_RECORD` (case-exact). Out-of-set values throw `GsrInvalidAvroRecordTypeError`. |
| `protobufMessageType` | enum string | *(unset)* | Protobuf message-type hint. `POJO` or `DYNAMIC_MESSAGE` (case-exact). Out-of-set values throw `GsrInvalidProtobufMessageTypeError`. |
| `avroReaderSchema` | string (JSON) | *(unset)* | Reader-schema JSON for Avro projection. Accepted unvalidated at parse time (the Avro parser lives in the serde layer, which core cannot import). Validation happens at projection time. |
| `schemaNameGenerationClass` | string | *(unset)* | Schema-name strategy hint. Consumed on the serde side. |
| `userAgentApp` | string | `""` | Raw user-agent app token. Preserved verbatim so callers can distinguish a default from an explicit input. |
| `assumeRoleArn` | string | *(unset)* | AssumeRole ARN. When set, the client uses temporary credentials via the AWS SDK's `fromTemporaryCredentials` provider. |
| `assumeRoleSessionName` | string | `aws-glue-schema-registry-js` when `assumeRoleArn` is set, else `""` | AssumeRole session name. |
| `secondaryDeserializer` | string | *(accept-and-ignore)* | Accepted for forward compatibility. Produces no observable effect (the Java-canonical secondary-deserializer path was removed upstream). |
| `retryMode` | `standard` \| `adaptive` | `standard` | SDK v3 retry mode. Out-of-set values fall back silently to the default. |
| `maxAttempts` | integer | `3` | SDK v3 retry budget. Non-integer values fall back silently to the default. |
| `tags.<k>` | string (prefix map) | *(empty map)* | Every `tags.<k>=<v>` entry is collected into the resulting `tags` map keyed by `<k>`. |
| `metadata.<k>` | string (prefix map) | *(empty map)* | Every `metadata.<k>=<v>` entry is collected into the resulting `metadata` map keyed by `<k>`. |

**Derived field on `GsrConfig` (not an input key):** `effectiveUserAgentApp` is computed as `userAgentApp || "default"` and is the value the always-on user-agent middleware writes. The raw `userAgentApp` is preserved separately so a consumer can tell `""` from an explicit value.

## Errors

The client throws typed error classes so callers can `instanceof`-branch on failure. Every class listed here is exported and is a real thrown class in the shipped code.

### `@gsr/core` — wire taxonomy

These errors originate on the wire-decode path. Base class is the built-in `Error`.

**`GsrIncompatibleDataError`**
- *When thrown:* Wire bytes are uninterpretable — the buffer is shorter than the 18-byte header, the version byte is not `0x03`, the compression byte is not `0x00` or `0x05`, the protobuf message-index varint is malformed, or the higher-level serde facade normalizes any decode-time failure into this class.
- *How to handle:* Treat as a permanent decode failure for the byte slice. Retrying with the same bytes will not help. Log the payload prefix (first 32 bytes as hex is enough), drop the message, and continue — the byte content is either corrupted or was not produced by a GSR-compatible writer. If a whole topic is failing, verify the producer is emitting the GSR wire format (`0x03` version byte at offset 0).

**`GsrMessageTypeNotFoundError`**
- *When thrown:* A protobuf `messageFullName` does not resolve to an index within the schema's sorted message set (or the caller failed to supply a `messageFullName` on decode when the schema declares multiple messages).
- *How to handle:* This is a caller/schema mismatch, not a corrupted byte. Confirm the schema definition the caller is compiling from matches the schema the producer registered — usually the fix is to register a superset schema on the write side or to compile the correct `.proto` on the read side.

### `@gsr/core` — control-plane taxonomy

These errors originate on config parsing, the encoder's auto-registration guard, and the Glue registration path. Base class is `GsrError`, which is itself an `Error`. A caller can `catch (err) { if (err instanceof GsrError) ... }` to branch on the whole control-plane family at once.

**`GsrError`**
- *When thrown:* Umbrella base. Not thrown directly by the shipped code, but the base for every class in this section.
- *How to handle:* Use as the catch-anchor for the whole control-plane family. Then narrow by concrete class to decide the next action.

**`GsrAutoRegistrationDisabledError`**
- *When thrown:* The encoder is asked to serialize a record whose schema is not yet registered, and `schemaAutoRegistrationEnabled` is `false`.
- *How to handle:* Either set `schemaAutoRegistrationEnabled=true` in the config so unknown schemas are registered on first sight, or pre-register the schema out of band and retry the serialize. This is a configuration/deployment decision, not a runtime retry.

**`GsrInvalidCompressionTypeError`**
- *When thrown:* `compression` (or the legacy `compressionType` alias) is set to a value outside `{NONE, ZLIB}` at `parseConfig` time.
- *How to handle:* Fix the config value. This surfaces at process start, not in the request path.

**`GsrInvalidCompatibilityError`**
- *When thrown:* `compatibility` is set to a value outside the case-exact 8-mode set at `parseConfig` time.
- *How to handle:* Fix the config value. Note the check is case-exact — `backward` is rejected; only `BACKWARD` is accepted.

**`GsrInvalidCacheTtlError`**
- *When thrown:* `timeToLiveMillis` is set to a value that is not a base-10 integer at `parseConfig` time.
- *How to handle:* Fix the config value. Decimal, alphanumeric, and whitespace-padded inputs are all rejected — the check mirrors Go's `strconv.Atoi` semantics.

**`GsrInvalidCacheSizeError`**
- *When thrown:* `cacheSize` is set to a value that is not a base-10 integer at `parseConfig` time.
- *How to handle:* Fix the config value.

**`GsrInvalidAvroRecordTypeError`**
- *When thrown:* `avroRecordType` is set to a value outside `{GENERIC_RECORD, SPECIFIC_RECORD}` at `parseConfig` time.
- *How to handle:* Fix the config value.

**`GsrInvalidProtobufMessageTypeError`**
- *When thrown:* `protobufMessageType` is set to a value outside `{POJO, DYNAMIC_MESSAGE}` at `parseConfig` time.
- *How to handle:* Fix the config value.

**`GsrRegistrationError`**
- *When thrown:* The registration or poll path failed. Wraps an underlying `@aws-sdk/client-glue` error, a poll timeout, or an unexpected schema-version status. The original error is preserved on the standard `.cause` property (Node native, ES2022).
- *How to handle:* Inspect `.cause` for the underlying failure — a Glue service error, a network timeout, or an unexpected schema-version status — and decide whether to retry, back off, or surface. Transient SDK errors on `.cause` are worth retrying; a `ValidationException` on `.cause` usually means fix-forward.

### `@gsr/serde` — data-fault taxonomy

These errors are exported by `@gsr/serde` (not this package) and are listed here so the customer error surface is complete in one place. Base class is the built-in `Error`.

**`JsonSchemaValidationError`**
- *When thrown:* A JSON instance fails Draft-07 validation against its schema during encode/decode. The full `ajv` error array is preserved on the `.errors` property.
- *How to handle:* Read `.errors` for the exact field-level failures. This is a caller data fault — no bytes are emitted (encode) or the decoded instance is rejected (decode).

**`GsrInvalidCompatibilityModeError`**
- *When thrown:* A compatibility-mode string passed to the serde-side compatibility helpers falls outside the case-exact 8-mode set. Distinct from the wire-taxonomy `GsrIncompatibleDataError` — this is a caller configuration fault, not a bytes fault.
- *How to handle:* Fix the compatibility-mode value. See the `compatibility` row in the config table above for the accepted set.

### Classifying Glue SDK errors — `classifyGlueError`

The registrar path in this package does not throw `@aws-sdk/client-glue` errors verbatim — it inspects them via `classifyGlueError(err) → GlueErrorKind`, where `GlueErrorKind` is `"entity-not-found" | "already-exists" | "other"`. The classifier reads `err.name` (AWS SDK v3 sets `.name` on every service-error instance to the exact Smithy shape name), which keeps this module independent of the SDK while remaining faithful to the same wire-level discriminant a Java or Go caller would branch on.

A caller who is instrumenting the registration path (e.g. counting the entity-not-found → create-schema fall-through vs the already-exists concurrent-producer race) can use `classifyGlueError` directly on a caught error rather than importing SDK exception classes.

```ts
import { classifyGlueError } from "@gsr/core";

try {
  await register(schema);
} catch (err) {
  const kind = classifyGlueError(err);
  if (kind === "entity-not-found") {
    // schema is not yet registered; the client will fall through to CreateSchema
  } else if (kind === "already-exists") {
    // concurrent producer registered it first; the client will fall through to RegisterSchemaVersion
  } else {
    // propagate — the client will wrap it in GsrRegistrationError on `.cause`
  }
}
```

Non-`Error` inputs (null, undefined, strings, plain objects) return `"other"`.

## Wire format

The wire format is defined by the Java-canonical GSR reference and this TypeScript implementation targets that byte layout. See [Evidence of byte-identity](#evidence-of-byte-identity) below for the scope of the tested parity claim. Every message is:

```
+---------+---------+----------------------------------+---------+
| version | compr.  |       schema-version UUID        | payload |
| 1 byte  | 1 byte  |            16 bytes              | N bytes |
+---------+---------+----------------------------------+---------+
```

- **Version byte** — always `0x03` (`WIRE_FORMAT_VERSION_BYTE`). A different value on the wire causes `decodeMessage` to throw `GsrIncompatibleDataError`.
- **Compression byte** — one of:
  - `0x00` (`COMPRESSION_BYTE_NONE`) — the payload is not compressed.
  - `0x05` (`COMPRESSION_BYTE_ZLIB`) — the payload is zlib-deflated.
  Any other value causes `decodeMessage` to throw `GsrIncompatibleDataError`.
- **Schema-version UUID** — 16 raw bytes. `SCHEMA_VERSION_ID_SIZE` is exported as `16`.
- **Payload** — format-specific body written by the serde layer (Avro binary, protobuf wire format prefixed by a message-index varint, or a canonicalized JSON document).

Total header size is 18 bytes (`WIRE_FORMAT_HEADER_SIZE`). Exported helpers:

- `encodeMessage`, `decodeMessage` — end-to-end message codec (header + payload).
- `encodeWireHeader`, `decodeWireHeader` — header-only codec, useful for callers wiring their own transport.
- Compression helpers over `CompressionType` = `"NONE" | "ZLIB"`.
- Message-index helpers for the protobuf case (a varint-prefixed message-set index precedes the protobuf body).

The wire constants are exported so callers instrumenting or logging on the byte layer do not need to hard-code them:

```ts
import {
  WIRE_FORMAT_VERSION_BYTE,
  COMPRESSION_BYTE_NONE,
  COMPRESSION_BYTE_ZLIB,
  WIRE_FORMAT_HEADER_SIZE,
  SCHEMA_VERSION_ID_SIZE,
} from "@gsr/core";
```

### Evidence of byte-identity

The byte-identity claim is proven offline against a committed set of Java-canonical golden vectors captured by the `golden-gen-java` tool (see [`testdata/golden-java/PROVENANCE.md`](../../../testdata/golden-java/PROVENANCE.md) for the full evidence-scope note). Concretely, the offline gate covers:

- 18 golden cells that collapse to 14 distinct SHA-256 wire-byte vectors (Avro Generic/Specific share bytes; Protobuf concrete/dynamic share bytes).
- **Avro**: one record shape (`avro-test-v1.avsc`), Generic and Specific reader types, `NONE` and `ZLIB` compression.
- **Protobuf**: one message type (`google.protobuf.StringValue`, single field `string value = 1`), concrete and dynamic reader types, `NONE` and `ZLIB` compression. Multi-field field-ordering divergence from the Java canonical is not probed by these vectors.
- **JSON-Schema**: five Draft-07 schema shapes × `NONE` and `ZLIB` compression.
- Vectors were captured on `2026-07-23` at a pinned reference-monorepo SHA and are committed alongside this repo. The gate re-runs against those committed bytes; live cross-language parity is exercised by the Tier-3 interop suites and the narrated demo, not by this offline gate.

Broadening the evidence (a second Avro record shape, a multi-field Protobuf message, or a fresher capture) is a one-command regeneration of the golden-vector set — see [`PROVENANCE.md`](../../../testdata/golden-java/PROVENANCE.md).

## What this package does not do

- **Serialization / deserialization.** Format-specific encode and decode live in `@gsr/serde` (Avro, Protobuf, JSON-Schema, plus the `GsrSerializer` / `GsrDeserializer` facade). This package owns the header and the byte codec around it, not the payload.
- **Schema evolution.** Reader-schema Avro projection and compatibility-mode helpers live in `@gsr/serde`.

## Related runbooks

- [`@gsr/serde` README](../serde/README.md) — customer-facing quick-start:
  serialize / deserialize round-trip, per-format guides (Avro, Protobuf,
  JSON-Schema), compression, and schema-name strategy selection.
- [Narrated interop demo](../integration-tests/demo/README.md) — 21
  cross-language wire-format round-trips against real AWS Glue + a Java
  sidecar + Kafka, each scenario printing its schema body, wire-byte hex
  dump, and PASS/FAIL line. Runs from `npm run demo:interop:real`.
- [Real-AWS integration tier runbook](../integration-tests/README-REAL-AWS.md)
  — Tier-3 real-Glue round-trip; the terser sibling to the narrated demo.
- [Publishing runbook](../PUBLISHING.md) — private-registry and public
  `npm` release paths, prerequisites, and gates.
