# @gsr/serde

Avro, Protobuf, and JSON-Schema serializers and deserializers for the AWS
Glue Schema Registry (GSR), wire-compatible with the Java and Go GSR
clients.

`@gsr/serde` is the customer-facing entry point. It composes the per-format
payloads with the byte-proven `@gsr/core` wire codec, so every message this
package produces uses the same 18-byte header layout the reference clients
consume — and vice versa.

## Install

The publish paths this repository supports (private-registry and
public-npm-registry) are documented in
[`../PUBLISHING.md`](../PUBLISHING.md). If you are consuming from a
private registry, configure your `npm` client to point at that
registry (via `~/.npmrc` or `--registry`) before running the install
below.

Install both packages together:

```
npm install @gsr/serde @gsr/core
```

`@gsr/core` is a peer dependency of `@gsr/serde`; the two packages are
versioned together. Each package publishes with `publishConfig.access =
"restricted"` — a restricted-scope install by default. On a public-npm
release, `npm publish` runs with `--access public` against the public
registry from the same tarballs.

### Dual ESM + CJS builds

Each package ships as a dual build: a `.mjs` ESM bundle, a `.cjs` CommonJS
bundle, and matching `.d.ts` / `.d.cts` type declarations. Node ≥ 20 picks
the right entry via the package `exports` map, so both styles work
out of the box:

```ts
// ESM (TypeScript, or Node with `"type": "module"` / .mjs files)
import { GsrSerializer, GsrDeserializer, DataFormat } from "@gsr/serde";
import { parseConfig } from "@gsr/core";
```

```js
// CommonJS (Node with `"type": "commonjs"` / .cjs / .js files)
const { GsrSerializer, GsrDeserializer, DataFormat } = require("@gsr/serde");
const { parseConfig } = require("@gsr/core");
```

The two entry points expose the exact same runtime surface; the choice is
purely how your project loads modules.

## Prerequisites

- Node.js **20 or newer**. Both packages declare `engines.node = ">=20"`.
  Older Node versions will refuse to install under strict `engine-strict=true`
  configurations.

## Quick-start — full round-trip

The block below is copied verbatim from
[`examples/quick-start.ts`](../../examples/quick-start.ts). It runs offline
and end-to-end: build a config, encode an Avro record into the GSR wire
format, hand the resulting `Buffer` to a matching deserializer, and confirm
the recovered record equals the input.

```ts
import { strict as assert } from "node:assert";

import { parseConfig } from "@gsr/core";
import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
  type DeserializeRequest,
  type SerializeRequest,
} from "@gsr/serde";

// 1. Build a config. `parseConfig` accepts a flat string map and applies
//    the same defaults the client uses in production (see the config-key
//    reference in the `@gsr/core` README).
const config = parseConfig({
  region: "us-east-2",
  "registry.name": "default-registry",
  compression: "NONE",
});

// The parsed config is used to configure the Glue seam in production. The
// serializer only needs the compression mode, which we thread through
// explicitly below.
console.log(
  `Using region=${config.region} registry=${config.registryName} compression=${config.compressionType}`,
);

// 2. Define an Avro schema and a matching record. The schema is the same
//    shape the deserializer will use to interpret the wire payload.
const orderSchema = {
  type: "record",
  name: "Order",
  namespace: "example.orders",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
    {
      name: "status",
      type: {
        type: "enum",
        name: "Status",
        symbols: ["OPEN", "PAID", "CANCELLED"],
      },
    },
  ],
};

interface OrderRecord {
  id: string;
  amount: number;
  quantity: number;
  status: "OPEN" | "PAID" | "CANCELLED";
}

const originalRecord: OrderRecord = {
  id: "ord-42",
  amount: 19.99,
  quantity: 3,
  status: "PAID",
};

// 3. Serialize. `schemaVersionId` is the Glue `SchemaVersionId` UUID; the
//    client writes it verbatim into the 18-byte wire header.
const serializer = new GsrSerializer({ compression: "NONE" });

const serializeRequest: SerializeRequest = {
  format: DataFormat.AVRO,
  schemaVersionId: "01020304-0506-0708-090a-0b0c0d0e0f10",
  topic: "orders",
  schema: orderSchema,
  data: originalRecord,
};

const wireMessage: Buffer = serializer.serialize(serializeRequest);
console.log(`Encoded ${wireMessage.length}-byte GSR wire message.`);

// 4. Deserialize. `GsrDeserializer` re-reads the header, applies any
//    configured decompression, and hands the payload to the Avro decoder.
const deserializer = new GsrDeserializer();

const deserializeRequest: DeserializeRequest = {
  format: DataFormat.AVRO,
  data: wireMessage,
  schema: orderSchema,
};

const recovered = deserializer.deserialize(deserializeRequest) as OrderRecord;

// 5. Confirm the round-trip recovered the original record.
assert.deepStrictEqual(recovered, originalRecord);
console.log("Recovered record matches the input:", recovered);
```

To run it:

```
npx tsx examples/quick-start.ts
```

## Configuring the Glue registry client

`parseConfig` accepts a flat `Record<string, string>` — the same shape the
Java and Go GSR clients accept, so a caller can port a Java config
verbatim. Every entry below is a real key `parseConfig` reads:

```ts
import { parseConfig } from "@gsr/core";

const config = parseConfig({
  region:            "us-east-2",       // AWS region (default "us-east-2")
  endpoint:          "",                // Custom Glue endpoint override (unset by default)
  "registry.name":   "default-registry", // Glue registry name (default "default-registry")
  compatibility:     "BACKWARD",        // Evolution compatibility mode (default "BACKWARD")
  compression:       "ZLIB",            // Wire-payload compression: NONE | ZLIB (default NONE)
  timeToLiveMillis:  "3600000",         // In-memory cache TTL, ms (default 86400000)
  cacheSize:         "500",             // In-memory cache size (default 200)
  schemaAutoRegistrationEnabled: "true", // Accepted and stored on the parsed config; NOT plumbed through the serde facade's serialize path today — caller supplies schemaVersionId (see "A note on schemaVersionId"). Default false.
  "tags.env":        "prod",            // Every tags.<k>=<v> is collected into config.tags
});

// config.region === "us-east-2", config.registryName === "default-registry",
// config.compatibility === "BACKWARD", config.tags === { env: "prod" }, …
```

The complete configuration-key reference — every accepted key, its
default, its type, and the exact error class thrown on invalid input —
lives in the [`@gsr/core` README](../core/README.md#configuration-reference).
`parseConfig` performs no network I/O; it just applies defaults, validates
enums and integers, and throws typed errors on invalid values.

## The three formats

`GsrSerializer` and `GsrDeserializer` route on the `DataFormat` tag on each
request:

| Format tag              | Schema shape                                    | Options field  |
|-------------------------|-------------------------------------------------|----------------|
| `DataFormat.AVRO`       | avsc-parseable object, JSON string, or `avsc.Type` | `avro`         |
| `DataFormat.PROTOBUF`   | `.proto` source text (string)                   | `protobuf`     |
| `DataFormat.JSON`       | Draft-07 JSON Schema (object or JSON string)    | *(none)*       |

The caller supplies the schema for both `serialize` and `deserialize` —
there is no live schema fetch. See
[`caller supplies schemaVersionId`](#a-note-on-schemaversionid) below.

### Avro

The Avro serde supports two decode shapes selected per call via
`AvroSerdeOptions`:

- **Generic** (default) — the decoded record is a plain JavaScript object.
- **Specific** — the decoded record retains its `avsc`-compiled shape, which
  is what callers using generated types typically want.

Both shapes read the same wire bytes; only the caller's runtime view
differs. The block below is copied from
[`examples/avro.ts`](../../examples/avro.ts):

```ts
import { strict as assert } from "node:assert";

import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
} from "@gsr/serde";

const SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

// Avro schema with a nested array + enum — the same surface the facade
// unit tests exercise for the general Avro path.
const orderSchema = {
  type: "record",
  name: "Order",
  namespace: "example.orders",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
    {
      name: "tags",
      type: { type: "array", items: "string" },
    },
    {
      name: "status",
      type: {
        type: "enum",
        name: "Status",
        symbols: ["OPEN", "PAID", "CANCELLED"],
      },
    },
  ],
};

interface OrderRecord {
  id: string;
  amount: number;
  quantity: number;
  tags: string[];
  status: "OPEN" | "PAID" | "CANCELLED";
}

const orderRecord: OrderRecord = {
  id: "ord-42",
  amount: 19.99,
  quantity: 3,
  tags: ["urgent", "prime"],
  status: "PAID",
};

// ---- GENERIC decode (default) ---------------------------------------------
const serializer = new GsrSerializer({ compression: "ZLIB" });
const deserializer = new GsrDeserializer();

const genericWire = serializer.serialize({
  format: DataFormat.AVRO,
  schemaVersionId: SCHEMA_VERSION_ID,
  topic: "orders",
  schema: orderSchema,
  data: orderRecord,
});

const genericDecoded = deserializer.deserialize({
  format: DataFormat.AVRO,
  data: genericWire,
  schema: orderSchema,
}) as OrderRecord;

assert.deepStrictEqual(genericDecoded, orderRecord);

// ---- SPECIFIC decode ------------------------------------------------------
// Select the SPECIFIC decode shape via the per-call `avro` option.
const specificDecoded = deserializer.deserialize({
  format: DataFormat.AVRO,
  data: genericWire,
  schema: orderSchema,
  avro: { recordType: "SPECIFIC" },
}) as OrderRecord;
```

#### Reader-schema projection

A reader schema drops fields the reader omits and fills reader defaults for
fields the writer never wrote. Excerpt from
[`examples/avro.ts`](../../examples/avro.ts):

```ts
const readerSchema = {
  type: "record",
  name: "Order",
  namespace: "example.orders",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
    { name: "status", type: { type: "enum", name: "Status", symbols: ["OPEN", "PAID", "CANCELLED"] } },
  ],
};

const projected = deserializer.deserialize({
  format: DataFormat.AVRO,
  data: genericWire,
  schema: orderSchema,
  readerSchema,
}) as Omit<OrderRecord, "tags">;
```

### Protobuf

The Protobuf serde requires two format-specific inputs:

- `schema` is the full `.proto` source text — `protobufjs` parses it.
- `messageFullName` is the fully-qualified message name. A single `.proto`
  can declare multiple messages, and the wire format's per-message-index
  needs to know which one to encode against. Missing or empty
  `messageFullName` raises `GsrMessageTypeNotFoundError`.

The block below is copied from
[`examples/protobuf.ts`](../../examples/protobuf.ts):

```ts
import { strict as assert } from "node:assert";

import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
} from "@gsr/serde";

const SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

// A two-message `.proto` — the sorted BFS order the wire format uses places
// `Customer` at index 0 and `Order` at index 1, so serializing an `Order`
// exercises the nonzero-message-index path.
const ORDERS_PROTO_TEXT = `
syntax = "proto3";
package example;

message Customer {
  string name = 1;
  int64  id   = 2;
}

message Order {
  string   sku      = 1;
  int32    quantity = 2;
  Customer buyer    = 3;
}
`;

interface OrderMessage {
  sku: string;
  quantity: number;
  buyer: { name: string; id: number | string };
}

const orderRecord: OrderMessage = {
  sku: "SKU-42",
  quantity: 3,
  buyer: { name: "Alice", id: 100 },
};

const serializer = new GsrSerializer({ compression: "NONE" });
const deserializer = new GsrDeserializer();

const wire = serializer.serialize({
  format: DataFormat.PROTOBUF,
  schemaVersionId: SCHEMA_VERSION_ID,
  topic: "orders",
  schema: ORDERS_PROTO_TEXT,
  messageFullName: "example.Order",
  data: orderRecord,
});

// ---- DYNAMIC decode (default) ---------------------------------------------
// The recovered record is a plain JS object built by protobufjs's
// `Type.toObject`. int64 fields are surfaced by protobufjs as strings by
// default, which is why `id` may come back as `"100"` rather than 100.
const dynamicDecoded = deserializer.deserialize({
  format: DataFormat.PROTOBUF,
  data: wire,
  schema: ORDERS_PROTO_TEXT,
  messageFullName: "example.Order",
}) as OrderMessage;

// ---- POJO decode ----------------------------------------------------------
// The `POJO` decode shape returns the concrete protobufjs message instance
// rather than the plain-object conversion.
const pojoDecoded = deserializer.deserialize({
  format: DataFormat.PROTOBUF,
  data: wire,
  schema: ORDERS_PROTO_TEXT,
  messageFullName: "example.Order",
  protobuf: { messageType: "POJO" },
}) as OrderMessage;

assert.equal(pojoDecoded.sku, orderRecord.sku);
```

### JSON-Schema

The JSON serde encodes the record as UTF-8 JSON bytes and validates the
instance against a Draft-07 schema on both encode and decode. A record that
fails validation raises `JsonSchemaValidationError`. Excerpt from
[`examples/json-schema.ts`](../../examples/json-schema.ts):

```ts
import { strict as assert } from "node:assert";

import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
  JsonSchemaValidationError,
} from "@gsr/serde";

const SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

const orderJsonSchema = {
  $schema: "http://json-schema.org/draft-07/schema#",
  type: "object",
  required: ["orderId", "total"],
  properties: {
    orderId: { type: "string", minLength: 1 },
    total: { type: "number", minimum: 0, multipleOf: 0.01 },
    tags: { type: "array", items: { type: "string" } },
  },
  additionalProperties: false,
};

interface OrderJson {
  orderId: string;
  total: number;
  tags?: string[];
}

const orderInstance: OrderJson = {
  orderId: "ord-2026-0001",
  total: 12.34,
  tags: ["premium", "expedited"],
};

const serializer = new GsrSerializer({ compression: "NONE" });
const deserializer = new GsrDeserializer();

// ---- Happy path -----------------------------------------------------------
const wire = serializer.serialize({
  format: DataFormat.JSON,
  schemaVersionId: SCHEMA_VERSION_ID,
  topic: "orders",
  schema: orderJsonSchema,
  data: orderInstance,
});

const decoded = deserializer.deserialize({
  format: DataFormat.JSON,
  data: wire,
  schema: orderJsonSchema,
}) as OrderJson;

assert.deepStrictEqual(decoded, orderInstance);

// ---- Validation failure ---------------------------------------------------
// An instance that violates the schema raises `JsonSchemaValidationError`.
// Here we send a negative total, which the schema forbids (`minimum: 0`).
const invalidInstance = {
  orderId: "ord-2026-0002",
  total: -1,
};

try {
  serializer.serialize({
    format: DataFormat.JSON,
    schemaVersionId: SCHEMA_VERSION_ID,
    topic: "orders",
    schema: orderJsonSchema,
    data: invalidInstance,
  });
  throw new Error("expected JsonSchemaValidationError");
} catch (err) {
  assert.ok(err instanceof JsonSchemaValidationError);
}
```

## Compression

Compression is selected once per serializer via `SerializerConfig.compression`
and applied to every message that serializer produces:

```ts
// From examples/avro.ts — ZLIB compression on every message.
const serializer = new GsrSerializer({ compression: "ZLIB" });
```

Accepted values are `"NONE"` (default) and `"ZLIB"`. The compression byte
occupies wire position 1 of the 18-byte header: `0x00` for `NONE` and `0x05`
for `ZLIB`. The deserializer reads the byte and applies the matching
decompression automatically — the caller does not configure it separately.

## Schema-name strategy

`SerializerConfig.schemaNameStrategy` selects how the serializer derives the
GSR schema name from the transport topic and the record. Two strategies
ship with the package:

- **`DefaultSchemaNameStrategy`** — returns the topic verbatim, ignoring the
  record. Widest-compatibility choice; matches the Java and Go references'
  default. This is the strategy used when `schemaNameStrategy` is omitted.
- **`RecordNameStrategy`** — composes the topic with a best-effort record
  name derived from the record's format-specific identifier
  (`data.$type.fullName` for Protobuf, `data.$id`/`data.title` for JSON
  Schema, `data.getSchema().name` for Avro Specific). Falls back to the
  topic alone when no identifier is present. Produces schema names of the
  form `<topic>-<record>`.

The `SerializerConfig` shape is:

```ts
export interface SerializerConfig {
  compression?: CompressionType;      // "NONE" | "ZLIB", default "NONE"
  schemaNameStrategy?: SchemaNameStrategy; // default DefaultSchemaNameStrategy
}
```

Callers implementing a custom strategy provide any object satisfying
`SchemaNameStrategy`:

```ts
export interface SchemaNameStrategy {
  getSchemaName(data: unknown, topic: string): string;
}
```

## Schema evolution

Schema evolution has two moving parts in this client:

**1. Reader-schema projection (Avro-only).** The Avro deserializer can
apply a *reader* schema that differs from the *writer* schema — Avro
drops fields the reader omits and fills reader defaults for fields the
writer never wrote. The block excerpted under
[Reader-schema projection](#reader-schema-projection) above shows the
end-to-end flow; the projection helper is also exposed directly for
callers that already have a decoded Avro record and just want to
re-project it:

```ts
import { projectAvroPayload, type AvroProjectionOptions } from "@gsr/serde";
```

Reader-schema projection is Avro-only. JSON-Schema and Protobuf handle
evolution via their own schema-level compatibility rules (additive fields,
optional fields, defaults) and do not need a separate reader-schema hook.

**2. Compatibility modes.** GSR checks whether a proposed new schema
version is compatible with the previously registered versions of the same
schema by applying a compatibility *mode* set on the schema. The mode is
enforced by the Glue service at `RegisterSchemaVersion` time — this
client's role is **acceptance + validation + carry**: it recognises the
eight mode strings the service accepts, rejects anything else
case-exactly, and passes the chosen mode through on the config.

```ts
import {
  CompatibilityMode,
  DEFAULT_COMPATIBILITY_MODE,
  parseCompatibilityMode,
  GsrInvalidCompatibilityModeError,
} from "@gsr/serde";

// DEFAULT_COMPATIBILITY_MODE === CompatibilityMode.BACKWARD.
// The 8-mode set is:
//   NONE, DISABLED, BACKWARD, BACKWARD_ALL, FORWARD, FORWARD_ALL, FULL, FULL_ALL.
const mode = parseCompatibilityMode("BACKWARD_ALL");
// mode === CompatibilityMode.BACKWARD_ALL === "BACKWARD_ALL"

try {
  parseCompatibilityMode("backward"); // case-exact — lower-case is rejected
} catch (err) {
  // err instanceof GsrInvalidCompatibilityModeError
}
```

The set is intentionally case-exact — `"backward"` is rejected, only
`"BACKWARD"` is accepted. This mirrors the Java `Compatibility.valueOf`
behaviour and the Go reference's `validateCompatibility` sentinel, so a
config that validates here validates on every reference client.

Compatibility mode is also exposed through `parseConfig` as the
`compatibility` key (see the
[configuration reference](../core/README.md#configuration-reference)) —
`parseCompatibilityMode` is the direct hook for callers that already have
a mode string in hand and want the same case-exact check with no full
config parse.

## A note on `schemaVersionId`

Every `SerializeRequest` carries a `schemaVersionId` (a UUID string) that
`@gsr/serde` writes verbatim into the 18-byte wire header. **The caller
supplies this value** — in production it is the `SchemaVersionId` returned
by Glue when the schema is registered. Live Glue registration through this
client is a later capability. Today, callers register schemas out-of-band
(via the AWS Console, CLI, SDK, or the Java or Go GSR clients) and pass the
resulting UUID into each request.

> **Documented limitation — `schemaAutoRegistrationEnabled`.** The
> configuration key is accepted by `parseConfig` and stored on the parsed
> `GsrConfig`, but the serde facade (`GsrSerializer.serialize`) does NOT
> currently invoke Glue on an unknown schema — it always uses the
> caller-supplied `schemaVersionId`. Auto-registration through the
> serializer facade is a later capability; the caller-supplied
> `schemaVersionId` path above is the supported path today.

## Configuration keys and error types — see `@gsr/core`

`@gsr/serde` re-uses two surfaces from `@gsr/core`:

- **Configuration** — `parseConfig` accepts a flat string map (the same
  shape the Java and Go GSR clients accept) and returns a validated
  `GsrConfig`. The complete reference for the accepted keys, their defaults,
  and their meanings lives in the [`@gsr/core` README](../core/README.md).
- **Error taxonomy** — wire-format failures surface as
  `GsrIncompatibleDataError` (uninterpretable bytes) or
  `GsrMessageTypeNotFoundError` (missing/unresolvable Protobuf message
  name). Configuration-validation failures raise typed subclasses of
  `GsrError` (e.g. `GsrInvalidCompressionTypeError`,
  `GsrInvalidCompatibilityError`). See the [`@gsr/core` README](../core/README.md)
  for the complete catalogue and each error's trigger.

`@gsr/serde` itself contributes two error types:

- `JsonSchemaValidationError` — raised by the JSON-Schema serde when the
  instance fails Draft-07 validation.
- `GsrInvalidCompatibilityModeError` — raised by
  `parseCompatibilityMode` when the compatibility-mode string is outside
  the case-exact 8-mode set (config fault, distinct from wire taxonomy).

## Runnable examples

Every code block above is copied from one of the five example programs in
[`multilang-schema-registry/typescript/examples/`](../../examples/):

- `quick-start.ts` — the round-trip in this README, from the top.
- `avro.ts` — Generic / Specific decode + reader-schema projection.
- `protobuf.ts` — two-message `.proto`, Dynamic + POJO decode.
- `json-schema.ts` — happy path and `JsonSchemaValidationError` demo.
- `config-and-errors.ts` — `parseConfig` walk-through plus the error
  taxonomy (see the [`@gsr/core` README](../core/README.md) for details).

Each program type-checks against the real published types under
`npm run docs:examples:typecheck` and can be executed directly:

```
npx tsx examples/quick-start.ts
```

## IAM permissions

`@gsr/serde` itself is offline — `GsrSerializer.serialize()` and
`GsrDeserializer.deserialize()` never call Glue. Every request carries a
caller-supplied `schemaVersionId` (see
[the note above](#a-note-on-schemaversionid)) and the schema is passed
in each call, so the serde codepath needs no AWS permissions.

The Glue calls happen in `@gsr/core`, specifically when a caller uses
`SchemaRegistrar` to register schemas or resolve a `schemaVersionId` from
Glue. Depending on which paths a caller exercises, the following actions
are needed (scope to your target region):

**Register + resolve** (the register-then-serialize flow):

```
glue:GetSchemaByDefinition   # definition → schema-version lookup
glue:CreateSchema            # first-registration path (concurrent-producer race falls through to RegisterSchemaVersion)
glue:RegisterSchemaVersion   # new-version registration for an already-created schema
glue:GetSchemaVersion        # version-id → schema (poll target; also the deserialize resolve path)
```

**Optional — transport-metadata tagging** (`SchemaRegistrar` sets a
transport-name metadata entry on each schema version):

```
glue:PutSchemaVersionMetadata
glue:QuerySchemaVersionMetadata
```

**Deserialize-only** (the caller already has `schemaVersionId` in hand):

```
glue:GetSchemaVersion        # version-id → schema
```

Registries themselves are assumed to exist — the client never calls
`CreateRegistry` or `DeleteRegistry`. Create your registry once out of
band (via the AWS Console, CLI, or SDK) and reference it by name via
the `registry.name` config key (default: `default-registry`, which Glue
auto-creates on the first `CreateSchema` call).

The narrated interop demo (see [Related runbooks](#related-runbooks))
exercises a superset of these actions because it also drives the Java
sidecar's `KafkaSerializer` / `KafkaDeserializer` and the schema-name
lookup by ARN — see
[the demo runbook's IAM section](../integration-tests/demo/README.md#prereqs)
for the extended list.

## Related runbooks

- [Narrated interop demo](../integration-tests/demo/README.md) — 21
  cross-language wire-format round-trips against real AWS Glue + a Java
  sidecar + Kafka, each scenario printing its schema body, wire-byte hex
  dump, and PASS/FAIL line so the compatibility claim is legible without
  reading test source. Runs from `npm run demo:interop:real`.
- [Real-AWS integration tier runbook](../integration-tests/README-REAL-AWS.md)
  — Tier-3 real-Glue round-trip. Same env-var gates as the demo, terser
  PASS/FAIL output.
- [Cross-language interop tier runbook](../integration-tests/README-INTEROP.md)
  — Tier-3 real-Glue + Java sidecar + Kafka, as terse vitest
  PASS/FAIL. Same interop the demo narrates.
- [Publishing runbook](../PUBLISHING.md) — private-registry and public
  `npm` release paths, prerequisites, and gates. Operator-gated; no
  automated publish runs from this repository.

## For maintainers only — internal harness

The `packages/integration-tests` workspace package (marked `"private": true`)
hosts the tiered integration, interop, demo, and benchmark harness used by
maintainers of this repository. It is **not customer surface** — it is not
published, its APIs are not stable, and it is not intended for downstream
consumption. Application code should depend only on `@gsr/serde` and
`@gsr/core`.
