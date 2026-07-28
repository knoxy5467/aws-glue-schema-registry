# Java sidecar for the TypeScript client's cross-language interop tier

A small JVM process that runs the **real** `aws-glue-schema-registry` Java
library and exposes its wire-format encode/decode helpers and full Kafka
customer API over HTTP, so the TypeScript client's wire format and Kafka-
in-the-loop behavior can be cross-checked against the canonical Java
reference implementation **live** rather than against checked-in byte
fixtures that drift the moment Java upstream nudges anything.

The wire-format endpoints (`/encode`, `/decode`) **never call AWS Glue**.
They only exercise the wire-format layer (header bytes + compression).
Schema-by-UUID lookups go against an in-memory map populated by the
`/encode` request itself; the UUID the test chooses is what ends up in the
GSR header.

The Kafka-in-the-loop endpoints (`/kafka-produce`, `/kafka-consume`) drive
the full Kafka customer API — `GlueSchemaRegistryKafkaSerializer.serialize`
and `GlueSchemaRegistryKafkaDeserializer.deserialize` — against real AWS
Glue and real Kafka. These bill AWS.

## Prerequisites

| Tool   | Version | Why                                                                       |
|--------|---------|---------------------------------------------------------------------------|
| JDK    | 17+     | Builds the sidecar and runs it. Any Java 17+ distribution works.          |
| Maven  | 3.9+    | Required by `maven-shade-plugin` 3.6 + Java 17 toolchain.                 |

If Maven isn't on PATH, install it locally:

```sh
mkdir -p ~/.local/opt && cd ~/.local/opt
curl -sSL -O https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz
tar xzf apache-maven-3.9.9-bin.tar.gz
export PATH="$HOME/.local/opt/apache-maven-3.9.9/bin:$PATH"
```

## Building

From the integration-tests package
(`multilang-schema-registry/typescript/packages/integration-tests/`):

```sh
npm run build:sidecar
```

or invoke Maven directly:

```sh
mvn -f java-interop/pom.xml -DskipTests package
```

Either command produces a self-contained fat JAR at
`java-interop/target/java-interop-sidecar.jar` (~65 MB; pulls in
`schema-registry-common` + `schema-registry-serde` pinned to **1.1.27**).

All runtime dependencies resolve from public Maven Central; the module
builds standalone with no dependency on any other subtree in the
repository.

## Running

Local mode (only supported mode for this harness):

```sh
java -jar java-interop/target/java-interop-sidecar.jar --port=0
```

The sidecar listens on a port chosen by `--port` (0 → ephemeral) and
prints `PORT: <n>` on the first stdout line so the TS launcher can scrape
it. The JVM stays alive until SIGTERM.

The TS integration harness invokes this for you when the interop tier
runs with the appropriate gates satisfied (`AWS_INTEGRATION=1`,
`GSR_GLUE=real`, JVM + built JAR available).

## HTTP surface

The sidecar exposes two families of endpoints:

- **Wire-format-only** (`/encode`, `/decode`) — no AWS calls; the schema-by-UUID
  map lives in-process. Fast inner-loop check that the two languages agree
  on the GSR header layout.
- **Kafka-in-the-loop** (`/kafka-produce`, `/kafka-consume`) — full real-Glue
  + real-Kafka path. The producer-side endpoint registers schemas with
  **real AWS Glue**; the consumer resolves UUIDs against **real AWS Glue**.

### `POST /encode`

Request:

```json
{
  "format":          "AVRO" | "JSON" | "PROTOBUF",
  "schema":          "<schema definition string>",
  "schemaName":      "<schema name>",
  "schemaVersionId": "<UUID>",
  "payload":         "<base64 of raw format-encoded bytes>",
  "compression":     "NONE" | "ZLIB"
}
```

Response:

```json
{ "bytes": "<base64 of GSR-framed bytes>" }
```

Behavior: calls the real Java `SerializationDataEncoder.write(...)` and
returns the resulting header + body bytes. Records
`schemaVersionId -> Schema` in the in-memory store so a later `/decode`
against the same UUID can return schema metadata too.

The payload arrives **pre-serialized by the caller** — the sidecar does
not run format-specific Avro/JSON/Protobuf record serialization here. The
purpose of the wire-format endpoints is framing parity, not format-record
parity.

### `POST /decode`

Request:

```json
{ "bytes": "<base64 of GSR-framed bytes>" }
```

Response:

```json
{
  "payload":          "<base64 of decompressed raw payload bytes>",
  "schemaVersionId":  "<UUID>",
  "schemaName":       "<name or null>",
  "schemaDefinition": "<def or null>",
  "dataFormat":       "<format or null>"
}
```

Calls the real Java `GlueSchemaRegistryDeserializerDataParser` (header
parse + zlib decompress) and returns the decompressed body plus stored
schema metadata. `schemaName` / `schemaDefinition` / `dataFormat` are
`null` when the UUID has never been seen by `/encode`.

### `POST /kafka-produce`

Bills AWS. Drives the FULL Kafka customer API:
`GlueSchemaRegistryKafkaSerializer.serialize(topic, record)` — the same
library entry point a real Java Kafka producer would use. Format-specific
serialization (Avro / JSON / protobuf message-index prefix) runs here.

Request:

```json
{
  "format":        "AVRO" | "JSON" | "PROTOBUF",
  "schema":        "<schema definition string>",
  "schemaName":    "<schema name>",
  "record":        { /* per-format envelope; see below */ },
  "compression":   "NONE" | "ZLIB",
  "bootstrap":     "<kafka bootstrap servers>",
  "topic":         "<kafka topic name>",
  "region":        "<aws region, optional>",
  "compatibility": "<compatibility setting, optional; default NONE>"
}
```

Per-format `record` envelope:

```json
// AVRO — reconstructed into a GenericRecord using Schema.Parser
{ "fields": { "<name>": <value>, ... } }

// JSON — reconstructed into JsonDataWithSchema
{ "schema": "<jsonSchema>", "payload": "<jsonDoc>" }

// PROTOBUF — reconstructed into a DynamicMessage built from the
// runtime-parsed FileDescriptor (FileDescriptorUtils.protoFileToFileDescriptor)
// and populated via JsonFormat.parser().merge(fieldsJson, builder).
{ "messageTypeFullName": "<test.TestMessage>", "fieldsJson": "<json>" }
```

The handler reconstructs the typed Java record, hands it to
`GlueSchemaRegistryKafkaSerializer.serialize(...)` which registers the
schema with real Glue (auto-registration is on by default in the
sidecar's config), then produces ONE record via a plain
`KafkaProducer<byte[], byte[]>`.

Response: `{schemaVersionId, bytes, offset, partition}`. `schemaVersionId`
is recovered by parsing the GSR header out of `bytes` since the Kafka
Serializer interface doesn't expose the UUID directly.

### `POST /kafka-consume`

Bills AWS. Drives `GlueSchemaRegistryKafkaDeserializer.deserialize(topic,
bytes)` — the symmetric public Kafka-customer entry point.

Request:

```json
{
  "bootstrap": "<kafka bootstrap servers>",
  "topic":     "<kafka topic name>",
  "format":    "AVRO" | "JSON" | "PROTOBUF",
  "groupId":   "<optional consumer group>",
  "region":    "<aws region, optional>",
  "timeoutMs": 30000
}
```

The handler polls one record via plain `KafkaConsumer<byte[], byte[]>`,
hands the bytes to `GlueSchemaRegistryKafkaDeserializer.deserialize(...)`
which fetches the schema definition from real Glue and produces a typed
record (`GenericRecord` / `JsonDataWithSchema` / `DynamicMessage`). The
record is then re-serialized into the same per-format JSON envelope shape
documented under `/kafka-produce` so the caller can assert
field-by-field equality.

Response:

```json
{
  "schemaVersionId":  "<UUID>",
  "dataFormat":       "AVRO" | "JSON" | "PROTOBUF",
  "schemaDefinition": "<def fetched from Glue>",
  "schemaArn":        "<arn>",
  "record":           { /* per-format envelope; see /kafka-produce */ }
}
```

### `GET /health`

Returns `200 OK` with body `ok`. The TS launcher polls this after parsing
`PORT: <n>` from stdout so callers only proceed once the JVM is ready.

## In-memory schema store

To bypass AWS Glue for the wire-format endpoints, the sidecar maintains a
`ConcurrentHashMap<UUID, Schema>` populated by each `/encode` call. This
implements the same contract the Java GSR client uses against Glue —
`GetSchemaByDefinition` / `GetSchemaVersion` both ultimately resolve
`UUID → Schema` — but does it in-process. The trade-off is that decode
requests against a UUID the sidecar has never seen return `null` schema
metadata; callers that need full metadata pre-seed via `/encode` first.

## Why HTTP rather than direct JNI / embedded JVM

An out-of-process HTTP surface keeps cross-language parity a live
property of the build without dragging the JVM into the TS client's own
runtime. The TS client stays pure TypeScript, and adding a new format to
the matrix does not require touching a native bridge.
