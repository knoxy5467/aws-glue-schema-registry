# Java ↔ Go Interop Sidecar

Phase 4.6 of the GSR Golang client port (plan §5.5).

A small JVM process that runs the **real** `aws-glue-schema-registry` Java
library and exposes its wire-format encode/decode helpers over HTTP, so the
Go client's wire-format envelope can be cross-checked against the Java
reference implementation **live** rather than against checked-in byte
fixtures that drift the moment Java upstream nudges anything.

The sidecar **never calls AWS Glue**. It only exercises the wire-format
layer (header bytes + compression). Schema-by-UUID lookups go against an
in-memory map populated by the `/encode` request itself; the UUID the test
chooses is what ends up in the GSR header.

## Prerequisites

| Tool   | Version    | Why                                                                                          |
|--------|------------|----------------------------------------------------------------------------------------------|
| JDK    | 17+        | Builds the sidecar and runs it in `GSR_INTEROP_MODE=local`. Corretto 17 on the dev host works. |
| Maven  | 3.9+       | Required by `maven-shade-plugin` 3.6 + Java 17 toolchain. Yum's 3.0.5 will not work.        |
| Docker | any recent | Optional. Required only for `GSR_INTEROP_MODE=container` and for `make java-sidecar-build`.  |

If Maven isn't on PATH, install it locally:

```sh
mkdir -p ~/.local/opt && cd ~/.local/opt
curl -sSL -O https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz
tar xzf apache-maven-3.9.9-bin.tar.gz
```

The Makefile target `java-sidecar-build` looks for `mvn` at
`~/.local/opt/apache-maven-3.9.9/bin/mvn` by default; override with
`JAVA_MVN=/path/to/mvn` if your install location differs.

## Building

From `native-schema-registry/golang/`:

```sh
make java-sidecar-build
```

This invocation does two things:

1. `mvn -DskipTests package` produces a self-contained fat JAR at
   `integration-tests/java-interop/target/java-interop-sidecar.jar`
   (~65 MB; pulls in `schema-registry-common` + `schema-registry-serde`
   pinned to **1.1.27**, the schema-registry-parent version declared by
   this repo's `/pom.xml`).
2. `docker build` builds a multi-stage image (Maven 3.9 + Eclipse Temurin 17
   to compile, Eclipse Temurin 21 JRE to run) tagged
   `gsr-go-it-java-sidecar:latest`. The container-mode launcher
   (`pkg/javasidecar`) picks the image up by that tag.

Bumping `<gsr.version>` in `pom.xml` requires a corresponding bump on the
surrounding repo's parent `pom.xml` so the sidecar runs against the same
Java library the rest of the multi-module project ships with.

## Running

### Local mode (default)

```sh
java -jar target/java-interop-sidecar.jar --port=0
```

The sidecar listens on a port chosen by `--port` (0 → ephemeral) and prints
`PORT: <n>` on the first stdout line so the Go launcher can scrape it. The
JVM stays alive until SIGTERM.

The Go integration harness invokes this for you when `GSR_INTEROP_MODE` is
unset or `local`. See `integration-tests/pkg/javasidecar`.

### Container mode

```sh
GSR_INTEROP_MODE=container make test-integ-interop-container
```

The Go launcher uses `testcontainers-go` to start
`gsr-go-it-java-sidecar:latest`, map the container's port 8080 to a
host-random port, and tear the container down on `t.Cleanup`.

## HTTP surface

The sidecar exposes two families of endpoints:

- **Wire-format-only** (`/encode`, `/decode`) — no AWS calls; the schema-by-UUID
  map lives in-process. Fast inner-loop check that the two languages agree
  on the GSR header layout.
- **Kafka-in-the-loop** (`/kafka-produce`, `/kafka-consume`) — full real-Glue
  + real-Kafka path. The producer registers schemas with **real AWS Glue**;
  the consumer resolves UUIDs against **real AWS Glue**. These exercise the
  scenario the harness exists for: a Java producer and a Go consumer (or
  the reverse) talking to the same Glue registry across a Kafka topic.

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

The payload arrives **pre-serialized by the Go side** — the sidecar does
not run format-specific Avro/JSON/Protobuf record serialization. The
purpose of the harness is wire-format parity, not format-record parity;
adding a Java-side record serializer would force a second cross-language
contract that isn't what these tests are here to prove.

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
`null` when the UUID has never been seen by `/encode` — that's expected
for tests that exercise the decode path in isolation.

### `POST /kafka-produce`

Bills AWS. Request:

```json
{
  "format":      "AVRO" | "JSON" | "PROTOBUF",
  "schema":      "<schema definition string>",
  "schemaName":  "<schema name>",
  "payload":     "<base64 of pre-encoded record bytes>",
  "compression": "NONE" | "ZLIB",
  "bootstrap":   "<kafka bootstrap servers>",
  "topic":       "<kafka topic name>",
  "region":      "<aws region, optional>"
}
```

The handler calls `SchemaByDefinitionFetcher.getORRegisterSchemaVersionId(...)`
against real Glue (auto-registration is on), frames the payload with
`SerializationDataEncoder`, and produces ONE record to the named topic.
Response: `{schemaVersionId, bytes, offset, partition}`.

### `POST /kafka-consume`

Bills AWS. Request:

```json
{
  "bootstrap": "<kafka bootstrap servers>",
  "topic":     "<kafka topic name>",
  "groupId":   "<optional consumer group>",
  "region":    "<aws region, optional>",
  "timeoutMs": 30000
}
```

The handler polls the topic for one record, parses the GSR header with
`GlueSchemaRegistryDeserializerDataParser`, then calls
`AWSSchemaRegistryClient.getSchemaVersionResponse(UUID)` against real
Glue. Response: `{payload, schemaVersionId, schemaDefinition, dataFormat, schemaArn}`.

### `GET /health`

Returns `200 OK` with body `ok`. The Go launcher polls this after parsing
`PORT: <n>` from stdout so callers only proceed once the JVM is ready.

## In-memory schema store

To bypass AWS Glue, the sidecar maintains a `ConcurrentHashMap<UUID, Schema>`
populated by each `/encode` call. This implements the same contract the
Java GSR client uses against Glue — `GetSchemaByDefinition` / `GetSchemaVersion`
both ultimately resolve `UUID → Schema` — but does it in-process. The
trade-off is that decode requests against a UUID the sidecar has never seen
return `null` schema metadata; the Go tests that need full metadata always
pre-seed via `/encode` first.

## Why HTTP rather than direct JNI / embedded JVM

The plan considered embedding the Java library in-process via a CGO bridge
(the original GraalVM `libnativeschemaregistry.so` approach). Phase 2 of
this plan explicitly **deletes** that path. The sidecar is the
out-of-process replacement: cross-language parity stays a live property of
the build, the Go client stays pure Go, and adding a new format to the
matrix doesn't require touching CGO.
