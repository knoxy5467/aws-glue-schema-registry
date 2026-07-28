/**
 * Cross-version + same-version scenario drivers for the narrated interop demo
 * (scenarios 1-18 of the 21-scenario catalogue).
 *
 * The scenario driver contract:
 *
 *   1. Print a section header with the narrated "scenario N of 21" number.
 *   2. Track every schema this scenario will register in the shared
 *      `CleanupTracker` BEFORE calling any `CreateSchema` / `RegisterSchemaVersion`
 *      — the tracker's create-scoped-cleanup guarantee only holds when the
 *      name is recorded before it hits Glue.
 *   3. Narrate the schema body and the Glue-returned schema-version UUID.
 *   4. Drive the cross-language round-trip:
 *        - TS→Java direction — TS `GsrSerializer` produces framed bytes,
 *          kafkajs writes them, the Java sidecar's `/kafka-consume` reads.
 *        - Java→TS direction — the Java sidecar's `/kafka-produce` writes
 *          framed bytes, kafkajs reads, TS `GsrDeserializer` decodes against
 *          the writer schema fetched from Glue by the header UUID.
 *   5. Hex-dump the wire bytes via `narrator.printHexDump` so the operator
 *      sees the frozen 18-byte header + payload layout.
 *   6. Assert field-for-field record equality PLUS schema-version-UUID
 *      equality via `narrator.printEqualityCheck` — the returned boolean is
 *      the scenario's PASS/FAIL bit, so the operator's eye and the summary
 *      table always agree by construction.
 *   7. Return a `ScenarioResult` and NEVER throw to abort the run — a caught
 *      error becomes `pass: false` with `error` populated, so the remaining
 *      scenarios still run and `CleanupTracker.run()` still fires.
 *
 * The module owns only the scenario sequencing. It re-implements no wire
 * framing, compression, Glue-registration, sidecar-HTTP, or Kafka client
 * code — every such primitive is imported from the harness modules.
 *
 * ## Key-order-insensitive record equality
 *
 * The shared `narrator.printEqualityCheck` uses `JSON.stringify` for
 * structural comparison, which is key-order sensitive. That is fine for
 * pinned scalars and UUID strings, but a decoded Avro/JSON record's key
 * order is decoder-dependent (avsc traversal order, JSON parser insertion
 * order, protobufjs `toObject` field order) and would produce spurious
 * FAILs when compared against a hand-written expected record.
 *
 * To keep the narrated PASS/FAIL line as the single source of truth
 * without editing narrator.ts, this module canonicalizes both sides by
 * projecting them through `canonicalizeRecord`, which rebuilds the object
 * with keys in a stable sorted order. The narrator then stringifies two
 * key-sorted objects and reports the same equality the caller would get
 * from a key-order-insensitive deep-equal.
 */

import {
  decodeMessage,
  SchemaRegistrar,
  type GlueClient,
  type GsrConfig,
} from "@gsr/core";
import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
} from "@gsr/serde";
import { Buffer } from "node:buffer";

import type { BrokerHandle } from "../src/kafka-broker.js";
import type { Sidecar } from "../src/java-sidecar.js";
import type { CleanupTracker } from "../src/real-glue.js";

import {
  AVRO_SCHEMA_V1,
  AVRO_SCHEMA_V2,
  CUSTOMER_RECORD_V1,
  INTEROP_PROTOBUF_FULL_NAME,
  JSON_SCHEMA_V1,
  JSON_SCHEMA_V2,
  PROTOBUF_SCHEMA_V1,
  PROTOBUF_SCHEMA_V2,
  buildAvroEnvelope,
  buildJsonEnvelope,
  buildProtobufEnvelope,
  readAvroEnvelopeV1,
  readJsonEnvelopeV1,
  readProtobufEnvelopeV1,
  type CustomerRecordV1,
  type DemoSchemaNamer,
} from "./demo-fixtures.js";
import {
  compressionName,
  printEqualityCheck,
  printHexDump,
  printRecord,
  printSchemaBody,
  printScenarioIntro,
  printSectionHeader,
  printStage,
  printVerdict,
  type NarratorSink,
} from "./narrator.js";

/**
 * A single scenario's outcome. Every field is read-only so a returned row
 * cannot be mutated by downstream summary-table code.
 */
export interface ScenarioResult {
  readonly index: number;
  readonly title: string;
  readonly format: "AVRO" | "JSON" | "PROTOBUF";
  readonly direction: string;
  readonly compression: "NONE" | "ZLIB";
  readonly schemaVersionId: string | null;
  readonly pass: boolean;
  readonly error?: string;
}

/**
 * Runtime dependencies threaded from `main.ts` to every scenario. Kept as
 * a single object so a scenario body can accept one parameter and every
 * new scenario picks up new context fields at compile time.
 */
export interface ScenarioContext {
  readonly io: NarratorSink;
  readonly sidecar: Sidecar;
  readonly broker: BrokerHandle;
  readonly registrar: SchemaRegistrar;
  readonly glueClient: GlueClient;
  readonly cleanup: CleanupTracker;
  readonly namer: DemoSchemaNamer;
  readonly region: string;
  readonly cfg: GsrConfig;
  /**
   * A short per-run token appended to every Kafka topic name so parallel
   * or repeated demo runs never collide on a topic. Matches the interop
   * suites' `runSuffix` idiom.
   */
  readonly runSuffix: string;
}

/**
 * Every scenario body has the same signature: consume the shared context,
 * do its round-trip, and hand back one `ScenarioResult`. Async so the
 * whole flow reads linearly under `await`.
 */
export type ScenarioFn = (ctx: ScenarioContext) => Promise<ScenarioResult>;

/**
 * The Glue registry every demo schema lives in. The demo NEVER creates or
 * deletes a registry — every schema goes into Glue's pre-existing default.
 */
export const REGISTRY_NAME = "default-registry";

/**
 * Kafka consumer poll window. Kept generous so a slow test-container
 * broker on a cold CI host does not spuriously mark a scenario FAILED;
 * bounded so a hung consumer surfaces within the demo's wall-clock budget.
 */
const CONSUME_TIMEOUT_MS = 60_000;

/**
 * Canonical UUID shape check for the Glue schema-version IDs. Any deviation
 * (empty string, ARN, garbage) surfaces immediately in the equality-check
 * line instead of hiding inside a "field-for-field" claim.
 */
const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// ---------------------------------------------------------------------------
// Scenario catalogue — one driver per row of the 18-row cross-version matrix
// ({format, direction, compression}), dispatched in order by the array below.
// Scenario 3's narrated direction label is "TS→Java" — the tabular
// `TS->TS/Java` shorthand is only there to make the round-trip readable
// as a table.
// ---------------------------------------------------------------------------

/**
 * The ordered driver array — scenarios 1..18. `main.ts` walks this array
 * in order, prepending any extra scenarios (19-21) implemented in the
 * sibling module. Each entry is a `ScenarioFn`, not the result — the fn
 * is invoked with the shared `ScenarioContext` at dispatch time.
 */
export const SCENARIOS_1_TO_18: readonly ScenarioFn[] = [
  scenario01AvroJavaToTsNone,
  scenario02AvroJavaToTsZlib,
  scenario03AvroTsToJavaNone,
  scenario04AvroTsToJavaZlib,
  scenario05JsonJavaToTsNone,
  scenario06JsonJavaToTsZlib,
  scenario07JsonTsToJavaNone,
  scenario08JsonTsToJavaZlib,
  scenario09ProtobufJavaToTsNone,
  scenario10ProtobufJavaToTsZlib,
  scenario11ProtobufTsToJavaNone,
  scenario12ProtobufTsToJavaZlib,
  scenario13AvroSameVersionTsToJava,
  scenario14AvroSameVersionJavaToTs,
  scenario15JsonSameVersionTsToJava,
  scenario16JsonSameVersionJavaToTs,
  scenario17ProtobufSameVersionTsToJava,
  scenario18ProtobufSameVersionJavaToTs,
];

// ---------------------------------------------------------------------------
// Cross-version — AVRO (rows 1-4)
// ---------------------------------------------------------------------------

async function scenario01AvroJavaToTsNone(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runJavaToTsCrossVersion(ctx, {
    index: 1,
    title: "Cross-version: Java writes v1, TS reads v2 (AVRO / NONE)",
    format: "AVRO",
    compression: "NONE",
    familyLabel: "avroCrossVersion",
    dataFormat: DataFormat.AVRO,
    sidecarFormat: "AVRO",
    schemaV1: AVRO_SCHEMA_V1,
    schemaV2: AVRO_SCHEMA_V2,
  });
}

async function scenario02AvroJavaToTsZlib(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runJavaToTsCrossVersion(ctx, {
    index: 2,
    title: "Cross-version: Java writes v1, TS reads v2 (AVRO / ZLIB)",
    format: "AVRO",
    compression: "ZLIB",
    familyLabel: "avroCrossVersion",
    dataFormat: DataFormat.AVRO,
    sidecarFormat: "AVRO",
    schemaV1: AVRO_SCHEMA_V1,
    schemaV2: AVRO_SCHEMA_V2,
  });
}

async function scenario03AvroTsToJavaNone(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runTsToJavaCrossVersion(ctx, {
    index: 3,
    title: "Cross-version: TS writes v1, Java reads v2 (AVRO / NONE)",
    format: "AVRO",
    compression: "NONE",
    familyLabel: "avroCrossVersion",
    dataFormat: DataFormat.AVRO,
    sidecarFormat: "AVRO",
    schemaV1: AVRO_SCHEMA_V1,
    schemaV2: AVRO_SCHEMA_V2,
  });
}

async function scenario04AvroTsToJavaZlib(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runTsToJavaCrossVersion(ctx, {
    index: 4,
    title: "Cross-version: TS writes v1, Java reads v2 (AVRO / ZLIB)",
    format: "AVRO",
    compression: "ZLIB",
    familyLabel: "avroCrossVersion",
    dataFormat: DataFormat.AVRO,
    sidecarFormat: "AVRO",
    schemaV1: AVRO_SCHEMA_V1,
    schemaV2: AVRO_SCHEMA_V2,
  });
}

// ---------------------------------------------------------------------------
// Cross-version — JSON (rows 5-8)
// ---------------------------------------------------------------------------

async function scenario05JsonJavaToTsNone(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runJavaToTsCrossVersion(ctx, {
    index: 5,
    title: "Cross-version: Java writes v1, TS reads v2 (JSON / NONE)",
    format: "JSON",
    compression: "NONE",
    familyLabel: "jsonCrossVersion",
    dataFormat: DataFormat.JSON,
    sidecarFormat: "JSON",
    schemaV1: JSON_SCHEMA_V1,
    schemaV2: JSON_SCHEMA_V2,
  });
}

async function scenario06JsonJavaToTsZlib(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runJavaToTsCrossVersion(ctx, {
    index: 6,
    title: "Cross-version: Java writes v1, TS reads v2 (JSON / ZLIB)",
    format: "JSON",
    compression: "ZLIB",
    familyLabel: "jsonCrossVersion",
    dataFormat: DataFormat.JSON,
    sidecarFormat: "JSON",
    schemaV1: JSON_SCHEMA_V1,
    schemaV2: JSON_SCHEMA_V2,
  });
}

async function scenario07JsonTsToJavaNone(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runTsToJavaCrossVersion(ctx, {
    index: 7,
    title: "Cross-version: TS writes v1, Java reads v2 (JSON / NONE)",
    format: "JSON",
    compression: "NONE",
    familyLabel: "jsonCrossVersion",
    dataFormat: DataFormat.JSON,
    sidecarFormat: "JSON",
    schemaV1: JSON_SCHEMA_V1,
    schemaV2: JSON_SCHEMA_V2,
  });
}

async function scenario08JsonTsToJavaZlib(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runTsToJavaCrossVersion(ctx, {
    index: 8,
    title: "Cross-version: TS writes v1, Java reads v2 (JSON / ZLIB)",
    format: "JSON",
    compression: "ZLIB",
    familyLabel: "jsonCrossVersion",
    dataFormat: DataFormat.JSON,
    sidecarFormat: "JSON",
    schemaV1: JSON_SCHEMA_V1,
    schemaV2: JSON_SCHEMA_V2,
  });
}

// ---------------------------------------------------------------------------
// Cross-version — PROTOBUF (rows 9-12)
// ---------------------------------------------------------------------------

async function scenario09ProtobufJavaToTsNone(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runJavaToTsCrossVersion(ctx, {
    index: 9,
    title: "Cross-version: Java writes v1, TS reads v2 (PROTOBUF / NONE)",
    format: "PROTOBUF",
    compression: "NONE",
    familyLabel: "protobufCrossVersion",
    dataFormat: DataFormat.PROTOBUF,
    sidecarFormat: "PROTOBUF",
    schemaV1: PROTOBUF_SCHEMA_V1,
    schemaV2: PROTOBUF_SCHEMA_V2,
  });
}

async function scenario10ProtobufJavaToTsZlib(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runJavaToTsCrossVersion(ctx, {
    index: 10,
    title: "Cross-version: Java writes v1, TS reads v2 (PROTOBUF / ZLIB)",
    format: "PROTOBUF",
    compression: "ZLIB",
    familyLabel: "protobufCrossVersion",
    dataFormat: DataFormat.PROTOBUF,
    sidecarFormat: "PROTOBUF",
    schemaV1: PROTOBUF_SCHEMA_V1,
    schemaV2: PROTOBUF_SCHEMA_V2,
  });
}

async function scenario11ProtobufTsToJavaNone(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runTsToJavaCrossVersion(ctx, {
    index: 11,
    title: "Cross-version: TS writes v1, Java reads v2 (PROTOBUF / NONE)",
    format: "PROTOBUF",
    compression: "NONE",
    familyLabel: "protobufCrossVersion",
    dataFormat: DataFormat.PROTOBUF,
    sidecarFormat: "PROTOBUF",
    schemaV1: PROTOBUF_SCHEMA_V1,
    schemaV2: PROTOBUF_SCHEMA_V2,
  });
}

async function scenario12ProtobufTsToJavaZlib(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runTsToJavaCrossVersion(ctx, {
    index: 12,
    title: "Cross-version: TS writes v1, Java reads v2 (PROTOBUF / ZLIB)",
    format: "PROTOBUF",
    compression: "ZLIB",
    familyLabel: "protobufCrossVersion",
    dataFormat: DataFormat.PROTOBUF,
    sidecarFormat: "PROTOBUF",
    schemaV1: PROTOBUF_SCHEMA_V1,
    schemaV2: PROTOBUF_SCHEMA_V2,
  });
}

// ---------------------------------------------------------------------------
// Same-version baseline (rows 13-18)
// ---------------------------------------------------------------------------

async function scenario13AvroSameVersionTsToJava(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runTsToJavaSameVersion(ctx, {
    index: 13,
    title: "Same-version baseline: TS writes, Java reads (AVRO)",
    format: "AVRO",
    familyLabel: "avroSameVersion",
    dataFormat: DataFormat.AVRO,
    sidecarFormat: "AVRO",
    schema: AVRO_SCHEMA_V1,
  });
}

async function scenario14AvroSameVersionJavaToTs(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runJavaToTsSameVersion(ctx, {
    index: 14,
    title: "Same-version baseline: Java writes, TS reads (AVRO)",
    format: "AVRO",
    familyLabel: "avroSameVersion",
    dataFormat: DataFormat.AVRO,
    sidecarFormat: "AVRO",
    schema: AVRO_SCHEMA_V1,
  });
}

async function scenario15JsonSameVersionTsToJava(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runTsToJavaSameVersion(ctx, {
    index: 15,
    title: "Same-version baseline: TS writes, Java reads (JSON)",
    format: "JSON",
    familyLabel: "jsonSameVersion",
    dataFormat: DataFormat.JSON,
    sidecarFormat: "JSON",
    schema: JSON_SCHEMA_V1,
  });
}

async function scenario16JsonSameVersionJavaToTs(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runJavaToTsSameVersion(ctx, {
    index: 16,
    title: "Same-version baseline: Java writes, TS reads (JSON)",
    format: "JSON",
    familyLabel: "jsonSameVersion",
    dataFormat: DataFormat.JSON,
    sidecarFormat: "JSON",
    schema: JSON_SCHEMA_V1,
  });
}

async function scenario17ProtobufSameVersionTsToJava(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runTsToJavaSameVersion(ctx, {
    index: 17,
    title: "Same-version baseline: TS writes, Java reads (PROTOBUF)",
    format: "PROTOBUF",
    familyLabel: "protobufSameVersion",
    dataFormat: DataFormat.PROTOBUF,
    sidecarFormat: "PROTOBUF",
    schema: PROTOBUF_SCHEMA_V1,
  });
}

async function scenario18ProtobufSameVersionJavaToTs(
  ctx: ScenarioContext,
): Promise<ScenarioResult> {
  return runJavaToTsSameVersion(ctx, {
    index: 18,
    title: "Same-version baseline: Java writes, TS reads (PROTOBUF)",
    format: "PROTOBUF",
    familyLabel: "protobufSameVersion",
    dataFormat: DataFormat.PROTOBUF,
    sidecarFormat: "PROTOBUF",
    schema: PROTOBUF_SCHEMA_V1,
  });
}

// ---------------------------------------------------------------------------
// Cross-version driver — TS→Java direction (rows 3, 4, 7, 8, 11, 12)
// ---------------------------------------------------------------------------

interface CrossVersionCell {
  readonly index: number;
  readonly title: string;
  readonly format: "AVRO" | "JSON" | "PROTOBUF";
  readonly compression: "NONE" | "ZLIB";
  readonly familyLabel:
    | "avroCrossVersion"
    | "jsonCrossVersion"
    | "protobufCrossVersion";
  readonly dataFormat: DataFormat;
  readonly sidecarFormat: "AVRO" | "JSON" | "PROTOBUF";
  readonly schemaV1: string;
  readonly schemaV2: string;
}

/**
 * TS writes v1 → Kafka → Java sidecar consumes with v2 registered.
 *
 * The v1/v2 registration is idempotent across scenarios in the same family
 * — the TS `SchemaRegistrar` caches on `${schemaName}:${dataFormat}` and
 * Glue itself de-duplicates schema-version bodies. The tracker's
 * `track(...)` is also idempotent, so registering the same family name
 * twice in a row does not create a duplicate delete on teardown.
 */
async function runTsToJavaCrossVersion(
  ctx: ScenarioContext,
  cell: CrossVersionCell,
): Promise<ScenarioResult> {
  const { io } = ctx;
  const direction = "TS→Java";
  printSectionHeader(io, `[scenario ${cell.index} of 21] ${cell.title}`);
  printScenarioIntro(io, {
    what: `TS ${cell.format} serializer encodes a v1 record with ${cell.compression} compression, kafkajs produces it, the Java sidecar consumes and decodes under a v2 reader schema.`,
    proves: `Cross-version wire-format compatibility (TS writer → Java reader with schema evolution) for ${cell.format} / ${cell.compression}: the same 18-byte GSR header + payload frame TS produces is exactly what the Java client accepts.`,
  });
  let schemaVersionId: string | null = null;
  try {
    const schemaName = ctx.namer.forFamily(cell.familyLabel);
    ctx.cleanup.track(REGISTRY_NAME, schemaName);
    printStage(io, "glue", `schemaName=${schemaName} (${cell.sidecarFormat})`);

    const { v1Uuid, v2Uuid } = await ensureCrossVersionRegistered(
      ctx,
      schemaName,
      cell.sidecarFormat,
      cell.schemaV1,
      cell.schemaV2,
    );
    schemaVersionId = v1Uuid;
    printStage(
      io,
      "glue",
      `v1 schemaVersionId=${v1Uuid}  v2 schemaVersionId=${v2Uuid}`,
    );
    printSchemaBody(io, "v1 body (writer schema)", cell.schemaV1);
    printSchemaBody(
      io,
      "v2 body (registered as reader-latest, not written)",
      cell.schemaV2,
    );

    const topic = topicFor(ctx, `xver-tsjava-${cell.index}`);
    const record = { ...CUSTOMER_RECORD_V1 };
    printRecord(io, "record about to be encoded (v1)", record);

    const serializer = new GsrSerializer({ compression: cell.compression });
    const wire = serializer.serialize({
      format: cell.dataFormat,
      schemaVersionId: v1Uuid,
      topic,
      schema: cell.schemaV1,
      data: record,
      ...(cell.dataFormat === DataFormat.PROTOBUF
        ? { messageFullName: INTEROP_PROTOBUF_FULL_NAME }
        : {}),
    });
    printStage(
      io,
      "encoder",
      `TS GsrSerializer produced ${wire.length} wire bytes (0x03 magic + compression byte + 16-byte UUID + payload)`,
    );
    printHexDump(io, "TS-encoded wire message", wire);
    printStage(
      io,
      "compression",
      `header byte 1 = 0x${(wire[1] as number).toString(16).padStart(2, "0")} (${compressionName(wire[1] as number)})`,
    );

    // Sanity — sidecar UUID vs the wire UUID. If the wire is malformed the
    // sidecar consume would surface a downstream mismatch anyway; catching
    // it here gives an actionable per-scenario diagnostic.
    const decodedHeader = decodeMessage(wire, {
      protobuf: cell.dataFormat === DataFormat.PROTOBUF,
    });
    const wireUuidOk = printEqualityCheck(
      io,
      "wire header UUID matches v1 registration",
      v1Uuid,
      decodedHeader.schemaVersionId,
    );

    printStage(io, "kafka", `producing 1 message to topic ${topic} (bootstrap ${ctx.broker.bootstrap})`);
    await produceOne(ctx.broker.bootstrap, topic, wire);
    printStage(io, "kafka", "produce complete — sidecar consume next");

    const consumeResp = await ctx.sidecar.kafkaConsume({
      bootstrap: ctx.broker.bootstrap,
      topic,
      format: cell.sidecarFormat,
      groupId: `demo-scenario-${cell.index}-${ctx.runSuffix}`,
      region: ctx.region,
      timeoutMs: CONSUME_TIMEOUT_MS,
    });
    printStage(
      io,
      "sidecar",
      `Java sidecar consumed dataFormat=${consumeResp.dataFormat} schemaVersionId=${consumeResp.schemaVersionId}`,
    );

    const decoded = decodeSidecarEnvelopeToV1(cell.sidecarFormat, consumeResp.record);
    printRecord(io, "record decoded by Java sidecar (projected to v1 fields)", decoded);
    const recordOk = printEqualityCheck(
      io,
      "decoded record fields (v1)",
      canonicalizeRecord(record),
      canonicalizeRecord(decoded),
    );
    const uuidOk = printEqualityCheck(
      io,
      "sidecar-reported schemaVersionId matches v1 UUID",
      v1Uuid,
      consumeResp.schemaVersionId,
    );
    const pass = wireUuidOk && recordOk && uuidOk;
    printVerdict(
      io,
      pass,
      pass
        ? `TS ${cell.format}/${cell.compression} v1 encode → Java v2 decode round-trip succeeded (wire UUID, record fields, and sidecar UUID all matched).`
        : `TS ${cell.format}/${cell.compression} v1 encode → Java v2 decode failed: at least one of wire-UUID / record-fields / sidecar-UUID mismatched.`,
    );
    return {
      index: cell.index,
      title: cell.title,
      format: cell.format,
      direction,
      compression: cell.compression,
      schemaVersionId,
      pass,
    };
  } catch (err) {
    return failed(cell, direction, schemaVersionId, err);
  }
}

/**
 * Java sidecar writes v1 → Kafka → TS decodes with the writer schema
 * fetched from Glue via the header UUID (v2 is the "latest" but the
 * header UUID drives the lookup, proving cross-version decode).
 */
async function runJavaToTsCrossVersion(
  ctx: ScenarioContext,
  cell: CrossVersionCell,
): Promise<ScenarioResult> {
  const { io } = ctx;
  const direction = "Java→TS";
  printSectionHeader(io, `[scenario ${cell.index} of 21] ${cell.title}`);
  printScenarioIntro(io, {
    what: `Java sidecar encodes a v1 ${cell.format} record with ${cell.compression} compression, kafkajs consumes it, and TS deserializes with the writer schema fetched from Glue by header UUID.`,
    proves: `Cross-version wire-format compatibility (Java writer → TS reader with schema evolution) for ${cell.format} / ${cell.compression}: TS honors the wire header UUID for writer-schema lookup and decodes even when v2 is registered as the reader-latest.`,
  });
  let schemaVersionId: string | null = null;
  try {
    const schemaName = ctx.namer.forFamily(cell.familyLabel);
    ctx.cleanup.track(REGISTRY_NAME, schemaName);
    printStage(io, "glue", `schemaName=${schemaName} (${cell.sidecarFormat})`);

    const { v1Uuid, v2Uuid } = await ensureCrossVersionRegistered(
      ctx,
      schemaName,
      cell.sidecarFormat,
      cell.schemaV1,
      cell.schemaV2,
    );
    schemaVersionId = v1Uuid;
    printStage(
      io,
      "glue",
      `v1 schemaVersionId=${v1Uuid}  v2 schemaVersionId=${v2Uuid}`,
    );
    printSchemaBody(io, "v1 body (writer schema, stamped in wire header)", cell.schemaV1);
    printSchemaBody(io, "v2 body (reader-latest, registered but not decoded against here)", cell.schemaV2);

    const topic = topicFor(ctx, `xver-javats-${cell.index}`);
    const record = { ...CUSTOMER_RECORD_V1 };
    printRecord(io, "record the Java sidecar will produce (v1)", record);

    // Java sidecar produces a v1 record; the sidecar's serializer resolves
    // v1's schema-version-id (already registered above) and stamps it in
    // the wire header.
    const produceResp = await ctx.sidecar.kafkaProduce({
      format: cell.sidecarFormat,
      schema: cell.schemaV1,
      schemaName,
      record: buildSidecarEnvelope(cell.sidecarFormat, record, cell.schemaV1),
      compression: cell.compression,
      bootstrap: ctx.broker.bootstrap,
      topic,
      region: ctx.region,
      compatibility: "BACKWARD",
    });
    printStage(
      io,
      "sidecar",
      `Java sidecar produced schemaVersionId=${produceResp.schemaVersionId} bytes=${produceResp.bytes.length}`,
    );
    printHexDump(io, "Java-produced wire message", produceResp.bytes);

    printStage(io, "kafka", `consuming from topic ${topic} (bootstrap ${ctx.broker.bootstrap})`);
    const framed = await consumeOne(
      ctx.broker.bootstrap,
      topic,
      `demo-scenario-${cell.index}-${ctx.runSuffix}`,
    );
    printStage(io, "kafka", `consumed ${framed.length} wire bytes — TS decode next`);

    const decodedHeader = decodeMessage(framed, {
      protobuf: cell.dataFormat === DataFormat.PROTOBUF,
    });
    const wireUuidOk = printEqualityCheck(
      io,
      "consumed wire header UUID matches sidecar-produced UUID",
      produceResp.schemaVersionId,
      decodedHeader.schemaVersionId,
    );

    // Fetch writer schema by header UUID — v2 is "latest" but the header
    // UUID points at v1, and that is what decode uses.
    const writerSchemaResp = await ctx.glueClient.getSchemaVersion({
      SchemaVersionId: produceResp.schemaVersionId,
    });
    const writerSchema = writerSchemaResp.SchemaDefinition ?? "";
    printStage(
      io,
      "glue",
      `GetSchemaVersion(${produceResp.schemaVersionId}) => dataFormat=${writerSchemaResp.DataFormat} bodyBytes=${writerSchema.length}`,
    );

    const deserializer = new GsrDeserializer();
    const decoded = deserializer.deserialize({
      format: cell.dataFormat,
      data: framed,
      schema: writerSchema,
      messageFullName:
        cell.dataFormat === DataFormat.PROTOBUF
          ? INTEROP_PROTOBUF_FULL_NAME
          : undefined,
    });
    const decodedV1 = coerceDecodedToV1(cell.dataFormat, decoded);
    printRecord(io, "record decoded by TS GsrDeserializer", decodedV1);

    const recordOk = printEqualityCheck(
      io,
      "decoded record fields (v1)",
      canonicalizeRecord(record),
      canonicalizeRecord(decodedV1),
    );
    const uuidOk = printEqualityCheck(
      io,
      "sidecar-produced schemaVersionId matches v1 UUID",
      v1Uuid,
      produceResp.schemaVersionId,
    );
    const pass = wireUuidOk && recordOk && uuidOk;
    printVerdict(
      io,
      pass,
      pass
        ? `Java ${cell.format}/${cell.compression} v1 encode → TS v2-registered decode round-trip succeeded (wire UUID, record fields, and sidecar UUID all matched).`
        : `Java ${cell.format}/${cell.compression} v1 encode → TS v2-registered decode failed: at least one of wire-UUID / record-fields / sidecar-UUID mismatched.`,
    );
    return {
      index: cell.index,
      title: cell.title,
      format: cell.format,
      direction,
      compression: cell.compression,
      schemaVersionId,
      pass,
    };
  } catch (err) {
    return failed(cell, direction, schemaVersionId, err);
  }
}

// ---------------------------------------------------------------------------
// Same-version driver (rows 13-18)
// ---------------------------------------------------------------------------

interface SameVersionCell {
  readonly index: number;
  readonly title: string;
  readonly format: "AVRO" | "JSON" | "PROTOBUF";
  readonly familyLabel:
    | "avroSameVersion"
    | "jsonSameVersion"
    | "protobufSameVersion";
  readonly dataFormat: DataFormat;
  readonly sidecarFormat: "AVRO" | "JSON" | "PROTOBUF";
  readonly schema: string;
}

/**
 * TS writes v1 → Kafka → Java sidecar consumes with the same v1 schema.
 * The same-version baseline is the byte-identity proof: no schema
 * evolution is involved, so a byte-for-byte round-trip PASS shows the
 * two clients agree on the payload layout under the frozen wire header.
 */
async function runTsToJavaSameVersion(
  ctx: ScenarioContext,
  cell: SameVersionCell,
): Promise<ScenarioResult> {
  const { io } = ctx;
  const direction = "TS→Java";
  const compression: "NONE" = "NONE";
  printSectionHeader(io, `[scenario ${cell.index} of 21] ${cell.title}`);
  printScenarioIntro(io, {
    what: `Baseline: TS ${cell.format} serializer encodes a v1 record with NONE compression, kafkajs produces it, the Java sidecar consumes and decodes with the SAME v1 schema.`,
    proves: `Same-version wire-format parity for ${cell.format} between the TS and Java clients — pins the byte-identity claim, so any cross-version FAIL later is genuinely an evolution issue, not a same-version regression.`,
  });
  let schemaVersionId: string | null = null;
  try {
    const schemaName = ctx.namer.forFamily(cell.familyLabel);
    ctx.cleanup.track(REGISTRY_NAME, schemaName);
    printStage(io, "glue", `schemaName=${schemaName} (${cell.sidecarFormat})`);

    const uuid = await ctx.registrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat: cell.sidecarFormat,
        schemaDefinition: cell.schema,
      },
      /* transportName */ `demo-scenario-${cell.index}`,
    );
    if (!UUID_REGEX.test(uuid)) {
      throw new Error(`schemaVersionId is not a canonical UUID: "${uuid}"`);
    }
    schemaVersionId = uuid;
    printStage(io, "glue", `schemaVersionId=${uuid}`);
    printSchemaBody(io, "v1 body (only version registered — same-version baseline)", cell.schema);

    const topic = topicFor(ctx, `same-tsjava-${cell.index}`);
    const record = { ...CUSTOMER_RECORD_V1 };
    printRecord(io, "record about to be encoded (v1)", record);

    const serializer = new GsrSerializer({ compression });
    const wire = serializer.serialize({
      format: cell.dataFormat,
      schemaVersionId: uuid,
      topic,
      schema: cell.schema,
      data: record,
      ...(cell.dataFormat === DataFormat.PROTOBUF
        ? { messageFullName: INTEROP_PROTOBUF_FULL_NAME }
        : {}),
    });
    printStage(
      io,
      "encoder",
      `TS GsrSerializer produced ${wire.length} wire bytes (0x03 magic + 0x00 NONE + 16-byte UUID + payload)`,
    );
    printHexDump(io, "TS-encoded wire message", wire);

    printStage(io, "kafka", `producing 1 message to topic ${topic}`);
    await produceOne(ctx.broker.bootstrap, topic, wire);

    const consumeResp = await ctx.sidecar.kafkaConsume({
      bootstrap: ctx.broker.bootstrap,
      topic,
      format: cell.sidecarFormat,
      groupId: `demo-scenario-${cell.index}-${ctx.runSuffix}`,
      region: ctx.region,
      timeoutMs: CONSUME_TIMEOUT_MS,
    });
    printStage(
      io,
      "sidecar",
      `Java sidecar consumed dataFormat=${consumeResp.dataFormat} schemaVersionId=${consumeResp.schemaVersionId}`,
    );

    const decoded = decodeSidecarEnvelopeToV1(cell.sidecarFormat, consumeResp.record);
    printRecord(io, "record decoded by Java sidecar", decoded);
    const recordOk = printEqualityCheck(
      io,
      "decoded record fields",
      canonicalizeRecord(record),
      canonicalizeRecord(decoded),
    );
    const uuidOk = printEqualityCheck(
      io,
      "sidecar-reported schemaVersionId matches",
      uuid,
      consumeResp.schemaVersionId,
    );
    const pass = recordOk && uuidOk;
    printVerdict(
      io,
      pass,
      pass
        ? `TS ${cell.format} same-version encode → Java decode round-trip succeeded (record + UUID match).`
        : `TS ${cell.format} same-version encode → Java decode failed: record or UUID mismatch.`,
    );
    return {
      index: cell.index,
      title: cell.title,
      format: cell.format,
      direction,
      compression,
      schemaVersionId,
      pass,
    };
  } catch (err) {
    return failed(
      { ...cell, compression },
      direction,
      schemaVersionId,
      err,
    );
  }
}

/**
 * Java sidecar writes v1 → Kafka → TS decodes with v1. Mirror of
 * `runTsToJavaSameVersion` — the same baseline, opposite direction.
 */
async function runJavaToTsSameVersion(
  ctx: ScenarioContext,
  cell: SameVersionCell,
): Promise<ScenarioResult> {
  const { io } = ctx;
  const direction = "Java→TS";
  const compression: "NONE" = "NONE";
  printSectionHeader(io, `[scenario ${cell.index} of 21] ${cell.title}`);
  printScenarioIntro(io, {
    what: `Baseline: Java sidecar encodes a v1 ${cell.format} record with NONE compression, kafkajs consumes it, TS deserializes with the SAME v1 schema.`,
    proves: `Same-version wire-format parity for ${cell.format} between the Java and TS clients — mirror of the TS→Java baseline, opposite direction.`,
  });
  let schemaVersionId: string | null = null;
  try {
    const schemaName = ctx.namer.forFamily(cell.familyLabel);
    ctx.cleanup.track(REGISTRY_NAME, schemaName);
    printStage(io, "glue", `schemaName=${schemaName} (${cell.sidecarFormat})`);

    // Idempotently ensure the schema is registered from the TS side so
    // the tracker owns the exact schema name it will later delete. Java's
    // subsequent `kafkaProduce` with the same body will short-circuit on
    // AlreadyExists → RegisterSchemaVersion → same UUID.
    const uuid = await ctx.registrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat: cell.sidecarFormat,
        schemaDefinition: cell.schema,
      },
      `demo-scenario-${cell.index}`,
    );
    schemaVersionId = uuid;
    printStage(io, "glue", `pre-registered schemaVersionId=${uuid}`);
    printSchemaBody(io, "v1 body (only version registered — same-version baseline)", cell.schema);

    const topic = topicFor(ctx, `same-javats-${cell.index}`);
    const record = { ...CUSTOMER_RECORD_V1 };
    printRecord(io, "record the Java sidecar will produce (v1)", record);

    const produceResp = await ctx.sidecar.kafkaProduce({
      format: cell.sidecarFormat,
      schema: cell.schema,
      schemaName,
      record: buildSidecarEnvelope(cell.sidecarFormat, record, cell.schema),
      compression,
      bootstrap: ctx.broker.bootstrap,
      topic,
      region: ctx.region,
      compatibility: "BACKWARD",
    });
    printStage(
      io,
      "sidecar",
      `Java sidecar produced schemaVersionId=${produceResp.schemaVersionId} bytes=${produceResp.bytes.length}`,
    );
    printHexDump(io, "Java-produced wire message", produceResp.bytes);

    printStage(io, "kafka", `consuming from topic ${topic}`);
    const framed = await consumeOne(
      ctx.broker.bootstrap,
      topic,
      `demo-scenario-${cell.index}-${ctx.runSuffix}`,
    );
    printStage(io, "kafka", `consumed ${framed.length} wire bytes — TS decode next`);

    const decodedHeader = decodeMessage(framed, {
      protobuf: cell.dataFormat === DataFormat.PROTOBUF,
    });
    const wireUuidOk = printEqualityCheck(
      io,
      "consumed wire header UUID matches sidecar-produced UUID",
      produceResp.schemaVersionId,
      decodedHeader.schemaVersionId,
    );

    const deserializer = new GsrDeserializer();
    const decoded = deserializer.deserialize({
      format: cell.dataFormat,
      data: framed,
      schema: cell.schema,
      messageFullName:
        cell.dataFormat === DataFormat.PROTOBUF
          ? INTEROP_PROTOBUF_FULL_NAME
          : undefined,
    });
    const decodedV1 = coerceDecodedToV1(cell.dataFormat, decoded);
    printRecord(io, "record decoded by TS GsrDeserializer", decodedV1);

    const recordOk = printEqualityCheck(
      io,
      "decoded record fields",
      canonicalizeRecord(record),
      canonicalizeRecord(decodedV1),
    );
    const uuidOk = printEqualityCheck(
      io,
      "sidecar-produced schemaVersionId matches pre-registered UUID",
      uuid,
      produceResp.schemaVersionId,
    );
    const pass = wireUuidOk && recordOk && uuidOk;
    printVerdict(
      io,
      pass,
      pass
        ? `Java ${cell.format} same-version encode → TS decode round-trip succeeded (wire UUID + record + UUID match).`
        : `Java ${cell.format} same-version encode → TS decode failed: at least one of wire-UUID / record / UUID mismatched.`,
    );
    return {
      index: cell.index,
      title: cell.title,
      format: cell.format,
      direction,
      compression,
      schemaVersionId,
      pass,
    };
  } catch (err) {
    return failed(
      { ...cell, compression },
      direction,
      schemaVersionId,
      err,
    );
  }
}

// ---------------------------------------------------------------------------
// Shared helpers — schema registration, sidecar envelopes, decoder coercion.
// ---------------------------------------------------------------------------

/**
 * Ensure the (schemaName, dataFormat) pair has BOTH v1 and v2 registered
 * in Glue. Returns the two version UUIDs. Idempotent by construction:
 *
 *   - `SchemaRegistrar.getSchemaVersionId` caches on
 *     `${schemaName}:${dataFormat}`, so v1 registers on first call and
 *     short-circuits on subsequent calls with the cached UUID.
 *   - The direct `registerSchemaVersion` call for v2 is content-idempotent
 *     in Glue (a re-register of the same body returns the same UUID) and
 *     the AVAILABLE-poll below matches the registrar's own discipline.
 *
 * Called once per family regardless of scenario count, so scenarios 1-4
 * (avroCrossVersion) share one v1 UUID and one v2 UUID.
 */
async function ensureCrossVersionRegistered(
  ctx: ScenarioContext,
  schemaName: string,
  sidecarFormat: string,
  schemaV1: string,
  schemaV2: string,
): Promise<{ v1Uuid: string; v2Uuid: string }> {
  const v1Uuid = await ctx.registrar.getSchemaVersionId(
    {
      schemaName,
      dataFormat: sidecarFormat,
      schemaDefinition: schemaV1,
    },
    `demo-crossver-${schemaName}`,
  );
  if (!UUID_REGEX.test(v1Uuid)) {
    throw new Error(`v1 schemaVersionId is not a canonical UUID: "${v1Uuid}"`);
  }

  // Register v2 directly — bypass the registrar's cache (keyed on
  // schemaName + dataFormat, not body) so v2 gets a distinct UUID.
  const regResp = await ctx.glueClient.registerSchemaVersion({
    SchemaId: {
      RegistryName: REGISTRY_NAME,
      SchemaName: schemaName,
    },
    SchemaDefinition: schemaV2,
  });
  if (!regResp.SchemaVersionId) {
    throw new Error(
      `registerSchemaVersion returned no SchemaVersionId for ${schemaName} v2`,
    );
  }
  const v2Uuid = regResp.SchemaVersionId;
  if (regResp.Status !== "AVAILABLE") {
    // Poll `GetSchemaVersion` until AVAILABLE — same cadence as the
    // registrar's own `pollUntilAvailable`, so demo timing matches
    // production. Failure to reach AVAILABLE throws with the last status.
    const maxAttempts = 10;
    const intervalMs = 3_000;
    let lastStatus: string | undefined = regResp.Status;
    for (let i = 0; i < maxAttempts; i++) {
      await sleep(intervalMs);
      const pollResp = await ctx.glueClient.getSchemaVersion({
        SchemaVersionId: v2Uuid,
      });
      lastStatus = pollResp.Status;
      if (lastStatus === "AVAILABLE") {
        return { v1Uuid, v2Uuid };
      }
      if (lastStatus !== "PENDING") {
        throw new Error(
          `v2 register poll: unexpected status schemaVersionId=${v2Uuid} status=${String(lastStatus)}`,
        );
      }
    }
    throw new Error(
      `v2 register poll exhausted (${maxAttempts} attempts) status=${String(lastStatus)}`,
    );
  }
  return { v1Uuid, v2Uuid };
}

/**
 * Build the per-format sidecar envelope for a v1 record. Every branch
 * delegates to a fixture-owned builder — no wire framing or schema
 * marshalling happens here.
 */
function buildSidecarEnvelope(
  sidecarFormat: "AVRO" | "JSON" | "PROTOBUF",
  record: CustomerRecordV1,
  schema: string,
): Record<string, unknown> {
  switch (sidecarFormat) {
    case "AVRO":
      return buildAvroEnvelope(record) as unknown as Record<string, unknown>;
    case "JSON":
      return buildJsonEnvelope(record, schema) as unknown as Record<
        string,
        unknown
      >;
    case "PROTOBUF":
      return buildProtobufEnvelope(record) as unknown as Record<
        string,
        unknown
      >;
    default: {
      const unreachable: never = sidecarFormat;
      throw new Error(`unreachable sidecarFormat: ${String(unreachable)}`);
    }
  }
}

/**
 * Read a sidecar-consume envelope back into a v1 record shape. Each
 * branch calls the corresponding fixture-owned envelope reader — none of
 * the parsing lives here.
 */
function decodeSidecarEnvelopeToV1(
  sidecarFormat: "AVRO" | "JSON" | "PROTOBUF",
  envelope: Record<string, unknown>,
): CustomerRecordV1 {
  switch (sidecarFormat) {
    case "AVRO": {
      const fields = (envelope as { fields?: Record<string, unknown> }).fields ?? {};
      return readAvroEnvelopeV1({ fields });
    }
    case "JSON": {
      return readJsonEnvelopeV1({
        schema: String((envelope as { schema?: unknown }).schema ?? ""),
        payload: String((envelope as { payload?: unknown }).payload ?? ""),
      });
    }
    case "PROTOBUF": {
      return readProtobufEnvelopeV1({
        messageTypeFullName: String(
          (envelope as { messageTypeFullName?: unknown }).messageTypeFullName ??
            "",
        ),
        fieldsJson: String(
          (envelope as { fieldsJson?: unknown }).fieldsJson ?? "{}",
        ),
      });
    }
    default: {
      const unreachable: never = sidecarFormat;
      throw new Error(`unreachable sidecarFormat: ${String(unreachable)}`);
    }
  }
}

/**
 * Coerce a TS-side `GsrDeserializer` output into a v1 record shape (id,
 * name, age). AVRO/JSON return plain objects with those fields directly;
 * PROTOBUF returns a protobufjs `toObject` result which is already a
 * plain object with the same field names.
 */
function coerceDecodedToV1(
  format: DataFormat,
  decoded: unknown,
): CustomerRecordV1 {
  const rec = decoded as { id?: unknown; name?: unknown; age?: unknown };
  if (
    typeof rec.id !== "string" ||
    typeof rec.name !== "string" ||
    typeof rec.age !== "number"
  ) {
    throw new Error(
      `decoded record missing v1 fields (format=${format} keys=${Object.keys(rec as object).join(",")})`,
    );
  }
  return { id: rec.id, name: rec.name, age: rec.age };
}

/**
 * Project a v1 record into a canonical shape: only the three v1 fields,
 * keys in sorted order. Applied to BOTH sides of the equality check so
 * `narrator.printEqualityCheck`'s JSON-stringify comparison is
 * key-order-insensitive without editing narrator.ts.
 *
 * See the module header's "Key-order-insensitive record equality" note
 * for the reasoning.
 */
function canonicalizeRecord(rec: CustomerRecordV1): Record<string, unknown> {
  const keys: Array<keyof CustomerRecordV1> = ["age", "id", "name"];
  const out: Record<string, unknown> = {};
  for (const k of keys) {
    out[k] = rec[k];
  }
  return out;
}

/**
 * Return a `ScenarioResult` for a failed scenario. Captures the error
 * message and preserves the schemaVersionId if one was resolved before
 * the failure — an operator scanning the summary can then tell whether
 * the run got past registration.
 */
function failed(
  cell: {
    index: number;
    title: string;
    format: "AVRO" | "JSON" | "PROTOBUF";
    compression: "NONE" | "ZLIB";
  },
  direction: string,
  schemaVersionId: string | null,
  err: unknown,
): ScenarioResult {
  return {
    index: cell.index,
    title: cell.title,
    format: cell.format,
    direction,
    compression: cell.compression,
    schemaVersionId,
    pass: false,
    error: errorMessage(err),
  };
}

/**
 * Best-effort human message for a thrown value. Errors carry `.message`;
 * everything else stringifies.
 */
function errorMessage(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}

/**
 * Compose a per-run-unique Kafka topic name. The demo does not own topic
 * cleanup (topics are testcontainers ephemera dropped when the broker
 * stops, or best-effort `deleteTopics` for an external broker), so the
 * suffix's job is uniqueness across parallel runs, not ownership.
 */
function topicFor(ctx: ScenarioContext, label: string): string {
  return `gsr-ts-it-demo-${label}-${ctx.runSuffix}`;
}

/**
 * Promise-flavored `setTimeout`. Local so the module has no runtime dep
 * beyond `node:buffer` for the wire byte handling.
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolveP) => setTimeout(resolveP, ms));
}

// ---------------------------------------------------------------------------
// Kafka helpers — thin wrappers over kafkajs, one connect/produce/disconnect
// per call so leaked handles never span two scenarios. Kept here (rather
// than in a shared demo/ module) because the interop suites use the same
// shape inline and every attempt to share it moves the shape further from
// the test's original form; a small copy is preferable to a leaky abstraction.
// ---------------------------------------------------------------------------

/**
 * Produce one message to `topic` with `value = wire`. Ensures the topic
 * exists first via admin, then produces, then disconnects.
 */
async function produceOne(
  bootstrap: string,
  topic: string,
  wire: Buffer | Uint8Array,
): Promise<void> {
  const { Kafka, logLevel } = await import("kafkajs");
  const kafka = new Kafka({
    clientId: "gsr-ts-demo-producer",
    brokers: [bootstrap],
    logLevel: logLevel.NOTHING,
  });
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      waitForLeaders: true,
    });
  } finally {
    await admin.disconnect();
  }
  const producer = kafka.producer();
  await producer.connect();
  try {
    await producer.send({
      topic,
      messages: [{ value: wire instanceof Buffer ? wire : Buffer.from(wire) }],
    });
  } finally {
    await producer.disconnect();
  }
}

/**
 * Consume one message from `topic` (from beginning). Times out after
 * `CONSUME_TIMEOUT_MS`; the returned `Buffer` is a copy of the message
 * value so the underlying kafkajs buffer can be released.
 */
async function consumeOne(
  bootstrap: string,
  topic: string,
  groupId: string,
): Promise<Buffer> {
  const { Kafka, logLevel } = await import("kafkajs");
  const kafka = new Kafka({
    clientId: "gsr-ts-demo-consumer",
    brokers: [bootstrap],
    logLevel: logLevel.NOTHING,
  });
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  try {
    return await new Promise<Buffer>((resolveP, rejectP) => {
      const timer = setTimeout(() => {
        rejectP(new Error(`consumeOne: timed out reading topic ${topic}`));
      }, CONSUME_TIMEOUT_MS);
      void consumer
        .run({
          eachMessage: async ({ message }) => {
            if (message.value !== null && message.value !== undefined) {
              clearTimeout(timer);
              resolveP(Buffer.from(message.value));
            }
          },
        })
        .catch((err) => {
          clearTimeout(timer);
          rejectP(err instanceof Error ? err : new Error(String(err)));
        });
    });
  } finally {
    await consumer.disconnect();
  }
}

