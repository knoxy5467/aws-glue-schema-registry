/**
 * Interop tier — TS produces, Java consumes, same version.
 *
 * The primary interop-matrix cells covering the direction "TS-encoded bytes
 * decoded by the canonical Java Glue Schema Registry client". Each of the
 * five TS-side subtype cells (Avro Generic, Avro Specific, Protobuf Dynamic,
 * Protobuf concrete, JSON-Schema) is exercised under compression `NONE` and
 * a `ZLIB` variant, yielding ten test invocations.
 *
 * Flow per cell:
 *
 *   1. Register a per-run-namespaced schema against the selected Glue
 *      backend via `SchemaRegistrar.getSchemaVersionId`. On the real path
 *      the tracker guarantees the schema is deleted on exit.
 *   2. Serialize a fixture record via `GsrSerializer` with the cell's
 *      compression mode, producing a framed 18-byte-header wire message.
 *   3. Produce the wire bytes to a testcontainers Kafka topic via `kafkajs`.
 *   4. Ask the Java sidecar to consume that topic via `/kafka-consume` in
 *      the matching data format.
 *   5. Assert: the sidecar-returned record envelope decodes back to the
 *      original fixture field-for-field, AND the schema-version UUID the
 *      sidecar reports matches the one written into the wire header on the
 *      TS side.
 *
 * The subtype axis is TS-side (Assumption 5 of the interop harness spec):
 * Generic / Specific for Avro and Dynamic / concrete for Protobuf produce
 * identical wire bytes, so the sidecar sees the same message; the cell
 * varies how the TS client builds and hands the record to `GsrSerializer`.
 *
 * Gates (three-gate discipline):
 *
 *   - The `*.integ.test.ts` file suffix keeps this file out of the default
 *     Tier-1 vitest project entirely; `npm test` never loads it.
 *   - `describeIntegration` short-circuits when `AWS_INTEGRATION!=1`.
 *   - `requireInterop()` short-circuits when `GSR_GLUE!=real`, the JVM /
 *     sidecar JAR / Docker are absent, or the interop partner is unset /
 *     `java` (any other value produces a loud error before we reach the
 *     network).
 *   - `requireRealCreds()` hard-fails (throws) when `GSR_GLUE=real` and no
 *     AWS credentials are resolvable — a silent skip on missing creds would
 *     look identical to a passing real-Glue run.
 *
 * Cleanup:
 *
 *   - Every registered schema is per-run namespaced via `realSchemaName`
 *     and tracked in `CleanupTracker` before it is committed to Glue.
 *   - `afterAll` tears down in reverse order: sidecar SIGTERM → Kafka
 *     broker stop → tracker delete. `EntityNotFoundException` during
 *     schema teardown is tolerated by the tracker.
 */
import {
  createCache,
  createMetadata,
  parseConfig,
  SchemaRegistrar,
  type GsrConfig,
} from "@gsr/core";
import { DataFormat, GsrSerializer, type SerializeRequest } from "@gsr/serde";
import avsc from "avsc";
import type { Kafka as KafkajsKafka } from "kafkajs";
import protobuf from "protobufjs";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  describeIntegration,
  requireRealCreds,
  resolveRegion,
} from "../../src/env-gate.js";
import { requireInterop } from "../../src/interop-gate.js";
import {
  isSkip,
  resolveBroker,
  type BrokerHandle,
} from "../../src/kafka-broker.js";
import { startSidecar, type Sidecar } from "../../src/java-sidecar.js";
import {
  CleanupTracker,
  realSchemaName,
  selectGlueBackend,
} from "../../src/real-glue.js";

import {
  AVRO_SCHEMA_V1,
  buildAvroEnvelope,
  buildJsonEnvelope,
  buildProtobufEnvelope,
  CUSTOMER_RECORD_V1,
  INTEROP_PROTOBUF_FULL_NAME,
  JSON_SCHEMA_V1,
  PROTOBUF_SCHEMA_V1,
  readAvroEnvelopeV1,
  readJsonEnvelopeV1,
  readProtobufEnvelopeV1,
  type AvroEnvelope,
  type JsonEnvelope,
  type ProtobufEnvelope,
} from "./interop-fixtures.js";

const SUITE_NAME = "interop/ts-to-java-same-version";
const REGISTRY_NAME = "default-registry";

/**
 * `console.warn` a machine-parseable `SKIP <name> — <reason>` line. Matches
 * the shared env-gate's emit format character-for-character (em-dash U+2014)
 * so a downstream log scanner treats every skip line — from the AWS gate,
 * the interop gate, and this suite's secondary gates — identically.
 */
function warnSkip(name: string, reason: string): void {
  // eslint-disable-next-line no-console
  console.warn(`SKIP ${name} — ${reason}`);
}

/**
 * One matrix row. The five subtype cells share the record and the schema
 * body; they differ only in `format` and in how the TS client constructs
 * the record it hands to `GsrSerializer` (via `buildData`) and how it
 * decodes the sidecar-returned envelope (via `readEnvelope`).
 */
interface Cell {
  readonly label: string;
  readonly format: DataFormat;
  /** `AVRO | JSON | PROTOBUF` — the wire tag the sidecar routes on. */
  readonly sidecarFormat: "AVRO" | "JSON" | "PROTOBUF";
  /** Stringified schema, ready for Glue and the sidecar. */
  readonly schema: string;
  /** Format-specific data passed to `GsrSerializer.serialize.data`. */
  readonly buildData: () => unknown;
  /** Extra serialize-side inputs (e.g. `messageFullName` for Protobuf). */
  readonly extraSerialize?: Partial<SerializeRequest>;
  /** Java-side envelope that the sidecar's `/kafka-produce` would expect. */
  readonly buildJavaEnvelope: () => AvroEnvelope | JsonEnvelope | ProtobufEnvelope;
  /**
   * Recover the TS record from the sidecar's `/kafka-consume` response
   * envelope so equality can be asserted field-for-field.
   */
  readonly readSidecarEnvelope: (env: unknown) => unknown;
}

/**
 * Narrow local view over avsc's `RecordType` that surfaces
 * `getRecordConstructor()`. avsc's own `.d.ts` is documented-incomplete at
 * the top of `node_modules/avsc/types/index.d.ts` and exposes only the
 * synonymous `.recordConstructor` property (typed `any`) — the method form
 * that returns the generated typed-record constructor is not declared, so
 * we widen locally rather than casting through `unknown`.
 */
type AvroRecordTypeWithCtor = avsc.types.RecordType & {
  getRecordConstructor: () => new (...args: unknown[]) => unknown;
};

/**
 * Pre-compiled Avro type used by the "Avro Specific" cell. Same schema body
 * as the Generic cell — the difference is the caller hands a typed-record
 * instance rather than a plain object. `omitRecordMethods: false` produces
 * an avsc `RecordType` whose `.getRecordConstructor()` returns the
 * *generated* typed-record constructor, mirroring the Java `SpecificRecord`
 * shape.
 */
const AVRO_SPECIFIC_TYPE = avsc.Type.forSchema(
  JSON.parse(AVRO_SCHEMA_V1) as avsc.schema.AvroSchema,
  { omitRecordMethods: false },
) as AvroRecordTypeWithCtor;

/**
 * The generated typed-record constructor for `AVRO_SPECIFIC_TYPE`. `avsc`
 * generates a plain JS function whose parameters are the record's fields in
 * declaration order (`id`, `name`, `age`) — it is NOT a single-object
 * constructor. Extracted once at module load so the cell body reads as a
 * straight positional `new` call.
 */
const AvroSpecificRecordCtor = AVRO_SPECIFIC_TYPE.getRecordConstructor() as new (
  id: string,
  name: string,
  age: number,
) => unknown;

/**
 * Pre-parsed protobuf root used by the "Protobuf concrete" cell. Same
 * schema body as the Dynamic cell — the difference is the caller hands a
 * protobufjs `Message` instance instead of a plain object.
 */
const PROTOBUF_ROOT = protobuf.parse(PROTOBUF_SCHEMA_V1).root;
const PROTOBUF_TYPE = PROTOBUF_ROOT.lookupType(INTEROP_PROTOBUF_FULL_NAME);

/**
 * Build the ten matrix cells (five subtypes × two compression modes). The
 * compression axis is layered by `describe.each` below so each cell reads
 * as one axis — subtype — and the compression variant is a per-describe
 * outer parameter.
 */
function buildCells(): readonly Cell[] {
  return [
    {
      label: "Avro Generic",
      format: DataFormat.AVRO,
      sidecarFormat: "AVRO",
      schema: AVRO_SCHEMA_V1,
      buildData: () => ({ ...CUSTOMER_RECORD_V1 }),
      buildJavaEnvelope: () => buildAvroEnvelope(CUSTOMER_RECORD_V1),
      readSidecarEnvelope: (env) =>
        readAvroEnvelopeV1(env as AvroEnvelope),
    },
    {
      label: "Avro Specific",
      format: DataFormat.AVRO,
      sidecarFormat: "AVRO",
      schema: AVRO_SCHEMA_V1,
      // Constructing a typed-record instance is the on-the-wire-invisible
      // distinction that separates SPECIFIC from GENERIC on the TS side.
      // The typed-record constructor is `AVRO_SPECIFIC_TYPE.getRecordConstructor()`
      // (a *separate* generated function — the `RecordType` itself is a
      // frozen instance, not a constructor). Its parameters are the record
      // fields in declaration order (`id`, `name`, `age`), not a single
      // object literal.
      buildData: () =>
        new AvroSpecificRecordCtor(
          CUSTOMER_RECORD_V1.id,
          CUSTOMER_RECORD_V1.name,
          CUSTOMER_RECORD_V1.age,
        ),
      buildJavaEnvelope: () => buildAvroEnvelope(CUSTOMER_RECORD_V1),
      readSidecarEnvelope: (env) =>
        readAvroEnvelopeV1(env as AvroEnvelope),
    },
    {
      label: "Protobuf Dynamic",
      format: DataFormat.PROTOBUF,
      sidecarFormat: "PROTOBUF",
      schema: PROTOBUF_SCHEMA_V1,
      buildData: () => ({ ...CUSTOMER_RECORD_V1 }),
      extraSerialize: { messageFullName: INTEROP_PROTOBUF_FULL_NAME },
      buildJavaEnvelope: () => buildProtobufEnvelope(CUSTOMER_RECORD_V1),
      readSidecarEnvelope: (env) =>
        readProtobufEnvelopeV1(env as ProtobufEnvelope),
    },
    {
      label: "Protobuf concrete",
      format: DataFormat.PROTOBUF,
      sidecarFormat: "PROTOBUF",
      schema: PROTOBUF_SCHEMA_V1,
      // A concrete protobufjs `Message` instance rather than a plain object —
      // mirrors the Java `SpecificMessage` seam. `protobufjs.Type.create`
      // returns a `Message<object>` bound to the parsed type; the encoder
      // treats it identically to `Type.fromObject` output on the wire.
      buildData: () => PROTOBUF_TYPE.create({ ...CUSTOMER_RECORD_V1 }),
      extraSerialize: { messageFullName: INTEROP_PROTOBUF_FULL_NAME },
      buildJavaEnvelope: () => buildProtobufEnvelope(CUSTOMER_RECORD_V1),
      readSidecarEnvelope: (env) =>
        readProtobufEnvelopeV1(env as ProtobufEnvelope),
    },
    {
      label: "JSON-Schema",
      format: DataFormat.JSON,
      sidecarFormat: "JSON",
      schema: JSON_SCHEMA_V1,
      buildData: () => ({ ...CUSTOMER_RECORD_V1 }),
      buildJavaEnvelope: () => buildJsonEnvelope(CUSTOMER_RECORD_V1, JSON_SCHEMA_V1),
      readSidecarEnvelope: (env) =>
        readJsonEnvelopeV1(env as JsonEnvelope),
    },
  ];
}

/**
 * Compression axis. Both modes are mandatory per the harness spec: `NONE`
 * asserts the framing / decode contract in isolation, `ZLIB` layers the
 * cross-runtime decompression parity on top.
 */
const COMPRESSION_MODES = ["NONE", "ZLIB"] as const;

describeIntegration(SUITE_NAME, () => {
  // Suite-scoped state resolved in `beforeAll`. Every non-null field is
  // paired with a teardown in `afterAll`. `skipReason` is set on any gate
  // miss so individual `it` bodies short-circuit uniformly.
  let cfg: GsrConfig;
  let registrar: SchemaRegistrar | null = null;
  let tracker: CleanupTracker | null = null;
  let broker: BrokerHandle | null = null;
  let sidecar: Sidecar | null = null;
  let kafka: KafkajsKafka | null = null;
  let skipReason: string | null = null;

  // Track topics the suite creates so `afterAll` can drop them on the way
  // out — the schema-level cleanup is handled by `CleanupTracker`; topics
  // are per-run-suffix ephemera the operator would otherwise have to prune
  // manually if the broker is external.
  const topicsCreated: string[] = [];

  beforeAll(async () => {
    // Interop gate — stacks the env gate, the partner check, and the JVM /
    // JAR probe. When any of those is unmet we mark the suite skipped and
    // emit exactly one machine-parseable line (the gate itself emits
    // through a `console.warn` matching env-gate's shape).
    const gate = requireInterop({ suiteName: SUITE_NAME });
    if (gate.skipped) {
      skipReason = gate.reason;
      return;
    }

    // Hard-throw when the operator opted into real Glue but has no creds.
    // Silent skip would misreport a "green" real-Glue run that never
    // actually billed the account.
    requireRealCreds();

    // Broker discovery — reuse of the shared tri-state `kafka-broker`
    // module that the interop tier standardized on.
    const resolved = await resolveBroker();
    if (isSkip(resolved)) {
      skipReason = resolved.skip;
      warnSkip(SUITE_NAME, resolved.skip);
      return;
    }
    broker = resolved;

    // Sidecar launch — the launcher throws on JAR-not-found or health-poll
    // timeout. The interop gate already probed for the JAR being present on
    // disk, so a throw here indicates a genuine startup failure; propagate
    // to fail the whole suite (the tracker/broker teardown still runs).
    sidecar = await startSidecar();

    cfg = parseConfig({
      region: resolveRegion(),
      registryName: REGISTRY_NAME,
      schemaAutoRegistrationEnabled: "true",
    });

    // Build the Glue backend + registrar the same way the Tier-3 suite
    // does. On the real path `selectGlueBackend` binds a `CleanupTracker`
    // to the same SDK config as the encoder client, so DeleteSchema on
    // teardown routes to the same account and region.
    const backend = selectGlueBackend(cfg);
    tracker = backend.cleanup;
    const cache = createCache<{ schemaVersionId: string }>({
      ttlMillis: cfg.timeToLiveMillis,
      size: cfg.cacheSize,
    });
    const metadata = createMetadata(backend.client, cfg);
    registrar = new SchemaRegistrar(backend.client, cache, cfg, metadata);

    // Kafka client — one per suite; producer / consumer are per-cell.
    const kafkajs = await import("kafkajs");
    kafka = new kafkajs.Kafka({
      clientId: "gsr-interop-ts-to-java",
      brokers: [broker.bootstrap],
      logLevel: kafkajs.logLevel.NOTHING,
    });
  }, 300_000);

  afterAll(async () => {
    // Reverse-order teardown. Each step is wrapped so a failure in one
    // does not abort the rest — the tracker's own error aggregation
    // handles the Glue side; broker/sidecar/topic failures are logged
    // and swallowed here because a mid-teardown throw would obscure the
    // primary test failure.
    if (sidecar !== null) {
      try {
        await sidecar.stop();
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn(`${SUITE_NAME}: sidecar stop failed: ${errorText(err)}`);
      }
      sidecar = null;
    }
    // Delete per-cell topics on the way out. When the broker was started
    // by testcontainers the `broker.stop()` below drops the volume
    // wholesale so this is redundant; when the broker was reused
    // (`KAFKA_BROKER=<host:port>`) this is the only way the operator
    // is not left with a growing tail of `gsr-ts-it-*` topics.
    if (kafka !== null && topicsCreated.length > 0) {
      try {
        const admin = kafka.admin();
        await admin.connect();
        try {
          await admin.deleteTopics({ topics: topicsCreated });
        } finally {
          await admin.disconnect();
        }
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn(`${SUITE_NAME}: topic cleanup failed: ${errorText(err)}`);
      }
    }
    if (broker !== null) {
      try {
        await broker.stop();
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn(`${SUITE_NAME}: broker stop failed: ${errorText(err)}`);
      }
      broker = null;
    }
    if (tracker !== null) {
      await tracker.run();
      tracker = null;
    }
  });

  // Compression is the outer axis so each `describe` block reads as one
  // compression variant and the five inner cells share it. Both variants
  // are mandatory per the interop harness spec's compression handling
  // section.
  describe.each(COMPRESSION_MODES)("compression=%s", (compression) => {
    // Build cells inside each describe so the closure captures the
    // per-variant compression tag; the cell definitions themselves are
    // pure data and cheap to re-materialize.
    const cells = buildCells();

    it.each(cells.map((c) => [c.label, c] as const))(
      "%s: Java decodes TS-encoded bytes; fields + UUID match",
      async (_label, cell) => {
        if (skipReason !== null) {
          // Suite-level short-circuit already emitted its skip line in
          // `beforeAll`; individual tests are no-ops so the run stays
          // green when gates are off.
          return;
        }
        if (
          registrar === null ||
          tracker === null ||
          broker === null ||
          sidecar === null ||
          kafka === null
        ) {
          throw new Error(
            `${SUITE_NAME}: beforeAll completed without a skip reason but suite state is incomplete`,
          );
        }

        // Every cell owns its own schema name + topic — parallel-safe.
        const schemaName = realSchemaName(
          `${slug(cell.label)}-${compression.toLowerCase()}`,
        );
        const topic = `gsr-ts-it-interop-${schemaName}`;
        // Track the schema BEFORE the register call. If the register
        // itself throws mid-flight (e.g. throttling after CreateSchema
        // succeeded on the service side but the response never reached
        // us), the tracker still has the name and cleanup deletes it.
        tracker.track(REGISTRY_NAME, schemaName);

        const schemaVersionId = await registrar.getSchemaVersionId(
          {
            schemaName,
            dataFormat: cell.sidecarFormat,
            schemaDefinition: cell.schema,
          },
          topic,
        );
        expect(schemaVersionId).toMatch(
          /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
        );

        // ---- TS-side produce.
        const serializer = new GsrSerializer({ compression });
        const wire = serializer.serialize({
          format: cell.format,
          schemaVersionId,
          topic,
          schema: cell.schema,
          data: cell.buildData(),
          ...cell.extraSerialize,
        });

        const admin = kafka.admin();
        await admin.connect();
        try {
          await admin.createTopics({
            topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
            waitForLeaders: true,
          });
          topicsCreated.push(topic);
        } finally {
          await admin.disconnect();
        }

        const producer = kafka.producer();
        await producer.connect();
        try {
          await producer.send({
            topic,
            messages: [{ value: wire }],
          });
        } finally {
          await producer.disconnect();
        }

        // ---- Java sidecar consume.
        const consumeResult = await sidecar.kafkaConsume({
          bootstrap: broker.bootstrap,
          topic,
          format: cell.sidecarFormat,
          // A per-cell group id so a rerun of the same suite does not
          // inherit an offset from an earlier run.
          groupId: `${topic}-group`,
          region: cfg.region,
          timeoutMs: 60_000,
        });

        // ---- Assertions.
        // The sidecar reports the wire header's schema-version UUID; it
        // must match the id the TS side wrote into the header, which is
        // the id Glue returned on registration.
        expect(consumeResult.schemaVersionId).toBe(schemaVersionId);
        expect(consumeResult.dataFormat).toBe(cell.sidecarFormat);

        // Field-for-field: the sidecar-returned envelope decodes back to
        // the same v1 record we produced. The envelope shape differs per
        // format; the cell's `readSidecarEnvelope` handles the routing.
        const decoded = cell.readSidecarEnvelope(consumeResult.record);
        expect(decoded).toEqual(CUSTOMER_RECORD_V1);
      },
      180_000,
    );
  });
});

/**
 * Reduce an arbitrary human cell label ("Avro Generic", "Protobuf concrete")
 * to a lowercased, dash-separated token safe to embed in schema names and
 * topics. Kept local because the exact reduction is a suite-internal
 * convention; other suites choosing a different reduction do not need to
 * agree.
 */
function slug(label: string): string {
  return label
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

/**
 * Extract a message string from any thrown value. Used by the teardown
 * console.warn calls so a non-Error throw (e.g. a plain string) still
 * produces a readable diagnostic instead of `[object Object]`.
 */
function errorText(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
