/**
 * Java→TS same-version interop suite (matrix cells 6–10).
 *
 * For each of the five TS-side subtype decode shapes (Avro Generic, Avro
 * Specific, Protobuf Dynamic, Protobuf concrete, JSON-Schema) under
 * compression `NONE` and again under `ZLIB`, this suite proves that a
 * record serialized by the *real Java GSR client* (wrapped in the Java
 * sidecar) can be decoded by the TypeScript `GsrDeserializer` field-for-
 * field, with the schema-version UUID in the wire header matching the
 * version the sidecar registered in Glue.
 *
 * The direction is the mirror of the TS→Java suite: the Java sidecar owns
 * both the Glue register step and the Kafka produce step (via its
 * `/kafka-produce` endpoint, which internally drives
 * `GlueSchemaRegistryKafkaSerializer.serialize`); the TS side pulls the
 * framed bytes off Kafka with `kafkajs`, calls `decodeMessage` from
 * `@gsr/core` to lift the schema-version UUID out of the header, and hands
 * the bytes to `GsrDeserializer.deserialize` with the fixture's schema and
 * the subtype-specific decode-shape option (`avro.recordType` /
 * `protobuf.messageType`). No TS-side Glue register happens here — the
 * sidecar is the authoritative registrar for this direction, and the
 * fixture schemas we hand `deserialize` are byte-identical to what the
 * sidecar registered.
 *
 * ## Three-gate discipline
 *
 * The file is gated at three levels, in order:
 *
 *   1. Suffix gate: `*.integ.test.ts`, so the default `npm test` (Tier-1)
 *      does not even collect this file. Enforced by `vitest.integration.config.ts`.
 *   2. Env gate: `describeIntegration` (imported from the shared env-gate)
 *      short-circuits with a machine-parseable `SKIP` line when
 *      `AWS_INTEGRATION!=1`.
 *   3. Interop gate: `requireInterop()` (imported from the interop gate)
 *      short-circuits when `GSR_GLUE!=real`, when the JVM/JAR is absent, or
 *      when `GSR_INTEROP_PARTNER` is anything other than `java`/unset.
 *
 * The tri-state Kafka broker discovery is delegated to the shared
 * `kafka-broker.ts` module: an operator can point `KAFKA_BROKER` at an
 * externally-managed broker, use the empty-string sentinel to loud-skip
 * without touching Docker, or leave it unset to start a testcontainers
 * KRaft broker. When broker resolution loud-skips, every test in the
 * suite short-circuits silently — the `SKIP` line already fired.
 *
 * ## Cleanup
 *
 * Every schema the sidecar registers goes into `CleanupTracker` so an
 * `afterAll` deletes them in reverse insertion order (`RUNBOOK_PREFIX`
 * scoping ensures a leak sweep by prefix has zero hits on a clean run).
 * The tracker never touches registries — only per-run schemas.
 */

import { afterAll, beforeAll, expect, it } from "vitest";

import { GlueClient as SdkGlueClient } from "@aws-sdk/client-glue";
import {
  buildGlueClientConfig,
  decodeMessage,
  parseConfig,
  type GsrConfig,
} from "@gsr/core";
import {
  DataFormat,
  GsrDeserializer,
  type AvroRecordType,
  type ProtobufMessageType,
} from "@gsr/serde";

import { describeIntegration, resolveRegion } from "../../src/env-gate.js";
import { startSidecar, type Sidecar } from "../../src/java-sidecar.js";
import { requireInterop } from "../../src/interop-gate.js";
import {
  isSkip,
  resolveBroker,
  type BrokerHandle,
} from "../../src/kafka-broker.js";
import {
  CleanupTracker,
  RUNBOOK_PREFIX,
  realSchemaName,
} from "../../src/real-glue.js";
import {
  AVRO_SCHEMA_V1,
  CUSTOMER_RECORD_V1,
  INTEROP_PROTOBUF_FULL_NAME,
  JSON_SCHEMA_V1,
  PROTOBUF_SCHEMA_V1,
  buildAvroEnvelope,
  buildJsonEnvelope,
  buildProtobufEnvelope,
} from "./interop-fixtures.js";

const SUITE_NAME = "interop/java-to-ts-same-version";

/**
 * Default Glue registry name used by the reference clients and every
 * `real-glue.ts` helper — `default-registry` is auto-created by Glue in
 * every account, so the harness never creates a registry.
 */
const DEFAULT_REGISTRY_NAME = "default-registry";

/**
 * How long we wait for a message to arrive on Kafka before giving up. The
 * sidecar's kafka-produce ack is synchronous, so the consumer lag is
 * dominated by broker startup and topic-creation propagation. 60s covers a
 * cold container start comfortably.
 */
const CONSUME_TIMEOUT_MS = 60_000;

/**
 * How long we let the sidecar spawn + `/health` before failing the suite.
 * Local mode: JVM boot + Glue-client init. 120s matches the Tier-2
 * container-start budget the shared `kafka-broker.ts` helper uses.
 */
const SIDECAR_START_TIMEOUT_MS = 120_000;

/**
 * Emit a machine-parseable `SKIP <suite> — <reason>` line. Matches the
 * shared env-gate's private `emitLoudSkip` shape (em-dash U+2014) so a
 * downstream log scanner treats both identically. The gates we depend on
 * emit their own SKIP lines; this helper is for the broker-discovery
 * secondary gate that fires only after both feature gates have opened.
 */
function warnSkip(name: string, reason: string): void {
  // eslint-disable-next-line no-console
  console.warn(`SKIP ${name} — ${reason}`);
}

/**
 * One matrix row. Each row pairs a TS-side subtype decode shape with the
 * format, the fixture schema, and the sidecar-side per-format envelope
 * builder + fully-qualified message name (Protobuf only).
 *
 * `format` is the wire-level `DataFormat` (three values). `subtype` is
 * the TS-side decode-shape distinction — five values across three
 * formats. Subtypes collapse to three formats on the wire; the subtype
 * axis is a TS-side decode-option distinction only.
 */
interface Cell {
  readonly label: string;
  readonly format: DataFormat;
  readonly schema: string;
  /**
   * The per-format sidecar envelope, widened to the launcher's generic
   * `RecordEnvelope` shape (`Record<string, unknown>`). The fixture builders
   * emit typed envelopes; the JSON serialization on the HTTP contract
   * flattens them to plain records, so a widening cast at the boundary is
   * both accurate and structural.
   */
  readonly envelope: Record<string, unknown>;
  readonly schemaLabel: string;
  readonly messageFullName?: string;
  readonly avro?: { readonly recordType: AvroRecordType };
  readonly protobuf?: { readonly messageType: ProtobufMessageType };
  /**
   * Assert `decoded` equals `CUSTOMER_RECORD_V1`. Subtypes emit different
   * runtime shapes (a plain object for GENERIC/DYNAMIC/JSON, an avsc typed
   * record for Avro SPECIFIC, a `protobufjs` `Message` for Protobuf POJO)
   * so the equality check is subtype-specific. All checks converge on the
   * same three fields — id/name/age match.
   */
  assertDecodedMatches(decoded: unknown): void;
}

/**
 * Verify the decoded value carries the three canonical v1 fields regardless
 * of runtime shape. Shared by every subtype's `assertDecodedMatches` so a
 * change to the fixture record shape shows up in exactly one place.
 */
function assertScalarsMatch(fields: Record<string, unknown>): void {
  expect(fields["id"]).toBe(CUSTOMER_RECORD_V1.id);
  expect(fields["name"]).toBe(CUSTOMER_RECORD_V1.name);
  expect(fields["age"]).toBe(CUSTOMER_RECORD_V1.age);
}

/**
 * Build the 5-cell matrix (one per TS-side subtype). Every cell uses the
 * same canonical v1 record and the interop-fixture schemas — the byte
 * distinction cells share within a format is exercised at the TS decode
 * boundary via the `avro`/`protobuf` opts on `GsrDeserializer.deserialize`.
 */
function buildMatrix(): readonly Cell[] {
  // Widening cast to the launcher's generic `RecordEnvelope` — the wire
  // representation is JSON, so structural equivalence holds.
  const avroEnvelope: Record<string, unknown> = {
    ...buildAvroEnvelope(CUSTOMER_RECORD_V1),
  };
  const jsonEnvelope: Record<string, unknown> = {
    ...buildJsonEnvelope(CUSTOMER_RECORD_V1, JSON_SCHEMA_V1),
  };
  const protobufEnvelope: Record<string, unknown> = {
    ...buildProtobufEnvelope(CUSTOMER_RECORD_V1),
  };
  return [
    {
      label: "Avro Generic",
      format: DataFormat.AVRO,
      schema: AVRO_SCHEMA_V1,
      envelope: avroEnvelope,
      schemaLabel: "avro-generic",
      avro: { recordType: "GENERIC" },
      assertDecodedMatches(decoded: unknown): void {
        expect(decoded).toEqual({
          id: CUSTOMER_RECORD_V1.id,
          name: CUSTOMER_RECORD_V1.name,
          age: CUSTOMER_RECORD_V1.age,
        });
      },
    },
    {
      label: "Avro Specific",
      format: DataFormat.AVRO,
      schema: AVRO_SCHEMA_V1,
      envelope: avroEnvelope,
      schemaLabel: "avro-specific",
      avro: { recordType: "SPECIFIC" },
      assertDecodedMatches(decoded: unknown): void {
        // avsc SPECIFIC returns a typed record whose fields are directly
        // addressable but the instance itself may not be a plain object;
        // check field-by-field so the assertion works either way.
        expect(decoded).not.toBeNull();
        expect(typeof decoded).toBe("object");
        const record = decoded as Record<string, unknown>;
        assertScalarsMatch(record);
      },
    },
    {
      label: "Protobuf Dynamic",
      format: DataFormat.PROTOBUF,
      schema: PROTOBUF_SCHEMA_V1,
      envelope: protobufEnvelope,
      schemaLabel: "protobuf-dynamic",
      messageFullName: INTEROP_PROTOBUF_FULL_NAME,
      protobuf: { messageType: "DYNAMIC" },
      assertDecodedMatches(decoded: unknown): void {
        // DYNAMIC decode goes through `Type.toObject({defaults:false})`,
        // yielding a plain object whose keys are the wire-present fields.
        // proto3 empty-string default: absent fields are dropped by
        // `defaults:false`, so an unset `email` shows as no key at all.
        expect(decoded).not.toBeNull();
        expect(typeof decoded).toBe("object");
        assertScalarsMatch(decoded as Record<string, unknown>);
      },
    },
    {
      label: "Protobuf concrete",
      format: DataFormat.PROTOBUF,
      schema: PROTOBUF_SCHEMA_V1,
      envelope: protobufEnvelope,
      schemaLabel: "protobuf-pojo",
      messageFullName: INTEROP_PROTOBUF_FULL_NAME,
      protobuf: { messageType: "POJO" },
      assertDecodedMatches(decoded: unknown): void {
        // POJO decode returns the `protobufjs` message instance directly.
        // Fields are enumerable properties on the instance; comparing by
        // property access lets this pass whether the returned object is a
        // Message subclass or a plain-object shim.
        expect(decoded).not.toBeNull();
        expect(typeof decoded).toBe("object");
        const record = decoded as Record<string, unknown>;
        assertScalarsMatch(record);
      },
    },
    {
      label: "JSON-Schema",
      format: DataFormat.JSON,
      schema: JSON_SCHEMA_V1,
      envelope: jsonEnvelope,
      schemaLabel: "json",
      assertDecodedMatches(decoded: unknown): void {
        // JSON decode is `JSON.parse` on the payload's UTF-8 text; the
        // record surfaces as a plain object equal to the input record.
        expect(decoded).toEqual({
          id: CUSTOMER_RECORD_V1.id,
          name: CUSTOMER_RECORD_V1.name,
          age: CUSTOMER_RECORD_V1.age,
        });
      },
    },
  ];
}

describeIntegration(SUITE_NAME, () => {
  // Set once in `beforeAll` after every gate has passed. When any of these
  // are still null on entry to a test body, the body no-ops (the outer
  // gate already emitted its SKIP).
  let sidecar: Sidecar | null = null;
  let broker: BrokerHandle | null = null;
  let cleanup: CleanupTracker | null = null;
  let gateSkipReason: string | null = null;
  let brokerSkipReason: string | null = null;
  const region: string = resolveRegion();

  beforeAll(async () => {
    // Interop gate: `GSR_GLUE=real`, JVM/JAR present, `GSR_INTEROP_PARTNER`
    // is `java` (or unset). Runs INSIDE `beforeAll` (not at describe body
    // evaluation time) so `describeIntegration`'s own AWS_INTEGRATION line
    // is not duplicated on env-gate misses — `describe.skip` still
    // evaluates the body, so any top-level `requireInterop()` would emit
    // a redundant SKIP line for the same missing gate.
    const gate = requireInterop({ suiteName: SUITE_NAME });
    if (gate.skipped) {
      gateSkipReason = gate.reason;
      return;
    }

    // Broker discovery is the secondary gate: even with real Glue + JVM
    // present, a missing Docker daemon (and no `KAFKA_BROKER=<external>`)
    // must loud-skip rather than fail. When it skips, we short-circuit
    // every test in the suite; the outer gate already logged the reason.
    const resolved = await resolveBroker();
    if (isSkip(resolved)) {
      brokerSkipReason = resolved.skip;
      warnSkip(SUITE_NAME, resolved.skip);
      return;
    }
    broker = resolved;

    // Sidecar start-up: spawns the JVM, scrapes the ephemeral port line,
    // polls /health. Once returned, the sidecar owns its own Glue client
    // (built from `AWS_REGION` inside the JVM) so we do not have to pass a
    // TS-side Glue client into the /kafka-produce call.
    sidecar = await startSidecar({
      startTimeoutMs: SIDECAR_START_TIMEOUT_MS,
    });

    // The TS side is only a consumer for this direction, but we still
    // build a raw AWS SDK Glue client so `CleanupTracker` can send
    // `DeleteSchemaCommand`s for every schema the sidecar registered. We
    // do NOT go through `@gsr/core`'s `createGlueClient` here — that
    // returns the narrow `GlueClient` seam (encode/decode-path operations
    // only), which the tracker does not accept. `buildGlueClientConfig`
    // is the shared config-builder both paths use, so DeleteSchema routes
    // to the same account/region/endpoint as the sidecar's writes.
    const cfg: GsrConfig = {
      ...parseConfig({}),
      region,
      registryName: DEFAULT_REGISTRY_NAME,
    };
    const cleanupSdk = new SdkGlueClient(buildGlueClientConfig(cfg));
    cleanup = new CleanupTracker(cleanupSdk);
  }, SIDECAR_START_TIMEOUT_MS + 60_000);

  afterAll(async () => {
    // Reverse the acquisition order: cleanup schemas → stop sidecar → stop
    // broker. A failure in one branch must NOT skip the remaining
    // teardown, so each phase is wrapped in its own try/finally.
    try {
      if (cleanup !== null) {
        await cleanup.run();
      }
    } finally {
      try {
        if (sidecar !== null) {
          await sidecar.stop();
          sidecar = null;
        }
      } finally {
        if (broker !== null) {
          await broker.stop();
          broker = null;
        }
      }
    }
  }, 120_000);

  for (const compression of ["NONE", "ZLIB"] as const) {
    for (const cell of buildMatrix()) {
      const title = `${cell.label} / ${compression}`;
      it(
        title,
        async () => {
          if (gateSkipReason !== null || brokerSkipReason !== null) {
            // A prior gate already emitted its SKIP line; the test body
            // short-circuits so the reporter output stays quiet. Vitest
            // still counts the test as passed, which matches the
            // loud-skip contract (never silently fail on missing gates).
            return;
          }
          if (sidecar === null || broker === null || cleanup === null) {
            throw new Error(
              `${SUITE_NAME}: expected sidecar/broker/cleanup after gates opened`,
            );
          }

          // Per-run namespacing: fresh schema name and fresh topic per
          // cell. `realSchemaName` embeds the runbook prefix + epoch +
          // random hex so parallel runs never collide.
          const schemaName = realSchemaName(
            `interop-${cell.schemaLabel}-${compression.toLowerCase()}`,
          );
          const topic = `${RUNBOOK_PREFIX}interop-${cell.schemaLabel}-${compression.toLowerCase()}-${Date.now()}`;

          // Track the schema BEFORE the sidecar registers it. If
          // /kafka-produce fails after CreateSchema, cleanup still deletes
          // the schema — the tracker is create-scoped by intent, not by
          // observation.
          cleanup.track(DEFAULT_REGISTRY_NAME, schemaName);

          // Java side registers + produces via /kafka-produce. The sidecar
          // internally drives `GlueSchemaRegistryKafkaSerializer.serialize`
          // against the real Java GSR client, so the wire bytes on Kafka
          // are what the canonical Java library emits.
          const produced = await sidecar.kafkaProduce({
            format: cell.format,
            schema: cell.schema,
            schemaName,
            record: cell.envelope,
            compression,
            bootstrap: broker.bootstrap,
            topic,
            region,
          });
          expect(typeof produced.schemaVersionId).toBe("string");
          expect(produced.schemaVersionId.length).toBeGreaterThan(0);
          expect(produced.bytes.length).toBeGreaterThan(0);

          // TS side consumes via `kafkajs`. Kept inline (rather than
          // pulled into the Tier-2 `kafka-roundtrip.integ.test.ts` file)
          // because that file is left intact per the spec's
          // "author kafka-broker once" rule — the transport layer stays a
          // per-suite concern.
          const { Kafka, logLevel } = await import("kafkajs");
          const clientId = "gsr-interop-consumer";
          const kafka = new Kafka({
            clientId,
            brokers: [broker.bootstrap],
            logLevel: logLevel.NOTHING,
          });

          const consumer = kafka.consumer({
            groupId: `${clientId}-${Date.now()}-${Math.random()
              .toString(16)
              .slice(2, 8)}`,
          });
          await consumer.connect();
          let framed: Buffer;
          try {
            await consumer.subscribe({ topic, fromBeginning: true });
            framed = await new Promise<Buffer>((resolveP, rejectP) => {
              const timer = setTimeout(
                () =>
                  rejectP(
                    new Error(
                      `timed out after ${CONSUME_TIMEOUT_MS}ms waiting for a message on ${topic}`,
                    ),
                  ),
                CONSUME_TIMEOUT_MS,
              );
              void consumer
                .run({
                  eachMessage: async ({ message }) => {
                    if (message.value !== null) {
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

          // UUID check — decode the wire header on the TS side and
          // confirm it matches the schema-version UUID the sidecar
          // reported. `decodeMessage` also parses the compression byte;
          // we assert that separately so a mismatch (e.g. the sidecar
          // shipped NONE bytes when we asked for ZLIB) is caught.
          const wire = decodeMessage(framed, {
            protobuf: cell.format === DataFormat.PROTOBUF,
          });
          expect(wire.schemaVersionId).toBe(produced.schemaVersionId);
          expect(wire.compressionType).toBe(compression);

          // Decode via `GsrDeserializer`. The facade drives the same
          // `decodeMessage` internally, then routes to the format-specific
          // serde with our subtype-selector opts.
          const deserializer = new GsrDeserializer();
          const decoded = deserializer.deserialize({
            format: cell.format,
            data: framed,
            schema: cell.schema,
            messageFullName: cell.messageFullName,
            avro: cell.avro,
            protobuf: cell.protobuf,
          });

          cell.assertDecodedMatches(decoded);
        },
        CONSUME_TIMEOUT_MS + 60_000,
      );
    }
  }
});

