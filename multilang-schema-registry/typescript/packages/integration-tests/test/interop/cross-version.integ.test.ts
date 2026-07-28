/**
 * Cross-version interop suite (both directions, three formats).
 *
 * The interop matrix cells this file realizes:
 *
 *   TS → Java (writer=TS, reader=Java)
 *     - Avro cross-version
 *     - Protobuf cross-version
 *     - JSON-Schema cross-version
 *   Java → TS (writer=Java, reader=TS)
 *     - Avro cross-version
 *     - Protobuf cross-version
 *     - JSON-Schema cross-version
 *
 * Each cell runs under compression `NONE` (mandatory) and a `ZLIB` variant,
 * giving 6 cells × 2 compressions × 2 directions = 12 sub-tests.
 *
 * The proof is *writer-schema lookup*: register v1 with `BACKWARD`
 * compatibility, register a successor v2 under the same schema name so v2 is
 * the "latest" version in the registry, then produce a record framed at v1
 * and prove the other runtime decodes v1 bytes by fetching the v1 writer
 * schema via the header UUID — v2's presence must not disrupt decoding.
 *
 * ## Three-gate discipline (loud-skip contract)
 *
 * 1. `*.integ.test.ts` suffix — the default `npm test` never selects this
 *    file (the default vitest config excludes the suffix).
 * 2. `describeIntegration` — env gate. Emits one loud-skip line when
 *    `AWS_INTEGRATION!=1`.
 * 3. `requireInterop()` inside `beforeAll` — the interop-specific gate,
 *    stacking `GSR_GLUE=real`, `GSR_INTEROP_PARTNER=(java|unset)`, and
 *    JVM-and-JAR availability. Loud-skip on any missing gate.
 * 4. Broker discovery via the shared `resolveBroker()` — loud-skip when
 *    `KAFKA_BROKER=""` or Docker is unavailable.
 *
 * When any gate skips, every test in the suite short-circuits without
 * failing.
 *
 * ## Create-scoped cleanup
 *
 * Every schema this suite registers uses `realSchemaName("xver")` so the
 * name is prefixed with `gsr-ts-it-` + a per-run suffix (epoch-second + hex).
 * The `CleanupTracker` records each name at registration time and deletes
 * them in reverse insertion order in `afterAll`, tolerating
 * `EntityNotFoundException`. No registry is ever created or deleted; every
 * schema lives inside the pre-existing Glue `default-registry`.
 *
 * ## Reuse discipline
 *
 * This file IMPORTS launcher, gate, fixtures, broker-discovery, env-gate,
 * and cleanup modules — it never re-implements them:
 *
 *   - `src/interop-gate.ts`   — `requireInterop()`
 *   - `src/java-sidecar.ts`   — `startSidecar()`
 *   - `src/kafka-broker.ts`   — `resolveBroker()` + broker teardown
 *   - `src/env-gate.ts`       — `describeIntegration`, `resolveRegion`, creds
 *   - `src/real-glue.ts`      — `CleanupTracker`, `realSchemaName`, backend
 *   - `test/interop/interop-fixtures.ts` — schemas + records + envelopes
 *   - `@gsr/core`             — registrar, cache, metadata, header decode
 *   - `@gsr/serde`            — GsrSerializer / GsrDeserializer / DataFormat
 */
import {
  createCache,
  createMetadata,
  decodeMessage,
  parseConfig,
  SchemaRegistrar,
  type CompressionType,
  type GlueClient,
  type GsrConfig,
} from "@gsr/core";
import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
} from "@gsr/serde";
import type { Consumer } from "kafkajs";
import { afterAll, beforeAll, expect, it } from "vitest";

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
import {
  CleanupTracker,
  realSchemaName,
} from "../../src/real-glue.js";
import {
  startSidecar,
  type Sidecar,
} from "../../src/java-sidecar.js";
import {
  AVRO_SCHEMA_V1,
  AVRO_SCHEMA_V2,
  CUSTOMER_RECORD_V1,
  INTEROP_NAMESPACE,
  INTEROP_PROTOBUF_FULL_NAME,
  INTEROP_RECORD_NAME,
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
} from "./interop-fixtures.js";

/**
 * Registry name used by every schema this suite registers. The suite never
 * creates or deletes a registry; `default-registry` is Glue's auto-created
 * default and exists in every account.
 */
const REGISTRY_NAME = "default-registry";

/**
 * Suite name used in the machine-parseable `SKIP <name> — <reason>` line
 * when any gate short-circuits the suite. Kept short so log-scanner regex
 * stays simple.
 */
const SUITE_NAME = "interop/cross-version";

/**
 * The (format, compression) matrix. Six cells: {AVRO,PROTOBUF,JSON} ×
 * {NONE,ZLIB}. Every cell exercises both directions, so the fully-realized
 * matrix is 12 sub-tests per direction container.
 *
 * `dataFormat` is the enum value passed to the serde facade; `sidecarFormat`
 * is the token the Java sidecar's HTTP contract expects (`AVRO|PROTOBUF|JSON`);
 * both must line up 1:1. Explicit typing keeps a rename on one side surfacing
 * at compile time on the other.
 */
interface CellDef {
  readonly name: string;
  readonly dataFormat: DataFormat;
  readonly sidecarFormat: "AVRO" | "PROTOBUF" | "JSON";
  readonly compression: CompressionType;
  readonly schemaV1: string;
  readonly schemaV2: string;
}

const CELLS: readonly CellDef[] = [
  {
    name: "AVRO_NONE",
    dataFormat: DataFormat.AVRO,
    sidecarFormat: "AVRO",
    compression: "NONE",
    schemaV1: AVRO_SCHEMA_V1,
    schemaV2: AVRO_SCHEMA_V2,
  },
  {
    name: "AVRO_ZLIB",
    dataFormat: DataFormat.AVRO,
    sidecarFormat: "AVRO",
    compression: "ZLIB",
    schemaV1: AVRO_SCHEMA_V1,
    schemaV2: AVRO_SCHEMA_V2,
  },
  {
    name: "PROTOBUF_NONE",
    dataFormat: DataFormat.PROTOBUF,
    sidecarFormat: "PROTOBUF",
    compression: "NONE",
    schemaV1: PROTOBUF_SCHEMA_V1,
    schemaV2: PROTOBUF_SCHEMA_V2,
  },
  {
    name: "PROTOBUF_ZLIB",
    dataFormat: DataFormat.PROTOBUF,
    sidecarFormat: "PROTOBUF",
    compression: "ZLIB",
    schemaV1: PROTOBUF_SCHEMA_V1,
    schemaV2: PROTOBUF_SCHEMA_V2,
  },
  {
    name: "JSON_NONE",
    dataFormat: DataFormat.JSON,
    sidecarFormat: "JSON",
    compression: "NONE",
    schemaV1: JSON_SCHEMA_V1,
    schemaV2: JSON_SCHEMA_V2,
  },
  {
    name: "JSON_ZLIB",
    dataFormat: DataFormat.JSON,
    sidecarFormat: "JSON",
    compression: "ZLIB",
    schemaV1: JSON_SCHEMA_V1,
    schemaV2: JSON_SCHEMA_V2,
  },
];

/**
 * Kafka consumer poll timeout for a single interop record. Cross-version
 * cells only publish one message; the read window can be short. The `120s`
 * hard cap on the test itself is the outer envelope.
 */
const CONSUME_TIMEOUT_MS = 60_000;

describeIntegration(SUITE_NAME, () => {
  // -----------------------------------------------------------------------
  // Suite-wide state resolved in `beforeAll`.
  // -----------------------------------------------------------------------

  /**
   * Non-null iff every gate opened. When null, every `it()` short-circuits
   * without failing — the loud-skip line was emitted in `beforeAll`, so the
   * suite already reported its status.
   */
  let ready:
    | null
    | {
        cfg: GsrConfig;
        registrar: SchemaRegistrar;
        cleanup: CleanupTracker;
        /**
         * The narrow `GlueClient` seam from `@gsr/core`. Used for the
         * writer-schema-lookup step in the Java→TS direction:
         * `getSchemaVersion({ SchemaVersionId })` returns the schema
         * definition the writer stamped in the wire header.
         */
        glueClient: GlueClient;
        broker: BrokerHandle;
        sidecar: Sidecar;
        region: string;
        runSuffix: string;
      } = null;

  let sidecarHandle: Sidecar | null = null;
  let brokerHandle: BrokerHandle | null = null;

  beforeAll(async () => {
    // ------------------------------------------------------------
    // Gate stack: creds → interop (JVM/JAR/partner) → broker.
    // Emit a single machine-parseable SKIP line on any missing gate.
    // ------------------------------------------------------------

    // Hard-fail (throw, don't skip) when `GSR_GLUE=real` and no creds resolve;
    // matches the credential-hard-fail contract every real-Glue suite honors.
    requireRealCreds();

    const gate = requireInterop({ suiteName: SUITE_NAME });
    if (gate.skipped) {
      // `requireInterop` already emitted the machine-parseable SKIP line.
      // Leaving `ready` null causes every `it()` in the suite to no-op.
      return;
    }

    const resolved = await resolveBroker();
    if (isSkip(resolved)) {
      warnSkip(SUITE_NAME, resolved.skip);
      return;
    }
    brokerHandle = resolved;

    // ------------------------------------------------------------
    // Sidecar + registrar wiring on the real-Glue path.
    // ------------------------------------------------------------
    const region = resolveRegion();
    const cfg = parseConfig({
      region,
      registryName: REGISTRY_NAME,
      schemaAutoRegistrationEnabled: "true",
      // BACKWARD is the compatibility mode the writer-schema-lookup proof
      // requires: v2 must be readable by a v1 reader, so the header UUID's
      // schema is a safe fallback for consumers that fetched v2 in advance.
      compatibility: "BACKWARD",
    });

    const { createGlueClient, buildGlueClientConfig } = await import("@gsr/core");
    const registrarClient = createGlueClient(cfg);
    const cache = createCache<{ schemaVersionId: string }>({
      ttlMillis: cfg.timeToLiveMillis,
      size: cfg.cacheSize,
    });
    const metadata = createMetadata(registrarClient, cfg);
    const registrar = new SchemaRegistrar(registrarClient, cache, cfg, metadata);

    // Cleanup goes through a raw SDK client because `DeleteSchemaCommand` is
    // not in the narrow `GlueClient` seam (teardown-only concern). Reuses
    // the identical config the registrar client was built from so both
    // agree on region + credentials.
    const { GlueClient: SdkGlueClient, DeleteSchemaCommand } = await import(
      "@aws-sdk/client-glue"
    );
    const sdkClient = new SdkGlueClient(buildGlueClientConfig(cfg));
    const cleanup = new CleanupTracker({
      send: async (cmd) => {
        // The typed shape here matches `CleanupClient.send` from
        // `real-glue.ts`: exactly `DeleteSchemaCommand` in, matching output
        // out. The runtime cast is unavoidable — the SDK `send` overload set
        // is opaque to the seam interface.
        void DeleteSchemaCommand;
        return await sdkClient.send(cmd);
      },
    });

    sidecarHandle = await startSidecar();

    ready = {
      cfg,
      registrar,
      cleanup,
      glueClient: registrarClient,
      broker: brokerHandle,
      sidecar: sidecarHandle,
      region,
      runSuffix: makeShortRunSuffix(),
    };
  }, 180_000);

  afterAll(async () => {
    // ------------------------------------------------------------
    // Teardown runs on success AND failure (afterAll is unconditional).
    // Cleanup order: sidecar → broker → schemas. Schemas are last so a
    // JVM hang during shutdown does not delay the delete calls.
    // ------------------------------------------------------------
    const errors: unknown[] = [];
    if (sidecarHandle !== null) {
      try {
        await sidecarHandle.stop();
      } catch (err) {
        errors.push(err);
      }
      sidecarHandle = null;
    }
    if (brokerHandle !== null) {
      try {
        await brokerHandle.stop();
      } catch (err) {
        errors.push(err);
      }
      brokerHandle = null;
    }
    if (ready !== null) {
      try {
        await ready.cleanup.run();
      } catch (err) {
        errors.push(err);
      }
    }
    if (errors.length > 0) {
      throw new AggregateError(
        errors.map((e) => (e instanceof Error ? e : new Error(String(e)))),
        `interop cross-version teardown: ${errors.length} error(s)`,
      );
    }
  });

  // -----------------------------------------------------------------------
  // Direction 1 — TS → Java
  //   TS registers v1 (BACKWARD) + v2, TS serializes v1, produces to Kafka;
  //   Java sidecar /kafka-consume reads and returns the v1 envelope.
  //   Assertions: v1 fields recovered, v2-only field absent.
  // -----------------------------------------------------------------------
  for (const cell of CELLS) {
    it(
      `TS→Java cross-version ${cell.name}`,
      async () => {
        if (ready === null) {
          // beforeAll emitted the loud skip; every test short-circuits.
          return;
        }
        const {
          registrar,
          cleanup,
          glueClient,
          broker,
          sidecar,
          region,
          runSuffix,
        } = ready;

        const schemaName = realSchemaName(`xver-tsjava-${cell.name}-${runSuffix}`);
        cleanup.track(REGISTRY_NAME, schemaName);
        const topic = `gsr-ts-it-xver-tsjava-${runSuffix}-${cell.name.toLowerCase()}`;

        // 1) Register v1 via the TS registrar. `CreateSchema` writes v1 with
        //    the configured `BACKWARD` compatibility.
        const v1Uuid = await registrar.getSchemaVersionId(
          {
            schemaName,
            dataFormat: cell.sidecarFormat,
            schemaDefinition: cell.schemaV1,
          },
          topic,
        );
        expect(v1Uuid).toMatch(UUID_REGEX);

        // 2) Register v2 under the same schema name — but NOT via the
        //    registrar. The registrar's cache key is `${schemaName}:${dataFormat}`
        //    (the schema *definition* is not part of the key), so a second
        //    `registrar.getSchemaVersionId(...)` call with the same schemaName
        //    + dataFormat short-circuits on the cache set by step 1 and
        //    returns v1's UUID with zero Glue calls — v2 would never be
        //    registered and the cross-version proof would collapse.
        //
        //    Call the narrow-seam `registerSchemaVersion` directly. This
        //    hits `RegisterSchemaVersion` on Glue unconditionally, gives v2
        //    a distinct UUID, and preserves the "v2 is latest" invariant.
        //    Poll `getSchemaVersion` until `AVAILABLE` because the API
        //    returns `PENDING` initially — matches the registrar's
        //    poll-until-available discipline (see `pollUntilAvailable` in
        //    `registrar.ts`).
        const v2Uuid = await registerV2DirectAndWait(
          glueClient,
          schemaName,
          cell.schemaV2,
        );
        expect(v2Uuid).toMatch(UUID_REGEX);
        expect(v2Uuid).not.toBe(v1Uuid);

        // 3) Encode a v1 record with the TS serializer, using v1's UUID. This
        //    is what proves writer-schema lookup: the receiver decodes v1
        //    bytes even though v2 is now the latest.
        const serializer = new GsrSerializer({ compression: cell.compression });
        const wire = serializer.serialize(buildTsRequest(cell, v1Uuid, topic));

        // Sanity-check the header UUID before shipping — a mis-encoded UUID
        // would be an ambiguous consumer-side error, not a decoder-side one.
        const decodedHeader = decodeMessage(wire, {
          protobuf: cell.dataFormat === DataFormat.PROTOBUF,
        });
        expect(decodedHeader.schemaVersionId).toBe(v1Uuid);

        // 4) Produce via kafkajs to the interop topic.
        await produceOne(broker.bootstrap, topic, wire);

        // 5) Java sidecar /kafka-consume — the sidecar decodes via the real
        //    Java GSR library which fetches the v1 writer schema from Glue.
        const groupId = `${SUITE_NAME}-tsjava-${cell.name}-${runSuffix}`;
        const response = await sidecar.kafkaConsume({
          bootstrap: broker.bootstrap,
          topic,
          format: cell.sidecarFormat,
          groupId,
          region,
          timeoutMs: CONSUME_TIMEOUT_MS,
        });

        // 6) Structural assertions on the response.
        expect(response.dataFormat).toBe(cell.sidecarFormat);
        expect(response.schemaVersionId).toBe(v1Uuid);
        // The sidecar returned the writer schema (v1) — not the latest (v2) —
        // because the header UUID drives the lookup.
        expectSchemaBodyEqual(cell.sidecarFormat, response.schemaDefinition, cell.schemaV1);

        // 7) Read the envelope back to v1 record shape and assert field-level
        //    recovery + evolution parity (v2-only `email` is absent).
        assertJavaEnvelopeV1(cell.sidecarFormat, response.record);
      },
      180_000,
    );
  }

  // -----------------------------------------------------------------------
  // Direction 2 — Java → TS
  //   Java sidecar produces v1 bytes; TS consumes them, extracts the header
  //   UUID, fetches the writer schema (v1) from Glue, and decodes via the
  //   `@gsr/serde` deserializer.
  //   Assertions: v1 fields recovered, v2-only field absent.
  //
  //   The writer-schema-lookup path is done at the test level (rather than
  //   inside the deserializer) because the `@gsr/serde` deserializer is
  //   transport-agnostic and does not fetch schemas from Glue itself; the
  //   integration point at cross-version scenarios is the header-UUID →
  //   `GetSchemaVersion` lookup layered on top.
  // -----------------------------------------------------------------------
  for (const cell of CELLS) {
    it(
      `Java→TS cross-version ${cell.name}`,
      async () => {
        if (ready === null) {
          return;
        }
        const {
          cleanup,
          glueClient,
          broker,
          sidecar,
          region,
          runSuffix,
        } = ready;

        const schemaName = realSchemaName(`xver-javats-${cell.name}-${runSuffix}`);
        cleanup.track(REGISTRY_NAME, schemaName);
        const topic = `gsr-ts-it-xver-javats-${runSuffix}-${cell.name.toLowerCase()}`;
        const throwawayV1Topic = `${topic}-v1reg`;
        const throwawayV2Topic = `${topic}-v2reg`;

        // 1) Register v1 via a Java-sidecar throwaway produce. The Java GSR
        //    library's `CreateSchema` path writes v1 with `BACKWARD` compat.
        //    (Sidecar honors the request's Compatibility parameter verbatim.)
        await sidecar.kafkaProduce({
          format: cell.sidecarFormat,
          schema: cell.schemaV1,
          schemaName,
          record: buildSidecarEnvelope(cell, /* useV1 */ true),
          compression: cell.compression,
          bootstrap: broker.bootstrap,
          topic: throwawayV1Topic,
          region,
          compatibility: "BACKWARD",
        });

        // 2) Register v2 under the same schema name via a second throwaway
        //    produce. Second `CreateSchema` sees `AlreadyExistsException` →
        //    the library falls through to `RegisterSchemaVersion` and writes
        //    v2 as a successor version (v2 is now the "latest" in Glue).
        await sidecar.kafkaProduce({
          format: cell.sidecarFormat,
          schema: cell.schemaV2,
          schemaName,
          record: buildSidecarEnvelope(cell, /* useV1 */ true),
          compression: cell.compression,
          bootstrap: broker.bootstrap,
          topic: throwawayV2Topic,
          region,
          compatibility: "BACKWARD",
        });

        // 3) Java sidecar /kafka-produce writes a v1 record to the main
        //    topic. The sidecar's serializer fetches v1's schema-version-id
        //    (already registered in step 1) and stamps it in the header.
        const produceResp = await sidecar.kafkaProduce({
          format: cell.sidecarFormat,
          schema: cell.schemaV1,
          schemaName,
          record: buildSidecarEnvelope(cell, /* useV1 */ true),
          compression: cell.compression,
          bootstrap: broker.bootstrap,
          topic,
          region,
          compatibility: "BACKWARD",
        });
        expect(produceResp.schemaVersionId).toMatch(UUID_REGEX);
        const v1Uuid = produceResp.schemaVersionId;

        // 4) TS consumes the v1-framed bytes from Kafka.
        const framed = await consumeOne(
          broker.bootstrap,
          topic,
          `${SUITE_NAME}-javats-${cell.name}-${runSuffix}`,
        );

        // Sanity-check: the header UUID on the wire matches the sidecar's
        // reported produce UUID. Off-by-one on the topic partition would
        // otherwise surface here as a schema-version mismatch.
        const isProtobuf = cell.dataFormat === DataFormat.PROTOBUF;
        const decodedHeader = decodeMessage(framed, { protobuf: isProtobuf });
        expect(decodedHeader.schemaVersionId).toBe(v1Uuid);

        // 5) Fetch the writer schema (v1) from Glue via the header UUID.
        //    This is the writer-schema-lookup step: v2 is the "latest"
        //    version in the registry, but the header carries v1's UUID and
        //    that is what we decode against.
        const writerSchemaResp = await glueClient.getSchemaVersion({
          SchemaVersionId: v1Uuid,
        });
        expect(writerSchemaResp.SchemaDefinition).toBeTruthy();
        expect(writerSchemaResp.DataFormat).toBe(cell.sidecarFormat);
        const writerSchema = writerSchemaResp.SchemaDefinition!;
        // The recovered writer schema is v1 — not v2 — proving the lookup.
        expectSchemaBodyEqual(cell.sidecarFormat, writerSchema, cell.schemaV1);

        // 6) Decode with the recovered writer schema via `@gsr/serde`. The
        //    facade routes on `format`; protobuf needs the message name.
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

        // 7) Assert v1 field-level recovery + v2-only-field-absence.
        assertTsDecodeV1(cell.dataFormat, decoded);
      },
      180_000,
    );
  }
});

// ---------------------------------------------------------------------------
// Serializer request builders + envelope assertions
// ---------------------------------------------------------------------------

/**
 * Build a `SerializeRequest` for the TS→Java direction. Protobuf carries the
 * fully-qualified message name; Avro / JSON encode a plain record whose
 * shape mirrors `CUSTOMER_RECORD_V1`.
 */
function buildTsRequest(
  cell: CellDef,
  schemaVersionId: string,
  topic: string,
): Parameters<GsrSerializer["serialize"]>[0] {
  const record = { ...CUSTOMER_RECORD_V1 };
  const base = {
    format: cell.dataFormat,
    schemaVersionId,
    topic,
    schema: cell.schemaV1,
    data: record,
  };
  if (cell.dataFormat === DataFormat.PROTOBUF) {
    return { ...base, messageFullName: INTEROP_PROTOBUF_FULL_NAME };
  }
  return base;
}

/**
 * Build the per-format sidecar envelope for a v1 record. Mirrors the
 * `record` field the sidecar's /kafka-produce contract accepts:
 * `AVRO { fields }`, `JSON { schema, payload }`, `PROTOBUF { messageTypeFullName, fieldsJson }`.
 */
function buildSidecarEnvelope(
  cell: CellDef,
  useV1: boolean,
): Record<string, unknown> {
  const record = { ...CUSTOMER_RECORD_V1 };
  // useV1 is currently always true — the sidecar's registration payload can
  // always ride a v1 record (v2 needs the extra `email` field but v2
  // registration under BACKWARD compat is a *schema* change, not a payload
  // change). The parameter is retained for readability: every call site is
  // explicit about which record shape it sends.
  void useV1;
  const schema = cell.schemaV1;
  switch (cell.sidecarFormat) {
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
      const unreachable: never = cell.sidecarFormat;
      throw new Error(`unreachable format: ${String(unreachable)}`);
    }
  }
}

/**
 * Assert the Java-side consumed envelope carries a v1 record shape: three
 * fields present, no `email` (or `email` present as null/empty from a v2
 * reader — see per-format notes).
 */
function assertJavaEnvelopeV1(
  format: "AVRO" | "PROTOBUF" | "JSON",
  envelope: Record<string, unknown>,
): void {
  switch (format) {
    case "AVRO": {
      // Envelope shape: { fields: { id, name, age } }.
      const v1 = readAvroEnvelopeV1({
        fields: (envelope as { fields?: Record<string, unknown> }).fields ?? {},
      });
      expect(v1.id).toBe(CUSTOMER_RECORD_V1.id);
      expect(v1.name).toBe(CUSTOMER_RECORD_V1.name);
      expect(v1.age).toBe(CUSTOMER_RECORD_V1.age);
      // The writer schema is v1 → the envelope must not carry the v2-only
      // `email` field. (Belt-and-braces: v2 reader receives null default.)
      const fields = (envelope as { fields?: Record<string, unknown> })
        .fields ?? {};
      const email = fields["email"];
      expect(email === undefined || email === null).toBe(true);
      return;
    }
    case "JSON": {
      const v1 = readJsonEnvelopeV1({
        schema: String((envelope as { schema?: unknown }).schema ?? ""),
        payload: String((envelope as { payload?: unknown }).payload ?? ""),
      });
      expect(v1.id).toBe(CUSTOMER_RECORD_V1.id);
      expect(v1.name).toBe(CUSTOMER_RECORD_V1.name);
      expect(v1.age).toBe(CUSTOMER_RECORD_V1.age);
      // The payload was written under v1 (three fields only), so a parse
      // of the payload has no `email` key. `readJsonEnvelopeV1` ignores
      // any incidental `email` if present; assert absence too.
      const parsed = JSON.parse(
        String((envelope as { payload?: unknown }).payload ?? "{}"),
      ) as Record<string, unknown>;
      expect(parsed["email"]).toBeUndefined();
      return;
    }
    case "PROTOBUF": {
      const v1 = readProtobufEnvelopeV1({
        messageTypeFullName: String(
          (envelope as { messageTypeFullName?: unknown })
            .messageTypeFullName ?? "",
        ),
        fieldsJson: String(
          (envelope as { fieldsJson?: unknown }).fieldsJson ?? "{}",
        ),
      });
      expect(v1.id).toBe(CUSTOMER_RECORD_V1.id);
      expect(v1.name).toBe(CUSTOMER_RECORD_V1.name);
      expect(v1.age).toBe(CUSTOMER_RECORD_V1.age);
      // Writer schema is v1 (3 fields). The v2 reader would receive the
      // proto3-scalar default (`""`) for `email`, but under v1 writer the
      // fieldsJson emitted from a v1-schema reader will not include the
      // key at all.
      const parsed = JSON.parse(
        String((envelope as { fieldsJson?: unknown }).fieldsJson ?? "{}"),
      ) as Record<string, unknown>;
      // A proto3 zero-string scalar sometimes materializes as an empty
      // string, sometimes absent depending on the JSON marshaller. Tolerate
      // both — the important claim is that the recovered v1 fields match.
      const email = parsed["email"];
      expect(email === undefined || email === "").toBe(true);
      return;
    }
    default: {
      const unreachable: never = format;
      throw new Error(`unreachable format: ${String(unreachable)}`);
    }
  }
}

/**
 * Assert the TS-decoded value carries a v1 record shape. Per-format decode
 * shapes differ: Avro produces a plain object, protobuf a `Message`-shaped
 * plain object under DYNAMIC decode, JSON the parsed instance.
 */
function assertTsDecodeV1(format: DataFormat, decoded: unknown): void {
  switch (format) {
    case DataFormat.AVRO: {
      const rec = decoded as { id?: string; name?: string; age?: number; email?: unknown };
      expect(rec.id).toBe(CUSTOMER_RECORD_V1.id);
      expect(rec.name).toBe(CUSTOMER_RECORD_V1.name);
      expect(rec.age).toBe(CUSTOMER_RECORD_V1.age);
      // Writer schema is v1 → `email` must be absent from the decoded map.
      expect(rec.email).toBeUndefined();
      return;
    }
    case DataFormat.PROTOBUF: {
      const rec = decoded as { id?: string; name?: string; age?: number; email?: unknown };
      expect(rec.id).toBe(CUSTOMER_RECORD_V1.id);
      expect(rec.name).toBe(CUSTOMER_RECORD_V1.name);
      expect(rec.age).toBe(CUSTOMER_RECORD_V1.age);
      // The writer schema was v1 (3 fields). protobufjs's `toObject` on a
      // v1-decoded message never surfaces `email` — the field is not in the
      // parsed schema — so the property is absent.
      expect(rec.email).toBeUndefined();
      return;
    }
    case DataFormat.JSON: {
      // The serde returns the parsed record instance directly.
      const rec = decoded as {
        id?: string;
        name?: string;
        age?: number;
        email?: unknown;
      };
      expect(rec.id).toBe(CUSTOMER_RECORD_V1.id);
      expect(rec.name).toBe(CUSTOMER_RECORD_V1.name);
      expect(rec.age).toBe(CUSTOMER_RECORD_V1.age);
      expect(rec.email).toBeUndefined();
      return;
    }
    default: {
      const unreachable: never = format;
      throw new Error(`unreachable DataFormat: ${String(unreachable)}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Kafka helpers (thin wrappers over kafkajs, kept out of the main flow)
// ---------------------------------------------------------------------------

/**
 * Produce one message to `topic` with `value = wire`. Ensures the topic
 * exists first (via admin), then produces, then disconnects. Keeps every
 * kafkajs handle scoped to this call so a leaked producer never spans two
 * matrix cells.
 */
async function produceOne(
  bootstrap: string,
  topic: string,
  wire: Buffer,
): Promise<void> {
  const { Kafka, logLevel } = await import("kafkajs");
  const kafka = new Kafka({
    clientId: `${SUITE_NAME}-producer`,
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
      messages: [{ value: wire }],
    });
  } finally {
    await producer.disconnect();
  }
}

/**
 * Consume one message from `topic` (from beginning), returning its value as
 * a `Buffer`. Times out after `CONSUME_TIMEOUT_MS` — the timer bounds the
 * suite, not the outer test wrapper.
 */
async function consumeOne(
  bootstrap: string,
  topic: string,
  groupId: string,
): Promise<Buffer> {
  const { Kafka, logLevel } = await import("kafkajs");
  const kafka = new Kafka({
    clientId: `${SUITE_NAME}-consumer`,
    brokers: [bootstrap],
    logLevel: logLevel.NOTHING,
  });
  const consumer: Consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  try {
    return await new Promise<Buffer>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`consumeOne: timed out reading topic ${topic}`));
      }, CONSUME_TIMEOUT_MS);
      void consumer
        .run({
          eachMessage: async ({ message }) => {
            if (message.value !== null && message.value !== undefined) {
              clearTimeout(timer);
              resolve(Buffer.from(message.value));
            }
          },
        })
        .catch((err) => {
          clearTimeout(timer);
          reject(err instanceof Error ? err : new Error(String(err)));
        });
    });
  } finally {
    await consumer.disconnect();
  }
}

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

/**
 * A regex that matches the canonical 36-char UUID shape (8-4-4-4-12 hex).
 * Every schema-version UUID Glue emits matches this, so a mis-decoded field
 * surfaces immediately.
 */
const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Emit a single machine-parseable `SKIP <name> — <reason>` line matching
 * the shape the shared env-gate uses. Kept as a local helper because the
 * shared module's emitter is not exported (the interop tier must not edit
 * the shared file). Character-for-character identical to `env-gate.ts`'s
 * `emitLoudSkip` including the em-dash U+2014.
 */
function warnSkip(name: string, reason: string): void {
  // eslint-disable-next-line no-console
  console.warn(`SKIP ${name} — ${reason}`);
}

/**
 * A short random suffix used to per-run-namespace kafka topics + inner
 * portion of schema names. `realSchemaName()` already carries an
 * epoch+hex suffix; this second layer is for topics (which do not need the
 * runbook prefix) and for keeping the schema-name label distinct across
 * matrix cells so a leaked schema name is scannable.
 */
function makeShortRunSuffix(): string {
  return `${Date.now().toString(36)}${Math.random().toString(16).slice(2, 6)}`;
}

/**
 * Register a successor schema version directly against the narrow Glue seam,
 * bypassing the TS registrar entirely.
 *
 * Why bypass the registrar: `SchemaRegistrar.getSchemaVersionId` caches on
 * `${schemaName}:${dataFormat}` — the schema definition is NOT part of the
 * cache key. Once v1 is registered, a second registrar call with the same
 * (schemaName, dataFormat) short-circuits on the cache and returns v1's UUID
 * without touching Glue. Calling the seam's `registerSchemaVersion` directly
 * forces `RegisterSchemaVersion` to run against the real API and produces a
 * distinct UUID for v2.
 *
 * Poll `getSchemaVersion` until `AVAILABLE`: Glue may return `Status: PENDING`
 * on the initial response for compatibility-checked registrations. Mirrors
 * the poll cadence used inside `SchemaRegistrar.pollUntilAvailable` (10
 * attempts, 3s pre-sleep) so timing behavior matches the registrar path.
 */
async function registerV2DirectAndWait(
  glueClient: GlueClient,
  schemaName: string,
  schemaDefinition: string,
): Promise<string> {
  const regResp = await glueClient.registerSchemaVersion({
    SchemaId: {
      RegistryName: REGISTRY_NAME,
      SchemaName: schemaName,
    },
    SchemaDefinition: schemaDefinition,
  });
  if (!regResp.SchemaVersionId) {
    throw new Error(
      `registerSchemaVersion returned no SchemaVersionId for ${schemaName}`,
    );
  }
  const uuid = regResp.SchemaVersionId;
  if (regResp.Status === "AVAILABLE") {
    return uuid;
  }

  const maxAttempts = 10;
  const intervalMs = 3_000;
  let lastStatus: string | undefined = regResp.Status;
  for (let i = 0; i < maxAttempts; i++) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, intervalMs);
    });
    const pollResp = await glueClient.getSchemaVersion({
      SchemaVersionId: uuid,
    });
    lastStatus = pollResp.Status;
    if (lastStatus === "AVAILABLE") {
      return uuid;
    }
    if (lastStatus !== "PENDING") {
      throw new Error(
        `direct registerSchemaVersion poll: unexpected status ` +
          `schemaVersionId=${uuid} status=${String(lastStatus)}`,
      );
    }
  }
  throw new Error(
    `direct registerSchemaVersion poll exhausted after ${maxAttempts} attempts: ` +
      `schemaVersionId=${uuid} lastStatus=${String(lastStatus)}`,
  );
}

/**
 * Assert two schema definitions describe the same shape. String-equality is
 * not safe: JSON schemas may re-order keys, Avro definitions may re-order
 * fields, protobuf definitions may vary in whitespace. Compare by canonical
 * form per format.
 */
function expectSchemaBodyEqual(
  format: "AVRO" | "PROTOBUF" | "JSON",
  actual: string,
  expected: string,
): void {
  switch (format) {
    case "AVRO":
    case "JSON": {
      // Both are JSON blobs — parse and compare deep-equal.
      const a = JSON.parse(actual) as unknown;
      const e = JSON.parse(expected) as unknown;
      expect(a).toEqual(e);
      return;
    }
    case "PROTOBUF": {
      // Proto source: normalize whitespace + verify the message name and
      // scalar field count are the v1 shape (3 fields, no `email`). Full
      // canonical equality would need a proto parser; the shape claim is
      // enough for the writer-schema-lookup proof.
      const stripped = actual.replace(/\s+/g, " ").trim();
      expect(stripped).toContain(`package ${INTEROP_NAMESPACE}`);
      expect(stripped).toContain(`message ${INTEROP_RECORD_NAME}`);
      expect(stripped).toContain("string id = 1");
      expect(stripped).toContain("string name = 2");
      expect(stripped).toContain("int32 age = 3");
      // v2-only field must not be present in the recovered writer schema.
      expect(stripped).not.toContain("string email = 4");
      // Reference to the expected v1 body so a reader can find the
      // canonical source; the string-compare above is intentionally
      // shape-based rather than byte-for-byte.
      void expected;
      return;
    }
    default: {
      const unreachable: never = format;
      throw new Error(`unreachable format: ${String(unreachable)}`);
    }
  }
}
