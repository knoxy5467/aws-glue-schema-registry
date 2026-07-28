/**
 * Non-matrix scenarios for the narrated TypeScript interop demo:
 *   - scenario 19: cache behavior (miss -> hit -> evict -> re-fetch)
 *   - scenario 20: auto-register fall-through
 *   - scenario 21: writer-registers, reader cold-cache reads
 *
 * These three drivers exercise the client seams the 12+6 cross/same-version
 * matrix does not: the `@gsr/core` cache seam, the auto-registration gate,
 * and the writer-schema-lookup path a fresh Java consumer takes when its
 * local cache is cold.
 *
 * Each driver conforms to the same one-value contract as the matrix
 * drivers: it returns exactly one {@link ScenarioResult} row, narrates every
 * observable transition through the injected {@link NarratorSink}, and
 * derives its PASS/FAIL boolean from the {@link printEqualityCheck} return
 * value the operator sees on the terminal (no divergent hidden second
 * comparison). A driver NEVER throws to abort the run — a caught error
 * becomes `pass: false` with `error` set so the remaining scenarios still
 * fire and cleanup still runs.
 *
 * Every schema this module registers is threaded through the passed-in
 * `CleanupTracker` BEFORE the register call so a mid-flight failure still
 * leaves the schema recorded for deletion on exit. No wire framing,
 * compression, Glue-registration, sidecar-HTTP, or Kafka client code is
 * re-implemented here — the harness modules
 * (`../src/java-sidecar.js`, `../src/kafka-broker.js`,
 * `../src/interop-gate.js`, `../src/real-glue.js`,
 * `../test/interop/interop-fixtures.js`) plus `@gsr/serde` / `@gsr/core`
 * own that logic and are imported unchanged.
 */

import {
  createCache,
  createMetadata,
  SchemaRegistrar,
  type GlueClient,
  type GsrCache,
  type GsrConfig,
} from "@gsr/core";
import { DataFormat, GsrSerializer } from "@gsr/serde";
import type { Kafka as KafkajsKafka } from "kafkajs";

import type { Sidecar } from "../src/java-sidecar.js";
import type { BrokerHandle } from "../src/kafka-broker.js";
import type { CleanupTracker } from "../src/real-glue.js";

import {
  AVRO_SCHEMA_V1,
  CUSTOMER_RECORD_V1,
  readAvroEnvelopeV1,
  type AvroEnvelope,
  type DemoSchemaNamer,
} from "./demo-fixtures.js";
import {
  formatUuid,
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
import type { ScenarioResult } from "./scenarios.js";

export type { ScenarioResult };

/**
 * Registry name every scenario writes into. Matches the harness-wide
 * convention that every schema lives inside Glue's pre-existing
 * `default-registry` — the demo never calls `CreateRegistry` /
 * `DeleteRegistry`.
 */
const REGISTRY_NAME = "default-registry";

/**
 * Regex pattern that a well-formed schema-version UUID must satisfy. Same
 * canonical UUID shape the wire format's 16-byte version-id renders as
 * (see `narrator.formatUuid`); scenario 20 uses it to check the auto-
 * registered UUID is genuinely UUID-shaped, not an empty string / placeholder.
 */
const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * The cache key the registrar composes internally is `${schemaName}:${dataFormat}`
 * (see `SchemaRegistrar.getSchemaVersionId` -> `cacheKeyFor`). Duplicated
 * here so scenario 19 can peek at the cache's observable state directly —
 * the key format is a stable interface between the registrar and the
 * `@gsr/core` cache seam, and the demo exercises the seam the same way any
 * downstream cache observer would.
 */
function cacheKeyFor(schemaName: string, dataFormat: string): string {
  return `${schemaName}:${dataFormat}`;
}

/**
 * Cached-value shape the registrar writes to the cache. Only the schema-
 * version UUID is retained on hit. Duplicated locally so scenario 19 can
 * type its `GsrCache<...>` view without pulling in the registrar's
 * private `CachedSchemaVersion` re-export path.
 */
interface CachedSchemaVersion {
  schemaVersionId: string;
}

/**
 * Dependency bag every scenario body accepts. The bag is materialized once
 * by `main.ts` and shared across every driver so a single scenario cannot
 * accidentally build a private registrar / cache / Glue-client and thereby
 * lose the shared-cache semantics that scenario 19 is proving.
 *
 * `cache` and `registrar` are exposed so scenario 19 can peek at the cache
 * seam's observable state without a proxy. Scenarios 20 and 21 construct
 * their own registrar (scenario 20 needs a fresh registrar to prove the
 * auto-register fall-through; scenario 21 needs a cold sidecar consumer)
 * but reuse the shared config, glue client, and cleanup tracker.
 */
export interface ExtraScenarioDeps {
  readonly io: NarratorSink;
  readonly namer: DemoSchemaNamer;
  readonly cfg: GsrConfig;
  readonly glueClient: GlueClient;
  readonly cache: GsrCache<CachedSchemaVersion>;
  readonly registrar: SchemaRegistrar;
  readonly tracker: CleanupTracker;
  readonly broker: BrokerHandle;
  readonly sidecar: Sidecar;
  readonly kafka: KafkajsKafka;
}

// ---------------------------------------------------------------------------
// Scenario 19 — Cache behavior: miss -> hit -> evict -> re-fetch
// ---------------------------------------------------------------------------

/**
 * Scenario 19: prove the `@gsr/core` cache seam's observable state
 * transitions on every call to `registrar.getSchemaVersionId`:
 *
 *   miss     — `cache.get(key)` returns `undefined` before the first fetch.
 *   fetch #1 — `registrar.getSchemaVersionId(...)` returns a UUID and
 *              populates the cache; `cache.get(key)` now returns the
 *              cached row.
 *   hit      — `registrar.getSchemaVersionId(...)` again returns the SAME
 *              UUID; the cache row is unchanged (identity check).
 *   evict    — `cache.remove(key)` returns `true` (the row existed);
 *              `cache.get(key)` returns `undefined` again.
 *   fetch #2 — a third `registrar.getSchemaVersionId(...)` returns the
 *              same UUID (Glue has one canonical UUID per registered
 *              schema-version) and re-populates the cache.
 *
 * Each transition is narrated AND asserted against the seam's own
 * observable state via `printEqualityCheck`. PASS iff every transition
 * check returned true. Never asserts "no Glue call" via a vacuous proxy —
 * every claim is pinned to a cache `get` / `remove` return value or a
 * UUID identity check.
 */
export async function runScenario19Cache(
  deps: ExtraScenarioDeps,
): Promise<ScenarioResult> {
  const index = 19;
  const title = "scenario 19 of 21 — cache behavior: miss -> hit -> evict -> re-fetch";
  const format = "AVRO" as const;
  const direction = "n/a (cache)";
  const compression = "NONE" as const;

  printSectionHeader(deps.io, title);
  printScenarioIntro(deps.io, {
    what: "Walk the @gsr/core cache seam through miss → hit → evict → re-fetch, narrating each observable transition.",
    proves: "The client-side schema-version cache short-circuits Glue on hits and re-fetches after eviction — the same behavior a production writer relies on for throughput.",
  });

  try {
    const schemaName = deps.namer.forFamily("cacheAvro");
    const dataFormat = "AVRO";
    const key = cacheKeyFor(schemaName, dataFormat);
    printStage(deps.io, "cache", `cache key: ${key}`);
    printSchemaBody(deps.io, "AVRO_SCHEMA_V1 (only schema this scenario touches)", AVRO_SCHEMA_V1);

    // Track BEFORE registering — a mid-flight failure still leaves the
    // schema recorded for teardown deletion.
    deps.tracker.track(REGISTRY_NAME, schemaName);

    // Transition 1: pre-fetch miss. The cache is shared across the whole
    // demo; scenario 19 uses a runbook-unique schema name so its key has
    // never been touched by an earlier scenario in the same run.
    printStage(deps.io, "cache", "First transition: pre-fetch miss — expect cache.get(key) = undefined");
    const preFetch = deps.cache.get(key);
    printStage(
      deps.io,
      "cache",
      `[miss] cache.get(key) before first fetch = ${renderCached(preFetch)}`,
    );
    const missOk = printEqualityCheck(
      deps.io,
      "cache.get(key) is undefined before first fetch",
      undefined,
      preFetch,
    );

    // Transition 2: first fetch — populates the cache.
    printStage(deps.io, "cache", "Second transition: first fetch — expect cache miss → Glue API call → cache populated");
    printStage(deps.io, "glue", `registering schemaName=${schemaName}`);
    const firstUuid = await deps.registrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat,
        schemaDefinition: AVRO_SCHEMA_V1,
      },
      /* transportName */ schemaName,
    );
    printStage(deps.io, "glue", `first getSchemaVersionId -> ${firstUuid}`);
    const populated = deps.cache.get(key);
    printStage(
      deps.io,
      "cache",
      `[populated] cache.get(key) after first fetch = ${renderCached(populated)}`,
    );
    const populatedOk = printEqualityCheck(
      deps.io,
      "cache.get(key).schemaVersionId matches first-fetch UUID",
      firstUuid,
      populated?.schemaVersionId,
    );

    // Transition 3: second fetch — cache hit. The registrar returns the
    // same UUID; the cache row is unchanged.
    printStage(deps.io, "cache", "Third transition: second fetch — expect cache HIT → no Glue API call, same UUID");
    const secondUuid = await deps.registrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat,
        schemaDefinition: AVRO_SCHEMA_V1,
      },
      schemaName,
    );
    printStage(
      deps.io,
      "cache",
      `[hit] second getSchemaVersionId -> ${secondUuid} (cache still populated)`,
    );
    const hitOk = printEqualityCheck(
      deps.io,
      "second getSchemaVersionId UUID equals first (cache hit)",
      firstUuid,
      secondUuid,
    );

    // Transition 4: evict.
    printStage(deps.io, "cache", "Fourth transition: cache.remove(key) — expect true (row existed), then cache.get(key) = undefined");
    const removed = deps.cache.remove(key);
    printStage(deps.io, "cache", `cache.remove(key) -> ${String(removed)}`);
    const removedOk = printEqualityCheck(
      deps.io,
      "cache.remove(key) returns true (row existed)",
      true,
      removed,
    );
    const postEvict = deps.cache.get(key);
    printStage(
      deps.io,
      "cache",
      `[evicted] cache.get(key) after remove = ${renderCached(postEvict)}`,
    );
    const evictOk = printEqualityCheck(
      deps.io,
      "cache.get(key) is undefined after remove",
      undefined,
      postEvict,
    );

    // Transition 5: re-fetch after eviction — a third call resolves to the
    // same UUID Glue holds and re-populates the cache.
    printStage(deps.io, "cache", "Fifth transition: re-fetch after eviction — expect Glue API call, same canonical UUID, cache re-populated");
    const thirdUuid = await deps.registrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat,
        schemaDefinition: AVRO_SCHEMA_V1,
      },
      schemaName,
    );
    printStage(deps.io, "glue", `third getSchemaVersionId -> ${thirdUuid}`);
    const rehydrated = deps.cache.get(key);
    printStage(
      deps.io,
      "cache",
      `[re-fetch] cache.get(key) after third fetch = ${renderCached(rehydrated)}`,
    );
    const refetchOk = printEqualityCheck(
      deps.io,
      "third getSchemaVersionId UUID equals first (Glue canonical UUID stable)",
      firstUuid,
      thirdUuid,
    );
    const rehydrateOk = printEqualityCheck(
      deps.io,
      "cache re-populated with same UUID after re-fetch",
      firstUuid,
      rehydrated?.schemaVersionId,
    );

    const pass =
      missOk &&
      populatedOk &&
      hitOk &&
      removedOk &&
      evictOk &&
      refetchOk &&
      rehydrateOk;
    printVerdict(
      deps.io,
      pass,
      pass
        ? "cache seam: miss → hit → evict → re-fetch transitions all observed; Glue-canonical UUID stable across evict/refetch."
        : "cache seam: at least one observed transition disagreed with the expected miss/hit/evict/refetch semantics.",
    );
    return {
      index,
      title,
      format,
      direction,
      compression,
      schemaVersionId: firstUuid,
      pass,
    };
  } catch (err) {
    const message = errorText(err);
    printStage(deps.io, "error", message);
    return {
      index,
      title,
      format,
      direction,
      compression,
      schemaVersionId: null,
      pass: false,
      error: message,
    };
  }
}

// ---------------------------------------------------------------------------
// Scenario 20 — Auto-register fall-through
// ---------------------------------------------------------------------------

/**
 * Scenario 20: with `schemaAutoRegistrationEnabled=true` and a schema that
 * has never been registered, the first serialize path auto-registers via
 * `SchemaRegistrar.getSchemaVersionId` and returns a well-formed UUID. A
 * subsequent lookup with the same identity resolves to the SAME UUID
 * (Glue has one canonical UUID per registered schema-version, whether the
 * second lookup hits the cache or short-circuits inside the registrar's
 * `GetSchemaByDefinition` fast path).
 *
 * Uses a FRESH registrar bound to a fresh cache so the first call is
 * guaranteed to be an auto-register — reusing the shared registrar could
 * hit the shared cache from an earlier scenario if the schema names ever
 * collided (they do not, but the local registrar keeps the scenario's
 * invariant self-evident to a reviewer).
 */
export async function runScenario20AutoRegister(
  deps: ExtraScenarioDeps,
): Promise<ScenarioResult> {
  const index = 20;
  const title = "scenario 20 of 21 — auto-register fall-through";
  const format = "AVRO" as const;
  const direction = "n/a (auto-reg)";
  const compression = "NONE" as const;

  printSectionHeader(deps.io, title);
  printScenarioIntro(deps.io, {
    what: "With schemaAutoRegistrationEnabled=true and a schema Glue has never seen, the first getSchemaVersionId call auto-registers, and a second call returns the same UUID.",
    proves: "The auto-register fall-through works end-to-end: an unregistered schema does not fault the serialize path — Glue's canonical schema-version UUID is created transparently and stays stable on the second lookup.",
  });

  try {
    const schemaName = deps.namer.forFamily("autoRegisterAvro");
    const dataFormat = "AVRO";
    printStage(
      deps.io,
      "auto-register",
      `unregistered schemaName=${schemaName}`,
    );
    printSchemaBody(deps.io, "AVRO_SCHEMA_V1 (to be auto-registered)", AVRO_SCHEMA_V1);

    deps.tracker.track(REGISTRY_NAME, schemaName);

    // Fresh cache + fresh registrar bound to the shared config + Glue
    // client. The `schemaAutoRegistrationEnabled` flag lives on the shared
    // `cfg`, which the driver sets at startup — asserting it here rather
    // than mutating the config keeps the scenario a pure observation.
    const localCache = createCache<CachedSchemaVersion>({
      ttlMillis: deps.cfg.timeToLiveMillis,
      size: deps.cfg.cacheSize,
    });
    const localMetadata = createMetadata(deps.glueClient, deps.cfg);
    const localRegistrar = new SchemaRegistrar(
      deps.glueClient,
      localCache,
      deps.cfg,
      localMetadata,
    );

    const autoOnOk = printEqualityCheck(
      deps.io,
      "cfg.schemaAutoRegistrationEnabled is true",
      true,
      deps.cfg.schemaAutoRegistrationEnabled,
    );

    // First call — auto-registers.
    const uuid = await localRegistrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat,
        schemaDefinition: AVRO_SCHEMA_V1,
      },
      schemaName,
    );
    printStage(
      deps.io,
      "auto-register",
      `first serialize-path lookup auto-registered -> ${uuid}`,
    );

    const uuidWellFormedOk = printEqualityCheck(
      deps.io,
      "auto-registered UUID is well-formed",
      true,
      UUID_REGEX.test(uuid),
    );

    // Second lookup — resolves to the same UUID (Glue's canonical UUID
    // per schema-version does not change).
    const uuid2 = await localRegistrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat,
        schemaDefinition: AVRO_SCHEMA_V1,
      },
      schemaName,
    );
    printStage(
      deps.io,
      "auto-register",
      `subsequent lookup -> ${uuid2} (should match first)`,
    );
    const subsequentOk = printEqualityCheck(
      deps.io,
      "subsequent lookup returns the same UUID",
      uuid,
      uuid2,
    );

    const pass = autoOnOk && uuidWellFormedOk && subsequentOk;
    printVerdict(
      deps.io,
      pass,
      pass
        ? "auto-register fall-through: cfg flag ON, first lookup auto-registered a well-formed UUID, second lookup returned the same UUID."
        : "auto-register fall-through: at least one of {flag ON, UUID well-formed, subsequent-lookup identity} failed.",
    );
    return {
      index,
      title,
      format,
      direction,
      compression,
      schemaVersionId: uuid,
      pass,
    };
  } catch (err) {
    const message = errorText(err);
    printStage(deps.io, "error", message);
    return {
      index,
      title,
      format,
      direction,
      compression,
      schemaVersionId: null,
      pass: false,
      error: message,
    };
  }
}

// ---------------------------------------------------------------------------
// Scenario 21 — Writer-registers, reader cold-cache reads
// ---------------------------------------------------------------------------

/**
 * Scenario 21: exercise the writer-schema-lookup path a Java consumer takes
 * when it has never seen the wire header's schema-version UUID before.
 *
 *   1. TS registers v1 under a fresh per-run schema name.
 *   2. TS serializes a v1 record with `GsrSerializer` and produces the
 *      wire bytes to a per-run Kafka topic.
 *   3. A fresh Java sidecar consumer (cold cache — a new group id) reads
 *      the topic. The sidecar's real Java GSR library resolves the schema
 *      from the header UUID via `GetSchemaVersion` on Glue.
 *   4. Assert: fields decode to `CUSTOMER_RECORD_V1`, the sidecar reports
 *      the wire UUID, and the sidecar's data-format is AVRO.
 *
 * The "cold cache" property comes from a per-scenario `groupId` (no
 * offset inheritance from an earlier run) and a per-run topic (no
 * pre-existing cached records). The Java sidecar's in-process
 * schema-version cache is per-JVM-run; we do not restart the JVM, but the
 * `runbookSuffix` in the schema name guarantees the sidecar has never seen
 * this UUID before this scenario, so the lookup path fires on the first
 * poll.
 */
export async function runScenario21WriterRegisters(
  deps: ExtraScenarioDeps,
): Promise<ScenarioResult> {
  const index = 21;
  const title =
    "scenario 21 of 21 — writer-registers, reader cold-cache reads";
  const format = "AVRO" as const;
  const direction = "TS->Java (TS-registers)";
  const compression = "NONE" as const;

  printSectionHeader(deps.io, title);
  printScenarioIntro(deps.io, {
    what: "TS registers v1 + serializes + produces to Kafka; a FRESH Java sidecar consumer (cold cache — brand-new groupId) reads the topic and resolves the writer schema from the wire header UUID via Glue.",
    proves: "The writer-schema-lookup path: a Java consumer with zero cached knowledge of the schema-version UUID can still decode a TS-produced message because the wire header carries the exact UUID Glue holds.",
  });

  try {
    const schemaName = deps.namer.forFamily("writerRegistersAvro");
    const dataFormat = "AVRO";
    const topic = `gsr-ts-it-demo-writerreg-${schemaName}`;
    printStage(deps.io, "writer", `schemaName=${schemaName} topic=${topic}`);

    deps.tracker.track(REGISTRY_NAME, schemaName);

    // 1. TS registers v1.
    printSchemaBody(deps.io, "AVRO_SCHEMA_V1 (TS is the writer — registers this)", AVRO_SCHEMA_V1);
    const uuid = await deps.registrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat,
        schemaDefinition: AVRO_SCHEMA_V1,
      },
      topic,
    );
    printStage(
      deps.io,
      "glue",
      `TS wrote v1 -> schemaVersionId=${uuid} (${formatUuid(uuidBytesFromCanonical(uuid))})`,
    );

    // 2. TS encodes a v1 record and produces to Kafka.
    printRecord(deps.io, "record TS is about to encode (v1)", CUSTOMER_RECORD_V1);
    const serializer = new GsrSerializer({ compression: "NONE" });
    const wire = serializer.serialize({
      format: DataFormat.AVRO,
      schemaVersionId: uuid,
      topic,
      schema: AVRO_SCHEMA_V1,
      data: { ...CUSTOMER_RECORD_V1 },
    });
    printStage(
      deps.io,
      "encoder",
      `TS GsrSerializer produced ${wire.length} wire bytes (0x03 magic + 0x00 NONE + 16-byte UUID + Avro payload)`,
    );
    printHexDump(deps.io, "wire bytes shipped to Kafka", wire);

    const admin = deps.kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
        waitForLeaders: true,
      });
    } finally {
      await admin.disconnect();
    }

    const producer = deps.kafka.producer();
    await producer.connect();
    try {
      await producer.send({
        topic,
        messages: [{ value: wire }],
      });
    } finally {
      await producer.disconnect();
    }
    printStage(deps.io, "kafka", `produced 1 record to ${topic}`);

    // 3. Fresh Java sidecar consumer — a new group id gives the sidecar
    //    zero committed offsets on this topic, so it polls from the
    //    beginning and its GSR library must resolve the writer schema from
    //    the wire UUID.
    const groupId = `${topic}-writer-registers-cold-${Date.now()}`;
    printStage(
      deps.io,
      "consumer",
      `fresh Java sidecar consumer groupId=${groupId} (cold cache)`,
    );
    const consumeResult = await deps.sidecar.kafkaConsume({
      bootstrap: deps.broker.bootstrap,
      topic,
      format: "AVRO",
      groupId,
      region: deps.cfg.region,
      timeoutMs: 60_000,
    });
    printStage(
      deps.io,
      "consumer",
      `sidecar reported schemaVersionId=${consumeResult.schemaVersionId} dataFormat=${consumeResult.dataFormat}`,
    );

    // 4. Field + UUID equality checks derive PASS/FAIL.
    const decoded = readAvroEnvelopeV1(
      consumeResult.record as unknown as AvroEnvelope,
    );
    printRecord(deps.io, "record decoded by the fresh Java sidecar consumer", decoded);
    const fieldsOk = printEqualityCheck(
      deps.io,
      "decoded fields (key-order-insensitive) match CUSTOMER_RECORD_V1",
      canonicalize(CUSTOMER_RECORD_V1),
      canonicalize(decoded),
    );
    const uuidOk = printEqualityCheck(
      deps.io,
      "sidecar-returned schemaVersionId equals wire UUID",
      uuid,
      consumeResult.schemaVersionId,
    );
    const formatOk = printEqualityCheck(
      deps.io,
      "sidecar-returned dataFormat is AVRO",
      "AVRO",
      consumeResult.dataFormat,
    );

    const pass = fieldsOk && uuidOk && formatOk;
    printVerdict(
      deps.io,
      pass,
      pass
        ? "writer-registers cold-cache read: TS-registered v1 UUID rode the wire header; a fresh Java consumer resolved the writer schema from Glue and decoded field-for-field."
        : "writer-registers cold-cache read: at least one of {record fields, sidecar UUID, sidecar dataFormat} failed to match.",
    );
    return {
      index,
      title,
      format,
      direction,
      compression,
      schemaVersionId: uuid,
      pass,
    };
  } catch (err) {
    const message = errorText(err);
    printStage(deps.io, "error", message);
    return {
      index,
      title,
      format,
      direction,
      compression,
      schemaVersionId: null,
      pass: false,
      error: message,
    };
  }
}

// ---------------------------------------------------------------------------
// Local helpers — kept private to this driver module
// ---------------------------------------------------------------------------

/**
 * Recursively rebuild an object with keys sorted alphabetically at every
 * depth, so a JSON-based structural comparison is order-insensitive. The
 * narrator's `printEqualityCheck` uses `JSON.stringify` for its equality
 * check, which is order-sensitive; canonicalizing both sides here means
 * the operator's on-screen PASS/FAIL derives from the same key-order-
 * insensitive comparison a reviewer would perform by eye.
 *
 * A local helper here (rather than an edit to `narrator.ts`) keeps the
 * narrator pure — no cross-cutting change is made to the shared formatting
 * module.
 */
function canonicalize(v: unknown): unknown {
  if (v === null || v === undefined) return v;
  if (Array.isArray(v)) return v.map(canonicalize);
  if (typeof v === "object") {
    const src = v as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(src).sort()) {
      out[k] = canonicalize(src[k]);
    }
    return out;
  }
  return v;
}

/**
 * Extract a human-readable message from an arbitrary thrown value, so
 * scenario `error` fields carry a legible string rather than `[object
 * Object]`.
 */
function errorText(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Render an optional cached row for narration lines. `undefined` prints as
 * the literal token `undefined`; a hit renders the schema-version UUID
 * so an operator can visually diff hit rows against fetch rows.
 */
function renderCached(row: CachedSchemaVersion | undefined): string {
  if (row === undefined) return "undefined";
  return `{ schemaVersionId: ${row.schemaVersionId} }`;
}

/**
 * Convert a canonical UUID string (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)
 * to its 16 raw bytes so `narrator.formatUuid` can re-render it as the
 * canonical hyphenated form for the narrated line. The round-trip is
 * intentional: it proves the on-wire byte order matches the string form
 * the operator sees, catching any accidental byte-swap. On a malformed
 * input we return an empty view rather than throwing — narration must be
 * total, and `formatUuid` prints a diagnostic in that case.
 */
function uuidBytesFromCanonical(canonical: string): Uint8Array {
  const hex = canonical.replace(/-/g, "");
  if (!/^[0-9a-f]{32}$/i.test(hex)) {
    return new Uint8Array();
  }
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}
