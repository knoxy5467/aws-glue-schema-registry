/**
 * Demo-fixtures — a THIN wrapper over the shared interop fixtures.
 *
 * The narrated interop demo reuses every schema body, record value, and
 * envelope builder/reader from the harness-shared fixture module
 * (`../test/interop/interop-fixtures.js`) so the demo and the interop test
 * suites cannot drift on wire-format-relevant data. This module owns only:
 *
 *   1. A **re-export** surface: the schema bodies (Avro/Protobuf/JSON v1/v2),
 *      the canonical record values, the envelope builders and readers, and
 *      the Protobuf fully-qualified name. No schema body is redefined here;
 *      the constants below are ES-module re-exports (the demo spec pins the
 *      "no-schema-body-redefinition" property as a hard falsifiable rule).
 *   2. **Per-scenario schema-name label helpers**: string labels for each
 *      scenario family in the 21-scenario catalogue, and a small resolver
 *      (`createDemoSchemaNamer`) that composes `realSchemaName(label)` from
 *      `../src/real-glue.js` — yielding runbook-anchored, per-run-unique
 *      schema names (prefix `gsr-ts-it-`) that the shared `CleanupTracker`
 *      deletes on exit.
 *
 * Every function here is pure. This module MUST NOT touch the network, spawn
 * a JVM, or read from disk at import time; it is Tier-1-safe and exercised
 * by an offline unit test co-located here.
 */

import { realSchemaName } from "../src/real-glue.js";

// Re-export the shared interop fixture surface verbatim. Namespacing this
// through `demo-fixtures` keeps every scenario driver's import list short
// (one path) without forking any schema body — the wire-format contract is
// owned by the interop-fixtures module and is inherited unchanged.
export {
  AVRO_SCHEMA_V1,
  AVRO_SCHEMA_V2,
  JSON_SCHEMA_V1,
  JSON_SCHEMA_V2,
  PROTOBUF_SCHEMA_V1,
  PROTOBUF_SCHEMA_V2,
  CUSTOMER_RECORD_V1,
  CUSTOMER_RECORD_V2,
  INTEROP_NAMESPACE,
  INTEROP_RECORD_NAME,
  INTEROP_PROTOBUF_FULL_NAME,
  buildAvroEnvelope,
  buildJsonEnvelope,
  buildProtobufEnvelope,
  readAvroEnvelopeV1,
  readAvroEnvelopeV2,
  readJsonEnvelopeV1,
  readJsonEnvelopeV2,
  readProtobufEnvelopeV1,
  readProtobufEnvelopeV2,
} from "../test/interop/interop-fixtures.js";
export type {
  AvroEnvelope,
  JsonEnvelope,
  ProtobufEnvelope,
  CustomerRecordV1,
  CustomerRecordV2,
} from "../test/interop/interop-fixtures.js";

/**
 * Per-scenario-family labels used by the narrated demo when composing
 * runbook-anchored schema names. Each label ends up embedded in the created
 * schema name via `realSchemaName(label)` so an operator eyeballing a leaked
 * Glue schema can attribute it to the family that created it.
 *
 * A "family" is a group of scenarios that share the same registered schema
 * pair (e.g. the four AVRO cross-version scenarios differ only by
 * direction + compression, all against one v1/v2 schema pair). Grouping by
 * family keeps the demo's Glue footprint small — one `CreateSchema` per
 * family, not one per scenario.
 *
 * Labels are lowercase kebab-case fragments; the final schema name is
 * `gsr-ts-it-<label>-<epochSeconds>-<hex>` (see `realSchemaName`).
 */
export const DEMO_SCHEMA_LABELS = Object.freeze({
  avroCrossVersion: "demo-avro-crossver",
  avroSameVersion: "demo-avro-samever",
  jsonCrossVersion: "demo-json-crossver",
  jsonSameVersion: "demo-json-samever",
  protobufCrossVersion: "demo-proto-crossver",
  protobufSameVersion: "demo-proto-samever",
  cacheAvro: "demo-cache",
  autoRegisterAvro: "demo-autoreg",
  writerRegistersAvro: "demo-writerreg",
} as const);

/**
 * The set of family keys understood by {@link createDemoSchemaNamer}.
 * Enumerated here so scenario drivers can request names by key without
 * threading raw label strings through their imports.
 */
export type DemoSchemaFamily = keyof typeof DEMO_SCHEMA_LABELS;

/**
 * The narrow contract a scenario driver sees. `forLabel` returns a stable
 * schema name for a raw label string; `forFamily` is a keyed convenience
 * that looks up the family's label in {@link DEMO_SCHEMA_LABELS} and returns
 * its stable name. Calling either method with the same argument within one
 * resolver instance yields the same schema name — so scenarios that share a
 * schema (cross-version pairs, compression variants) register it once and
 * reuse the version-id.
 */
export interface DemoSchemaNamer {
  forLabel(label: string): string;
  forFamily(family: DemoSchemaFamily): string;
}

/**
 * Build a per-run namer that caches `realSchemaName(label)` by label. The
 * first call for a given label composes a fresh, runbook-anchored
 * per-run-unique schema name; subsequent calls with the same label return
 * the cached name.
 *
 * The namer's lifetime is one demo run — main.ts constructs one instance,
 * hands it to every scenario, and drops it on exit. `CleanupTracker` still
 * owns the created-schema list; the namer just keeps names stable across
 * scenarios so the same schema is registered once.
 */
export function createDemoSchemaNamer(): DemoSchemaNamer {
  const cache = new Map<string, string>();
  const resolve = (label: string): string => {
    const cached = cache.get(label);
    if (cached !== undefined) {
      return cached;
    }
    const name = realSchemaName(label);
    cache.set(label, name);
    return name;
  };
  return {
    forLabel(label: string): string {
      return resolve(label);
    },
    forFamily(family: DemoSchemaFamily): string {
      return resolve(DEMO_SCHEMA_LABELS[family]);
    },
  };
}
