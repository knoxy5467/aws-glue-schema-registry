/**
 * Tier-1 offline unit tests for the demo-fixtures re-export surface and the
 * schema-name label helpers.
 *
 * Proves two properties, without touching the network, JVM, Kafka, or Glue:
 *
 *   1. The re-exports resolve to the same underlying values as the shared
 *      interop-fixtures module. Any accidental fork here (redefining a
 *      schema body instead of re-exporting one) would silently drift the
 *      demo's wire bytes off the interop harness contract — the reference
 *      equality assertion below prevents that.
 *   2. The label helpers produce correctly-namespaced, per-run-unique schema
 *      names: every name starts with the runbook prefix `gsr-ts-it-`,
 *      embeds the label, and includes the epoch-seconds/hex suffix from
 *      `perRunSuffix`. Repeat calls with the same label return the same
 *      cached name so scenarios that share a schema register it once.
 */

import { describe, expect, it } from "vitest";

import * as demoFixtures from "./demo-fixtures.js";
import * as interopFixtures from "../test/interop/interop-fixtures.js";
import { RUNBOOK_PREFIX } from "../src/real-glue.js";

describe("demo-fixtures re-exports", () => {
  it("re-exports schema bodies with reference-identity to the shared interop-fixtures module", () => {
    // Reference identity (not deep-equal) — proves the constant is a live
    // ES-module re-export, not a copy. A copy would let a future refactor
    // drift the demo's wire format off the harness contract without
    // triggering any deep-equal assertion.
    expect(demoFixtures.AVRO_SCHEMA_V1).toBe(interopFixtures.AVRO_SCHEMA_V1);
    expect(demoFixtures.AVRO_SCHEMA_V2).toBe(interopFixtures.AVRO_SCHEMA_V2);
    expect(demoFixtures.JSON_SCHEMA_V1).toBe(interopFixtures.JSON_SCHEMA_V1);
    expect(demoFixtures.JSON_SCHEMA_V2).toBe(interopFixtures.JSON_SCHEMA_V2);
    expect(demoFixtures.PROTOBUF_SCHEMA_V1).toBe(
      interopFixtures.PROTOBUF_SCHEMA_V1,
    );
    expect(demoFixtures.PROTOBUF_SCHEMA_V2).toBe(
      interopFixtures.PROTOBUF_SCHEMA_V2,
    );
  });

  it("re-exports the canonical record values", () => {
    expect(demoFixtures.CUSTOMER_RECORD_V1).toBe(
      interopFixtures.CUSTOMER_RECORD_V1,
    );
    expect(demoFixtures.CUSTOMER_RECORD_V2).toBe(
      interopFixtures.CUSTOMER_RECORD_V2,
    );
  });

  it("re-exports the namespace + protobuf full-name constants", () => {
    expect(demoFixtures.INTEROP_NAMESPACE).toBe(
      interopFixtures.INTEROP_NAMESPACE,
    );
    expect(demoFixtures.INTEROP_RECORD_NAME).toBe(
      interopFixtures.INTEROP_RECORD_NAME,
    );
    expect(demoFixtures.INTEROP_PROTOBUF_FULL_NAME).toBe(
      interopFixtures.INTEROP_PROTOBUF_FULL_NAME,
    );
  });

  it("re-exports envelope builders and readers as callable functions", () => {
    expect(demoFixtures.buildAvroEnvelope).toBe(
      interopFixtures.buildAvroEnvelope,
    );
    expect(demoFixtures.buildJsonEnvelope).toBe(
      interopFixtures.buildJsonEnvelope,
    );
    expect(demoFixtures.buildProtobufEnvelope).toBe(
      interopFixtures.buildProtobufEnvelope,
    );
    expect(demoFixtures.readAvroEnvelopeV1).toBe(
      interopFixtures.readAvroEnvelopeV1,
    );
    expect(demoFixtures.readAvroEnvelopeV2).toBe(
      interopFixtures.readAvroEnvelopeV2,
    );
    expect(demoFixtures.readJsonEnvelopeV1).toBe(
      interopFixtures.readJsonEnvelopeV1,
    );
    expect(demoFixtures.readJsonEnvelopeV2).toBe(
      interopFixtures.readJsonEnvelopeV2,
    );
    expect(demoFixtures.readProtobufEnvelopeV1).toBe(
      interopFixtures.readProtobufEnvelopeV1,
    );
    expect(demoFixtures.readProtobufEnvelopeV2).toBe(
      interopFixtures.readProtobufEnvelopeV2,
    );
  });

  it("round-trips a v1 Avro envelope through the re-exported builder/reader", () => {
    // Sanity-check that the re-exported functions are wired correctly and
    // not accidentally shadowed by something with the same name. A behavioral
    // check catches signature-shaped mistakes reference-identity misses.
    const built = demoFixtures.buildAvroEnvelope(demoFixtures.CUSTOMER_RECORD_V1);
    const read = demoFixtures.readAvroEnvelopeV1(built);
    expect(read).toEqual(demoFixtures.CUSTOMER_RECORD_V1);
  });
});

describe("DEMO_SCHEMA_LABELS", () => {
  it("declares one label per scenario family (nine families)", () => {
    // Nine families cover the 21-scenario catalogue: three formats × two
    // version-shapes = 6, plus cache / auto-register / writer-registers = 9.
    const keys = Object.keys(demoFixtures.DEMO_SCHEMA_LABELS).sort();
    expect(keys).toEqual([
      "autoRegisterAvro",
      "avroCrossVersion",
      "avroSameVersion",
      "cacheAvro",
      "jsonCrossVersion",
      "jsonSameVersion",
      "protobufCrossVersion",
      "protobufSameVersion",
      "writerRegistersAvro",
    ]);
  });

  it("declares distinct kebab-case label strings", () => {
    const values = Object.values(demoFixtures.DEMO_SCHEMA_LABELS);
    expect(new Set(values).size).toBe(values.length);
    for (const v of values) {
      // Labels are lowercase kebab-case fragments — no whitespace, no
      // capitals, no leading/trailing dash. This is what ends up embedded in
      // the Glue schema name, so it must be Glue-name-safe.
      expect(v).toMatch(/^[a-z][a-z0-9-]*[a-z0-9]$/);
    }
  });

  it("is frozen (no accidental mutation across the demo run)", () => {
    expect(Object.isFrozen(demoFixtures.DEMO_SCHEMA_LABELS)).toBe(true);
  });
});

describe("createDemoSchemaNamer", () => {
  it("produces runbook-prefixed names embedding the label + per-run suffix", () => {
    const namer = demoFixtures.createDemoSchemaNamer();
    const name = namer.forLabel("demo-probe");
    expect(name.startsWith(RUNBOOK_PREFIX)).toBe(true);
    expect(name).toMatch(/^gsr-ts-it-demo-probe-\d{10}-[0-9a-f]{4}$/);
  });

  it("returns the same name on repeated calls with the same label (per-run cache)", () => {
    const namer = demoFixtures.createDemoSchemaNamer();
    const first = namer.forLabel("demo-probe");
    const second = namer.forLabel("demo-probe");
    expect(second).toBe(first);
  });

  it("returns distinct names for distinct labels within one namer", () => {
    const namer = demoFixtures.createDemoSchemaNamer();
    const a = namer.forLabel("demo-probe-a");
    const b = namer.forLabel("demo-probe-b");
    expect(a).not.toBe(b);
    expect(a).toMatch(/^gsr-ts-it-demo-probe-a-/);
    expect(b).toMatch(/^gsr-ts-it-demo-probe-b-/);
  });

  it("returns distinct names across independent namer instances (per-run-unique)", () => {
    // Each namer instance represents one demo run. Two runs should never
    // collide on schema names, so distinct namers must generate distinct
    // suffixes even for the same label.
    const namers = Array.from({ length: 16 }, () =>
      demoFixtures.createDemoSchemaNamer(),
    );
    const names = new Set(namers.map((n) => n.forLabel("demo-probe")));
    // 16 random draws from a 16-bit space (perRunSuffix hex) yields a
    // birthday-collision probability well below 1%; occasional collisions
    // here would signal an RNG regression, not test flakiness.
    expect(names.size).toBe(16);
  });

  it("routes forFamily lookups through the same cache as forLabel", () => {
    // forFamily is a keyed convenience that resolves the family's raw label
    // and then hits the same cache. Two calls — one keyed, one raw — for the
    // same underlying label must return the same name.
    const namer = demoFixtures.createDemoSchemaNamer();
    const keyed = namer.forFamily("avroCrossVersion");
    const raw = namer.forLabel(
      demoFixtures.DEMO_SCHEMA_LABELS.avroCrossVersion,
    );
    expect(keyed).toBe(raw);
    expect(keyed).toMatch(/^gsr-ts-it-demo-avro-crossver-/);
  });

  it("assigns distinct names to distinct families within one namer", () => {
    const namer = demoFixtures.createDemoSchemaNamer();
    const families: demoFixtures.DemoSchemaFamily[] = [
      "avroCrossVersion",
      "avroSameVersion",
      "jsonCrossVersion",
      "jsonSameVersion",
      "protobufCrossVersion",
      "protobufSameVersion",
      "cacheAvro",
      "autoRegisterAvro",
      "writerRegistersAvro",
    ];
    const names = new Set(families.map((f) => namer.forFamily(f)));
    expect(names.size).toBe(families.length);
    for (const n of names) {
      expect(n.startsWith(RUNBOOK_PREFIX)).toBe(true);
    }
  });
});
