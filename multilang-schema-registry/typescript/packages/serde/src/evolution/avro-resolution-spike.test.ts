/**
 * Reader/writer schema-resolution parity spike for `avsc`.
 *
 * This is a test-only spike that establishes an early go/no-go for the
 * downstream Avro reader-schema projection module. It probes whether
 * `avsc`'s resolver API reproduces the three resolution-rule classes the
 * reference implementations enforce:
 *
 *   - Field drop      — a field present in the writer schema is dropped
 *                       when the reader schema omits it.
 *   - Default fill    — a field added by the reader with a declared default
 *                       is filled with that default when the writer never
 *                       wrote it.
 *   - Type promotion  — a writer's numeric field is decoded into the reader
 *                       as a wider numeric type. This spike exercises
 *                       int -> long, float -> double, and int -> double
 *                       to cover the three primitive promotion widths the
 *                       reference implementations allow (Java
 *                       `ResolvingDecoder`, Go hamba/avro's
 *                       `SchemaCompatibility.Resolve`).
 *
 * Grounded API (from the pinned `avsc` typings at `node_modules/avsc/types`):
 *
 *   readerType.createResolver(writerType) -> Resolver
 *   readerType.fromBuffer(payloadBuf, resolver) -> decoded datum
 *
 * The spike is deliberately independent of the shipped encode path and the
 * uniform wire-format taxonomy: it constructs writer/reader schemas
 * in-memory, encodes with `avsc.Type.toBuffer` on the writer type only,
 * then decodes with the reader type's resolver. No shipped module is
 * imported and no encode-path or header-code is touched.
 *
 * Outcome contract (recorded in the completing task summary):
 *   GO    — all three rule classes pass; the projection module can build on
 *           `readerType.createResolver` directly.
 *   NO-GO — one or more classes fails; the failing rule + schema pair is
 *           surfaced in the assertion failure and a fallback direction
 *           (custom shim over avsc primitives / alternate library) is
 *           captured out-of-band via a follow-up task.
 *
 * Loud-fail contract: if the resolver API the spike depends on is missing
 * at runtime (e.g. a stripped `avsc` build, a version bump that removed
 * `createResolver`, or a broken module load), the spike THROWS a
 * `SPIKE FAIL: ...` error from a top-level `beforeAll`. A silent skip would
 * be indistinguishable from a passing spike and would bury the go/no-go
 * signal the projection module depends on.
 */

import avsc from "avsc";
import { beforeAll, describe, expect, it } from "vitest";

/**
 * Local mirror of `assertSpikePrecondition` from `@gsr/integration-tests`.
 * Duplicated inline (5 lines) rather than imported to avoid a serde →
 * integration-tests package-graph reversal (integration-tests already
 * consumes serde). If more spikes appear in this package, this helper can
 * migrate into a shared serde test util in a follow-up.
 */
function assertSpikePrecondition(
  name: string,
  ok: boolean,
  reason: string,
): void {
  if (!ok) {
    throw new Error(`SPIKE FAIL: ${name} — ${reason}`);
  }
}

const SPIKE_NAME = "serde/evolution/avro-resolution-spike";

/**
 * Compile a writer/reader schema pair into avsc `Type` instances and build
 * a resolver for the reader against the writer. Returns a helper that
 * encodes a datum against the writer schema and decodes it back into the
 * reader shape via `readerType.fromBuffer(buf, resolver)`.
 *
 * `omitRecordMethods: true` yields plain data objects on decode so the
 * assertions can use structural equality (`toEqual` / `toMatchObject`)
 * without the avsc typed-record helpers appearing on the returned value.
 */
function buildProjector(
  writerSchema: avsc.schema.AvroSchema,
  readerSchema: avsc.schema.AvroSchema,
): (writerDatum: unknown) => unknown {
  const writerType = avsc.Type.forSchema(writerSchema, {
    omitRecordMethods: true,
  });
  const readerType = avsc.Type.forSchema(readerSchema, {
    omitRecordMethods: true,
  });
  const resolver = readerType.createResolver(writerType);

  return (writerDatum: unknown) => {
    const buf = writerType.toBuffer(writerDatum);
    return readerType.fromBuffer(buf, resolver);
  };
}

describe("serde/evolution/avro-resolution-spike", () => {
  // Loud-fail precondition guard: the go/no-go rules below all route through
  // `avsc.Type.forSchema(...).createResolver(...)`. If either method is
  // missing at runtime, the spike would otherwise throw a raw
  // `TypeError: ... is not a function` from deep inside a helper — a shape
  // that reads as an incidental defect rather than the go/no-go verdict the
  // spike exists to render. Failing loudly at the suite root replaces that
  // signal with an explicit `SPIKE FAIL: ...` line.
  beforeAll(() => {
    assertSpikePrecondition(
      SPIKE_NAME,
      typeof avsc?.Type?.forSchema === "function",
      "avsc.Type.forSchema is missing at runtime",
    );
    const probeType = avsc.Type.forSchema({ type: "string" });
    assertSpikePrecondition(
      SPIKE_NAME,
      typeof (probeType as { createResolver?: unknown }).createResolver ===
        "function",
      "avsc reader Type.createResolver is missing at runtime",
    );
  });

  describe("spike-precondition guard", () => {
    // Unit-prove the loud-fail helper directly: an unmet precondition MUST
    // surface a `SPIKE FAIL: ...` throw, never a silent skip. The suite
    // itself relies on the same helper in beforeAll; asserting on it here
    // pins the shape without needing to poison the shared `avsc` module.
    it("throws SPIKE FAIL when the precondition is not met", () => {
      expect(() =>
        assertSpikePrecondition(SPIKE_NAME, false, "probe-condition-absent"),
      ).toThrow(
        new RegExp(`SPIKE FAIL: ${SPIKE_NAME} — probe-condition-absent`),
      );
    });

    it("is a no-op when the precondition holds", () => {
      expect(() =>
        assertSpikePrecondition(SPIKE_NAME, true, "unused"),
      ).not.toThrow();
    });
  });

  describe("field drop — writer field the reader omits is removed", () => {
    it("drops a scalar field the reader schema omits", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserWriter",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "name", type: "string" },
          { name: "legacy", type: "string" },
        ],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserWriter",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "name", type: "string" },
        ],
      };

      const project = buildProjector(writerSchema, readerSchema);
      const projected = project({
        id: "u-1",
        name: "Ada",
        legacy: "please-drop",
      });

      expect(projected).toEqual({ id: "u-1", name: "Ada" });
      expect(projected as Record<string, unknown>).not.toHaveProperty(
        "legacy",
      );
    });

    it("drops multiple writer fields the reader omits", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "OrderWriter",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "internalRoute", type: "string" },
          { name: "internalHash", type: "int" },
        ],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "OrderWriter",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };

      const project = buildProjector(writerSchema, readerSchema);
      const projected = project({
        id: "ord-9",
        internalRoute: "warehouse-a",
        internalHash: 42,
      });

      expect(projected).toEqual({ id: "ord-9" });
    });
  });

  describe("default fill — reader-added field with a default is populated", () => {
    it("fills a string default the writer never wrote", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserWriter",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserWriter",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "status", type: "string", default: "ACTIVE" },
        ],
      };

      const project = buildProjector(writerSchema, readerSchema);
      const projected = project({ id: "u-42" });

      expect(projected).toEqual({ id: "u-42", status: "ACTIVE" });
    });

    it("fills a nullable union default (null) for a writer-absent field", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserWriter",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserWriter",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "note", type: ["null", "string"], default: null },
        ],
      };

      const project = buildProjector(writerSchema, readerSchema);
      const projected = project({ id: "u-42" });

      expect(projected).toEqual({ id: "u-42", note: null });
    });

    it("fills an integer default for a writer-absent field", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "CounterWriter",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "CounterWriter",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "retries", type: "int", default: 0 },
        ],
      };

      const project = buildProjector(writerSchema, readerSchema);
      const projected = project({ id: "c-1" });

      expect(projected).toEqual({ id: "c-1", retries: 0 });
    });
  });

  describe("type promotion — writer numeric widens into a reader numeric", () => {
    it("promotes writer int to reader long", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericWriter",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "int" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericWriter",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "long" }],
      };

      const project = buildProjector(writerSchema, readerSchema);
      const projected = project({ value: 123 });

      // avsc represents both int and long as plain JavaScript numbers when
      // the value fits in a 32-bit range, so the promoted long is still a
      // number here — the parity claim is that the decode succeeds and
      // preserves the value, matching the reference behavior.
      expect(projected).toEqual({ value: 123 });
    });

    it("promotes writer float to reader double", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericWriter",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "float" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericWriter",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "double" }],
      };

      const project = buildProjector(writerSchema, readerSchema);
      const projected = project({ value: 1.5 });

      // 1.5 is exactly representable as both a float32 and a float64, so
      // the promoted double is bit-exact — no epsilon comparison is needed
      // to prove the promotion happened.
      expect(projected).toEqual({ value: 1.5 });
    });

    it("promotes writer int to reader double (extra reference-allowed rule)", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericWriter",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "int" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericWriter",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "double" }],
      };

      const project = buildProjector(writerSchema, readerSchema);
      const projected = project({ value: 7 });

      // Integer 7 promoted to double reads back as JavaScript number 7,
      // structurally equal to the reader-shape expectation.
      expect(projected).toEqual({ value: 7 });
    });
  });

  describe("combined resolution — all three rule classes in one pair", () => {
    it("drops + fills default + promotes int to long simultaneously", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "CombinedWriter",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "count", type: "int" },
          { name: "legacy", type: "string" },
        ],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "CombinedWriter",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "count", type: "long" },
          { name: "status", type: "string", default: "OK" },
        ],
      };

      const project = buildProjector(writerSchema, readerSchema);
      const projected = project({
        id: "row-1",
        count: 99,
        legacy: "gone",
      });

      expect(projected).toEqual({
        id: "row-1",
        count: 99,
        status: "OK",
      });
    });
  });
});
