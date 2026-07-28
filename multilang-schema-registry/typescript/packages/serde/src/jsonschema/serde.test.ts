import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { GsrIncompatibleDataError } from "@gsr/core";

import {
  deserializeJson,
  JsonSchemaValidationError,
  serializeJson,
} from "./serde.js";
import {
  GOLDEN_JAVA_DIR,
  referenceRootExists,
  SHARED_TEST_DIR,
} from "../test-support/reference-root.js";

/**
 * The 18-byte wire header prefix in each golden `.bin`. Kept as a local
 * constant so this test file has no cross-package import for the header
 * layout — that codec lives in `@gsr/core` and is exercised by the
 * byte-identity gate suite; here we only need to skip the fixed-size prefix
 * to reach the payload region.
 */
const WIRE_FORMAT_HEADER_SIZE = 18;

/**
 * Load a shared-test schema file as a parsed object — the full-breadth
 * serde accepts schema objects or strings directly (no filesystem I/O in
 * its own code).
 */
function readSchemaObject(relativePath: string): object {
  const text = readFileSync(resolve(SHARED_TEST_DIR, relativePath), "utf8");
  return JSON.parse(text) as object;
}

/**
 * Load the payload region (bytes 18..) of a NONE-compressed golden vector
 * — the canonicalized JSON payload the Java reference emitted.
 */
function readGoldenPayload(tag: string): Buffer {
  const goldenFile = resolve(
    GOLDEN_JAVA_DIR,
    `jsonschema__draft07__comp-NONE__${tag}.bin`,
  );
  return readFileSync(goldenFile).subarray(WIRE_FORMAT_HEADER_SIZE);
}

// A tiny inline schema + instance pair for arbitrary-schema breadth tests —
// deliberately not one of the golden-vector fixtures so the arbitrary-schema
// path is exercised end-to-end.
const ORDER_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  type: "object",
  required: ["orderId", "total"],
  properties: {
    orderId: { type: "string", minLength: 1 },
    total: { type: "number", minimum: 0, multipleOf: 0.01 },
    tags: { type: "array", items: { type: "string" } },
  },
  additionalProperties: false,
};

const ORDER_INSTANCE = {
  orderId: "ord-2026-0001",
  total: 12.34,
  tags: ["premium", "expedited"],
};

describe("serde/jsonschema/serde — full-breadth Draft-07", () => {
  describe("serializeJson", () => {
    it("round-trips an arbitrary Draft-07 instance", () => {
      const bytes = serializeJson(ORDER_SCHEMA, ORDER_INSTANCE);
      const decoded = deserializeJson(bytes, ORDER_SCHEMA);
      expect(decoded).toEqual(ORDER_INSTANCE);
    });

    it("accepts a JSON-string schema equivalently to a parsed object", () => {
      const asObject = serializeJson(ORDER_SCHEMA, ORDER_INSTANCE);
      const asString = serializeJson(
        JSON.stringify(ORDER_SCHEMA),
        ORDER_INSTANCE,
      );
      expect(asObject.equals(asString)).toBe(true);
    });

    it("emits canonicalized JSON — compact, insertion-order keys, no whitespace", () => {
      const bytes = serializeJson(ORDER_SCHEMA, ORDER_INSTANCE);
      const text = bytes.toString("utf8");
      // Compact form: no whitespace between tokens.
      expect(text).not.toMatch(/\s/);
      // Insertion-order keys: orderId before total before tags.
      expect(text.indexOf('"orderId"')).toBeLessThan(text.indexOf('"total"'));
      expect(text.indexOf('"total"')).toBeLessThan(text.indexOf('"tags"'));
    });

    it.skipIf(!referenceRootExists)("reproduces the product-v1 golden payload byte-for-byte", () => {
      const schema = readSchemaObject(
        "jsonschema/none/product_v1.schema.json",
      );
      const instance = {
        id: 42,
        name: "Widget",
        pricing: { amount: 19.99, currency: "USD" },
      };
      const encoded = serializeJson(schema, instance);
      const golden = readGoldenPayload("product-v1");
      expect(encoded.equals(golden)).toBe(true);
    });

    it("throws JsonSchemaValidationError on a schema-invalid instance and emits no bytes", () => {
      // orderId must be a non-empty string; total must be >= 0.
      expect(() =>
        serializeJson(ORDER_SCHEMA, { orderId: "", total: -1 }),
      ).toThrow(JsonSchemaValidationError);
    });

    it("keeps ajv out of the byte path — output is JSON.stringify(instance) verbatim", () => {
      // Author keys in a non-alphabetical order — the byte output MUST
      // reflect insertion order, not any ajv-imposed ordering.
      const instance = {
        total: 5,
        orderId: "z-first-by-value",
        tags: ["a"],
      };
      const bytes = serializeJson(ORDER_SCHEMA, instance);
      const text = bytes.toString("utf8");
      expect(text.indexOf('"total"')).toBeLessThan(text.indexOf('"orderId"'));
    });

    it("compiles the same schema only once (per-schema validator cache)", () => {
      // A schema-recursive test proxy — pass the SAME object twice, verify
      // no throw and identical output. The cache is behavioral (a compile
      // failure the second time would surface as a divergent error), so
      // stable output on repeat is the observable check.
      const first = serializeJson(ORDER_SCHEMA, ORDER_INSTANCE);
      const second = serializeJson(ORDER_SCHEMA, ORDER_INSTANCE);
      expect(first.equals(second)).toBe(true);
    });
  });

  describe("deserializeJson", () => {
    it.skipIf(!referenceRootExists)("reconstructs the product-v1 instance from the NONE golden payload", () => {
      const schema = readSchemaObject(
        "jsonschema/none/product_v1.schema.json",
      );
      const goldenPayload = readGoldenPayload("product-v1");
      const decoded = deserializeJson(goldenPayload, schema);
      expect(decoded).toEqual({
        id: 42,
        name: "Widget",
        pricing: { amount: 19.99, currency: "USD" },
      });
    });

    it("throws GsrIncompatibleDataError on unparseable (non-JSON) bytes", () => {
      const garbage = Buffer.from([0xff, 0xfe, 0xfd, 0xfc]);
      expect(() => deserializeJson(garbage, ORDER_SCHEMA)).toThrow(
        GsrIncompatibleDataError,
      );
    });

    it("preserves the original SyntaxError on GsrIncompatibleDataError.cause for diagnostics", () => {
      const garbage = Buffer.from("not-json", "utf8");
      try {
        deserializeJson(garbage, ORDER_SCHEMA);
        throw new Error("expected deserializeJson to throw");
      } catch (err) {
        expect(err).toBeInstanceOf(GsrIncompatibleDataError);
        expect((err as { cause?: unknown }).cause).toBeInstanceOf(SyntaxError);
      }
    });

    it("throws JsonSchemaValidationError on a well-formed but schema-mismatched payload", () => {
      const badPayload = Buffer.from(
        JSON.stringify({ orderId: "", total: -1 }),
        "utf8",
      );
      expect(() => deserializeJson(badPayload, ORDER_SCHEMA)).toThrow(
        JsonSchemaValidationError,
      );
    });

    it("does not classify a schema-mismatch as GsrIncompatibleDataError", () => {
      // The two error classes must stay distinct — a well-formed JSON that
      // fails schema validation is NOT a wire-format error.
      const badPayload = Buffer.from(
        JSON.stringify({ orderId: "", total: -1 }),
        "utf8",
      );
      expect(() => deserializeJson(badPayload, ORDER_SCHEMA)).not.toThrow(
        GsrIncompatibleDataError,
      );
    });
  });

  describe("schema-string parsing", () => {
    it("throws GsrIncompatibleDataError when a schema string is not valid JSON on serializeJson", () => {
      const badSchemaString = "{ this is not JSON";
      expect(() => serializeJson(badSchemaString, { any: "instance" })).toThrow(
        GsrIncompatibleDataError,
      );
      expect(() => serializeJson(badSchemaString, { any: "instance" })).toThrow(
        /failed to parse JSON-Schema string/,
      );
    });

    it("throws GsrIncompatibleDataError when a schema string is not valid JSON on deserializeJson", () => {
      const badSchemaString = "{ this is not JSON";
      const payload = Buffer.from(JSON.stringify({ any: "value" }), "utf8");
      expect(() => deserializeJson(payload, badSchemaString)).toThrow(
        GsrIncompatibleDataError,
      );
    });

    it("accepts a valid JSON schema string and canonicalizes it via the cache", () => {
      // Two structurally-identical schema strings with different whitespace
      // must both produce a valid payload (exercises the string-branch of
      // parseSchema and the cache-key canonicalization).
      const schemaCompact = '{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}';
      const schemaWithWhitespace = `{
  "type": "object",
  "properties": { "a": { "type": "string" } },
  "required": ["a"]
}`;
      const instance = { a: "hello" };
      const payloadA = serializeJson(schemaCompact, instance);
      const payloadB = serializeJson(schemaWithWhitespace, instance);
      expect(payloadA.equals(payloadB)).toBe(true);
    });
  });
});
