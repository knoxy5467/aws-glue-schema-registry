import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import {
  decodeJsonSchema,
  encodeJsonSchema,
  JsonSchemaValidationError,
  jsonSchemaHazardRecords,
} from "./encode-slice.js";
import {
  GOLDEN_JAVA_DIR,
  referenceRootExists,
  SHARED_TEST_DIR,
} from "../test-support/reference-root.js";

/**
 * The 18-byte wire header prefix in each golden `.bin`. Kept as a local
 * constant so this test file has no cross-package import — the header codec
 * lives in `@gsr/core` and is exercised by the byte-identity gate suite; the
 * slice only needs to skip the fixed-size prefix to reach the payload region.
 */
const WIRE_FORMAT_HEADER_SIZE = 18;

/**
 * Resolve a fixture record's `schemaPath` (relative to `shared/test/`) to the
 * absolute path the slice's file-reading helpers accept.
 */
function absSchema(tag: string): string {
  const entry = jsonSchemaHazardRecords.find((r) => r.tag === tag);
  if (entry === undefined) throw new Error(`no fixture record for ${tag}`);
  return resolve(SHARED_TEST_DIR, entry.schemaPath);
}

/**
 * The uncompressed canonicalized JSON payload for the `product-v1` instance,
 * taken from the payload region (bytes 18..) of the NONE-compressed
 * jsonschema golden vector. Kept as a literal alongside the golden-file check
 * so the assertion stays legible on failure.
 */
const PRODUCT_V1_CANONICAL_PAYLOAD = Buffer.from(
  `{"id":42,"name":"Widget","pricing":{"amount":19.99,"currency":"USD"}}`,
  "utf8",
);

describe("serde/jsonschema/encode-slice", () => {
  describe("jsonSchemaHazardRecords", () => {
    it("exposes the five golden-vector instances in order", () => {
      expect(jsonSchemaHazardRecords).toHaveLength(5);
      const tags = jsonSchemaHazardRecords.map((r) => r.tag);
      expect(tags).toEqual([
        "product-v1",
        "customer-v1",
        "invoice-v1",
        "event-v1",
        "single",
      ]);
    });

    it("pairs each hazard tag with the matching shared/test schema path", () => {
      const byTag = new Map(
        jsonSchemaHazardRecords.map((r) => [r.tag, r.schemaPath]),
      );
      expect(byTag.get("product-v1")).toBe(
        "jsonschema/none/product_v1.schema.json",
      );
      expect(byTag.get("customer-v1")).toBe(
        "jsonschema/backward/customer_v1.schema.json",
      );
      expect(byTag.get("invoice-v1")).toBe(
        "jsonschema/forward/invoice_v1.schema.json",
      );
      expect(byTag.get("event-v1")).toBe(
        "jsonschema/full/event_v1.schema.json",
      );
      expect(byTag.get("single")).toBe(
        "jsonschema/disabled/single.schema.json",
      );
    });
  });

  describe.skipIf(!referenceRootExists)("encodeJsonSchema", () => {
    it("produces the payload region of the NONE-compressed product-v1 golden vector", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "jsonschema__draft07__comp-NONE__product-v1.bin",
      );
      const golden = readFileSync(goldenFile);
      const goldenPayload = golden.subarray(WIRE_FORMAT_HEADER_SIZE);

      const entry = jsonSchemaHazardRecords[0];
      expect(entry).toBeDefined();

      const encoded = encodeJsonSchema(
        absSchema(entry!.tag),
        entry!.instance,
      );

      expect(encoded.equals(goldenPayload)).toBe(true);
    });

    it("matches the recorded canonicalized JSON byte literal for product-v1", () => {
      const entry = jsonSchemaHazardRecords[0];
      const encoded = encodeJsonSchema(
        absSchema(entry!.tag),
        entry!.instance,
      );
      expect(encoded.equals(PRODUCT_V1_CANONICAL_PAYLOAD)).toBe(true);
    });

    it.each(jsonSchemaHazardRecords.map((r) => r.tag))(
      "produces the NONE-compressed golden payload region for %s",
      (tag) => {
        const entry = jsonSchemaHazardRecords.find((r) => r.tag === tag)!;
        const goldenFile = resolve(
          GOLDEN_JAVA_DIR,
          `jsonschema__draft07__comp-NONE__${tag}.bin`,
        );
        const goldenPayload = readFileSync(goldenFile).subarray(
          WIRE_FORMAT_HEADER_SIZE,
        );

        const encoded = encodeJsonSchema(
          absSchema(tag),
          entry.instance,
        );

        expect(encoded.equals(goldenPayload)).toBe(true);
      },
    );

    it("rejects a schema-invalid instance with JsonSchemaValidationError", () => {
      // product-v1 requires id, name, pricing; { id: -1 } is missing name +
      // pricing and violates the id minimum.
      expect(() =>
        encodeJsonSchema(absSchema("product-v1"), { id: -1 }),
      ).toThrow(JsonSchemaValidationError);
    });

    it("keeps ajv out of the byte path — output is JSON.stringify(instance), not ajv's coerced view", () => {
      // JSON.stringify emits keys in insertion order for string keys, so an
      // instance authored with keys in the same order the golden vector
      // records must produce the golden bytes even if the schema declares a
      // different property order.
      const entry = jsonSchemaHazardRecords[0]; // product-v1
      const encoded = encodeJsonSchema(
        absSchema(entry!.tag),
        entry!.instance,
      );
      const asText = encoded.toString("utf8");
      // No whitespace between tokens — compact form only.
      expect(asText).not.toMatch(/\s/);
      // Key order is object-literal order, not alphabetical.
      expect(asText.indexOf('"id"')).toBeLessThan(asText.indexOf('"name"'));
      expect(asText.indexOf('"name"')).toBeLessThan(asText.indexOf('"pricing"'));
    });
  });

  describe.skipIf(!referenceRootExists)("decodeJsonSchema", () => {
    it("reconstructs the product-v1 instance from the golden payload region", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "jsonschema__draft07__comp-NONE__product-v1.bin",
      );
      const goldenPayload = readFileSync(goldenFile).subarray(
        WIRE_FORMAT_HEADER_SIZE,
      );

      const entry = jsonSchemaHazardRecords[0];
      const decoded = decodeJsonSchema(goldenPayload, absSchema(entry!.tag));

      expect(decoded).toEqual(entry!.instance);
    });

    it.each(jsonSchemaHazardRecords.map((r) => r.tag))(
      "round-trips %s from encode to decode",
      (tag) => {
        const entry = jsonSchemaHazardRecords.find((r) => r.tag === tag)!;
        const encoded = encodeJsonSchema(absSchema(tag), entry.instance);
        const decoded = decodeJsonSchema(encoded, absSchema(tag));
        expect(decoded).toEqual(entry.instance);
      },
    );

    it("rejects a schema-invalid decoded payload with JsonSchemaValidationError", () => {
      const badPayload = Buffer.from(
        JSON.stringify({ id: -1 }),
        "utf8",
      );
      expect(() =>
        decodeJsonSchema(badPayload, absSchema("product-v1")),
      ).toThrow(JsonSchemaValidationError);
    });
  });
});
