/**
 * JSON-Schema round-trip example.
 *
 * The JSON serde encodes the record as UTF-8 JSON bytes and validates the
 * instance against a Draft-07 schema on both encode and decode. A record
 * that fails validation raises `JsonSchemaValidationError`.
 *
 * Run this file with a TypeScript runner such as `tsx` or `vite-node`:
 *
 *     npx tsx examples/json-schema.ts
 */

import { strict as assert } from "node:assert";

import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
  JsonSchemaValidationError,
} from "@gsr/serde";

const SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

const orderJsonSchema = {
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

interface OrderJson {
  orderId: string;
  total: number;
  tags?: string[];
}

const orderInstance: OrderJson = {
  orderId: "ord-2026-0001",
  total: 12.34,
  tags: ["premium", "expedited"],
};

const serializer = new GsrSerializer({ compression: "NONE" });
const deserializer = new GsrDeserializer();

// ---- Happy path -----------------------------------------------------------
const wire = serializer.serialize({
  format: DataFormat.JSON,
  schemaVersionId: SCHEMA_VERSION_ID,
  topic: "orders",
  schema: orderJsonSchema,
  data: orderInstance,
});
console.log(`Encoded ${wire.length}-byte JSON-Schema wire message.`);

const decoded = deserializer.deserialize({
  format: DataFormat.JSON,
  data: wire,
  schema: orderJsonSchema,
}) as OrderJson;

assert.deepStrictEqual(decoded, orderInstance);
console.log("JSON-Schema round-trip OK:", decoded);

// ---- Validation failure ---------------------------------------------------
// An instance that violates the schema raises `JsonSchemaValidationError`.
// Here we send a negative total, which the schema forbids (`minimum: 0`).
const invalidInstance = {
  orderId: "ord-2026-0002",
  total: -1,
};

try {
  serializer.serialize({
    format: DataFormat.JSON,
    schemaVersionId: SCHEMA_VERSION_ID,
    topic: "orders",
    schema: orderJsonSchema,
    data: invalidInstance,
  });
  throw new Error("expected JsonSchemaValidationError");
} catch (err) {
  assert.ok(err instanceof JsonSchemaValidationError);
  console.log(
    "Expected validation failure:",
    err.message.slice(0, 80),
  );
}
