/**
 * Avro round-trip example.
 *
 * Shows the two decode shapes the Avro serde supports:
 *
 *   - GENERIC (default) — the decoded record is a plain JS object.
 *   - SPECIFIC — the decoded record retains its `avsc`-compiled shape,
 *     which is what callers using generated types typically want.
 *
 * Both shapes read the same wire bytes; only the caller's view differs.
 *
 * Run this file with a TypeScript runner such as `tsx` or `vite-node`:
 *
 *     npx tsx examples/avro.ts
 */

import { strict as assert } from "node:assert";

import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
} from "@gsr/serde";

const SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

// Avro schema with a nested array + enum — the same surface the facade
// unit tests exercise for the general Avro path.
const orderSchema = {
  type: "record",
  name: "Order",
  namespace: "example.orders",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
    {
      name: "tags",
      type: { type: "array", items: "string" },
    },
    {
      name: "status",
      type: {
        type: "enum",
        name: "Status",
        symbols: ["OPEN", "PAID", "CANCELLED"],
      },
    },
  ],
};

interface OrderRecord {
  id: string;
  amount: number;
  quantity: number;
  tags: string[];
  status: "OPEN" | "PAID" | "CANCELLED";
}

const orderRecord: OrderRecord = {
  id: "ord-42",
  amount: 19.99,
  quantity: 3,
  tags: ["urgent", "prime"],
  status: "PAID",
};

// ---- GENERIC decode (default) ---------------------------------------------
const serializer = new GsrSerializer({ compression: "ZLIB" });
const deserializer = new GsrDeserializer();

const genericWire = serializer.serialize({
  format: DataFormat.AVRO,
  schemaVersionId: SCHEMA_VERSION_ID,
  topic: "orders",
  schema: orderSchema,
  data: orderRecord,
});

// The ZLIB compression byte occupies wire[1]; leaving it at NONE would
// produce wire[1] === 0x00 instead.
console.log(
  `Generic wire message: ${genericWire.length} bytes, compression byte = 0x${genericWire[1]?.toString(16).padStart(2, "0") ?? "??"}`,
);

const genericDecoded = deserializer.deserialize({
  format: DataFormat.AVRO,
  data: genericWire,
  schema: orderSchema,
}) as OrderRecord;

assert.deepStrictEqual(genericDecoded, orderRecord);
console.log("GENERIC decode round-trip OK:", genericDecoded);

// ---- SPECIFIC decode ------------------------------------------------------
// Select the SPECIFIC decode shape via the per-call `avro` option.
const specificDecoded = deserializer.deserialize({
  format: DataFormat.AVRO,
  data: genericWire,
  schema: orderSchema,
  avro: { recordType: "SPECIFIC" },
}) as OrderRecord;

// Field values are preserved regardless of shape — the wire bytes are
// identical; only the caller's runtime view differs.
assert.equal(specificDecoded.id, orderRecord.id);
assert.equal(specificDecoded.amount, orderRecord.amount);
console.log("SPECIFIC decode round-trip OK.");

// ---- Reader-schema projection --------------------------------------------
// A reader schema drops fields the reader omits and fills reader defaults
// for fields the writer never wrote. Here we project onto a subset of the
// writer schema — the `tags` field is dropped from the reader's view.
const readerSchema = {
  type: "record",
  name: "Order",
  namespace: "example.orders",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
    { name: "status", type: { type: "enum", name: "Status", symbols: ["OPEN", "PAID", "CANCELLED"] } },
  ],
};

const projected = deserializer.deserialize({
  format: DataFormat.AVRO,
  data: genericWire,
  schema: orderSchema,
  readerSchema,
}) as Omit<OrderRecord, "tags">;

assert.equal(projected.id, orderRecord.id);
assert.equal(projected.status, orderRecord.status);
console.log("Reader-schema projection OK:", projected);
