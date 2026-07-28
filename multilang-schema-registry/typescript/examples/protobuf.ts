/**
 * Protobuf round-trip example.
 *
 * Illustrates the two Protobuf-specific inputs the facade requires:
 *
 *   - `schema` is the full `.proto` source text (protobufjs parses it).
 *   - `messageFullName` is the fully-qualified message name — required
 *     because a single `.proto` file can declare multiple messages, and
 *     the wire format's per-message-index needs to know which one.
 *
 * Run this file with a TypeScript runner such as `tsx` or `vite-node`:
 *
 *     npx tsx examples/protobuf.ts
 */

import { strict as assert } from "node:assert";

import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
} from "@gsr/serde";

const SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

// A two-message `.proto` — the sorted BFS order the wire format uses places
// `Customer` at index 0 and `Order` at index 1, so serializing an `Order`
// exercises the nonzero-message-index path.
const ORDERS_PROTO_TEXT = `
syntax = "proto3";
package example;

message Customer {
  string name = 1;
  int64  id   = 2;
}

message Order {
  string   sku      = 1;
  int32    quantity = 2;
  Customer buyer    = 3;
}
`;

interface OrderMessage {
  sku: string;
  quantity: number;
  buyer: { name: string; id: number | string };
}

const orderRecord: OrderMessage = {
  sku: "SKU-42",
  quantity: 3,
  buyer: { name: "Alice", id: 100 },
};

const serializer = new GsrSerializer({ compression: "NONE" });
const deserializer = new GsrDeserializer();

const wire = serializer.serialize({
  format: DataFormat.PROTOBUF,
  schemaVersionId: SCHEMA_VERSION_ID,
  topic: "orders",
  schema: ORDERS_PROTO_TEXT,
  messageFullName: "example.Order",
  data: orderRecord,
});
console.log(`Encoded ${wire.length}-byte Protobuf wire message.`);

// ---- DYNAMIC decode (default) ---------------------------------------------
// The recovered record is a plain JS object built by protobufjs's
// `Type.toObject`. int64 fields are surfaced by protobufjs as strings by
// default, which is why `id` may come back as `"100"` rather than 100.
const dynamicDecoded = deserializer.deserialize({
  format: DataFormat.PROTOBUF,
  data: wire,
  schema: ORDERS_PROTO_TEXT,
  messageFullName: "example.Order",
}) as OrderMessage;

assert.equal(dynamicDecoded.sku, orderRecord.sku);
assert.equal(dynamicDecoded.quantity, orderRecord.quantity);
assert.equal(dynamicDecoded.buyer.name, orderRecord.buyer.name);
console.log("DYNAMIC decode round-trip OK:", dynamicDecoded);

// ---- POJO decode ----------------------------------------------------------
// The `POJO` decode shape returns the concrete protobufjs message instance
// rather than the plain-object conversion.
const pojoDecoded = deserializer.deserialize({
  format: DataFormat.PROTOBUF,
  data: wire,
  schema: ORDERS_PROTO_TEXT,
  messageFullName: "example.Order",
  protobuf: { messageType: "POJO" },
}) as OrderMessage;

assert.equal(pojoDecoded.sku, orderRecord.sku);
console.log("POJO decode round-trip OK.");
