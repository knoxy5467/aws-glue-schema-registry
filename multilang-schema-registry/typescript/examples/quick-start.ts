/**
 * Quick-start: full serialize -> deserialize round-trip.
 *
 * Copy-paste this program to see the client work end-to-end with no live
 * AWS calls. It builds a serializer, encodes an Avro record into the GSR
 * wire format, hands the resulting Buffer to a matching deserializer, and
 * confirms the recovered record equals the input.
 *
 * The `schemaVersionId` is a caller-supplied UUID. In production it is the
 * `SchemaVersionId` returned by Glue when the schema is registered; here
 * we pass a fixed value so the example is deterministic and offline.
 *
 * Run this file with a TypeScript runner such as `tsx` or `vite-node`:
 *
 *     npx tsx examples/quick-start.ts
 */

import { strict as assert } from "node:assert";

import { parseConfig } from "@gsr/core";
import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
  type DeserializeRequest,
  type SerializeRequest,
} from "@gsr/serde";

// 1. Build a config. `parseConfig` accepts a flat string map and applies
//    the same defaults the client uses in production (see the config-key
//    reference in the `@gsr/core` README).
const config = parseConfig({
  region: "us-east-2",
  "registry.name": "default-registry",
  compression: "NONE",
});

// The parsed config is used to configure the Glue seam in production. The
// serializer only needs the compression mode, which we thread through
// explicitly below.
console.log(
  `Using region=${config.region} registry=${config.registryName} compression=${config.compressionType}`,
);

// 2. Define an Avro schema and a matching record. The schema is the same
//    shape the deserializer will use to interpret the wire payload.
const orderSchema = {
  type: "record",
  name: "Order",
  namespace: "example.orders",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
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
  status: "OPEN" | "PAID" | "CANCELLED";
}

const originalRecord: OrderRecord = {
  id: "ord-42",
  amount: 19.99,
  quantity: 3,
  status: "PAID",
};

// 3. Serialize. `schemaVersionId` is the Glue `SchemaVersionId` UUID; the
//    client writes it verbatim into the 18-byte wire header.
const serializer = new GsrSerializer({ compression: "NONE" });

const serializeRequest: SerializeRequest = {
  format: DataFormat.AVRO,
  schemaVersionId: "01020304-0506-0708-090a-0b0c0d0e0f10",
  topic: "orders",
  schema: orderSchema,
  data: originalRecord,
};

const wireMessage: Buffer = serializer.serialize(serializeRequest);
console.log(`Encoded ${wireMessage.length}-byte GSR wire message.`);

// 4. Deserialize. `GsrDeserializer` re-reads the header, applies any
//    configured decompression, and hands the payload to the Avro decoder.
const deserializer = new GsrDeserializer();

const deserializeRequest: DeserializeRequest = {
  format: DataFormat.AVRO,
  data: wireMessage,
  schema: orderSchema,
};

const recovered = deserializer.deserialize(deserializeRequest) as OrderRecord;

// 5. Confirm the round-trip recovered the original record.
assert.deepStrictEqual(recovered, originalRecord);
console.log("Recovered record matches the input:", recovered);
