/**
 * `@gsr/serde/internal` — non-public composition surface used by the offline
 * wire byte-identity gate suite in `@gsr/integration-tests`.
 *
 * These per-format encode/decode slices and the fixture arrays they carry
 * are the primitives the gate composes with `@gsr/core.encodeMessage` /
 * `decodeMessage` to reproduce every Java-canonical golden vector byte-for-
 * byte. They are NOT part of the customer-facing surface — customer code
 * uses the full-breadth serdes and the `GsrSerializer` / `GsrDeserializer`
 * facades re-exported from the package's main entry point.
 *
 * This subpath entry exists so the gate keeps a stable, versioned import
 * path without leaking fixture symbols into the public `dist/index.d.ts`.
 */

export {
  encodeAvro,
  decodeAvro,
  avroHazardRecords,
  type AvroHazardRecord,
} from "./avro/encode-slice.js";
export {
  encodeProtobuf,
  decodeProtobuf,
  protobufHazardRecords,
  WRAPPERS_PROTO_TEXT,
  type ProtobufHazardRecord,
} from "./protobuf/encode-slice.js";
export {
  encodeJsonSchema,
  decodeJsonSchema,
  jsonSchemaHazardRecords,
  type JsonSchemaHazardRecord,
} from "./jsonschema/encode-slice.js";
