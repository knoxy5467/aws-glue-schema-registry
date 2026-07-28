/**
 * `@gsr/serde` public surface — the customer-facing entry points that wire
 * a `DataFormat` to the matching per-format serde and compose the format
 * payload with the byte-proven `@gsr/core` wire codec.
 *
 * The three per-format serdes (Avro, Protobuf, JSON-Schema) each accept
 * arbitrary schemas and records and produce/consume the payload region
 * that sits after the 18-byte wire header. The common facade classes
 * (`GsrSerializer` / `GsrDeserializer`) route on `DataFormat` and delegate
 * all header, compression, and (for protobuf) message-index handling to
 * `@gsr/core`.
 *
 * The per-format encode/decode slices and their fixture arrays are NOT
 * exported here — they are the offline byte-identity gate suite's
 * composition primitives, not customer surface. They live at the
 * `@gsr/serde/internal` subpath and are consumed by
 * `@gsr/integration-tests` alone.
 */

// Full-breadth per-format serdes.
export {
  deserializeAvro,
  serializeAvro,
  type AvroRecordType,
  type AvroSchemaInput,
  type AvroSerdeOptions,
} from "./avro/serde.js";
export {
  deserializeProtobufBody,
  serializeProtobufBody,
  type ProtobufMessageType,
  type ProtobufSerdeOptions,
} from "./protobuf/serde.js";
export {
  deserializeJson,
  JsonSchemaValidationError,
  serializeJson,
} from "./jsonschema/serde.js";

// Facade surface.
export { DataFormat } from "./facade/types.js";
export {
  DefaultSchemaNameStrategy,
  RecordNameStrategy,
  type SchemaNameStrategy,
} from "./facade/schema-name-strategy.js";
export {
  GsrSerializer,
  type SerializeRequest,
  type SerializerConfig,
} from "./facade/serializer.js";
export {
  GsrDeserializer,
  type DeserializeRequest,
} from "./facade/deserializer.js";

// Schema-evolution surface: reader-schema projection (Avro-only) plus
// the 8 Glue compatibility modes with case-exact validation.
export {
  projectAvroPayload,
  type AvroProjectionOptions,
} from "./evolution/avro-projection.js";
export {
  CompatibilityMode,
  DEFAULT_COMPATIBILITY_MODE,
  GsrInvalidCompatibilityModeError,
  isCompatibilityMode,
  parseCompatibilityMode,
} from "./evolution/compatibility.js";
