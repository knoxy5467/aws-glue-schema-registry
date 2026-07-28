/**
 * Shared facade types for the customer-facing serialize/deserialize surface.
 *
 * Kept intentionally small: the only export here is the format tag every
 * facade entry point routes on. Per-format serde options (`AvroSerdeOptions`,
 * `ProtobufSerdeOptions`) live next to their serde implementations; request
 * and config shapes for the common serializer/deserializer are declared where
 * those facades are defined so the type surface stays co-located with the
 * code that owns it.
 */

/**
 * Format tag routed on by the common serializer/deserializer facade. Mirrors
 * the Java `DataFormat` enum and the Go `common.DataFormat` constants — the
 * string values are the wire-visible names both references emit.
 */
export enum DataFormat {
  AVRO = "AVRO",
  PROTOBUF = "PROTOBUF",
  JSON = "JSON",
}
