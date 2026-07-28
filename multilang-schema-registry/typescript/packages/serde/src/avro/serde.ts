/**
 * Full-breadth Avro serializer and deserializer.
 *
 * Promotes the format's minimum-viable encode/decode slice into a
 * customer-facing surface that accepts arbitrary avsc-parseable schemas and
 * records, and exposes both the Generic-datum and Specific-record decode
 * shapes. The wire bytes are identical across the two record-type shapes —
 * the option only selects what the returned object looks like on decode.
 *
 * The functions produce/consume payload bytes only — the region that sits
 * after the 18-byte wire header. Header assembly, compression, and the
 * transport-agnostic codec composition live in `@gsr/core`; this module
 * deliberately stays header-agnostic so the common facade can compose it
 * with the byte-proven core codec without duplicating that logic here.
 *
 * Reader/writer schema resolution and schema evolution are out of scope for
 * this module.
 */

import avsc from "avsc";

import { GsrIncompatibleDataError } from "@gsr/core";

/**
 * Selects the decode return shape. Wire bytes are identical across both
 * values — this option only changes the type of object the deserializer
 * returns.
 *
 * - `"GENERIC"` — a plain datum object, mirroring the Java canonical
 *   `GenericRecord` and the Go reference `AvroRecordType.GenericRecord`.
 *   This is the default because it does not require the caller to know the
 *   avsc typed-record constructor and matches the parity behaviour used by
 *   the per-format encode-slice.
 * - `"SPECIFIC"` — the avsc typed-record instance (a `RecordType` prototype
 *   with helper methods such as `.toBuffer()` and `.clone()`), mirroring
 *   `AvroRecordType.SpecificRecord`.
 */
export type AvroRecordType = "GENERIC" | "SPECIFIC";

/** Options accepted by {@link serializeAvro} and {@link deserializeAvro}. */
export interface AvroSerdeOptions {
  /** Selects the decode return shape; defaults to `"GENERIC"`. */
  recordType?: AvroRecordType;
}

/**
 * A schema input the serde can accept:
 * - `string` — JSON-encoded avsc schema (the raw contents of a `.avsc` file).
 * - `object` — a parsed avsc schema (already `JSON.parse`d, or an object
 *   literal).
 * - `avsc.Type` — a compiled avsc type, passed through unchanged.
 */
export type AvroSchemaInput = string | object | avsc.Type;

/**
 * Compile the caller's schema input into an avsc `Type`.
 *
 * `omitRecordMethods` toggles avsc's typed-record constructor: when true, the
 * `Type.fromBuffer` result is a plain datum object; when false, it is a
 * typed record instance with prototype helpers. The wire bytes are unaffected
 * by this flag — it only controls the shape of the decoded object.
 *
 * An already-compiled `avsc.Type` is passed through unchanged; the caller
 * pre-committed the record-type shape when they compiled it and re-compiling
 * here would silently override that choice.
 */
function compileType(
  schema: AvroSchemaInput,
  omitRecordMethods: boolean,
): avsc.Type {
  if (schema instanceof avsc.Type) {
    return schema;
  }

  const parsed: avsc.schema.AvroSchema =
    typeof schema === "string"
      ? (JSON.parse(schema) as avsc.schema.AvroSchema)
      : (schema as avsc.schema.AvroSchema);

  return avsc.Type.forSchema(parsed, { omitRecordMethods });
}

/**
 * Encode an Avro datum against `schema` and return the payload bytes that
 * would sit after the 18-byte wire header.
 *
 * The caller composes this payload with the wire header (and optional ZLIB
 * compression) via the `@gsr/core` codec — this function is header-agnostic
 * on purpose. The record-type option has no effect on the produced bytes;
 * avsc's `Type.toBuffer` writes the same encoded form for plain data and
 * typed-record instances.
 */
export function serializeAvro(
  schema: AvroSchemaInput,
  datum: unknown,
  _opts?: AvroSerdeOptions,
): Buffer {
  // `omitRecordMethods: true` is fine for encode either way — the flag only
  // affects the decoder's return shape, not the encoder's write path.
  const type = compileType(schema, true);
  return type.toBuffer(datum);
}

/**
 * Decode an Avro payload (bytes AFTER the 18-byte wire header, already
 * decompressed if the transport was ZLIB) back into a datum.
 *
 * `opts.recordType` selects the return shape: `"GENERIC"` yields a plain
 * datum object; `"SPECIFIC"` yields the avsc typed-record instance. The wire
 * bytes read by `Type.fromBuffer` are identical in both cases.
 *
 * Any throw from avsc — truncated buffer, malformed varint, unexpected tag —
 * is wrapped as {@link GsrIncompatibleDataError} so the public decode surface
 * has a single wire-format error type. The original throw is preserved on
 * the wrapper's `cause` for diagnostics.
 */
export function deserializeAvro(
  payload: Buffer,
  schema: AvroSchemaInput,
  opts?: AvroSerdeOptions,
): unknown {
  const recordType: AvroRecordType = opts?.recordType ?? "GENERIC";
  const omitRecordMethods = recordType === "GENERIC";
  const type = compileType(schema, omitRecordMethods);

  try {
    return type.fromBuffer(payload);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    const wrapped = new GsrIncompatibleDataError(
      `failed to decode Avro payload: ${reason}`,
    );
    wrapped.cause = cause;
    throw wrapped;
  }
}
