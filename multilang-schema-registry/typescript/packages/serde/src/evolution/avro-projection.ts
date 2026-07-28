/**
 * Avro reader-schema projection.
 *
 * Projects a writer-encoded Avro payload into a reader schema's shape:
 * drops fields the reader omits, fills reader defaults for fields the
 * writer never wrote, and applies avsc-supported type promotions
 * (int -> long, float -> double, etc.).
 *
 * The projection consumes a decoded, decompressed Avro payload — the
 * bytes that sit after the 18-byte wire header. Header assembly,
 * compression, and the protobuf message-index all live in `@gsr/core`
 * and are never touched here.
 *
 * Any avsc throw — an unparseable reader schema, an unresolvable
 * writer/reader pair, or a malformed payload — is wrapped as
 * {@link GsrIncompatibleDataError} so the public decode surface keeps a
 * single wire-format error type. The original throw is preserved on the
 * wrapper's `cause` for diagnostics.
 *
 * Reader-schema projection is Avro-only: Protobuf compatibility is
 * intrinsic to the wire format (field-number based) and JSON Schema
 * documents are self-describing, so neither has a client-side reader/
 * writer resolution step.
 */

import avsc from "avsc";

import { GsrIncompatibleDataError } from "@gsr/core";

import type { AvroRecordType } from "../avro/serde.js";

/** Options accepted by {@link projectAvroPayload}. */
export interface AvroProjectionOptions {
  /**
   * Selects the projected datum's return shape; defaults to `"GENERIC"`.
   * The choice is decode-shape only — the underlying wire bytes are read
   * identically in both cases, and the projected values are structurally
   * identical. `"GENERIC"` returns a plain datum object; `"SPECIFIC"`
   * returns an avsc typed-record instance whose prototype exposes helper
   * methods such as `.toBuffer()` and `.clone()`.
   */
  recordType?: AvroRecordType;
}

/**
 * A schema input this module accepts. Deliberately narrower than the
 * per-format serde's `AvroSchemaInput`: projection compiles the writer
 * AND reader schemas together to build a resolver, so an
 * already-compiled `avsc.Type` (which has already fixed its own
 * `omitRecordMethods`) cannot be composed with an arbitrary reader
 * shape without re-compiling anyway.
 */
type ProjectionSchemaInput = string | object;

/**
 * Compile a schema JSON string or a parsed schema object into an avsc
 * `Type`.
 *
 * `omitRecordMethods` toggles avsc's typed-record constructor: when
 * true, `fromBuffer` returns a plain datum object; when false, it
 * returns a typed-record instance with prototype helpers. Both writer
 * and reader are compiled with the same flag so the projected shape is
 * consistent — the resolver reads from a datum produced by the writer
 * type and lifts it into the reader type.
 *
 * A parse or compile failure is wrapped as {@link GsrIncompatibleDataError}
 * with a message naming the failing side so the caller can distinguish
 * an unparseable reader schema from an unparseable writer schema; the
 * original throw is preserved on `.cause`.
 */
function compileSchema(
  schema: ProjectionSchemaInput,
  side: "writer" | "reader",
  omitRecordMethods: boolean,
): avsc.Type {
  try {
    const parsed: avsc.schema.AvroSchema =
      typeof schema === "string"
        ? (JSON.parse(schema) as avsc.schema.AvroSchema)
        : (schema as avsc.schema.AvroSchema);
    return avsc.Type.forSchema(parsed, { omitRecordMethods });
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    const wrapped = new GsrIncompatibleDataError(
      `failed to parse Avro ${side} schema: ${reason}`,
    );
    wrapped.cause = cause;
    throw wrapped;
  }
}

/**
 * Project a writer-encoded Avro payload into the reader schema's shape.
 *
 * Compiles both schemas via avsc, builds a resolver with
 * `readerType.createResolver(writerType)`, and decodes the payload
 * through the resolver so the returned datum is in the reader shape:
 * fields the reader omits are dropped, reader-declared defaults fill
 * for fields the writer never wrote, and numeric writer fields are
 * promoted into wider reader types (int -> long, float -> double, and
 * other avsc-supported promotions).
 *
 * Failure modes are folded into a single decode-error type:
 * - An unparseable reader schema (or writer schema) throws
 *   {@link GsrIncompatibleDataError} naming the parse failure.
 * - An unresolvable writer/reader pair (avsc rejects
 *   `createResolver`) throws {@link GsrIncompatibleDataError}.
 * - A payload that cannot be decoded against the resolver throws
 *   {@link GsrIncompatibleDataError}.
 * In every case the original avsc throw is preserved on `.cause`; no
 * raw avsc error escapes the public decode surface.
 *
 * @param payload      The Avro payload bytes to project — post-header,
 *                     post-decompress.
 * @param writerSchema The schema the payload was encoded with. Accepts
 *                     the JSON string form or a parsed schema object.
 * @param readerSchema The consumer's desired shape. Accepts the JSON
 *                     string form or a parsed schema object.
 * @param opts         Optional projection options; `recordType`
 *                     selects the return shape (default `"GENERIC"`).
 */
export function projectAvroPayload(
  payload: Buffer,
  writerSchema: ProjectionSchemaInput,
  readerSchema: ProjectionSchemaInput,
  opts?: AvroProjectionOptions,
): unknown {
  const recordType: AvroRecordType = opts?.recordType ?? "GENERIC";
  const omitRecordMethods = recordType === "GENERIC";

  const writerType = compileSchema(writerSchema, "writer", omitRecordMethods);
  const readerType = compileSchema(readerSchema, "reader", omitRecordMethods);

  let resolver: avsc.Resolver;
  try {
    resolver = readerType.createResolver(writerType);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    const wrapped = new GsrIncompatibleDataError(
      `reader schema is incompatible with writer schema: ${reason}`,
    );
    wrapped.cause = cause;
    throw wrapped;
  }

  try {
    return readerType.fromBuffer(payload, resolver);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    const wrapped = new GsrIncompatibleDataError(
      `failed to project Avro payload into reader schema: ${reason}`,
    );
    wrapped.cause = cause;
    throw wrapped;
  }
}
