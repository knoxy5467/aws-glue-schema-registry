/**
 * Minimal Avro encode/decode slice.
 *
 * The slice's sole job is to reproduce the Java-canonical Avro payload bytes
 * (the region that follows the 18-byte wire header) for the `test_v1.avsc`
 * `{data, count}` record, and to decode that same payload back to the canonical
 * record. Full Avro breadth — Specific-record codegen, reader/writer resolution,
 * logical types beyond the primitive int/string here — is out of scope for
 * this slice.
 *
 * The wire header, compression, and codec composition live in `@gsr/core`; this
 * module deliberately produces only the format-specific payload bytes so the
 * gate test can compose them with the core codec and compare against every
 * golden cell.
 */

import { readFileSync } from "node:fs";

import avsc from "avsc";

/**
 * Canonical record exposed by the Avro slice. Each entry pairs a
 * golden-vector `payloadTag` with the fixture schema path (relative to the
 * `shared/test/` reference tree) and the record the encode slice serializes.
 */
export interface AvroHazardRecord {
  /** Matches the `payloadTag` field of the Java-golden `.json` descriptor. */
  readonly tag: string;
  /** Schema path relative to `shared/test/` — resolved by the caller. */
  readonly schemaPath: string;
  /** The canonical record the encode slice serializes. */
  readonly record: Record<string, unknown>;
}

/**
 * The slice exposes exactly one record — the `test_v1.avsc` `{data, count}`
 * record — because the Java-golden generic and specific cells for this tag
 * share identical payload bytes (equal SHA-256 in `PROVENANCE.md`), so a single
 * avsc encode/decode pair covers both record-type golden cells for the gate.
 */
export const avroHazardRecords: readonly AvroHazardRecord[] = [
  {
    tag: "test-v1",
    schemaPath: "avro/none/test_v1.avsc",
    record: { data: "hello", count: 7 },
  },
];

/**
 * Read an `.avsc` schema file from disk and return the compiled avsc `Type`.
 * Kept internal so callers pass the schema path (which the resolver produces)
 * rather than pre-parsed schema objects; that mirrors the Go reference's
 * "encode by schema path" seam.
 */
function typeForSchemaFile(schemaPath: string): avsc.Type {
  const schemaJson = readFileSync(schemaPath, "utf8");
  const schema: avsc.schema.AvroSchema = JSON.parse(schemaJson);
  return avsc.Type.forSchema(schema);
}

/**
 * Encode a record against the given `.avsc` schema and return the payload
 * bytes that would sit after the 18-byte wire header. The caller composes
 * this with the wire header (and optional ZLIB compression) via the
 * `@gsr/core` codec — this function is header-agnostic on purpose.
 */
export function encodeAvro(
  schemaPath: string,
  record: unknown,
): Buffer {
  const type = typeForSchemaFile(schemaPath);
  return type.toBuffer(record);
}

/**
 * Decode an avsc payload (bytes AFTER the 18-byte wire header, already
 * decompressed if the transport was ZLIB) back into the canonical record.
 */
export function decodeAvro(payload: Buffer, schemaPath: string): unknown {
  const type = typeForSchemaFile(schemaPath);
  return type.fromBuffer(payload);
}
