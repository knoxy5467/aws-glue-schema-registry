/**
 * Shared interop fixtures for the cross-language harness.
 *
 * Pure data + pure functions. This module MUST NOT touch the network, spawn a
 * JVM, or read from disk at import time — every interop test suite depends on
 * it, but the module itself is Tier-1-safe and is exercised by an offline unit
 * test co-located here.
 *
 * The module exposes three groups of surface:
 *
 *   1. Schema definitions. `v1` and `v2` schema bodies for Avro, Protobuf, and
 *      JSON-Schema. `v2` is a BACKWARD-compatible evolution of `v1` in every
 *      format so the cross-version cells (writer-schema lookup) exercise a
 *      realistic evolution shape rather than an accidental byte-identity
 *      coincidence.
 *   2. Logical records. `v1` and `v2` records for each format, as strictly
 *      typed TS objects. These are what the TS client encodes and what the
 *      inverse envelope readers decode back to for equality checks.
 *   3. Envelope builders + readers. The Java sidecar's `/kafka-produce` and
 *      `/kafka-consume` endpoints exchange the logical record as a per-format
 *      JSON envelope (see the interop harness spec's "Sidecar HTTP contract"
 *      section for the shapes). `build*Envelope` produces the request-side
 *      shape from a typed record; `read*Envelope` recovers the typed record
 *      from a response-side envelope. Both directions are pure and
 *      idempotent under round-trip, which is the property the unit test
 *      here proves.
 *
 * Schema bodies are inlined rather than sourced from the shared fixture root
 * because no matching interop fixture exists in that tree yet (the shared
 * tree carries broad round-trip fixtures; the interop harness needs a small
 * paired v1/v2 evolution). The inlined bodies mirror the cross-version schema
 * shapes proven in the Go reference client's cross-version interop test so
 * the Java sidecar's decode behavior is exercised on a known-good schema
 * pair. When the shared tree gains a co-located `interop/` fixture directory,
 * `listFixtures` from `../../src/fixture-root.js` can supersede these inlined
 * bodies without changing the builder/reader contract.
 */

/**
 * Namespace used in every schema and record type in this module. Kept short
 * and stable so the schema-name-strategy produces a deterministic Glue
 * schema-name per fixture.
 */
export const INTEROP_NAMESPACE = "gsrinterop" as const;

/**
 * Top-level record/message name used across all three formats and both
 * versions. Choosing one name across formats keeps the interop matrix table
 * readable — every cell reads "encode a Customer, decode a Customer".
 */
export const INTEROP_RECORD_NAME = "Customer" as const;

/**
 * Fully qualified message name for the Protobuf `messageTypeFullName`
 * envelope field. The `.proto` bodies below declare `package gsrinterop;`
 * so the fully-qualified name is `gsrinterop.Customer`.
 */
export const INTEROP_PROTOBUF_FULL_NAME =
  `${INTEROP_NAMESPACE}.${INTEROP_RECORD_NAME}` as const;

// ---------------------------------------------------------------------------
// Schema definitions — Avro
// ---------------------------------------------------------------------------

/**
 * Avro v1 schema: a Customer record with `id`, `name`, `age`. Stringified so
 * it is ready to hand to Glue's `RegisterSchema` and to the sidecar's
 * `/encode` request body.
 */
export const AVRO_SCHEMA_V1 = JSON.stringify({
  type: "record",
  name: INTEROP_RECORD_NAME,
  namespace: INTEROP_NAMESPACE,
  fields: [
    { name: "id", type: "string" },
    { name: "name", type: "string" },
    { name: "age", type: "int" },
  ],
});

/**
 * Avro v2 schema: adds an optional nullable `email` field with a `null`
 * default. Backward-compatible: a v1 reader can decode v2 bytes (the added
 * field has a default), and a v2 reader can decode v1 bytes (the union
 * defaults to null).
 */
export const AVRO_SCHEMA_V2 = JSON.stringify({
  type: "record",
  name: INTEROP_RECORD_NAME,
  namespace: INTEROP_NAMESPACE,
  fields: [
    { name: "id", type: "string" },
    { name: "name", type: "string" },
    { name: "age", type: "int" },
    { name: "email", type: ["null", "string"], default: null },
  ],
});

// ---------------------------------------------------------------------------
// Schema definitions — JSON-Schema
// ---------------------------------------------------------------------------

/**
 * JSON-Schema v1 (Draft 7): the same three fields required. `additionalProperties`
 * is false so evolution behavior is unambiguous — v2 adds `email` explicitly.
 */
export const JSON_SCHEMA_V1 = JSON.stringify({
  $schema: "http://json-schema.org/draft-07/schema#",
  title: INTEROP_RECORD_NAME,
  type: "object",
  properties: {
    id: { type: "string" },
    name: { type: "string" },
    age: { type: "integer" },
  },
  required: ["id", "name", "age"],
  additionalProperties: false,
});

/**
 * JSON-Schema v2: adds optional `email`. Backward-compatible because `email`
 * is not required — a v1 doc is still valid under v2, and a v2 doc without
 * `email` is valid under v1 (or, with `additionalProperties: false`, would
 * still validate as long as `email` is absent).
 */
export const JSON_SCHEMA_V2 = JSON.stringify({
  $schema: "http://json-schema.org/draft-07/schema#",
  title: INTEROP_RECORD_NAME,
  type: "object",
  properties: {
    id: { type: "string" },
    name: { type: "string" },
    age: { type: "integer" },
    email: { type: "string" },
  },
  required: ["id", "name", "age"],
  additionalProperties: false,
});

// ---------------------------------------------------------------------------
// Schema definitions — Protobuf
// ---------------------------------------------------------------------------

/**
 * Protobuf v1 schema body. `proto3` with three scalar fields. The `package`
 * matches `INTEROP_NAMESPACE` so the fully-qualified message name is
 * `gsrinterop.Customer`.
 */
export const PROTOBUF_SCHEMA_V1 = `syntax = "proto3";
package ${INTEROP_NAMESPACE};
message ${INTEROP_RECORD_NAME} {
  string id = 1;
  string name = 2;
  int32 age = 3;
}
`;

/**
 * Protobuf v2 schema body: adds an optional `email` field at tag 4. Backward
 * compatible because proto3 scalar fields are implicitly optional and default
 * to the empty string when absent on the wire — a v1 reader receives an
 * unknown field it ignores, and a v2 reader receives an empty `email` when
 * reading v1 bytes.
 */
export const PROTOBUF_SCHEMA_V2 = `syntax = "proto3";
package ${INTEROP_NAMESPACE};
message ${INTEROP_RECORD_NAME} {
  string id = 1;
  string name = 2;
  int32 age = 3;
  string email = 4;
}
`;

// ---------------------------------------------------------------------------
// Logical record types + literal records
// ---------------------------------------------------------------------------

/**
 * The v1 record shape shared across all three formats: three scalars, no
 * `email`. The interop cells that exercise same-version decode use a v1
 * record on both sides; cross-version cells use a v1 record produced under
 * a v1 schema and read under v2.
 */
export interface CustomerRecordV1 {
  readonly id: string;
  readonly name: string;
  readonly age: number;
}

/**
 * The v2 record shape: v1 plus an optional `email`. Modeled as a required
 * string here (never `undefined`) so the envelope builders can produce a
 * deterministic byte-shape without branching on presence; a v2 record with
 * an empty-string `email` still encodes correctly under every format.
 */
export interface CustomerRecordV2 extends CustomerRecordV1 {
  readonly email: string;
}

/**
 * Canonical v1 record used by every same-version interop cell.
 */
export const CUSTOMER_RECORD_V1: CustomerRecordV1 = {
  id: "cust-001",
  name: "Ada Lovelace",
  age: 36,
};

/**
 * Canonical v2 record used by every cross-version interop cell that writes
 * at v2. The `email` field is populated so the writer-schema-lookup case
 * shows the field is genuinely on the wire.
 */
export const CUSTOMER_RECORD_V2: CustomerRecordV2 = {
  id: "cust-002",
  name: "Grace Hopper",
  age: 44,
  email: "grace@example.com",
};

// ---------------------------------------------------------------------------
// Envelope shapes — the sidecar HTTP contract
// ---------------------------------------------------------------------------

/**
 * `AVRO` envelope shape: `{ "fields": { "<name>": <value>, ... } }`. Kept
 * open (`Record<string, unknown>`) because Avro union branches and complex
 * types can shape into arbitrary JSON; the round-trip test asserts field
 * equality by walking the readers, not by static typing the envelope.
 */
export interface AvroEnvelope {
  readonly fields: Readonly<Record<string, unknown>>;
}

/**
 * `JSON` envelope shape: `{ "schema": "<jsonSchema>", "payload": "<jsonDoc>" }`.
 * Both fields are stringified JSON so the sidecar can hand `schema` straight
 * to a `JsonDataFormat` binding and `payload` to the deserializer.
 */
export interface JsonEnvelope {
  readonly schema: string;
  readonly payload: string;
}

/**
 * `PROTOBUF` envelope shape:
 * `{ "messageTypeFullName": "<pkg.Message>", "fieldsJson": "<json>" }`.
 * `fieldsJson` is `protojson.Marshaler`-style output on the Java side.
 */
export interface ProtobufEnvelope {
  readonly messageTypeFullName: string;
  readonly fieldsJson: string;
}

// ---------------------------------------------------------------------------
// Envelope builders — typed record -> sidecar-request envelope
// ---------------------------------------------------------------------------

/**
 * Build an `AVRO` envelope from a v1 or v2 record. The `fields` bag mirrors
 * the record verbatim; hamba/avro and Java's Avro `GenericRecord` both accept
 * the same field-name-keyed shape.
 */
export function buildAvroEnvelope(
  record: CustomerRecordV1 | CustomerRecordV2,
): AvroEnvelope {
  const fields: Record<string, unknown> = {
    id: record.id,
    name: record.name,
    age: record.age,
  };
  if ("email" in record) {
    fields["email"] = record.email;
  }
  return { fields };
}

/**
 * Build a `JSON` envelope from a v1 or v2 record. The `schema` is one of
 * `JSON_SCHEMA_V1` / `JSON_SCHEMA_V2` (stringified JSON-Schema Draft-7); the
 * `payload` is the record itself stringified.
 */
export function buildJsonEnvelope(
  record: CustomerRecordV1 | CustomerRecordV2,
  schema: string,
): JsonEnvelope {
  return {
    schema,
    payload: JSON.stringify(record),
  };
}

/**
 * Build a `PROTOBUF` envelope from a v1 or v2 record. `messageTypeFullName`
 * is fixed to `gsrinterop.Customer` (the qualified name declared by
 * `PROTOBUF_SCHEMA_V*`); `fieldsJson` is the record stringified as JSON, which
 * is what `protojson.Marshal` on the Java side produces from a `DynamicMessage`.
 */
export function buildProtobufEnvelope(
  record: CustomerRecordV1 | CustomerRecordV2,
): ProtobufEnvelope {
  return {
    messageTypeFullName: INTEROP_PROTOBUF_FULL_NAME,
    fieldsJson: JSON.stringify(record),
  };
}

// ---------------------------------------------------------------------------
// Envelope readers — sidecar-response envelope -> typed record
// ---------------------------------------------------------------------------

/**
 * Assert a value is a plain string, throwing with a diagnostic if it is not.
 * The sidecar contract is stringly-typed on the wire, and every envelope
 * reader normalizes into a strict record shape.
 */
function requireString(value: unknown, path: string): string {
  if (typeof value !== "string") {
    throw new Error(
      `interop-fixtures: expected string at ${path}, got ${typeof value}`,
    );
  }
  return value;
}

/**
 * Assert a value is a finite number, throwing if it is not.
 */
function requireNumber(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(
      `interop-fixtures: expected finite number at ${path}, got ${typeof value}`,
    );
  }
  return value;
}

/**
 * Read an `AVRO` envelope back into a v1 record (three-field decode). If the
 * envelope carries an `email` field it is ignored — callers reading a v2
 * envelope through the v1 reader are exercising the "v1 reader ignores v2's
 * added optional field" property.
 */
export function readAvroEnvelopeV1(env: AvroEnvelope): CustomerRecordV1 {
  const f = env.fields;
  return {
    id: requireString(f["id"], "fields.id"),
    name: requireString(f["name"], "fields.name"),
    age: requireNumber(f["age"], "fields.age"),
  };
}

/**
 * Read an `AVRO` envelope back into a v2 record. Missing `email` (e.g. reading
 * a v1 envelope through the v2 reader) resolves to the empty string — the
 * same convention proto3 scalars use, keeping the reader defensive.
 */
export function readAvroEnvelopeV2(env: AvroEnvelope): CustomerRecordV2 {
  const v1 = readAvroEnvelopeV1(env);
  const email = env.fields["email"];
  return {
    ...v1,
    email: email === undefined || email === null ? "" : requireString(email, "fields.email"),
  };
}

/**
 * Read a `JSON` envelope back into a v1 record. `payload` is JSON-parsed and
 * the three v1 fields are extracted; a `payload` that is not a JSON object
 * throws with a diagnostic.
 */
export function readJsonEnvelopeV1(env: JsonEnvelope): CustomerRecordV1 {
  const parsed = safeParseObject(env.payload, "payload");
  return {
    id: requireString(parsed["id"], "payload.id"),
    name: requireString(parsed["name"], "payload.name"),
    age: requireNumber(parsed["age"], "payload.age"),
  };
}

/**
 * Read a `JSON` envelope back into a v2 record. `email` is optional in the
 * v2 record shape when reading a v1 payload (defaults to empty string).
 */
export function readJsonEnvelopeV2(env: JsonEnvelope): CustomerRecordV2 {
  const parsed = safeParseObject(env.payload, "payload");
  const v1 = {
    id: requireString(parsed["id"], "payload.id"),
    name: requireString(parsed["name"], "payload.name"),
    age: requireNumber(parsed["age"], "payload.age"),
  };
  const email = parsed["email"];
  return {
    ...v1,
    email: email === undefined || email === null ? "" : requireString(email, "payload.email"),
  };
}

/**
 * Read a `PROTOBUF` envelope back into a v1 record. `messageTypeFullName`
 * must match `INTEROP_PROTOBUF_FULL_NAME`; otherwise the sidecar decoded
 * something we did not send.
 */
export function readProtobufEnvelopeV1(env: ProtobufEnvelope): CustomerRecordV1 {
  requireProtobufFullName(env);
  const parsed = safeParseObject(env.fieldsJson, "fieldsJson");
  return {
    id: requireString(parsed["id"], "fieldsJson.id"),
    name: requireString(parsed["name"], "fieldsJson.name"),
    age: requireNumber(parsed["age"], "fieldsJson.age"),
  };
}

/**
 * Read a `PROTOBUF` envelope back into a v2 record. Reads `email` as an
 * empty string when absent — mirroring proto3's scalar default.
 */
export function readProtobufEnvelopeV2(env: ProtobufEnvelope): CustomerRecordV2 {
  requireProtobufFullName(env);
  const parsed = safeParseObject(env.fieldsJson, "fieldsJson");
  const v1 = {
    id: requireString(parsed["id"], "fieldsJson.id"),
    name: requireString(parsed["name"], "fieldsJson.name"),
    age: requireNumber(parsed["age"], "fieldsJson.age"),
  };
  const email = parsed["email"];
  return {
    ...v1,
    email: email === undefined || email === null ? "" : requireString(email, "fieldsJson.email"),
  };
}

/**
 * Parse a JSON string and require the result be a non-null object. Arrays
 * and primitives are rejected — every envelope payload we handle is an
 * object.
 */
function safeParseObject(
  text: string,
  path: string,
): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (cause) {
    throw new Error(
      `interop-fixtures: ${path} is not valid JSON: ${(cause as Error).message}`,
    );
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(
      `interop-fixtures: ${path} did not parse to a JSON object`,
    );
  }
  return parsed as Record<string, unknown>;
}

/**
 * Require the Protobuf envelope's `messageTypeFullName` match the fixed
 * fixture name. A mismatch means the sidecar hydrated a different message
 * type than the one we expect, and every downstream field access would be
 * meaningless.
 */
function requireProtobufFullName(env: ProtobufEnvelope): void {
  if (env.messageTypeFullName !== INTEROP_PROTOBUF_FULL_NAME) {
    throw new Error(
      `interop-fixtures: expected messageTypeFullName="${INTEROP_PROTOBUF_FULL_NAME}", got "${env.messageTypeFullName}"`,
    );
  }
}
