/**
 * Tier-1 unit tests for the shared interop fixtures.
 *
 * Exercises the envelope builders and readers offline, with no network, no
 * JVM, and no sidecar. The property we prove here is that for every format
 * (Avro, Protobuf, JSON-Schema) and every version (v1, v2), running
 * `build*Envelope` followed by the matching `read*Envelope` recovers the
 * input record field-for-field. This is the harness's contract: the
 * envelope shapes are the sidecar-facing surface, and the readers are what
 * every interop suite uses to assert Java-side output.
 */

import { describe, expect, it } from "vitest";

import {
  AVRO_SCHEMA_V1,
  AVRO_SCHEMA_V2,
  CUSTOMER_RECORD_V1,
  CUSTOMER_RECORD_V2,
  INTEROP_NAMESPACE,
  INTEROP_PROTOBUF_FULL_NAME,
  INTEROP_RECORD_NAME,
  JSON_SCHEMA_V1,
  JSON_SCHEMA_V2,
  PROTOBUF_SCHEMA_V1,
  PROTOBUF_SCHEMA_V2,
  buildAvroEnvelope,
  buildJsonEnvelope,
  buildProtobufEnvelope,
  readAvroEnvelopeV1,
  readAvroEnvelopeV2,
  readJsonEnvelopeV1,
  readJsonEnvelopeV2,
  readProtobufEnvelopeV1,
  readProtobufEnvelopeV2,
} from "./interop-fixtures.js";

describe("interop fixture constants", () => {
  it("uses a fully-qualified Protobuf name built from namespace + record name", () => {
    expect(INTEROP_PROTOBUF_FULL_NAME).toBe(
      `${INTEROP_NAMESPACE}.${INTEROP_RECORD_NAME}`,
    );
  });

  it("declares Avro v1 as a record schema with three fields", () => {
    const parsed = JSON.parse(AVRO_SCHEMA_V1);
    expect(parsed.type).toBe("record");
    expect(parsed.name).toBe(INTEROP_RECORD_NAME);
    expect(parsed.namespace).toBe(INTEROP_NAMESPACE);
    expect(parsed.fields).toHaveLength(3);
  });

  it("declares Avro v2 as a backward-compatible evolution of v1 (adds nullable email)", () => {
    const parsed = JSON.parse(AVRO_SCHEMA_V2);
    expect(parsed.fields).toHaveLength(4);
    const email = parsed.fields.find(
      (f: { name: string }) => f.name === "email",
    );
    expect(email).toBeDefined();
    // Backward compatibility requires a default value on the added field so
    // v1 readers can decode v2 bytes.
    expect(email.default).toBeNull();
    expect(email.type).toEqual(["null", "string"]);
  });

  it("declares JSON-Schema v1 with three required scalars", () => {
    const parsed = JSON.parse(JSON_SCHEMA_V1);
    expect(parsed.type).toBe("object");
    expect(parsed.required).toEqual(["id", "name", "age"]);
    expect(Object.keys(parsed.properties).sort()).toEqual(
      ["age", "id", "name"],
    );
  });

  it("declares JSON-Schema v2 with an added optional email property", () => {
    const parsed = JSON.parse(JSON_SCHEMA_V2);
    expect(parsed.required).toEqual(["id", "name", "age"]);
    expect(Object.keys(parsed.properties).sort()).toEqual(
      ["age", "email", "id", "name"],
    );
  });

  it("declares proto3 schemas with the interop namespace as the proto package", () => {
    expect(PROTOBUF_SCHEMA_V1).toContain(`syntax = "proto3";`);
    expect(PROTOBUF_SCHEMA_V1).toContain(`package ${INTEROP_NAMESPACE};`);
    expect(PROTOBUF_SCHEMA_V1).toContain(`message ${INTEROP_RECORD_NAME}`);
    expect(PROTOBUF_SCHEMA_V2).toContain("string email = 4;");
  });
});

describe("Avro envelope round-trip", () => {
  it("round-trips a v1 record through buildAvroEnvelope + readAvroEnvelopeV1", () => {
    const env = buildAvroEnvelope(CUSTOMER_RECORD_V1);
    expect(env.fields).toEqual({
      id: CUSTOMER_RECORD_V1.id,
      name: CUSTOMER_RECORD_V1.name,
      age: CUSTOMER_RECORD_V1.age,
    });
    expect("email" in env.fields).toBe(false);
    expect(readAvroEnvelopeV1(env)).toEqual(CUSTOMER_RECORD_V1);
  });

  it("round-trips a v2 record through buildAvroEnvelope + readAvroEnvelopeV2", () => {
    const env = buildAvroEnvelope(CUSTOMER_RECORD_V2);
    expect(env.fields).toEqual({
      id: CUSTOMER_RECORD_V2.id,
      name: CUSTOMER_RECORD_V2.name,
      age: CUSTOMER_RECORD_V2.age,
      email: CUSTOMER_RECORD_V2.email,
    });
    expect(readAvroEnvelopeV2(env)).toEqual(CUSTOMER_RECORD_V2);
  });

  it("cross-version read: v1 envelope through v2 reader defaults email to empty string", () => {
    const env = buildAvroEnvelope(CUSTOMER_RECORD_V1);
    const decoded = readAvroEnvelopeV2(env);
    expect(decoded).toEqual({ ...CUSTOMER_RECORD_V1, email: "" });
  });

  it("cross-version read: v2 envelope through v1 reader drops email", () => {
    const env = buildAvroEnvelope(CUSTOMER_RECORD_V2);
    const decoded = readAvroEnvelopeV1(env);
    expect(decoded).toEqual({
      id: CUSTOMER_RECORD_V2.id,
      name: CUSTOMER_RECORD_V2.name,
      age: CUSTOMER_RECORD_V2.age,
    });
  });

  it("throws with a diagnostic when a required field is missing", () => {
    expect(() => readAvroEnvelopeV1({ fields: { id: "x", name: "y" } })).toThrow(
      /fields.age/,
    );
  });
});

describe("JSON envelope round-trip", () => {
  it("round-trips a v1 record through buildJsonEnvelope + readJsonEnvelopeV1", () => {
    const env = buildJsonEnvelope(CUSTOMER_RECORD_V1, JSON_SCHEMA_V1);
    expect(env.schema).toBe(JSON_SCHEMA_V1);
    expect(JSON.parse(env.payload)).toEqual(CUSTOMER_RECORD_V1);
    expect(readJsonEnvelopeV1(env)).toEqual(CUSTOMER_RECORD_V1);
  });

  it("round-trips a v2 record through buildJsonEnvelope + readJsonEnvelopeV2", () => {
    const env = buildJsonEnvelope(CUSTOMER_RECORD_V2, JSON_SCHEMA_V2);
    expect(env.schema).toBe(JSON_SCHEMA_V2);
    expect(JSON.parse(env.payload)).toEqual(CUSTOMER_RECORD_V2);
    expect(readJsonEnvelopeV2(env)).toEqual(CUSTOMER_RECORD_V2);
  });

  it("cross-version read: v1 payload through v2 reader defaults email to empty string", () => {
    const env = buildJsonEnvelope(CUSTOMER_RECORD_V1, JSON_SCHEMA_V1);
    const decoded = readJsonEnvelopeV2(env);
    expect(decoded).toEqual({ ...CUSTOMER_RECORD_V1, email: "" });
  });

  it("throws with a diagnostic when payload is not valid JSON", () => {
    expect(() =>
      readJsonEnvelopeV1({ schema: JSON_SCHEMA_V1, payload: "not-json" }),
    ).toThrow(/payload is not valid JSON/);
  });

  it("throws with a diagnostic when payload is not a JSON object", () => {
    expect(() =>
      readJsonEnvelopeV1({ schema: JSON_SCHEMA_V1, payload: "[1,2,3]" }),
    ).toThrow(/did not parse to a JSON object/);
  });
});

describe("Protobuf envelope round-trip", () => {
  it("round-trips a v1 record through buildProtobufEnvelope + readProtobufEnvelopeV1", () => {
    const env = buildProtobufEnvelope(CUSTOMER_RECORD_V1);
    expect(env.messageTypeFullName).toBe(INTEROP_PROTOBUF_FULL_NAME);
    expect(JSON.parse(env.fieldsJson)).toEqual(CUSTOMER_RECORD_V1);
    expect(readProtobufEnvelopeV1(env)).toEqual(CUSTOMER_RECORD_V1);
  });

  it("round-trips a v2 record through buildProtobufEnvelope + readProtobufEnvelopeV2", () => {
    const env = buildProtobufEnvelope(CUSTOMER_RECORD_V2);
    expect(env.messageTypeFullName).toBe(INTEROP_PROTOBUF_FULL_NAME);
    expect(JSON.parse(env.fieldsJson)).toEqual(CUSTOMER_RECORD_V2);
    expect(readProtobufEnvelopeV2(env)).toEqual(CUSTOMER_RECORD_V2);
  });

  it("cross-version read: v1 fields through v2 reader defaults email to empty string", () => {
    const env = buildProtobufEnvelope(CUSTOMER_RECORD_V1);
    const decoded = readProtobufEnvelopeV2(env);
    expect(decoded).toEqual({ ...CUSTOMER_RECORD_V1, email: "" });
  });

  it("throws when messageTypeFullName does not match the fixture name", () => {
    expect(() =>
      readProtobufEnvelopeV1({
        messageTypeFullName: "other.Message",
        fieldsJson: JSON.stringify(CUSTOMER_RECORD_V1),
      }),
    ).toThrow(/messageTypeFullName/);
  });

  it("uses the same fully-qualified name for v2 envelopes", () => {
    const env = buildProtobufEnvelope(CUSTOMER_RECORD_V2);
    expect(env.messageTypeFullName).toBe(INTEROP_PROTOBUF_FULL_NAME);
  });
});
