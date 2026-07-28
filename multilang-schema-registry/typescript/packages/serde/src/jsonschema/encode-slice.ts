/**
 * Minimal JSON-Schema encode/decode slice.
 *
 * JSON Schema is different from Avro and Protobuf: the wire payload IS the
 * JSON document bytes themselves (canonicalized), and the schema is used only
 * for validation — not for framing. The determinism hazard is therefore JSON
 * canonicalization (key ordering, number formatting, unicode escaping,
 * whitespace), not schema-driven byte layout.
 *
 * The Java reference canonicalizes via `JSON.parse` → `JSON.stringify` (the
 * JSON.org default): compact separators (no spaces), object keys emitted in
 * insertion order, numbers in shortest round-trippable form. Node's built-in
 * `JSON.stringify` reproduces those bytes byte-for-byte for the instances
 * this slice exposes (verified against every NONE-compressed golden payload
 * region).
 *
 * `ajv` sits alongside the byte path — it validates the instance against the
 * Draft-07 schema so callers get schema-mismatch errors early — but the bytes
 * it produces are not part of the wire payload. Full JSON-Schema breadth
 * (evolution, custom keywords, generic-vs-specific dispatch) is out of scope
 * for this slice.
 *
 * The wire header, compression, and codec composition live in `@gsr/core`;
 * this module deliberately produces only the format-specific payload bytes
 * so the gate test can compose them with the core codec and compare against
 * every golden cell.
 */

import { readFileSync } from "node:fs";

import { Ajv, type AnySchema, type ValidateFunction } from "ajv";

/**
 * Canonical instance exposed by the JSON-Schema slice. Each entry
 * pairs a golden-vector `payloadTag` with the fixture schema path
 * (relative to the `shared/test/` reference tree) and the instance the encode
 * slice serializes.
 */
export interface JsonSchemaHazardRecord {
  /** Matches the `payloadTag` field of the Java-golden `.json` descriptor. */
  readonly tag: string;
  /** Schema path relative to `shared/test/` — resolved by the caller. */
  readonly schemaPath: string;
  /** The canonical instance the encode slice serializes. */
  readonly instance: Record<string, unknown>;
}

/**
 * The five instances, one per golden `payloadTag`. Each is the
 * literal object the Java reference serialized to produce the corresponding
 * `.bin`; the ordering of keys in the object literals is the byte-order the
 * canonicalized payload preserves (Node's `JSON.stringify` walks own keys in
 * insertion order for string keys, matching the Java reference's Jackson
 * output for these fixtures).
 */
export const jsonSchemaHazardRecords: readonly JsonSchemaHazardRecord[] = [
  {
    tag: "product-v1",
    schemaPath: "jsonschema/none/product_v1.schema.json",
    instance: {
      id: 42,
      name: "Widget",
      pricing: { amount: 19.99, currency: "USD" },
    },
  },
  {
    tag: "customer-v1",
    schemaPath: "jsonschema/backward/customer_v1.schema.json",
    instance: {
      id: "cust-001",
      email: "user@example.com",
      address: {
        street: "410 Terry Ave N",
        city: "Seattle",
        postalCode: "98109",
      },
      active: true,
    },
  },
  {
    tag: "invoice-v1",
    schemaPath: "jsonschema/forward/invoice_v1.schema.json",
    instance: {
      invoiceId: "INV-2026-0001",
      issued: "2026-07-23T00:00:00Z",
      lineItems: [
        { description: "Widget", quantity: 2, unitPrice: 19.99 },
        { description: "Gadget", quantity: 1, unitPrice: 49.5 },
      ],
      total: 89.48,
    },
  },
  {
    tag: "event-v1",
    schemaPath: "jsonschema/full/event_v1.schema.json",
    instance: {
      eventId: "evt-0a1b2c3d",
      source: "https://events.example.amazon.com/producer",
      timestamp: 1785196800,
      verified: true,
    },
  },
  {
    tag: "single",
    schemaPath: "jsonschema/disabled/single.schema.json",
    instance: { key: "primary", active: true },
  },
];

/**
 * A single shared `Ajv` — `strict: false` lets fixture schemas with unknown
 * format keywords (`email`, `date-time`, `uri`) compile as annotations rather
 * than being rejected; `multipleOfPrecision: 6` avoids spurious rejections
 * on the invoice/product `multipleOf: 0.01` constraint under IEEE-754;
 * `logger: false` silences the "unknown format ignored" console warnings that
 * would otherwise appear on every compile of a schema declaring a format
 * keyword.
 */
const ajv = new Ajv({
  strict: false,
  multipleOfPrecision: 6,
  logger: false,
});

/**
 * Compile and cache validators per schema path — repeated encode/decode of
 * the same schema is cheap after the first call.
 */
const validatorCache = new Map<string, ValidateFunction>();

function validatorForSchemaFile(schemaPath: string): ValidateFunction {
  const cached = validatorCache.get(schemaPath);
  if (cached !== undefined) return cached;

  const schemaJson = readFileSync(schemaPath, "utf8");
  const schema = JSON.parse(schemaJson) as AnySchema;
  const compiled = ajv.compile(schema);
  validatorCache.set(schemaPath, compiled);
  return compiled;
}

/** Error thrown when `ajv` rejects an instance against the compiled schema. */
export class JsonSchemaValidationError extends Error {
  readonly errors: ReadonlyArray<unknown>;

  constructor(message: string, errors: ReadonlyArray<unknown>) {
    super(message);
    this.name = "JsonSchemaValidationError";
    this.errors = errors;
  }
}

/**
 * Encode a JSON instance against the given Draft-07 schema and return the
 * payload bytes that would sit after the 18-byte wire header.
 *
 * The bytes are the canonicalized JSON document — `JSON.parse` →
 * `JSON.stringify` (Node compact form) — encoded as UTF-8. `ajv` validates
 * the instance before serialization; a schema-invalid instance throws
 * `JsonSchemaValidationError` and no bytes are emitted. `ajv` is
 * deliberately not in the byte path — raw passthrough (`JSON.stringify(ajv
 * output)`) fails Java parity because ajv may reorder or coerce.
 *
 * The caller composes this payload with the wire header (and optional ZLIB
 * compression) via the `@gsr/core` codec — this function is header-agnostic
 * on purpose.
 */
export function encodeJsonSchema(
  schemaPath: string,
  instance: unknown,
): Buffer {
  const validate = validatorForSchemaFile(schemaPath);
  if (!validate(instance)) {
    throw new JsonSchemaValidationError(
      `instance failed JSON-Schema validation at ${schemaPath}`,
      (validate.errors ?? []) as ReadonlyArray<unknown>,
    );
  }
  const canonical = JSON.stringify(instance);
  return Buffer.from(canonical, "utf8");
}

/**
 * Decode a JSON-Schema payload (bytes AFTER the 18-byte wire header, already
 * decompressed if the transport was ZLIB) back into the canonical instance.
 *
 * The payload is the canonicalized JSON document; decode is `JSON.parse` on
 * its UTF-8 text. The schema is compiled (and validated against) so
 * schema-mismatched payloads surface as `JsonSchemaValidationError` — the
 * decoder is not silent about corrupt data.
 */
export function decodeJsonSchema(
  payload: Buffer,
  schemaPath: string,
): unknown {
  const decoded = JSON.parse(payload.toString("utf8")) as unknown;
  const validate = validatorForSchemaFile(schemaPath);
  if (!validate(decoded)) {
    throw new JsonSchemaValidationError(
      `decoded payload failed JSON-Schema validation at ${schemaPath}`,
      (validate.errors ?? []) as ReadonlyArray<unknown>,
    );
  }
  return decoded;
}
