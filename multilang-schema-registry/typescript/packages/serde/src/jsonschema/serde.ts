/**
 * Full-breadth JSON-Schema serde (Draft-07).
 *
 * Accepts arbitrary Draft-07 schemas and instances — schemas may be passed as
 * a parsed JSON object or a JSON string. Unlike the per-format encode-slice
 * this module never touches the filesystem: callers supply the schema
 * bytes/object directly.
 *
 * The wire payload IS the canonicalized JSON document — `JSON.parse` →
 * `JSON.stringify` (Node compact form, insertion-order keys, shortest
 * round-trippable number form) — encoded as UTF-8. Node's built-in
 * `JSON.stringify` matches the Java reference's Jackson output byte-for-byte
 * for the golden fixtures. `ajv` validates the instance against the schema
 * but its output is deliberately NOT in the byte path: raw passthrough
 * (`JSON.stringify(ajv output)`) fails Java parity because ajv may coerce or
 * reorder.
 *
 * `ajv` configuration mirrors the encode-slice setup: `strict: false` lets
 * fixture schemas with unknown format keywords (`email`, `date-time`, `uri`)
 * compile as annotations rather than being rejected; `multipleOfPrecision: 6`
 * avoids spurious rejections under IEEE-754 on constraints like `multipleOf:
 * 0.01`; `logger: false` silences "unknown format ignored" console warnings.
 *
 * Compiled validators are cached per canonicalized schema key so repeated
 * encode/decode of the same schema is cheap after the first call.
 *
 * The wire header, compression, and codec composition live in `@gsr/core`;
 * this module deliberately produces only the format-specific payload bytes
 * so a higher-layer facade can compose them with the core codec.
 */

import { Ajv, type AnySchema, type ValidateFunction } from "ajv";

import { GsrIncompatibleDataError } from "@gsr/core";

export { JsonSchemaValidationError } from "./encode-slice.js";
import { JsonSchemaValidationError } from "./encode-slice.js";

/**
 * A single shared `Ajv` for the full-breadth serde. Config parity with the
 * per-format encode-slice — `strict: false`, `multipleOfPrecision: 6`,
 * `logger: false` — keeps schema-acceptance behavior identical between the
 * two modules.
 */
const ajv = new Ajv({
  strict: false,
  multipleOfPrecision: 6,
  logger: false,
});

/**
 * Compiled-validator cache keyed by the canonicalized schema JSON. Passing
 * the same schema twice (whether as object or string) resolves to a single
 * compiled validator.
 */
const validatorCache = new Map<string, ValidateFunction>();

/**
 * Coerce the caller-supplied schema (object or JSON string) into a parsed
 * schema object and a canonical string cache key.
 */
function parseSchema(schema: object | string): {
  parsed: AnySchema;
  cacheKey: string;
} {
  if (typeof schema === "string") {
    let parsed: unknown;
    try {
      parsed = JSON.parse(schema);
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      throw new GsrIncompatibleDataError(
        `failed to parse JSON-Schema string: ${reason}`,
      );
    }
    // Canonicalize the string so structurally-equal schemas share a cache
    // entry regardless of source whitespace.
    return {
      parsed: parsed as AnySchema,
      cacheKey: JSON.stringify(parsed),
    };
  }
  return {
    parsed: schema as AnySchema,
    cacheKey: JSON.stringify(schema),
  };
}

function validatorFor(schema: object | string): ValidateFunction {
  const { parsed, cacheKey } = parseSchema(schema);
  const cached = validatorCache.get(cacheKey);
  if (cached !== undefined) return cached;
  const compiled = ajv.compile(parsed);
  validatorCache.set(cacheKey, compiled);
  return compiled;
}

/**
 * Encode a JSON instance against the given Draft-07 schema and return the
 * payload bytes that would sit after the 18-byte wire header.
 *
 * The bytes are the canonicalized JSON document — `JSON.parse` →
 * `JSON.stringify` (Node compact form) — encoded as UTF-8. `ajv` validates
 * the instance before serialization; a schema-invalid instance throws
 * `JsonSchemaValidationError` and no bytes are emitted.
 *
 * Canonicalization is REQUIRED — raw passthrough of an already-stringified
 * instance would fail Java parity (whitespace, key order, number
 * formatting). Callers that pass a JSON string still get the canonical
 * form: we `JSON.parse` then `JSON.stringify` (via the validated instance).
 */
export function serializeJson(
  schema: object | string,
  instance: unknown,
): Buffer {
  const validate = validatorFor(schema);
  if (!validate(instance)) {
    throw new JsonSchemaValidationError(
      "instance failed JSON-Schema validation",
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
 * its UTF-8 text. Unparseable bytes surface as `GsrIncompatibleDataError`
 * (never a raw `SyntaxError` — the wire-format error taxonomy stays
 * uniform); schema-mismatched but well-formed JSON surfaces as
 * `JsonSchemaValidationError`.
 */
export function deserializeJson(
  payload: Buffer,
  schema: object | string,
): unknown {
  let decoded: unknown;
  try {
    decoded = JSON.parse(payload.toString("utf8"));
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    const wrapped = new GsrIncompatibleDataError(
      `failed to parse JSON-Schema payload: ${reason}`,
    );
    wrapped.cause = cause;
    throw wrapped;
  }

  const validate = validatorFor(schema);
  if (!validate(decoded)) {
    throw new JsonSchemaValidationError(
      "decoded payload failed JSON-Schema validation",
      (validate.errors ?? []) as ReadonlyArray<unknown>,
    );
  }
  return decoded;
}
