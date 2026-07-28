/**
 * Config and error handling walkthrough.
 *
 * `parseConfig` turns a flat string map (the shape Java and Go GSR clients
 * accept) into a typed, validated `GsrConfig`. Every key is optional: keys
 * that are absent fall back to well-defined defaults documented in the
 * `@gsr/core` README.
 *
 * This example exercises three surfaces:
 *
 *   1. Defaults — an empty map returns a fully-populated `GsrConfig`.
 *   2. Custom values — non-defaults override the built-in ones.
 *   3. Error taxonomy — bad values throw specific exported error types.
 *
 * Run this file with a TypeScript runner such as `tsx` or `vite-node`:
 *
 *     npx tsx examples/config-and-errors.ts
 */

import { strict as assert } from "node:assert";

import {
  DEFAULT_CACHE_SIZE,
  DEFAULT_COMPATIBILITY,
  DEFAULT_REGION,
  DEFAULT_REGISTRY_NAME,
  GsrIncompatibleDataError,
  GsrInvalidCacheSizeError,
  GsrInvalidCompatibilityError,
  GsrInvalidCompressionTypeError,
  GsrMessageTypeNotFoundError,
  classifyGlueError,
  parseConfig,
  type GlueErrorKind,
  type GsrConfig,
} from "@gsr/core";
import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
} from "@gsr/serde";

// ---- 1. Defaults ----------------------------------------------------------
const defaults: GsrConfig = parseConfig({});
assert.equal(defaults.region, DEFAULT_REGION); // "us-east-2"
assert.equal(defaults.registryName, DEFAULT_REGISTRY_NAME); // "default-registry"
assert.equal(defaults.compatibility, DEFAULT_COMPATIBILITY); // "BACKWARD"
assert.equal(defaults.cacheSize, DEFAULT_CACHE_SIZE); // 200
console.log("Empty map -> defaults:", {
  region: defaults.region,
  registryName: defaults.registryName,
  compatibility: defaults.compatibility,
  cacheSize: defaults.cacheSize,
});

// ---- 2. Custom values -----------------------------------------------------
const custom: GsrConfig = parseConfig({
  region: "eu-west-1",
  "registry.name": "my-registry",
  compatibility: "FULL_ALL",
  compression: "ZLIB",
  cacheSize: "500",
  schemaAutoRegistrationEnabled: "true",
  userAgentApp: "my-service",
  "tags.env": "prod",
  "tags.team": "platform",
});
assert.equal(custom.region, "eu-west-1");
assert.equal(custom.registryName, "my-registry");
assert.equal(custom.compatibility, "FULL_ALL");
assert.equal(custom.compressionType, "ZLIB");
assert.equal(custom.cacheSize, 500);
assert.equal(custom.schemaAutoRegistrationEnabled, true);
assert.deepStrictEqual(custom.tags, { env: "prod", team: "platform" });
console.log("Custom values applied:", {
  region: custom.region,
  compression: custom.compressionType,
  tags: custom.tags,
});

// ---- 3. Config validation errors -----------------------------------------
// Out-of-set compression values throw `GsrInvalidCompressionTypeError`.
try {
  parseConfig({ compression: "SNAPPY" });
  throw new Error("expected GsrInvalidCompressionTypeError");
} catch (err) {
  assert.ok(err instanceof GsrInvalidCompressionTypeError);
  console.log("Bad compression -> GsrInvalidCompressionTypeError:", err.message);
}

// Out-of-set compatibility values throw `GsrInvalidCompatibilityError`.
try {
  parseConfig({ compatibility: "backward" }); // lower-case is not in the 8-mode set
  throw new Error("expected GsrInvalidCompatibilityError");
} catch (err) {
  assert.ok(err instanceof GsrInvalidCompatibilityError);
  console.log("Bad compatibility -> GsrInvalidCompatibilityError:", err.message);
}

// Non-integer cache size throws `GsrInvalidCacheSizeError`.
try {
  parseConfig({ cacheSize: "not-a-number" });
  throw new Error("expected GsrInvalidCacheSizeError");
} catch (err) {
  assert.ok(err instanceof GsrInvalidCacheSizeError);
  console.log("Bad cacheSize -> GsrInvalidCacheSizeError:", err.message);
}

// ---- 4. Wire-format decode errors ----------------------------------------
// Bad wire bytes surface as `GsrIncompatibleDataError` — one uniform error
// type covers every wire-layer failure (bad header, unknown compression,
// truncated payload, schema mismatch).
const truncatedWire = Buffer.from([0x03, 0x00, 0x01, 0x02]);
try {
  new GsrDeserializer().deserialize({
    format: DataFormat.AVRO,
    data: truncatedWire,
    schema: { type: "string" },
  });
  throw new Error("expected GsrIncompatibleDataError");
} catch (err) {
  assert.ok(err instanceof GsrIncompatibleDataError);
  console.log(
    "Truncated wire -> GsrIncompatibleDataError:",
    err.message.slice(0, 80),
  );
}

// A protobuf request that omits `messageFullName` surfaces as
// `GsrMessageTypeNotFoundError` — a distinct type so callers can distinguish
// a missing message-name configuration from a corrupt payload.
try {
  new GsrSerializer().serialize({
    format: DataFormat.PROTOBUF,
    schemaVersionId: "01020304-0506-0708-090a-0b0c0d0e0f10",
    topic: "orders",
    schema: 'syntax = "proto3"; message Empty {}',
    data: {},
    // messageFullName intentionally omitted.
  });
  throw new Error("expected GsrMessageTypeNotFoundError");
} catch (err) {
  assert.ok(err instanceof GsrMessageTypeNotFoundError);
  console.log(
    "Missing messageFullName -> GsrMessageTypeNotFoundError:",
    err.message,
  );
}

// ---- 5. Classifying Glue SDK errors --------------------------------------
// `classifyGlueError` maps an SDK v3 service error to a small discriminant
// the registrar uses on the fall-through path. Non-Error inputs fall
// through to `"other"`.
const kind: GlueErrorKind = classifyGlueError({
  name: "EntityNotFoundException",
});
assert.equal(kind, "entity-not-found");
console.log(`classifyGlueError -> ${kind}`);
