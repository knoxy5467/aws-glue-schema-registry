/**
 * Config surface for the `@gsr/core` Glue-integration layer.
 *
 * Mirrors the Go reference `core/config.go` `LoadConfigFromMap`. Callers pass
 * a Java-style flat `Record<string, string>` (`map[string]string` in Go) and
 * receive a typed, validated `GsrConfig`. Parsing is pure — no SDK call, no
 * network I/O — so the surface is fully unit-testable without live AWS.
 *
 * Case-exactness on enum keys (`compatibility`, `avroRecordType`,
 * `protobufMessageType`) mirrors Java's `Enum.valueOf` semantics, which the
 * Go reference reproduces via case-exact `map` lookups.
 *
 * Deliberate divergences from Go parity — the applied `us-east-2` default
 * for `region`, the `-js` AssumeRole session suffix, the accept-and-store
 * (not validate) posture on `avroReaderSchema`, and the accept-and-ignore
 * posture on `proxyUrl` — are noted inline at each site.
 */
import {
  GsrInvalidAvroRecordTypeError,
  GsrInvalidCacheSizeError,
  GsrInvalidCacheTtlError,
  GsrInvalidCompatibilityError,
  GsrInvalidCompressionTypeError,
  GsrInvalidProtobufMessageTypeError,
} from "../errors/gsr-errors.js";

/** Default AWS region applied when the `region` config key is absent. */
export const DEFAULT_REGION = "us-east-2";

/** Default Glue registry name (matches Java / Go `default-registry`). */
export const DEFAULT_REGISTRY_NAME = "default-registry";

/** Default schema compatibility mode. */
export const DEFAULT_COMPATIBILITY = "BACKWARD";

/** Default wire-payload compression type. */
export const DEFAULT_COMPRESSION_TYPE = "NONE";

/** Default cache entry TTL in milliseconds (24 hours). */
export const DEFAULT_CACHE_TTL_MILLIS = 86_400_000;

/** Default cache size (entry count). */
export const DEFAULT_CACHE_SIZE = 200;

/** Default effective user-agent app token when `userAgentApp` is empty. */
export const DEFAULT_USER_AGENT_APP = "default";

/**
 * Default AssumeRole session name (applied when `assumeRoleArn` is set and
 * no explicit `assumeRoleSessionName` is provided). Diverges from Go's `-go`
 * suffix by design — the `-js` suffix mirrors the language-token pattern.
 */
export const DEFAULT_ASSUME_ROLE_SESSION = "aws-glue-schema-registry-js";

/** Default SDK v3 retry `maxAttempts` (matches Go's standard-retryer budget). */
export const DEFAULT_MAX_ATTEMPTS = 3;

/** Default SDK v3 retry mode. */
export const DEFAULT_RETRY_MODE: "standard" | "adaptive" = "standard";

/** Config-key constants — string-identical to Java `AWSSchemaRegistryConstants`. */
const KEY_REGION = "region";
const KEY_ENDPOINT = "endpoint";
const KEY_PROXY_URL = "proxyUrl";
const KEY_REGISTRY_NAME = "registry.name";
const KEY_COMPATIBILITY = "compatibility";
const KEY_DESCRIPTION = "description";
const KEY_AUTO_REGISTRATION = "schemaAutoRegistrationEnabled";
const KEY_COMPRESSION = "compression";
const KEY_COMPRESSION_TYPE_LEGACY = "compressionType";
const KEY_CACHE_TTL_MILLIS = "timeToLiveMillis";
const KEY_CACHE_SIZE = "cacheSize";
const KEY_AVRO_RECORD_TYPE = "avroRecordType";
const KEY_PROTOBUF_MESSAGE_TYPE = "protobufMessageType";
const KEY_AVRO_READER_SCHEMA = "avroReaderSchema";
const KEY_SCHEMA_NAME_GENERATION_CLASS = "schemaNameGenerationClass";
const KEY_USER_AGENT_APP = "userAgentApp";
const KEY_ASSUME_ROLE_ARN = "assumeRoleArn";
const KEY_ASSUME_ROLE_SESSION_NAME = "assumeRoleSessionName";
const KEY_SECONDARY_DESERIALIZER = "secondaryDeserializer";
const KEY_RETRY_MODE = "retryMode";
const KEY_MAX_ATTEMPTS = "maxAttempts";
const PREFIX_TAGS = "tags.";
const PREFIX_METADATA = "metadata.";

/** Case-exact set matching Java's `Compatibility.knownValues()`. */
const VALID_COMPATIBILITIES: ReadonlySet<string> = new Set([
  "NONE",
  "DISABLED",
  "BACKWARD",
  "BACKWARD_ALL",
  "FORWARD",
  "FORWARD_ALL",
  "FULL",
  "FULL_ALL",
]);

/** Case-exact set matching Java's `AvroRecordType` enum. */
const VALID_AVRO_RECORD_TYPES: ReadonlySet<string> = new Set([
  "GENERIC_RECORD",
  "SPECIFIC_RECORD",
]);

/** Case-exact set matching Java's `ProtobufMessageType` enum. */
const VALID_PROTOBUF_MESSAGE_TYPES: ReadonlySet<string> = new Set([
  "POJO",
  "DYNAMIC_MESSAGE",
]);

/**
 * Case-insensitive set matching Java's `COMPRESSION` enum. Java's
 * `Compression.valueOf` is invoked after upper-casing, so the validation
 * check upper-cases before matching; the caller's original casing is
 * preserved in the returned `compressionType` field for parity with Go.
 */
const VALID_COMPRESSION_TYPES_UPPER: ReadonlySet<string> = new Set(["NONE", "ZLIB"]);

/**
 * Parsed configuration surface consumed by the Glue-integration layer.
 *
 * Every field is populated by `parseConfig`; optional fields are absent from
 * the caller's `configMap` and left `undefined` here. `parseConfig` performs
 * no SDK call and no network I/O.
 */
export interface GsrConfig {
  /** AWS region. Defaults to {@link DEFAULT_REGION} when the key is absent. */
  region: string;
  /** Custom Glue endpoint override; applied regardless of `region` when set. */
  endpoint?: string;
  /** Glue registry name. Defaults to {@link DEFAULT_REGISTRY_NAME}. */
  registryName: string;
  /**
   * Schema compatibility mode. One of the 8 modes; validated case-exact.
   * Carried into `CreateSchema.Compatibility` — the client never computes
   * compatibility (Glue-service enforced).
   */
  compatibility: string;
  /**
   * Description. When the caller does not set `description`, it is
   * synthesized as `DEFAULT-DESCRIPTION-<region>-<registryName>` after
   * defaults resolve (matches Go `config.go:293-296`).
   */
  description: string;
  /** Auto-register unknown schemas on first serialize. Lenient bool: only "true" (case-insensitive) is true. */
  schemaAutoRegistrationEnabled: boolean;
  /** Compression algorithm for the wire payload. Defaults to `NONE`. */
  compressionType: string;
  /** Cache entry TTL in milliseconds. Defaults to 24h. */
  timeToLiveMillis: number;
  /** Cache size (entry count). Defaults to 200. */
  cacheSize: number;
  /** Avro record shape hint for the serde layer; case-exact when set. */
  avroRecordType?: string;
  /** Protobuf message-type hint for the serde layer; case-exact when set. */
  protobufMessageType?: string;
  /**
   * Reader-schema JSON. Accepted and stored unvalidated in `core` — the
   * Avro parser lives on the serde layer, which `core` cannot import.
   * Validation happens at projection time in the schema-evolution module.
   */
  avroReaderSchema?: string;
  /** Schema-name generation strategy hint (consumed on the serde side). */
  schemaNameGenerationClass?: string;
  /** Raw `userAgentApp` value (empty when absent). */
  userAgentApp: string;
  /**
   * Resolved user-agent app token: `userAgentApp || DEFAULT_USER_AGENT_APP`.
   * Used by the always-on user-agent middleware; the raw value is
   * preserved so callers can distinguish default from explicit input.
   */
  effectiveUserAgentApp: string;
  /** AssumeRole ARN. When set, the client uses `fromTemporaryCredentials`. */
  assumeRoleArn?: string;
  /**
   * Resolved AssumeRole session name: caller value if provided, else
   * {@link DEFAULT_ASSUME_ROLE_SESSION} when `assumeRoleArn` is set,
   * else `""` (parity with Go's dormant-path behavior).
   */
  assumeRoleSessionName: string;
  /** SDK v3 retry mode (TS-additive; Go relies on the SDK default retryer). */
  retryMode: "standard" | "adaptive";
  /** SDK v3 retry `maxAttempts` (TS-additive; default 3 matches Go). */
  maxAttempts: number;
  /** Tags extracted from `tags.<k>=<v>` config-map entries. */
  tags: Record<string, string>;
  /** Metadata extracted from `metadata.<k>=<v>` config-map entries. */
  metadata: Record<string, string>;
}

/**
 * Parse and validate a caller-provided flat config map into a typed
 * `GsrConfig`. Pure — performs no SDK call and no network I/O.
 *
 * Throws:
 * - `GsrInvalidCompatibilityError` on an out-of-set `compatibility` value.
 * - `GsrInvalidCompressionTypeError` on an explicit out-of-set `compression`
 *   value. An absent `compression` key falls through to the default without
 *   error (Go's explicit-only validation, `config.go:227-231`).
 * - `GsrInvalidCacheTtlError` on a non-integer `timeToLiveMillis`.
 * - `GsrInvalidCacheSizeError` on a non-integer `cacheSize`.
 * - `GsrInvalidAvroRecordTypeError` on an out-of-set `avroRecordType`.
 * - `GsrInvalidProtobufMessageTypeError` on an out-of-set `protobufMessageType`.
 *
 * Accept-and-ignore keys: `secondaryDeserializer` and `proxyUrl` are
 * accepted but produce no observable effect in `GsrConfig` (the Go client
 * previously removed `SecondaryDeserializer`; `proxyUrl` proxy wiring is
 * deferred to the real-AWS transport layer).
 *
 * @example
 * Parse a caller-supplied config map. Every key is optional — absent keys
 * fall back to well-defined defaults. Excerpted from
 * `examples/config-and-errors.ts` (type-checked by
 * `npm run docs:examples:typecheck`).
 * ```ts
 * import { strict as assert } from "node:assert";
 *
 * import {
 *   DEFAULT_CACHE_SIZE,
 *   DEFAULT_COMPATIBILITY,
 *   DEFAULT_REGION,
 *   DEFAULT_REGISTRY_NAME,
 *   parseConfig,
 *   type GsrConfig,
 * } from "@gsr/core";
 *
 * // ---- 1. Defaults ----------------------------------------------------------
 * const defaults: GsrConfig = parseConfig({});
 * assert.equal(defaults.region, DEFAULT_REGION); // "us-east-2"
 * assert.equal(defaults.registryName, DEFAULT_REGISTRY_NAME); // "default-registry"
 * assert.equal(defaults.compatibility, DEFAULT_COMPATIBILITY); // "BACKWARD"
 * assert.equal(defaults.cacheSize, DEFAULT_CACHE_SIZE); // 200
 * ```
 */
export function parseConfig(configMap: Record<string, string>): GsrConfig {
  // Region — default applied when the key is absent (parity divergence from
  // Go's default-chain reliance; the TS client applies an explicit default).
  const region = configMap[KEY_REGION] !== undefined && configMap[KEY_REGION] !== ""
    ? configMap[KEY_REGION]
    : DEFAULT_REGION;

  const endpoint = configMap[KEY_ENDPOINT] !== undefined && configMap[KEY_ENDPOINT] !== ""
    ? configMap[KEY_ENDPOINT]
    : undefined;

  const registryName = configMap[KEY_REGISTRY_NAME] !== undefined && configMap[KEY_REGISTRY_NAME] !== ""
    ? configMap[KEY_REGISTRY_NAME]
    : DEFAULT_REGISTRY_NAME;

  let compatibility = DEFAULT_COMPATIBILITY;
  const compatibilityRaw = configMap[KEY_COMPATIBILITY];
  if (compatibilityRaw !== undefined) {
    if (!VALID_COMPATIBILITIES.has(compatibilityRaw)) {
      throw new GsrInvalidCompatibilityError(
        `invalid compatibility: ${JSON.stringify(compatibilityRaw)} (want one of ${[...VALID_COMPATIBILITIES].join(", ")})`,
      );
    }
    compatibility = compatibilityRaw;
  }

  // Compression — accept both the Java key "compression" and the legacy
  // "compressionType". Java wins when both are set. Validate only when an
  // explicit non-empty value was supplied; absent keys fall through to the
  // default without error (Go `config.go:225-231`).
  let compressionType = DEFAULT_COMPRESSION_TYPE;
  let compressionExplicit = false;
  const compressionLegacy = configMap[KEY_COMPRESSION_TYPE_LEGACY];
  if (compressionLegacy !== undefined && compressionLegacy !== "") {
    compressionType = compressionLegacy;
    compressionExplicit = true;
  }
  const compressionJava = configMap[KEY_COMPRESSION];
  if (compressionJava !== undefined && compressionJava !== "") {
    compressionType = compressionJava;
    compressionExplicit = true;
  }
  if (compressionExplicit && !VALID_COMPRESSION_TYPES_UPPER.has(compressionType.toUpperCase())) {
    throw new GsrInvalidCompressionTypeError(
      `invalid compression: ${JSON.stringify(compressionType)} (want one of NONE, ZLIB)`,
    );
  }

  const schemaAutoRegistrationEnabled = parseLenientBool(configMap[KEY_AUTO_REGISTRATION]);

  const timeToLiveMillis = parseIntegerField(
    configMap[KEY_CACHE_TTL_MILLIS],
    DEFAULT_CACHE_TTL_MILLIS,
    (raw) => new GsrInvalidCacheTtlError(`invalid timeToLiveMillis: ${JSON.stringify(raw)}`),
  );

  const cacheSize = parseIntegerField(
    configMap[KEY_CACHE_SIZE],
    DEFAULT_CACHE_SIZE,
    (raw) => new GsrInvalidCacheSizeError(`invalid cacheSize: ${JSON.stringify(raw)}`),
  );

  const avroRecordTypeRaw = configMap[KEY_AVRO_RECORD_TYPE];
  let avroRecordType: string | undefined;
  if (avroRecordTypeRaw !== undefined && avroRecordTypeRaw !== "") {
    if (!VALID_AVRO_RECORD_TYPES.has(avroRecordTypeRaw)) {
      throw new GsrInvalidAvroRecordTypeError(
        `invalid avroRecordType: ${JSON.stringify(avroRecordTypeRaw)} (want one of GENERIC_RECORD, SPECIFIC_RECORD)`,
      );
    }
    avroRecordType = avroRecordTypeRaw;
  }

  const protobufMessageTypeRaw = configMap[KEY_PROTOBUF_MESSAGE_TYPE];
  let protobufMessageType: string | undefined;
  if (protobufMessageTypeRaw !== undefined && protobufMessageTypeRaw !== "") {
    if (!VALID_PROTOBUF_MESSAGE_TYPES.has(protobufMessageTypeRaw)) {
      throw new GsrInvalidProtobufMessageTypeError(
        `invalid protobufMessageType: ${JSON.stringify(protobufMessageTypeRaw)} (want one of POJO, DYNAMIC_MESSAGE)`,
      );
    }
    protobufMessageType = protobufMessageTypeRaw;
  }

  // avroReaderSchema is accepted and stored UNVALIDATED. The Avro parser
  // lives in the serde layer (avsc) which core cannot import; projection-time
  // validation covers a malformed value.
  const avroReaderSchema = configMap[KEY_AVRO_READER_SCHEMA] !== undefined && configMap[KEY_AVRO_READER_SCHEMA] !== ""
    ? configMap[KEY_AVRO_READER_SCHEMA]
    : undefined;

  const schemaNameGenerationClass = configMap[KEY_SCHEMA_NAME_GENERATION_CLASS] !== undefined
    && configMap[KEY_SCHEMA_NAME_GENERATION_CLASS] !== ""
    ? configMap[KEY_SCHEMA_NAME_GENERATION_CLASS]
    : undefined;

  const userAgentApp = configMap[KEY_USER_AGENT_APP] ?? "";
  const effectiveUserAgentApp = userAgentApp !== "" ? userAgentApp : DEFAULT_USER_AGENT_APP;

  const assumeRoleArnRaw = configMap[KEY_ASSUME_ROLE_ARN];
  const assumeRoleArn = assumeRoleArnRaw !== undefined && assumeRoleArnRaw !== ""
    ? assumeRoleArnRaw
    : undefined;

  const rawAssumeRoleSession = configMap[KEY_ASSUME_ROLE_SESSION_NAME] ?? "";
  let assumeRoleSessionName: string;
  if (rawAssumeRoleSession !== "") {
    assumeRoleSessionName = rawAssumeRoleSession;
  } else if (assumeRoleArn !== undefined) {
    assumeRoleSessionName = DEFAULT_ASSUME_ROLE_SESSION;
  } else {
    assumeRoleSessionName = "";
  }

  const retryMode = parseRetryMode(configMap[KEY_RETRY_MODE]);
  const maxAttempts = parseLenientInteger(configMap[KEY_MAX_ATTEMPTS], DEFAULT_MAX_ATTEMPTS);

  const tags = collectPrefixedMap(configMap, PREFIX_TAGS);
  const metadata = collectPrefixedMap(configMap, PREFIX_METADATA);

  // Description synthesized AFTER defaults resolve — matches Go
  // `config.go:293-296` so the registry-name segment reflects
  // the post-default fallback.
  const descriptionRaw = configMap[KEY_DESCRIPTION];
  const description = descriptionRaw !== undefined && descriptionRaw !== ""
    ? descriptionRaw
    : `DEFAULT-DESCRIPTION-${region}-${registryName}`;

  // secondaryDeserializer and proxyUrl are accept-and-ignore — read once so
  // an explicit-empty is not treated as invalid; no field is produced.
  void configMap[KEY_SECONDARY_DESERIALIZER];
  void configMap[KEY_PROXY_URL];

  const out: GsrConfig = {
    region,
    registryName,
    compatibility,
    description,
    schemaAutoRegistrationEnabled,
    compressionType,
    timeToLiveMillis,
    cacheSize,
    userAgentApp,
    effectiveUserAgentApp,
    assumeRoleSessionName,
    retryMode,
    maxAttempts,
    tags,
    metadata,
  };
  if (endpoint !== undefined) out.endpoint = endpoint;
  if (avroRecordType !== undefined) out.avroRecordType = avroRecordType;
  if (protobufMessageType !== undefined) out.protobufMessageType = protobufMessageType;
  if (avroReaderSchema !== undefined) out.avroReaderSchema = avroReaderSchema;
  if (schemaNameGenerationClass !== undefined) out.schemaNameGenerationClass = schemaNameGenerationClass;
  if (assumeRoleArn !== undefined) out.assumeRoleArn = assumeRoleArn;
  return out;
}

/** Java `Boolean.parseBoolean` parity: only `"true"` (case-insensitive) is true. */
function parseLenientBool(raw: string | undefined): boolean {
  if (raw === undefined || raw === "") return false;
  return raw.toLowerCase() === "true";
}

/**
 * Parse an integer-valued numeric field. Absent or empty falls back to the
 * default. Any non-integer form (decimal, alphanumeric, whitespace) throws
 * the caller-supplied typed error.
 */
function parseIntegerField(
  raw: string | undefined,
  fallback: number,
  makeError: (raw: string) => Error,
): number {
  if (raw === undefined || raw === "") return fallback;
  // Strict integer match: optional sign, digits only. `Number.parseInt` would
  // accept "10abc" (returns 10); an explicit regex plus safe-integer check
  // rejects the tolerant path Go's `strconv.Atoi` also rejects.
  if (!/^-?\d+$/.test(raw)) {
    throw makeError(raw);
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed)) {
    throw makeError(raw);
  }
  return parsed;
}

/** Parse the TS-additive `retryMode` key. Defaults to `standard`. */
function parseRetryMode(raw: string | undefined): "standard" | "adaptive" {
  if (raw === undefined || raw === "") return DEFAULT_RETRY_MODE;
  if (raw === "standard" || raw === "adaptive") return raw;
  // No typed error class specified in the spec for retryMode — treat any
  // non-empty out-of-set value as a fall-back to the default. Documented
  // parity divergence: Go has no retryMode key at all.
  return DEFAULT_RETRY_MODE;
}

/**
 * Parse the TS-additive `maxAttempts` key. Non-integer input falls back to
 * the default rather than throwing — no typed error class exists for the
 * TS-additive retry surface; SDK v3 native validation catches a downstream
 * client-config-time mistake when the value is later consumed.
 */
function parseLenientInteger(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw === "") return fallback;
  if (!/^-?\d+$/.test(raw)) return fallback;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) ? parsed : fallback;
}

/**
 * Extract every entry whose key starts with `prefix` into a map keyed by the
 * stripped suffix. Used for `tags.<k>=<v>` and `metadata.<k>=<v>` expansion.
 */
function collectPrefixedMap(
  configMap: Record<string, string>,
  prefix: string,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(configMap)) {
    if (k.startsWith(prefix)) {
      out[k.slice(prefix.length)] = v;
    }
  }
  return out;
}
