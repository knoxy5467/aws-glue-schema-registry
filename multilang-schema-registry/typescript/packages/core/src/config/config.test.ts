import { describe, expect, it } from "vitest";

import {
  GsrInvalidAvroRecordTypeError,
  GsrInvalidCacheSizeError,
  GsrInvalidCacheTtlError,
  GsrInvalidCompatibilityError,
  GsrInvalidCompressionTypeError,
  GsrInvalidProtobufMessageTypeError,
} from "../errors/gsr-errors.js";

import {
  DEFAULT_ASSUME_ROLE_SESSION,
  DEFAULT_CACHE_SIZE,
  DEFAULT_CACHE_TTL_MILLIS,
  DEFAULT_COMPATIBILITY,
  DEFAULT_COMPRESSION_TYPE,
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_REGION,
  DEFAULT_REGISTRY_NAME,
  DEFAULT_RETRY_MODE,
  DEFAULT_USER_AGENT_APP,
  parseConfig,
} from "./config.js";

describe("parseConfig — defaults on an empty config map", () => {
  it("applies every Java/Go-parity default and synthesizes the description", () => {
    const cfg = parseConfig({});

    expect(cfg.region).toBe(DEFAULT_REGION);
    expect(cfg.registryName).toBe(DEFAULT_REGISTRY_NAME);
    expect(cfg.compatibility).toBe(DEFAULT_COMPATIBILITY);
    expect(cfg.compressionType).toBe(DEFAULT_COMPRESSION_TYPE);
    expect(cfg.timeToLiveMillis).toBe(DEFAULT_CACHE_TTL_MILLIS);
    expect(cfg.cacheSize).toBe(DEFAULT_CACHE_SIZE);
    expect(cfg.effectiveUserAgentApp).toBe(DEFAULT_USER_AGENT_APP);
    expect(cfg.retryMode).toBe(DEFAULT_RETRY_MODE);
    expect(cfg.maxAttempts).toBe(DEFAULT_MAX_ATTEMPTS);
    expect(cfg.schemaAutoRegistrationEnabled).toBe(false);

    // Synthesized description reflects post-default region + registryName.
    expect(cfg.description).toBe(
      `DEFAULT-DESCRIPTION-${DEFAULT_REGION}-${DEFAULT_REGISTRY_NAME}`,
    );

    // Optional keys are absent on the default surface.
    expect(cfg.endpoint).toBeUndefined();
    expect(cfg.avroRecordType).toBeUndefined();
    expect(cfg.protobufMessageType).toBeUndefined();
    expect(cfg.avroReaderSchema).toBeUndefined();
    expect(cfg.schemaNameGenerationClass).toBeUndefined();
    expect(cfg.assumeRoleArn).toBeUndefined();

    // Raw userAgentApp is preserved as "" so callers can distinguish default
    // from explicit; the resolved value on effectiveUserAgentApp is "default".
    expect(cfg.userAgentApp).toBe("");
    // Session name is empty when no ARN is configured (Go dormant-path parity).
    expect(cfg.assumeRoleSessionName).toBe("");

    // tags / metadata prefix-maps are always present, empty on default input.
    expect(cfg.tags).toEqual({});
    expect(cfg.metadata).toEqual({});
  });
});

describe("parseConfig — compatibility validation (case-exact, 8 modes)", () => {
  const validModes = [
    "NONE",
    "DISABLED",
    "BACKWARD",
    "BACKWARD_ALL",
    "FORWARD",
    "FORWARD_ALL",
    "FULL",
    "FULL_ALL",
  ] as const;

  it.each(validModes)("accepts %s case-exact", (mode) => {
    expect(parseConfig({ compatibility: mode }).compatibility).toBe(mode);
  });

  it.each(["backward", "FORWARDS", "backward_all", ""])(
    "rejects %j",
    (bad) => {
      expect(() => parseConfig({ compatibility: bad })).toThrow(
        GsrInvalidCompatibilityError,
      );
    },
  );
});

describe("parseConfig — compression validation (explicit-only)", () => {
  it("accepts NONE and ZLIB", () => {
    expect(parseConfig({ compression: "NONE" }).compressionType).toBe("NONE");
    expect(parseConfig({ compression: "ZLIB" }).compressionType).toBe("ZLIB");
  });

  it("throws GsrInvalidCompressionTypeError on explicit out-of-set value", () => {
    expect(() => parseConfig({ compression: "GZIP" })).toThrow(
      GsrInvalidCompressionTypeError,
    );
  });

  it("falls through to NONE default when the compression key is absent (no throw)", () => {
    const cfg = parseConfig({});
    expect(cfg.compressionType).toBe("NONE");
  });

  it("also accepts the legacy compressionType key, with the Java key winning on collision", () => {
    const legacyOnly = parseConfig({ compressionType: "ZLIB" });
    expect(legacyOnly.compressionType).toBe("ZLIB");

    const bothSet = parseConfig({ compressionType: "NONE", compression: "ZLIB" });
    expect(bothSet.compressionType).toBe("ZLIB");
  });
});

describe("parseConfig — integer validation on cache keys", () => {
  it("parses valid integer strings", () => {
    const cfg = parseConfig({ timeToLiveMillis: "60000", cacheSize: "500" });
    expect(cfg.timeToLiveMillis).toBe(60_000);
    expect(cfg.cacheSize).toBe(500);
  });

  it.each(["notanumber", "1.5", "1e6", "10abc", " 10 ", ""])(
    "throws GsrInvalidCacheTtlError on non-integer timeToLiveMillis %j (except empty which falls back)",
    (raw) => {
      if (raw === "") {
        expect(parseConfig({ timeToLiveMillis: raw }).timeToLiveMillis).toBe(
          DEFAULT_CACHE_TTL_MILLIS,
        );
      } else {
        expect(() => parseConfig({ timeToLiveMillis: raw })).toThrow(
          GsrInvalidCacheTtlError,
        );
      }
    },
  );

  it("throws GsrInvalidCacheSizeError on non-integer cacheSize", () => {
    expect(() => parseConfig({ cacheSize: "abc" })).toThrow(
      GsrInvalidCacheSizeError,
    );
    expect(() => parseConfig({ cacheSize: "1.5" })).toThrow(
      GsrInvalidCacheSizeError,
    );
  });
});

describe("parseConfig — avroRecordType / protobufMessageType (case-exact)", () => {
  it("accepts the case-exact avroRecordType values", () => {
    expect(parseConfig({ avroRecordType: "GENERIC_RECORD" }).avroRecordType).toBe(
      "GENERIC_RECORD",
    );
    expect(parseConfig({ avroRecordType: "SPECIFIC_RECORD" }).avroRecordType).toBe(
      "SPECIFIC_RECORD",
    );
  });

  it("throws GsrInvalidAvroRecordTypeError on lowercase or typo variants", () => {
    expect(() => parseConfig({ avroRecordType: "generic_record" })).toThrow(
      GsrInvalidAvroRecordTypeError,
    );
    expect(() => parseConfig({ avroRecordType: "GenericRecord" })).toThrow(
      GsrInvalidAvroRecordTypeError,
    );
  });

  it("accepts the case-exact protobufMessageType values", () => {
    expect(parseConfig({ protobufMessageType: "POJO" }).protobufMessageType).toBe(
      "POJO",
    );
    expect(
      parseConfig({ protobufMessageType: "DYNAMIC_MESSAGE" }).protobufMessageType,
    ).toBe("DYNAMIC_MESSAGE");
  });

  it("throws GsrInvalidProtobufMessageTypeError on lowercase or typo variants", () => {
    expect(() => parseConfig({ protobufMessageType: "pojo" })).toThrow(
      GsrInvalidProtobufMessageTypeError,
    );
    expect(() => parseConfig({ protobufMessageType: "dynamic" })).toThrow(
      GsrInvalidProtobufMessageTypeError,
    );
  });

  it("leaves both fields undefined when the keys are absent", () => {
    const cfg = parseConfig({});
    expect(cfg.avroRecordType).toBeUndefined();
    expect(cfg.protobufMessageType).toBeUndefined();
  });
});

describe("parseConfig — accept-and-ignore + accept-and-store keys", () => {
  it("does not throw on secondaryDeserializer (accept-and-ignore, removed)", () => {
    const cfg = parseConfig({ secondaryDeserializer: "com.example.Legacy" });
    // No public field surfaces the value.
    expect(cfg).not.toHaveProperty("secondaryDeserializer");
  });

  it("does not throw on proxyUrl (accept-and-ignore, deferred to real-AWS wiring)", () => {
    const cfg = parseConfig({ proxyUrl: "http://proxy.internal:3128" });
    expect(cfg).not.toHaveProperty("proxyUrl");
  });

  it("accepts avroReaderSchema and stores it unvalidated (deliberate Go-parity divergence)", () => {
    // A syntactically-broken value is deliberately NOT rejected at parse time —
    // core cannot import an Avro parser. Validation is deferred to projection.
    const broken = "{ not valid avro json";
    const cfg = parseConfig({ avroReaderSchema: broken });
    expect(cfg.avroReaderSchema).toBe(broken);
  });

  it("accepts schemaNameGenerationClass and stores the raw string", () => {
    const cfg = parseConfig({
      schemaNameGenerationClass: "com.example.MyStrategy",
    });
    expect(cfg.schemaNameGenerationClass).toBe("com.example.MyStrategy");
  });
});

describe("parseConfig — user-agent resolution", () => {
  it("preserves the raw userAgentApp and resolves the effective token", () => {
    const cfg = parseConfig({ userAgentApp: "myapp" });
    expect(cfg.userAgentApp).toBe("myapp");
    expect(cfg.effectiveUserAgentApp).toBe("myapp");
  });

  it("falls back to the default when userAgentApp is empty", () => {
    const cfg = parseConfig({ userAgentApp: "" });
    expect(cfg.userAgentApp).toBe("");
    expect(cfg.effectiveUserAgentApp).toBe(DEFAULT_USER_AGENT_APP);
  });
});

describe("parseConfig — AssumeRole session name resolution", () => {
  it("resolves to the caller-provided value when both keys are set", () => {
    const cfg = parseConfig({
      assumeRoleArn: "arn:aws:iam::123456789012:role/gsr",
      assumeRoleSessionName: "override",
    });
    expect(cfg.assumeRoleArn).toBe("arn:aws:iam::123456789012:role/gsr");
    expect(cfg.assumeRoleSessionName).toBe("override");
  });

  it("resolves to the default when ARN is set but session name is absent", () => {
    const cfg = parseConfig({
      assumeRoleArn: "arn:aws:iam::123456789012:role/gsr",
    });
    expect(cfg.assumeRoleSessionName).toBe(DEFAULT_ASSUME_ROLE_SESSION);
  });

  it("leaves session name empty when no ARN is configured (dormant path)", () => {
    const cfg = parseConfig({});
    expect(cfg.assumeRoleSessionName).toBe("");
    expect(cfg.assumeRoleArn).toBeUndefined();
  });
});

describe("parseConfig — tag and metadata prefix-expansion", () => {
  it("expands tags.<k>=<v> and metadata.<k>=<v> entries into their maps", () => {
    const cfg = parseConfig({
      "tags.owner": "team-gsr",
      "tags.env": "beta",
      "metadata.build": "abc123",
      "metadata.git": "sha:def",
      // Non-prefixed key MUST NOT bleed into either map.
      "registry.name": "custom",
    });
    expect(cfg.tags).toEqual({ owner: "team-gsr", env: "beta" });
    expect(cfg.metadata).toEqual({ build: "abc123", git: "sha:def" });
    expect(cfg.registryName).toBe("custom");
  });

  it("returns empty maps when no prefixed keys are present", () => {
    const cfg = parseConfig({ region: "eu-west-1" });
    expect(cfg.tags).toEqual({});
    expect(cfg.metadata).toEqual({});
  });
});

describe("parseConfig — schemaAutoRegistrationEnabled (lenient bool)", () => {
  it.each(["true", "True", "TRUE"])("treats %j as true", (raw) => {
    expect(parseConfig({ schemaAutoRegistrationEnabled: raw }).schemaAutoRegistrationEnabled).toBe(
      true,
    );
  });

  it.each(["false", "yes", "1", "", "no"])("treats %j as false", (raw) => {
    expect(parseConfig({ schemaAutoRegistrationEnabled: raw }).schemaAutoRegistrationEnabled).toBe(
      false,
    );
  });
});

describe("parseConfig — retry surface (TS-additive)", () => {
  it("accepts the two documented retry modes", () => {
    expect(parseConfig({ retryMode: "standard" }).retryMode).toBe("standard");
    expect(parseConfig({ retryMode: "adaptive" }).retryMode).toBe("adaptive");
  });

  it("silently falls back to the default on out-of-set retryMode values (Go has no key)", () => {
    expect(parseConfig({ retryMode: "aggressive" }).retryMode).toBe(
      DEFAULT_RETRY_MODE,
    );
  });

  it("parses maxAttempts as an integer, falling back on non-integer input", () => {
    expect(parseConfig({ maxAttempts: "5" }).maxAttempts).toBe(5);
    expect(parseConfig({ maxAttempts: "1.5" }).maxAttempts).toBe(
      DEFAULT_MAX_ATTEMPTS,
    );
    expect(parseConfig({ maxAttempts: "abc" }).maxAttempts).toBe(
      DEFAULT_MAX_ATTEMPTS,
    );
  });
});

describe("parseConfig — region / endpoint / description resolution", () => {
  it("uses the caller-provided region and endpoint when set", () => {
    const cfg = parseConfig({
      region: "eu-west-1",
      endpoint: "https://glue.local",
    });
    expect(cfg.region).toBe("eu-west-1");
    expect(cfg.endpoint).toBe("https://glue.local");
  });

  it("applies the region default when the key is empty", () => {
    const cfg = parseConfig({ region: "" });
    expect(cfg.region).toBe(DEFAULT_REGION);
  });

  it("respects an explicit description over the synthesized default", () => {
    const cfg = parseConfig({ description: "custom-desc" });
    expect(cfg.description).toBe("custom-desc");
  });

  it("re-synthesizes description after applied registry-name default", () => {
    const cfg = parseConfig({ region: "eu-west-1", "registry.name": "prod" });
    expect(cfg.description).toBe("DEFAULT-DESCRIPTION-eu-west-1-prod");
  });
});

describe("parseConfig — purity", () => {
  it("does not mutate the caller's config map", () => {
    const original: Record<string, string> = {
      region: "eu-west-1",
      "tags.env": "beta",
    };
    const before = JSON.stringify(original);
    parseConfig(original);
    expect(JSON.stringify(original)).toBe(before);
  });
});
