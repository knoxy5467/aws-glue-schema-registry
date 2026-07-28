import { beforeEach, describe, expect, it, vi } from "vitest";

// Mock the credential provider before importing the module under test, so
// the AssumeRole tests can observe the exact params `buildGlueClientConfig`
// forwards. `vi.hoisted` runs the factory alongside the hoisted `vi.mock`
// call, so the same mock function is visible inside the mocked module and
// in the assertions below.
const { fromTemporaryCredentialsMock } = vi.hoisted(() => ({
  fromTemporaryCredentialsMock: vi.fn(() => {
    const sentinel = async () => {
      throw new Error(
        "test sentinel credential provider — must not be resolved in unit tests",
      );
    };
    Object.defineProperty(sentinel, "__gsrTestSentinel", {
      value: true,
      enumerable: false,
    });
    return sentinel;
  }),
}));

vi.mock("@aws-sdk/credential-providers", () => ({
  fromTemporaryCredentials: fromTemporaryCredentialsMock,
}));

import { parseConfig } from "./config.js";
import {
  USER_AGENT_LANGUAGE_TOKEN,
  buildGlueClientConfig,
} from "./client-options.js";

beforeEach(() => {
  fromTemporaryCredentialsMock.mockClear();
});

describe("buildGlueClientConfig — AssumeRole capability", () => {
  it("wires fromTemporaryCredentials when assumeRoleArn is set", () => {
    const cfg = parseConfig({
      assumeRoleArn: "arn:aws:iam::123456789012:role/GsrProducer",
    });

    const clientCfg = buildGlueClientConfig(cfg);

    expect(fromTemporaryCredentialsMock).toHaveBeenCalledTimes(1);
    expect(fromTemporaryCredentialsMock).toHaveBeenCalledWith({
      params: {
        RoleArn: "arn:aws:iam::123456789012:role/GsrProducer",
        RoleSessionName: "aws-glue-schema-registry-js",
      },
    });
    expect(clientCfg.credentials).toBe(
      fromTemporaryCredentialsMock.mock.results[0]!.value,
    );
    // The resolved session name is also observable on GsrConfig (Tier-1 seam).
    expect(cfg.assumeRoleSessionName).toBe("aws-glue-schema-registry-js");
  });

  it("honors a caller-supplied assumeRoleSessionName", () => {
    const cfg = parseConfig({
      assumeRoleArn: "arn:aws:iam::123456789012:role/GsrProducer",
      assumeRoleSessionName: "producer-session-42",
    });

    buildGlueClientConfig(cfg);

    expect(fromTemporaryCredentialsMock).toHaveBeenCalledWith({
      params: {
        RoleArn: "arn:aws:iam::123456789012:role/GsrProducer",
        RoleSessionName: "producer-session-42",
      },
    });
    expect(cfg.assumeRoleSessionName).toBe("producer-session-42");
  });

  it("leaves credentials to the SDK default chain when no assumeRoleArn", () => {
    const cfg = parseConfig({});

    const clientCfg = buildGlueClientConfig(cfg);

    expect(fromTemporaryCredentialsMock).not.toHaveBeenCalled();
    expect(clientCfg.credentials).toBeUndefined();
    // Session-name field is empty on the dormant path (Go parity).
    expect(cfg.assumeRoleSessionName).toBe("");
  });
});

describe("buildGlueClientConfig — region and custom endpoint", () => {
  it("defaults region to us-east-2 and omits endpoint when unset", () => {
    const clientCfg = buildGlueClientConfig(parseConfig({}));

    expect(clientCfg.region).toBe("us-east-2");
    expect(clientCfg.endpoint).toBeUndefined();
  });

  it("passes an explicit region through", () => {
    const clientCfg = buildGlueClientConfig(parseConfig({ region: "eu-west-1" }));

    expect(clientCfg.region).toBe("eu-west-1");
    expect(clientCfg.endpoint).toBeUndefined();
  });

  it("applies a custom endpoint alongside a non-default region", () => {
    const clientCfg = buildGlueClientConfig(
      parseConfig({
        region: "eu-west-1",
        endpoint: "https://glue.local",
      }),
    );

    expect(clientCfg.region).toBe("eu-west-1");
    expect(clientCfg.endpoint).toBe("https://glue.local");
  });

  it("applies a custom endpoint on the default region (regardless-of-region rule)", () => {
    const clientCfg = buildGlueClientConfig(
      parseConfig({ endpoint: "https://glue.local" }),
    );

    expect(clientCfg.region).toBe("us-east-2");
    expect(clientCfg.endpoint).toBe("https://glue.local");
  });
});

describe("buildGlueClientConfig — retry configuration", () => {
  it("defaults retryMode=standard and maxAttempts=3", () => {
    const clientCfg = buildGlueClientConfig(parseConfig({}));

    expect(clientCfg.retryMode).toBe("standard");
    expect(clientCfg.maxAttempts).toBe(3);
  });

  it("honors an explicit adaptive retryMode override", () => {
    const clientCfg = buildGlueClientConfig(
      parseConfig({ retryMode: "adaptive" }),
    );

    expect(clientCfg.retryMode).toBe("adaptive");
    expect(clientCfg.maxAttempts).toBe(3);
  });

  it("honors an explicit maxAttempts override", () => {
    const clientCfg = buildGlueClientConfig(
      parseConfig({ maxAttempts: "10" }),
    );

    expect(clientCfg.retryMode).toBe("standard");
    expect(clientCfg.maxAttempts).toBe(10);
  });
});

describe("buildGlueClientConfig — user-agent identity", () => {
  it("stamps glue-schema-registry-js/default when userAgentApp is absent", () => {
    const clientCfg = buildGlueClientConfig(parseConfig({}));

    expect(clientCfg.customUserAgent).toEqual([
      ["glue-schema-registry-js", "default"],
    ]);
  });

  it("stamps glue-schema-registry-js/<app> when userAgentApp is provided", () => {
    const clientCfg = buildGlueClientConfig(
      parseConfig({ userAgentApp: "myapp" }),
    );

    expect(clientCfg.customUserAgent).toEqual([
      ["glue-schema-registry-js", "myapp"],
    ]);
  });

  it("exports the language token so other modules cannot drift", () => {
    expect(USER_AGENT_LANGUAGE_TOKEN).toBe("glue-schema-registry-js");
  });

  it("uses key/value pair form so the emitted header preserves the / separator", () => {
    const clientCfg = buildGlueClientConfig(
      parseConfig({ userAgentApp: "myapp" }),
    );

    // The @smithy/types UserAgent type is `UserAgentPair[]`, and each
    // UserAgentPair is `[name, version]`. When serialized, this pair form
    // emits `name/version` — mirroring Go's `AddUserAgentKeyValue`.
    const ua = clientCfg.customUserAgent as unknown as ReadonlyArray<
      readonly [string, string]
    >;
    expect(Array.isArray(ua)).toBe(true);
    expect(ua).toHaveLength(1);
    const [pair] = ua;
    if (pair === undefined) {
      throw new Error("expected a user-agent pair");
    }
    expect(pair).toHaveLength(2);
    expect(pair[0]).toBe("glue-schema-registry-js");
    expect(pair[1]).toBe("myapp");
  });
});

describe("buildGlueClientConfig — purity and shape", () => {
  it("returns a fresh object on each call (no shared state)", () => {
    const cfg = parseConfig({});

    const a = buildGlueClientConfig(cfg);
    const b = buildGlueClientConfig(cfg);

    expect(a).not.toBe(b);
    expect(a.customUserAgent).not.toBe(b.customUserAgent);
  });

  it("performs no SDK call on the credentials path when ARN is absent", () => {
    const cfg = parseConfig({ region: "us-west-2" });

    buildGlueClientConfig(cfg);

    expect(fromTemporaryCredentialsMock).not.toHaveBeenCalled();
  });
});
