import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  DEFAULT_AWS_REGION,
  assertSpikePrecondition,
  describeIntegration,
  getLoudSkipRegistry,
  isAwsIntegrationEnabled,
  isRealGlue,
  requireAwsIntegration,
  requireRealCreds,
  resetLoudSkipRegistry,
  resolveRegion,
  type LoudSkipEntry,
} from "./env-gate.js";

/**
 * Tier-1 unit coverage for the runtime env gate. Every case here stubs env
 * vars via `vi.stubEnv` (auto-restored by `unstubAllEnvs`) so the tests run
 * offline and are independent of the host shell.
 */

beforeEach(() => {
  // Explicit clean slate: unset every variable the module reads so a leaky
  // shell env cannot poison a case. Individual tests then stub the exact
  // vars they exercise.
  vi.stubEnv("AWS_INTEGRATION", "");
  vi.stubEnv("GSR_GLUE", "");
  vi.stubEnv("AWS_REGION", "");
  vi.stubEnv("AWS_PROFILE", "");
  vi.stubEnv("AWS_ACCESS_KEY_ID", "");
  vi.stubEnv("AWS_SECRET_ACCESS_KEY", "");
  // Every case that exercises the loud-skip contract needs a clean registry
  // so it can assert on the exact set of entries it produced.
  resetLoudSkipRegistry();
});

afterEach(() => {
  vi.unstubAllEnvs();
  resetLoudSkipRegistry();
});

describe("isAwsIntegrationEnabled", () => {
  it("returns true only when AWS_INTEGRATION is exactly '1'", () => {
    vi.stubEnv("AWS_INTEGRATION", "1");
    expect(isAwsIntegrationEnabled()).toBe(true);
  });

  it("returns false when AWS_INTEGRATION is unset (empty)", () => {
    expect(isAwsIntegrationEnabled()).toBe(false);
  });

  it("returns false for values other than '1' (verbatim env matrix)", () => {
    for (const v of ["true", "yes", "on", "0", "TRUE", " 1", "1 "]) {
      vi.stubEnv("AWS_INTEGRATION", v);
      expect(isAwsIntegrationEnabled()).toBe(false);
    }
  });
});

describe("isRealGlue", () => {
  it("returns true when GSR_GLUE=real", () => {
    vi.stubEnv("GSR_GLUE", "real");
    expect(isRealGlue()).toBe(true);
  });

  it("returns false when GSR_GLUE is unset", () => {
    expect(isRealGlue()).toBe(false);
  });

  it("returns false for 'fake' and other values (default-to-fake safety)", () => {
    for (const v of ["fake", "", "REAL", "rael", "true"]) {
      vi.stubEnv("GSR_GLUE", v);
      expect(isRealGlue()).toBe(false);
    }
  });
});

describe("requireAwsIntegration", () => {
  it("returns {skipped:false} when AWS_INTEGRATION=1", () => {
    vi.stubEnv("AWS_INTEGRATION", "1");
    expect(requireAwsIntegration()).toEqual({ skipped: false });
  });

  it("returns {skipped:true, reason:'AWS_INTEGRATION!=1'} when gate is off", () => {
    const result = requireAwsIntegration();
    expect(result.skipped).toBe(true);
    if (result.skipped) {
      expect(result.reason).toBe("AWS_INTEGRATION!=1");
    }
  });

  it("returns skipped for AWS_INTEGRATION values other than exact '1'", () => {
    for (const v of ["true", "yes", "0", " 1"]) {
      vi.stubEnv("AWS_INTEGRATION", v);
      const result = requireAwsIntegration();
      expect(result.skipped).toBe(true);
    }
  });
});

describe("requireRealCreds", () => {
  it("is a no-op when GSR_GLUE is not 'real'", () => {
    // Even with no creds present, non-real mode must not throw.
    expect(() => requireRealCreds()).not.toThrow();
  });

  it("is a no-op when GSR_GLUE is 'fake' (default backend)", () => {
    vi.stubEnv("GSR_GLUE", "fake");
    expect(() => requireRealCreds()).not.toThrow();
  });

  it("throws (not skips) when GSR_GLUE=real and no AWS_PROFILE/AWS_ACCESS_KEY_ID", () => {
    vi.stubEnv("GSR_GLUE", "real");
    expect(() => requireRealCreds()).toThrowError(/AWS credentials/i);
  });

  it("throws with a message mentioning both credential env vars", () => {
    vi.stubEnv("GSR_GLUE", "real");
    let caught: unknown;
    try {
      requireRealCreds();
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    const message = (caught as Error).message;
    expect(message).toContain("AWS_PROFILE");
    expect(message).toContain("AWS_ACCESS_KEY_ID");
  });

  it("passes when GSR_GLUE=real and AWS_PROFILE is set", () => {
    vi.stubEnv("GSR_GLUE", "real");
    vi.stubEnv("AWS_PROFILE", "gsr-dev");
    expect(() => requireRealCreds()).not.toThrow();
  });

  it("passes when GSR_GLUE=real and AWS_ACCESS_KEY_ID is set", () => {
    vi.stubEnv("GSR_GLUE", "real");
    vi.stubEnv("AWS_ACCESS_KEY_ID", "AKIA0000000000000000");
    expect(() => requireRealCreds()).not.toThrow();
  });

  it("treats empty AWS_PROFILE / AWS_ACCESS_KEY_ID as missing", () => {
    // Empty-string values must not count as present — the shell exports the
    // name-with-empty-value form when a user unsets a var via `AWS_PROFILE=`
    // on the command line, and that pattern must still hard-fail.
    vi.stubEnv("GSR_GLUE", "real");
    vi.stubEnv("AWS_PROFILE", "");
    vi.stubEnv("AWS_ACCESS_KEY_ID", "");
    expect(() => requireRealCreds()).toThrow();
  });
});

describe("resolveRegion", () => {
  it("returns the default us-east-2 when AWS_REGION is unset", () => {
    expect(resolveRegion()).toBe(DEFAULT_AWS_REGION);
    expect(resolveRegion()).toBe("us-east-2");
  });

  it("returns the default when AWS_REGION is set to an empty string", () => {
    vi.stubEnv("AWS_REGION", "");
    expect(resolveRegion()).toBe(DEFAULT_AWS_REGION);
  });

  it("returns the explicit AWS_REGION when set", () => {
    vi.stubEnv("AWS_REGION", "eu-west-1");
    expect(resolveRegion()).toBe("eu-west-1");
  });

  it("returns AWS_REGION verbatim without normalization", () => {
    // Any normalization (case-folding, trimming) would silently mask typos —
    // Glue calls fail loudly if the region is wrong; the helper does not
    // second-guess the operator.
    vi.stubEnv("AWS_REGION", "US-East-2");
    expect(resolveRegion()).toBe("US-East-2");
  });
});

describe("describeIntegration", () => {
  it("registers a describe.skip and emits a loud one-line skip reason when gate is off", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const body = vi.fn();
      describeIntegration("sample-suite", body);
      // Body must not have been called when the suite is skipped.
      expect(body).not.toHaveBeenCalled();
      // A single loud-skip line must have been emitted naming the suite and
      // the missing gate.
      expect(warnSpy).toHaveBeenCalledTimes(1);
      const line = warnSpy.mock.calls[0]![0] as string;
      expect(line).toContain("SKIP sample-suite");
      expect(line).toContain("AWS_INTEGRATION!=1");
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("records the skip in the loud-skip registry with name + reason", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      describeIntegration("registry-suite", () => {});
      const entries = getLoudSkipRegistry();
      expect(entries).toHaveLength(1);
      expect(entries[0]).toEqual({
        name: "registry-suite",
        reason: "AWS_INTEGRATION!=1",
      });
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("accumulates one registry entry per skipped suite in order", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      describeIntegration("first", () => {});
      describeIntegration("second", () => {});
      describeIntegration("third", () => {});
      const entries = getLoudSkipRegistry();
      expect(entries).toHaveLength(3);
      expect(entries.map((e) => e.name)).toEqual(["first", "second", "third"]);
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("does not emit the loud-skip line, does not throw, and does not record when the gate is open", () => {
    // Vitest's `describe` only executes its callback during file-level
    // collection, so we cannot observe the body being invoked from inside
    // an `it`. Instead we verify the branching contract: when the gate is
    // open, `describeIntegration` must NOT emit a skip line, must NOT
    // throw, and must NOT push to the registry.
    vi.stubEnv("AWS_INTEGRATION", "1");
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      expect(() =>
        describeIntegration("sample-open-suite", () => {}),
      ).not.toThrow();
      expect(warnSpy).not.toHaveBeenCalled();
      expect(getLoudSkipRegistry()).toHaveLength(0);
    } finally {
      warnSpy.mockRestore();
    }
  });
});

describe("loud-skip registry", () => {
  it("returns a snapshot copy that is safe to mutate without touching state", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      describeIntegration("snapshot-suite", () => {});
      const snapshot = getLoudSkipRegistry() as LoudSkipEntry[];
      // Mutating the snapshot must not affect the backing registry.
      snapshot.length = 0;
      expect(getLoudSkipRegistry()).toHaveLength(1);
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("resetLoudSkipRegistry empties the registry", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      describeIntegration("to-be-reset", () => {});
      expect(getLoudSkipRegistry()).toHaveLength(1);
      resetLoudSkipRegistry();
      expect(getLoudSkipRegistry()).toHaveLength(0);
    } finally {
      warnSpy.mockRestore();
    }
  });
});

describe("assertSpikePrecondition", () => {
  it("is a no-op when the precondition holds", () => {
    expect(() =>
      assertSpikePrecondition("some-spike", true, "unused"),
    ).not.toThrow();
  });

  it("throws SPIKE FAIL with the name and reason when the precondition fails", () => {
    expect(() =>
      assertSpikePrecondition(
        "avro-resolution-spike",
        false,
        "avsc.Type.createResolver missing",
      ),
    ).toThrow(
      /SPIKE FAIL: avro-resolution-spike — avsc\.Type\.createResolver missing/,
    );
  });

  it("throws an Error instance (not just a string)", () => {
    let caught: unknown;
    try {
      assertSpikePrecondition("spike", false, "reason");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).message).toBe("SPIKE FAIL: spike — reason");
  });
});

