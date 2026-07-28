/**
 * Tier-1 unit coverage for `interop-gate.ts`.
 *
 * Every case drives `requireInterop()` through its injectable seams so the
 * unit run stays offline: no JVM is spawned, no filesystem is touched for
 * the JAR probe, and no host env var is read (the `env` opt is a plain
 * object). The recorded emitter captures every `SKIP <name> — <reason>`
 * call so the loud-skip contract is asserted directly on the emitted line.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  DEFAULT_INTEROP_SUITE_NAME,
  requireInterop,
  type InteropGateResult,
  type RequireInteropOptions,
} from "./interop-gate.js";

/**
 * One recorded emit call. The gate calls its emitter with the suite name
 * and the reason as separate args (kept separate so a downstream reporter
 * can re-format them); the test buffers each call verbatim.
 */
interface EmitCall {
  readonly name: string;
  readonly reason: string;
}

/**
 * Build a `RequireInteropOptions` with every seam wired to a green default
 * (env passing, partner accepted, JVM+JAR resolvable, emit recorded to the
 * `calls` array). Callers override only the seams they need to exercise.
 * The green default is important: it lets each negative case flip exactly
 * one gate and assert the short-circuit stops before the following gates
 * are consulted.
 */
function makeGreenOpts(
  overrides: Partial<RequireInteropOptions> = {},
): {
  readonly opts: RequireInteropOptions;
  readonly calls: EmitCall[];
} {
  const calls: EmitCall[] = [];
  const opts: RequireInteropOptions = {
    env: {},
    awsIntegrationEnabled: () => true,
    realGlue: () => true,
    jvmAvailable: () => true,
    resolveJar: () => "/tmp/fake-sidecar.jar",
    emit: (name, reason) => {
      calls.push({ name, reason });
    },
    ...overrides,
  };
  return { opts, calls };
}

/**
 * Assert the discriminant is `skipped: true` and narrow the type so
 * downstream property reads compile.
 */
function assertSkipped(
  result: InteropGateResult,
): asserts result is { skipped: true; reason: string } {
  expect(result.skipped).toBe(true);
}

describe("requireInterop — happy path", () => {
  it("returns {skipped:false} and emits nothing when every gate passes", () => {
    const { opts, calls } = makeGreenOpts();
    const result = requireInterop(opts);
    expect(result).toEqual({ skipped: false });
    expect(calls).toEqual([]);
  });

  it("accepts GSR_INTEROP_PARTNER=java as an explicit selector", () => {
    const { opts, calls } = makeGreenOpts({
      env: { GSR_INTEROP_PARTNER: "java" },
    });
    const result = requireInterop(opts);
    expect(result).toEqual({ skipped: false });
    expect(calls).toEqual([]);
  });

  it("accepts GSR_INTEROP_PARTNER unset (falls through to java)", () => {
    const { opts, calls } = makeGreenOpts({
      env: {}, // no GSR_INTEROP_PARTNER
    });
    const result = requireInterop(opts);
    expect(result).toEqual({ skipped: false });
    expect(calls).toEqual([]);
  });

  it("accepts GSR_INTEROP_PARTNER empty string (empty is treated as unset)", () => {
    const { opts, calls } = makeGreenOpts({
      env: { GSR_INTEROP_PARTNER: "" },
    });
    const result = requireInterop(opts);
    expect(result).toEqual({ skipped: false });
    expect(calls).toEqual([]);
  });
});

describe("requireInterop — env gate branches", () => {
  it("loud-skips with 'AWS_INTEGRATION!=1' when the AWS env gate is off", () => {
    const { opts, calls } = makeGreenOpts({
      awsIntegrationEnabled: () => false,
    });
    const result = requireInterop(opts);
    assertSkipped(result);
    expect(result.reason).toBe("AWS_INTEGRATION!=1");
    expect(calls).toEqual([
      { name: DEFAULT_INTEROP_SUITE_NAME, reason: "AWS_INTEGRATION!=1" },
    ]);
  });

  it("loud-skips with 'GSR_GLUE!=real' when real-Glue is off", () => {
    const { opts, calls } = makeGreenOpts({
      realGlue: () => false,
    });
    const result = requireInterop(opts);
    assertSkipped(result);
    expect(result.reason).toBe("GSR_GLUE!=real");
    expect(calls).toEqual([
      { name: DEFAULT_INTEROP_SUITE_NAME, reason: "GSR_GLUE!=real" },
    ]);
  });

  it("short-circuits on the AWS gate before probing the JVM (avoids misleading downstream skips)", () => {
    let jvmCalls = 0;
    let jarCalls = 0;
    const { opts } = makeGreenOpts({
      awsIntegrationEnabled: () => false,
      jvmAvailable: () => {
        jvmCalls += 1;
        return true;
      },
      resolveJar: () => {
        jarCalls += 1;
        return "/tmp/x.jar";
      },
    });
    requireInterop(opts);
    expect(jvmCalls).toBe(0);
    expect(jarCalls).toBe(0);
  });

  it("short-circuits on the Glue gate before probing the JVM", () => {
    let jvmCalls = 0;
    const { opts } = makeGreenOpts({
      realGlue: () => false,
      jvmAvailable: () => {
        jvmCalls += 1;
        return true;
      },
    });
    requireInterop(opts);
    expect(jvmCalls).toBe(0);
  });
});

describe("requireInterop — partner selector (loud error, not skip)", () => {
  it("throws when GSR_INTEROP_PARTNER=go (unsupported partner)", () => {
    const { opts, calls } = makeGreenOpts({
      env: { GSR_INTEROP_PARTNER: "go" },
    });
    expect(() => requireInterop(opts)).toThrowError(
      /GSR_INTEROP_PARTNER=go is not supported.*only 'java'/,
    );
    // A loud error must NOT emit a skip line — skipping would look like a
    // benign "run again with the right env" while the harness has actually
    // refused to run.
    expect(calls).toEqual([]);
  });

  it("throws when GSR_INTEROP_PARTNER=rust", () => {
    const { opts } = makeGreenOpts({
      env: { GSR_INTEROP_PARTNER: "rust" },
    });
    expect(() => requireInterop(opts)).toThrow(/rust/);
  });

  it("throws with a message naming the unsupported value and 'java' as the accepted partner", () => {
    const { opts } = makeGreenOpts({
      env: { GSR_INTEROP_PARTNER: "python" },
    });
    let caught: unknown;
    try {
      requireInterop(opts);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    const message = (caught as Error).message;
    expect(message).toContain("python");
    expect(message).toContain("java");
  });

  it("throws BEFORE probing the JVM (partner check precedes availability probes)", () => {
    let jvmCalls = 0;
    let jarCalls = 0;
    const { opts } = makeGreenOpts({
      env: { GSR_INTEROP_PARTNER: "go" },
      jvmAvailable: () => {
        jvmCalls += 1;
        return true;
      },
      resolveJar: () => {
        jarCalls += 1;
        return "/tmp/x.jar";
      },
    });
    expect(() => requireInterop(opts)).toThrow();
    expect(jvmCalls).toBe(0);
    expect(jarCalls).toBe(0);
  });
});

describe("requireInterop — JVM/JAR availability branches", () => {
  it("loud-skips when the JVM is not available", () => {
    const { opts, calls } = makeGreenOpts({
      jvmAvailable: () => false,
    });
    const result = requireInterop(opts);
    assertSkipped(result);
    expect(result.reason).toMatch(/JVM not available/);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.name).toBe(DEFAULT_INTEROP_SUITE_NAME);
    expect(calls[0]!.reason).toMatch(/JVM not available/);
  });

  it("names the GSR_INTEROP_JAVA env var in the JVM skip reason (operator hint)", () => {
    const { opts } = makeGreenOpts({
      jvmAvailable: () => false,
    });
    const result = requireInterop(opts);
    assertSkipped(result);
    expect(result.reason).toContain("GSR_INTEROP_JAVA");
  });

  it("loud-skips when the sidecar JAR resolver throws (JAR not built)", () => {
    const { opts, calls } = makeGreenOpts({
      resolveJar: () => {
        throw new Error(
          "could not locate java-interop/target/java-interop-sidecar.jar",
        );
      },
    });
    const result = requireInterop(opts);
    assertSkipped(result);
    expect(result.reason).toMatch(/sidecar JAR not built/);
    expect(result.reason).toContain("java-interop-sidecar.jar");
    expect(calls).toHaveLength(1);
  });

  it("tolerates a non-Error thrown from the JAR resolver", () => {
    const { opts } = makeGreenOpts({
      resolveJar: () => {
        // eslint-disable-next-line @typescript-eslint/only-throw-error
        throw "raw string reject";
      },
    });
    const result = requireInterop(opts);
    assertSkipped(result);
    expect(result.reason).toContain("raw string reject");
  });

  it("short-circuits on the JVM branch without invoking the JAR resolver", () => {
    let jarCalls = 0;
    const { opts } = makeGreenOpts({
      jvmAvailable: () => false,
      resolveJar: () => {
        jarCalls += 1;
        return "/tmp/x.jar";
      },
    });
    requireInterop(opts);
    expect(jarCalls).toBe(0);
  });
});

describe("requireInterop — loud-skip contract", () => {
  it("emits exactly one skip line per call (short-circuit is enforced)", () => {
    // Every gate is off; we still expect exactly one line — the FIRST
    // missing gate — because the function must short-circuit.
    const { opts, calls } = makeGreenOpts({
      awsIntegrationEnabled: () => false,
      realGlue: () => false,
      jvmAvailable: () => false,
      resolveJar: () => {
        throw new Error("nope");
      },
    });
    requireInterop(opts);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.reason).toBe("AWS_INTEGRATION!=1");
  });

  it("respects the caller-supplied suite name in the emitted line", () => {
    const { opts, calls } = makeGreenOpts({
      suiteName: "ts-to-java-same-version",
      awsIntegrationEnabled: () => false,
    });
    requireInterop(opts);
    expect(calls[0]!.name).toBe("ts-to-java-same-version");
  });

  it("defaults the suite name to 'interop' when the caller omits it", () => {
    const { opts, calls } = makeGreenOpts({
      awsIntegrationEnabled: () => false,
    });
    requireInterop(opts);
    expect(calls[0]!.name).toBe("interop");
    expect(DEFAULT_INTEROP_SUITE_NAME).toBe("interop");
  });

  it("the emitted reason matches the returned .reason (single source of truth)", () => {
    const cases: Array<Partial<RequireInteropOptions>> = [
      { awsIntegrationEnabled: () => false },
      { realGlue: () => false },
      { jvmAvailable: () => false },
      {
        resolveJar: () => {
          throw new Error("jar missing");
        },
      },
    ];
    for (const override of cases) {
      const { opts, calls } = makeGreenOpts(override);
      const result = requireInterop(opts);
      assertSkipped(result);
      expect(calls).toHaveLength(1);
      expect(calls[0]!.reason).toBe(result.reason);
    }
  });

  it("does not emit any line on the happy path (default `npm test` stays quiet)", () => {
    const { opts, calls } = makeGreenOpts();
    requireInterop(opts);
    expect(calls).toEqual([]);
  });
});

describe("requireInterop — default emitter shape", () => {
  // The default emitter is `console.warn(\`SKIP ${name} — ${reason}\`)`.
  // These cases prove that shape is written verbatim (em-dash U+2014,
  // exact spacing) so a downstream log scanner can pattern-match on it.
  let warnSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    warnSpy.mockRestore();
  });

  it("writes `SKIP <name> — <reason>` to console.warn (em-dash U+2014)", () => {
    // Drop the `emit` override so the default path is exercised.
    const { opts } = makeGreenOpts({
      awsIntegrationEnabled: () => false,
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const withoutEmit: RequireInteropOptions = { ...opts, emit: undefined as any };
    requireInterop(withoutEmit);
    expect(warnSpy).toHaveBeenCalledTimes(1);
    const line = warnSpy.mock.calls[0]![0] as string;
    expect(line).toBe("SKIP interop — AWS_INTEGRATION!=1");
  });
});

describe("requireInterop — production defaults", () => {
  // These cases confirm the defaults hook the shared env-gate + java-sidecar
  // helpers as advertised, without stubbing the seams. They keep the
  // production-wire branches out of dead-code detection.

  const originalAwsIntegration = process.env.AWS_INTEGRATION;
  const originalGsrGlue = process.env.GSR_GLUE;
  const originalPartner = process.env.GSR_INTEROP_PARTNER;

  afterEach(() => {
    // Restore env state so we do not poison neighbouring suites in the
    // same worker process.
    process.env.AWS_INTEGRATION = originalAwsIntegration;
    process.env.GSR_GLUE = originalGsrGlue;
    process.env.GSR_INTEROP_PARTNER = originalPartner;
    vi.unstubAllEnvs();
  });

  it("defaults to the shared env-gate: AWS_INTEGRATION!=1 short-circuits first", () => {
    vi.stubEnv("AWS_INTEGRATION", "");
    // Suppress the default console.warn so vitest's reporter stays clean.
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const result = requireInterop();
      assertSkipped(result);
      expect(result.reason).toBe("AWS_INTEGRATION!=1");
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("defaults to the shared env-gate: GSR_GLUE!=real short-circuits second", () => {
    vi.stubEnv("AWS_INTEGRATION", "1");
    vi.stubEnv("GSR_GLUE", "fake");
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const result = requireInterop();
      assertSkipped(result);
      expect(result.reason).toBe("GSR_GLUE!=real");
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("defaults to a loud error for an unsupported partner", () => {
    vi.stubEnv("AWS_INTEGRATION", "1");
    vi.stubEnv("GSR_GLUE", "real");
    vi.stubEnv("GSR_INTEROP_PARTNER", "go");
    // No warn spy needed — a throw does not emit.
    expect(() => requireInterop()).toThrow(/go/);
  });
});
