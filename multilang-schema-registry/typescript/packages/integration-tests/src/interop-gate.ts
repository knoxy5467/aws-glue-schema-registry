/**
 * `requireInterop()` — the three-gate discipline for the interop test tier.
 *
 * An interop suite must clear THREE gates before it may run against a real
 * Java sidecar + real AWS Glue + real Kafka:
 *
 *   1. The reused environment gate: `AWS_INTEGRATION=1` AND `GSR_GLUE=real`.
 *      Both booleans come from the shared env-gate helpers so the interop
 *      tier and the Tier-2/3 tier agree byte-for-byte on what "integration
 *      mode" and "real Glue" mean.
 *   2. The `GSR_INTEROP_PARTNER` selector: accepts `java` or unset only.
 *      Any other value (`go`, `rust`, empty non-blank, …) is a LOUD ERROR
 *      — not a silent skip — because a downgrade to a nonexistent sidecar
 *      would ship a false green.
 *   3. JVM-and-JAR availability: a `java` binary must resolve (via
 *      `isJvmAvailable`) AND the vendored sidecar fat JAR must be present
 *      (via `resolveJarPath`, which throws when the built JAR is not on
 *      disk).
 *
 * When any gate is unmet the function returns
 * `{ skipped: true, reason: "<one-line reason>" }` and emits exactly one
 * machine-parseable `SKIP <suite-name> — <reason>` line on `console.warn`
 * (mirroring the shape the shared env-gate emits for its own suite-level
 * skips). When every gate is met, it returns `{ skipped: false }` and
 * emits nothing.
 *
 * ## Injection seam
 *
 * Every runtime dependency is injectable via `RequireInteropOptions` so the
 * Tier-1 unit test can drive each branch without touching a real JVM, real
 * filesystem, or the host shell env. Production callers pass no options and
 * get the real wire.
 *
 * ## Reuse discipline
 *
 * The shared env-gate helpers (`env-gate.ts`) are imported by name; nothing
 * in this module re-implements them. The env-gate registers its own
 * suite-level loud skips through the private `describeIntegration`
 * wrapper; this module emits the sub-gate skip line directly because those
 * private hooks are not exported (adding one would edit the shared
 * env-gate, which the interop tier is not permitted to do).
 */

import {
  isAwsIntegrationEnabled,
  isRealGlue,
} from "./env-gate.js";
import { isJvmAvailable, resolveJarPath } from "./java-sidecar.js";

/**
 * Default suite name used in the `SKIP <name> — <reason>` line when the
 * caller does not pass one. Callers that host multiple sub-suites should
 * pass a more specific name so the machine-parseable output distinguishes
 * them.
 */
export const DEFAULT_INTEROP_SUITE_NAME = "interop";

/**
 * Result of `requireInterop()`. Mirrors `SuiteGateResult` in the env-gate:
 * a discriminated union carrying either an open verdict or a one-line
 * reason describing why the suite must loud-skip.
 */
export type InteropGateResult =
  | { readonly skipped: false }
  | { readonly skipped: true; readonly reason: string };

/**
 * Injectable dependency bag for `requireInterop`. Every field is optional;
 * omitted fields fall back to the real production wire.
 *
 * The five seams the unit test uses:
 *
 *  - `suiteName` — the human name printed in the `SKIP` line.
 *  - `env` — the environment map read for the two feature gates and the
 *    `GSR_INTEROP_PARTNER` selector. Defaults to `process.env`.
 *  - `awsIntegrationEnabled` / `realGlue` — the two boolean predicates the
 *    env-gate exports. Injectable so a case can exercise one gate branch
 *    while holding the other constant, without mutating global state.
 *  - `jvmAvailable` — the JVM probe from `java-sidecar`.
 *  - `resolveJar` — the JAR-path resolver from `java-sidecar`. Throws when
 *    the built JAR is absent; the gate turns that throw into a skip line.
 *  - `emit` — the loud-skip emitter. Defaults to a `console.warn` writer
 *    that matches the shared env-gate's emit format character-for-character.
 */
export interface RequireInteropOptions {
  readonly suiteName?: string;
  readonly env?: NodeJS.ProcessEnv;
  readonly awsIntegrationEnabled?: () => boolean;
  readonly realGlue?: () => boolean;
  readonly jvmAvailable?: () => boolean;
  readonly resolveJar?: () => string;
  readonly emit?: (name: string, reason: string) => void;
}

/**
 * Default emitter. Matches the format the shared env-gate uses for its
 * private `emitLoudSkip` (`SKIP <name> — <reason>`, em-dash U+2014) so a
 * downstream log scanner treats both identically.
 */
function defaultEmit(name: string, reason: string): void {
  // eslint-disable-next-line no-console
  console.warn(`SKIP ${name} — ${reason}`);
}

/**
 * Set of accepted `GSR_INTEROP_PARTNER` values. `java` is the only real
 * partner; an unset (or empty-string) value falls through to the same
 * partner. Any other value is a loud error.
 */
const JAVA_PARTNER = "java";

/**
 * Stack the three interop gates in the order the spec fixes: env → partner
 * → JVM/JAR. The order matters — a caller that has neither AWS credentials
 * nor a JVM would otherwise see a JVM-availability skip line, obscuring the
 * more fundamental env gate.
 *
 * The partner check is intentionally sandwiched between the env gate and
 * the JVM/JAR probe: it is a MUST-fail (not skip) check, so it runs after
 * the env has committed the operator to real interop mode but before we
 * report the more benign "JVM unavailable" skip. That order matches the
 * Go reference's `ModeFromEnv` strict rejection.
 */
export function requireInterop(
  opts?: RequireInteropOptions,
): InteropGateResult {
  const suiteName = opts?.suiteName ?? DEFAULT_INTEROP_SUITE_NAME;
  const env = opts?.env ?? process.env;
  const awsIntegration =
    opts?.awsIntegrationEnabled ?? isAwsIntegrationEnabled;
  const realGlue = opts?.realGlue ?? isRealGlue;
  const jvm = opts?.jvmAvailable ?? isJvmAvailable;
  const jar = opts?.resolveJar ?? resolveJarPath;
  const emit = opts?.emit ?? defaultEmit;

  if (!awsIntegration()) {
    return skipWith(emit, suiteName, "AWS_INTEGRATION!=1");
  }
  if (!realGlue()) {
    return skipWith(emit, suiteName, "GSR_GLUE!=real");
  }

  const partner = env["GSR_INTEROP_PARTNER"];
  if (
    partner !== undefined &&
    partner !== "" &&
    partner !== JAVA_PARTNER
  ) {
    // Loud error — the interop harness must not silently fall back to a
    // sidecar it does not implement. Mirrors the Go reference's strict
    // `ModeFromEnv` rejection.
    throw new Error(
      `GSR_INTEROP_PARTNER=${partner} is not supported: only 'java' ` +
        "(or unset) is accepted. The Java sidecar is the only interop " +
        "partner this harness implements — refusing to run rather than " +
        "silently degrade.",
    );
  }

  if (!jvm()) {
    return skipWith(
      emit,
      suiteName,
      "JVM not available (set GSR_INTEROP_JAVA or install a JDK on PATH)",
    );
  }

  try {
    jar();
  } catch (err) {
    const detail = err instanceof Error ? err.message : String(err);
    return skipWith(
      emit,
      suiteName,
      `sidecar JAR not built: ${detail}`,
    );
  }

  return { skipped: false };
}

/**
 * Emit exactly one `SKIP` line via the injectable emitter and return the
 * matching gate result. Extracted so every branch above shares the same
 * emit-then-return sequence; refactoring one branch cannot desynchronize
 * the emitted line from the returned reason.
 */
function skipWith(
  emit: (name: string, reason: string) => void,
  name: string,
  reason: string,
): InteropGateResult {
  emit(name, reason);
  return { skipped: true, reason };
}
