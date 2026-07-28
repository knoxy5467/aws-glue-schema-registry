/**
 * Runtime env-gate helpers, split out of `env-gate.ts` so callers that are
 * NOT running under vitest — the narrated cross-language interop demo and
 * any future diagnostic tool — can consume `isAwsIntegrationEnabled`,
 * `isRealGlue`, `requireRealCreds`, `resolveRegion`, and the loud-skip
 * registry without transitively importing vitest.
 *
 * The vitest-dependent `describeIntegration` wrapper lives in `env-gate.ts`
 * and re-exports everything from this module so existing test-side imports
 * (`.integ.test.ts` suites, `env-gate.test.ts`, `interop-gate.ts`) keep
 * working byte-identically. This file must remain free of any `vitest`
 * import; adding one would defeat the split and put us right back at the
 * "Vitest failed to access its internal state" crash the demo trips on.
 *
 * Behavior contract (same as before the split):
 * - Every helper is called on every invocation — the env is read live, not
 *   cached — so tests using `vi.stubEnv` observe changes without needing a
 *   fresh module.
 * - Loud-skip discipline: every skipped Tier-2/3 suite emits exactly one
 *   reporter line naming the suite and the missing gate, and the emission is
 *   recorded in a module-level registry. A process-exit hook prints an
 *   aggregated summary line when the registry is non-empty, so a default
 *   `npm test` run (Tier-2/3 files not selected → zero skips) stays silent
 *   while an integration run makes every gate-off skip machine-parseable.
 * - `assertSpikePrecondition` is the corresponding hardening for feasibility
 *   spikes: a spike that cannot render its go/no-go verdict THROWS with a
 *   `SPIKE FAIL: ...` one-liner rather than silently skipping.
 *
 * Mirrors the Go reference (`integration-tests/tests/scenario_helper_test.go`
 * + `glue_selector_test.go`): `requireAwsIntegration` is the analogue of
 * `requireAWSIntegration(t)`, `isRealGlue` maps `GSR_GLUE=real`, and
 * `requireRealCreds` mirrors the Makefile hard-fail so `test:integ:real`
 * refuses to run when neither `AWS_PROFILE` nor `AWS_ACCESS_KEY_ID` is set.
 */

/**
 * Default AWS region for Tier-3 real-Glue runs when `AWS_REGION` is not set.
 * Matches the ratified plan decision and the Go reference default.
 */
export const DEFAULT_AWS_REGION = "us-east-2";

/**
 * Read an env var without eagerly caching. Called on every helper invocation
 * so tests that stub the environment (e.g. `vi.stubEnv`) observe the change.
 */
function readEnv(name: string): string | undefined {
  return process.env[name];
}

/**
 * `true` when `AWS_INTEGRATION === "1"`. Nothing else counts — a value of
 * `"true"`, `"yes"`, or `"on"` is deliberately rejected so the env matrix
 * in the spec is verbatim ("1" is the only acceptance token).
 */
export function isAwsIntegrationEnabled(): boolean {
  return readEnv("AWS_INTEGRATION") === "1";
}

/**
 * `true` when `GSR_GLUE === "real"`. Any other value (unset, "", "fake",
 * or a typo) resolves to false — the fake backend is the default so a
 * misconfigured env never silently bills AWS.
 */
export function isRealGlue(): boolean {
  return readEnv("GSR_GLUE") === "real";
}

/**
 * Suite-gate return shape. `describeIntegration` uses this to pick between
 * `describe` and `describe.skip` while emitting a one-line reason for the
 * loud-skip contract.
 */
export type SuiteGateResult =
  | { skipped: false }
  | { skipped: true; reason: string };

/**
 * Non-throwing suite gate for Tier-2/3 files: returns `{skipped: false}` when
 * `AWS_INTEGRATION=1`, else `{skipped: true, reason: "..."}`. Callers pass
 * the reason into a loud-skip reporter so no suite is ever silently skipped.
 *
 * Callers that want a one-shot `describe.skip` wrapper should use
 * `describeIntegration` instead; this is the low-level primitive.
 */
export function requireAwsIntegration(): SuiteGateResult {
  if (!isAwsIntegrationEnabled()) {
    return {
      skipped: true,
      reason: "AWS_INTEGRATION!=1",
    };
  }
  return { skipped: false };
}

/**
 * Hard-fail (throw) when `GSR_GLUE=real` and neither `AWS_PROFILE` nor
 * `AWS_ACCESS_KEY_ID` is resolvable. Mirrors the Go Makefile hard-fail —
 * silent skip on missing creds would leave the operator thinking they had
 * exercised real Glue when they had not. Throws, does not skip; do NOT
 * wrap this in a try/catch that downgrades the failure to a warning.
 *
 * A no-op when `GSR_GLUE` is not `"real"`; safe to call unconditionally at
 * the top of a real-Glue setup helper.
 */
export function requireRealCreds(): void {
  if (!isRealGlue()) {
    return;
  }
  const hasProfile = (readEnv("AWS_PROFILE") ?? "") !== "";
  const hasAccessKey = (readEnv("AWS_ACCESS_KEY_ID") ?? "") !== "";
  if (!hasProfile && !hasAccessKey) {
    throw new Error(
      "GSR_GLUE=real requires AWS credentials: set AWS_PROFILE or " +
        "AWS_ACCESS_KEY_ID (with AWS_SECRET_ACCESS_KEY). Refusing to run — " +
        "see packages/integration-tests/README-REAL-AWS.md for the runbook.",
    );
  }
}

/**
 * Resolve the target AWS region for Tier-3 runs. Reads `AWS_REGION`, falling
 * back to the ratified default (`us-east-2`) when unset or empty.
 */
export function resolveRegion(): string {
  const region = readEnv("AWS_REGION");
  if (region === undefined || region === "") {
    return DEFAULT_AWS_REGION;
  }
  return region;
}

/**
 * One recorded loud-skip. Emitted per suite that `describeIntegration` had
 * to skip; consumed by the process-exit summary and by tests asserting the
 * shape of the emitted line.
 */
export interface LoudSkipEntry {
  readonly name: string;
  readonly reason: string;
}

/**
 * Backing store for loud-skip entries. Kept module-local so callers cannot
 * mutate history; snapshots are handed out via `getLoudSkipRegistry`.
 */
const loudSkipRegistry: LoudSkipEntry[] = [];

/**
 * Return a shallow copy of every loud-skip recorded so far in this process.
 * Used by tests and the exit-summary printer; callers must not rely on
 * mutating the returned array.
 */
export function getLoudSkipRegistry(): readonly LoudSkipEntry[] {
  return loudSkipRegistry.slice();
}

/**
 * Empty the registry. Intended for unit tests that arrange/inspect a single
 * loud-skip in isolation; production code should never call this.
 */
export function resetLoudSkipRegistry(): void {
  loudSkipRegistry.length = 0;
}

/**
 * Emit a single one-line skip reason and record it in the registry. Exported
 * so `describeIntegration` in the sibling `env-gate.ts` module can share the
 * exact same emit path — the reporter contract lives in exactly one place.
 * The `console.warn` line is the machine-parseable per-suite reason, and
 * the registry drives the aggregated end-of-run count.
 */
export function emitLoudSkip(name: string, reason: string): void {
  loudSkipRegistry.push({ name, reason });
  // eslint-disable-next-line no-console
  console.warn(`SKIP ${name} — ${reason}`);
}

/**
 * Install a one-shot process-exit hook that prints the aggregated skip count
 * plus one bullet per recorded loud-skip. Silent when no suites were skipped
 * so a default `npm test` run (Tier-2/3 files not selected → zero skips)
 * produces no extra output.
 *
 * Idempotent: the guard flag prevents duplicate hooks if the module is
 * evaluated more than once (vitest can re-import module graphs across
 * worker boundaries).
 */
let exitSummaryInstalled = false;
function installExitSummaryHook(): void {
  if (exitSummaryInstalled) {
    return;
  }
  exitSummaryInstalled = true;
  process.on("exit", () => {
    if (loudSkipRegistry.length === 0) {
      return;
    }
    // eslint-disable-next-line no-console
    console.warn(
      `Loud-skipped integration suites: ${loudSkipRegistry.length}`,
    );
    for (const entry of loudSkipRegistry) {
      // eslint-disable-next-line no-console
      console.warn(`  - ${entry.name} — ${entry.reason}`);
    }
  });
}
installExitSummaryHook();

/**
 * Loud-fail helper for feasibility spikes.
 *
 * A spike (e.g. `avro-resolution-spike.test.ts`) exists to render a go/no-go
 * verdict on a shipped-library API. If the precondition the spike depends on
 * is not present at runtime (the API is missing, a required dependency did
 * not load, an environment shape is wrong), the spike must NOT skip silently
 * — a silent skip would look identical to a PASS in the reporter output and
 * bury the go/no-go signal. Instead the spike throws a `SPIKE FAIL: ...`
 * error, which vitest reports as a suite failure.
 *
 * Callers pass a precondition boolean and the reason. When `ok` is true the
 * helper is a no-op; when `ok` is false it throws.
 */
export function assertSpikePrecondition(
  name: string,
  ok: boolean,
  reason: string,
): void {
  if (!ok) {
    throw new Error(`SPIKE FAIL: ${name} — ${reason}`);
  }
}
