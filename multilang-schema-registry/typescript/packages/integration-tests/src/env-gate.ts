/**
 * Two-gate discipline for the integration test tier — public entrypoint.
 *
 * This module is the vitest-side wrapper. The pure runtime helpers
 * (`isAwsIntegrationEnabled`, `isRealGlue`, `requireAwsIntegration`,
 * `requireRealCreds`, `resolveRegion`, `DEFAULT_AWS_REGION`, the loud-skip
 * registry, `assertSpikePrecondition`) live in `env-gate.runtime.ts` and are
 * re-exported below so every existing test-side import keeps working
 * byte-identically. The one addition here is `describeIntegration`, which
 * needs vitest's `describe` — importing that at module scope is fine for
 * test files but poisonous to any non-vitest consumer (the narrated demo
 * entrypoint, any future diagnostic tool) because vitest's internal-state
 * guard aborts the module graph. Non-vitest consumers MUST import from
 * `./env-gate.runtime.js` directly.
 *
 * TypeScript has no build tags, so the tiered test discipline is realized as
 * a compile/select-time gate (the `*.integ.test.ts` suffix, globbed only by
 * the integration vitest config) STACKED WITH a runtime env gate. Every
 * Tier-2/3 suite must call `requireAwsIntegration()` at the top of its
 * `describe` (or use `describeIntegration`), so that even if a caller runs
 * the integration project directly, an unset `AWS_INTEGRATION` still
 * short-circuits the suite before any AWS SDK client is constructed.
 */

import { describe } from "vitest";

import {
  emitLoudSkip,
  requireAwsIntegration,
} from "./env-gate.runtime.js";

export {
  DEFAULT_AWS_REGION,
  assertSpikePrecondition,
  emitLoudSkip,
  getLoudSkipRegistry,
  isAwsIntegrationEnabled,
  isRealGlue,
  requireAwsIntegration,
  requireRealCreds,
  resetLoudSkipRegistry,
  resolveRegion,
  type LoudSkipEntry,
  type SuiteGateResult,
} from "./env-gate.runtime.js";

/**
 * Signature of a vitest `describe` callback body.
 */
type DescribeBody = () => void | Promise<void>;

/**
 * Wraps `describe(name, fn)` with the Tier-2/3 env gate: when
 * `AWS_INTEGRATION=1` the suite runs, else the suite is registered as
 * skipped and a single line is emitted to the reporter naming the suite
 * and the missing gate. Callers use this instead of a raw `describe.skip`
 * so no suite is silently skipped.
 *
 * A downstream module hardens the reporter contract (aggregated skip-count
 * summary + machine-parseable skip lines); this implementation provides the
 * per-suite one-line reason that contract is built on.
 */
export function describeIntegration(name: string, fn: DescribeBody): void {
  const gate = requireAwsIntegration();
  if (gate.skipped) {
    emitLoudSkip(name, gate.reason);
    describe.skip(name, fn);
    return;
  }
  describe(name, fn);
}
