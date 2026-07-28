/**
 * Guard-rail unit tests for `scripts/check-no-ai-artifacts.mjs`.
 *
 * The script is the CI gate that fails the build on any token from the
 * committed pattern set (documented in the script's own header). This
 * suite exercises the script's `--test` self-test AND runs it against the
 * live tree, so a whitelist regression is caught by `npm test` — not only
 * by the dedicated CI step in `typescript-ci.yml`.
 *
 * The tests are shell-outs to the script rather than in-process imports so
 * the script stays a plain Node CLI with no test-only surface. Both runs
 * cost the same as a `node` startup plus a small file walk (~200ms
 * combined on a fresh checkout).
 */

import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const HERE = dirname(fileURLToPath(import.meta.url));
// Walk from `packages/integration-tests/test/gate/` to
// `typescript/scripts/check-no-ai-artifacts.mjs`.
const TYPESCRIPT_ROOT = resolve(HERE, "..", "..", "..", "..");
const SCRIPT_PATH = resolve(
  TYPESCRIPT_ROOT,
  "scripts",
  "check-no-ai-artifacts.mjs",
);

describe("check-no-ai-artifacts CLI", () => {
  it("passes its own self-test suite (`--test`)", () => {
    const result = spawnSync("node", [SCRIPT_PATH, "--test"], {
      cwd: TYPESCRIPT_ROOT,
      encoding: "utf8",
    });
    // Surface any failure directly on the assertion output.
    if (result.status !== 0) {
      throw new Error(
        `self-test failed with status ${result.status}\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
      );
    }
    expect(result.status).toBe(0);
    expect(result.stdout).toContain("SELF-TEST PASSED");
  });

  it("returns exit 0 on the clean tree (no artifact tokens present)", () => {
    const result = spawnSync("node", [SCRIPT_PATH], {
      cwd: TYPESCRIPT_ROOT,
      encoding: "utf8",
    });
    if (result.status !== 0) {
      throw new Error(
        `scan reported hits — the tree is NOT clean.\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
      );
    }
    expect(result.status).toBe(0);
    // Confidence check: the OK banner reports the file count so a
    // silent-skip regression (walk skipping every file) would flip this.
    expect(result.stdout).toMatch(/OK — 0 hits \(scanned \d+ files\)/);
  });
});
