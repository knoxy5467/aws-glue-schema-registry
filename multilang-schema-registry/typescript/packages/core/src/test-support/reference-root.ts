/**
 * Read-only resolver for the Java golden-vector oracle used by the
 * wire/compression byte-identity tests.
 *
 * The reference-fixture tree (`multilang-schema-registry/testdata/golden-java`)
 * lives IN THIS MONOREPO as a sibling of `multilang-schema-registry/typescript/`.
 * Resolution precedence:
 *
 *   1. In-repo — walk up from this file to the `multilang-schema-registry/`
 *      root and read `testdata/golden-java` there. This is the standard path
 *      on every clone (fresh clone, CI, developer box).
 *   2. Env override — `GSR_REFERENCE_ROOT` points at an external checkout of
 *      `multilang-schema-registry` (used when regenerating against a
 *      different reference SHA). Appended with `testdata/golden-java`.
 *
 * When neither resolves the tests must SKIP cleanly — a hard ENOENT during
 * collection would fail `npm test`. The `referenceRootExists` flag is
 * designed for `describe.skipIf(!referenceRootExists)`.
 *
 * NOT part of the shipped package surface: this module is under `src/` for
 * TypeScript-project unification but is not re-exported from `index.ts`, so
 * `tsup` (entry `src/index.ts`) will not include it in `dist/`.
 */

import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * The `multilang-schema-registry/` monorepo root computed relative to this
 * module. From `packages/core/src/test-support/reference-root.ts` the mono-
 * repo root sits five levels up: test-support → src → core → packages →
 * typescript → multilang-schema-registry.
 */
const IN_REPO_MONOREPO_ROOT: string = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "..",
  "..",
  "..",
  "..",
);

function resolveReferenceRoot(): string {
  const envRoot = process.env["GSR_REFERENCE_ROOT"];
  if (envRoot !== undefined && envRoot !== "" && existsSync(envRoot)) {
    return envRoot;
  }
  return IN_REPO_MONOREPO_ROOT;
}

/**
 * Resolved reference-repo root. In-repo-first; `GSR_REFERENCE_ROOT` wins only
 * when set to a directory that exists.
 */
export const REFERENCE_ROOT: string = resolveReferenceRoot();

/** `testdata/golden-java` under the resolved reference root. */
export const GOLDEN_JAVA_DIR: string = resolve(REFERENCE_ROOT, "testdata/golden-java");

/**
 * True when the resolved `GOLDEN_JAVA_DIR` exists on disk. Used by tests as
 * `describe.skipIf(!referenceRootExists)` so a fresh clone / CI without the
 * golden vectors cleanly SKIPS the reference-dependent suites instead of
 * throwing ENOENT during test collection.
 */
export const referenceRootExists: boolean = existsSync(GOLDEN_JAVA_DIR);
