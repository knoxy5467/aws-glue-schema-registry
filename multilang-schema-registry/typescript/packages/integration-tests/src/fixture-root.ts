/**
 * Single fixture-root resolver plus a `listFixtures` walk and a SHA-256
 * manifest helper.
 *
 * The shared reference-repo `shared/test/` tree carries the Avro schemas,
 * protobuf schemas, JSON-Schema documents, and JSON instances that every
 * language's client uses to prove parity. The offline wire-byte-identity
 * gate under `packages/integration-tests/test/gate/` has its own inline
 * resolver pointing at a fixed reference-checkout path; this module is the
 * shared successor used by every Tier-2/3 integration test that needs to
 * enumerate the fixtures without hard-coding a path.
 *
 * Resolution precedence (highest to lowest):
 *   1. In-repo sibling — `<repo-root>/multilang-schema-registry/shared/test`
 *      relative to this file's location. Used once the shared fixture tree
 *      is co-located inside this repo.
 *   2. Env override — `GSR_REFERENCE_ROOT` (pointing at a Go reference
 *      checkout of `multilang-schema-registry`); the resolver appends
 *      `/shared/test`. Matches the existing wire-byte-identity gate's env
 *      variable so a local operator that already has `GSR_REFERENCE_ROOT`
 *      exported keeps it working.
 *
 * `resolveFixtureRoot` accepts an options bag so unit tests can inject
 * `envRoot`, `inRepoCandidate`, and an `exists` predicate — the module can
 * then be exercised offline in Tier-1 with no real filesystem access.
 *
 * `listFixtures(kind)` enumerates every `.avsc` (Avro), `.proto` (Protobuf),
 * or `.schema.json` (JSON-Schema) file under the resolved root, returning
 * absolute and relative paths so callers can hand them straight to the
 * serde slices. `computeFixtureManifest` produces a stable sorted array of
 * `{relativePath, kind, sha256}` entries which the fixture-sweep test
 * compares against the checked-in `fixture-manifest.json` to fail on drift
 * (adding, removing, or mutating a fixture without a manifest update).
 */

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * The three fixture kinds the sweep covers, keyed by file extension:
 * `avsc` = `.avsc`, `proto` = `.proto`, `jsonschema` = `.schema.json`. Kept
 * as a union rather than an enum so `listFixtures(kind)` reads at call sites
 * as `listFixtures("avsc")`.
 */
export type FixtureKind = "avsc" | "proto" | "jsonschema";

/**
 * A fixture discovered by `listFixtures`. `absolutePath` is the resolved
 * on-disk path suitable for `readFileSync`; `relativePath` is
 * shared-test-root-relative (POSIX separators for cross-platform stability)
 * so it can act as a manifest key.
 */
export interface ListedFixture {
  readonly absolutePath: string;
  readonly relativePath: string;
  readonly kind: FixtureKind;
}

/**
 * A single manifest row. `sha256` is the hex digest of the raw on-disk
 * bytes; the manifest is a sorted array so adding a fixture is a diff-visible
 * change.
 */
export interface FixtureManifestEntry {
  readonly relativePath: string;
  readonly kind: FixtureKind;
  readonly sha256: string;
}

/**
 * On-disk manifest shape (`fixture-manifest.json`).
 */
export interface FixtureManifest {
  readonly root: "shared/test";
  readonly entries: readonly FixtureManifestEntry[];
}

/** Options for `resolveFixtureRoot` — every field is injectable for tests. */
export interface ResolveFixtureRootOptions {
  /** Overrides the in-repo candidate path. */
  readonly inRepoCandidate?: string;
  /** Overrides `process.env.GSR_REFERENCE_ROOT`. */
  readonly envRoot?: string | undefined;
  /** Predicate used to check whether a candidate directory exists. */
  readonly exists?: (path: string) => boolean;
}

/**
 * Compute the in-repo candidate path relative to this module's location.
 * From `packages/integration-tests/src/fixture-root.ts` the shared tree sits
 * at `../../../../shared/test` — four levels up gets us to
 * `multilang-schema-registry/`, then `shared/test` selects the fixture root.
 *
 * Exported so callers can inspect what the default candidate is, and so
 * unit tests can compare against it without recomputing the arithmetic.
 */
export const IN_REPO_SHARED_TEST_PATH: string = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "..",
  "..",
  "..",
  "shared",
  "test",
);

/**
 * Read the env var lazily on every call so tests that stub the environment
 * (e.g. `vi.stubEnv`) observe the change. Same discipline as `env-gate.ts`.
 */
function currentEnvRoot(): string | undefined {
  return process.env["GSR_REFERENCE_ROOT"];
}

/**
 * Resolve the shared/test fixture-root directory.
 *
 * Precedence:
 *   1. `inRepoCandidate` (defaults to `IN_REPO_SHARED_TEST_PATH`) — used
 *      when the shared fixtures are co-located in this repo.
 *   2. `envRoot ?? process.env.GSR_REFERENCE_ROOT` — appended with
 *      `shared/test` — used to point at a Go reference checkout.
 *
 * Throws with a diagnostic listing everything it tried when neither
 * candidate resolves to an existing directory. Callers should treat this
 * as fatal: the whole shared-fixture surface is inaccessible.
 */
export function resolveFixtureRoot(opts?: ResolveFixtureRootOptions): string {
  const inRepo = opts?.inRepoCandidate ?? IN_REPO_SHARED_TEST_PATH;
  const envRoot = opts?.envRoot ?? currentEnvRoot();
  const exists = opts?.exists ?? existsSyncPredicate;

  if (exists(inRepo)) {
    return inRepo;
  }

  if (envRoot !== undefined && envRoot !== "") {
    const envPath = join(envRoot, "shared", "test");
    if (exists(envPath)) {
      return envPath;
    }
    throw new Error(
      `shared-fixture root not found. Tried in-repo path "${inRepo}" and ` +
        `GSR_REFERENCE_ROOT-derived "${envPath}"; neither exists.`,
    );
  }

  throw new Error(
    `shared-fixture root not found. Tried in-repo path "${inRepo}"; ` +
      `GSR_REFERENCE_ROOT is unset. Set GSR_REFERENCE_ROOT to a checkout ` +
      `of multilang-schema-registry, or co-locate the shared/test tree ` +
      `in-repo.`,
  );
}

function existsSyncPredicate(path: string): boolean {
  try {
    return existsSync(path) && statSync(path).isDirectory();
  } catch {
    return false;
  }
}

/**
 * Extension-to-kind lookup. `*.schema.json` (JSON-Schema documents) are the
 * only `.json` files the sweep classifies; the companion `*.data.json`
 * instances are NOT fixtures themselves — they are inputs to the JSON-Schema
 * round-trip step and are keyed by their neighbouring schema.
 */
function classify(fileName: string): FixtureKind | undefined {
  if (fileName.endsWith(".avsc")) return "avsc";
  if (fileName.endsWith(".proto")) return "proto";
  if (fileName.endsWith(".schema.json")) return "jsonschema";
  return undefined;
}

/**
 * Walk `dir` recursively and return every file path, follow-directory-only
 * (no symlink chasing). Returned in `readdirSync` order per directory; the
 * caller sorts.
 */
function walkFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const info = statSync(full);
    if (info.isDirectory()) {
      out.push(...walkFiles(full));
    } else if (info.isFile()) {
      out.push(full);
    }
  }
  return out;
}

/**
 * Normalize an absolute file path to a POSIX-separator path relative to
 * `root`. The manifest uses POSIX separators so it stays stable across
 * Windows / POSIX builders.
 */
function toRelativePosix(root: string, absolute: string): string {
  const withoutRoot = absolute.startsWith(root)
    ? absolute.slice(root.length)
    : absolute;
  const trimmed = withoutRoot.replace(/^[/\\]+/, "");
  return trimmed.split(/[/\\]/).join("/");
}

/**
 * Enumerate every fixture of the given kind under `root` (defaults to the
 * resolved fixture root). Results are sorted by relative path so callers
 * (and the manifest) get a stable order.
 */
export function listFixtures(
  kind: FixtureKind,
  root: string = resolveFixtureRoot(),
): readonly ListedFixture[] {
  const files = walkFiles(root);
  const out: ListedFixture[] = [];
  for (const abs of files) {
    const fileKind = classify(abs);
    if (fileKind !== kind) continue;
    out.push({
      absolutePath: abs,
      relativePath: toRelativePosix(root, abs),
      kind: fileKind,
    });
  }
  out.sort((a, b) => a.relativePath.localeCompare(b.relativePath));
  return out;
}

/**
 * SHA-256 of a file's raw bytes as a hex string. Used to detect content
 * drift: if a fixture's bytes change, the sweep fails until the manifest
 * is regenerated to match.
 */
export function hashFile(absolutePath: string): string {
  const bytes = readFileSync(absolutePath);
  return createHash("sha256").update(bytes).digest("hex");
}

/**
 * Compute the manifest for `root` (defaults to the resolved fixture root):
 * every `.avsc`, `.proto`, and `.schema.json` file under the tree with its
 * SHA-256. Entries are sorted by relative path so the JSON representation
 * is stable and diffs cleanly.
 */
export function computeFixtureManifest(
  root: string = resolveFixtureRoot(),
): FixtureManifest {
  const kinds: readonly FixtureKind[] = ["avsc", "proto", "jsonschema"];
  const entries: FixtureManifestEntry[] = [];
  for (const kind of kinds) {
    for (const fixture of listFixtures(kind, root)) {
      entries.push({
        relativePath: fixture.relativePath,
        kind,
        sha256: hashFile(fixture.absolutePath),
      });
    }
  }
  entries.sort((a, b) => a.relativePath.localeCompare(b.relativePath));
  return { root: "shared/test", entries };
}
