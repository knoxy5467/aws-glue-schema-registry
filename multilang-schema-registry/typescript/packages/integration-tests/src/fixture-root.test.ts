/**
 * Tier-1 unit tests for the fixture-root resolver.
 *
 * The resolver is pure filesystem/env plumbing so its whole surface is
 * exercisable with an injected `exists` predicate and stubbed env, with no
 * real filesystem access. The sweep test file — living under
 * `packages/integration-tests/test/fixtures/fixture-sweep.integ.test.ts` —
 * asserts the resolver against the real on-disk tree and is gated by
 * `AWS_INTEGRATION=1`, so it stays out of the default `npm test` run.
 */

import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  IN_REPO_SHARED_TEST_PATH,
  computeFixtureManifest,
  hashFile,
  listFixtures,
  resolveFixtureRoot,
} from "./fixture-root.js";

describe("resolveFixtureRoot", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("returns the in-repo candidate when it exists", () => {
    const candidate = "/repo/multilang-schema-registry/shared/test";
    const exists = vi.fn((p: string) => p === candidate);
    const resolved = resolveFixtureRoot({
      inRepoCandidate: candidate,
      envRoot: "/somewhere-else",
      exists,
    });
    expect(resolved).toBe(candidate);
    // The env override is never consulted when the in-repo path exists —
    // the fn is called exactly once, with the in-repo candidate.
    expect(exists).toHaveBeenCalledTimes(1);
    expect(exists).toHaveBeenCalledWith(candidate);
  });

  it("falls back to GSR_REFERENCE_ROOT/shared/test when the in-repo path is missing", () => {
    const inRepo = "/repo/multilang-schema-registry/shared/test";
    const envRoot = "/checkout/multilang-schema-registry";
    const envPath = "/checkout/multilang-schema-registry/shared/test";
    const exists = vi.fn((p: string) => p === envPath);
    const resolved = resolveFixtureRoot({
      inRepoCandidate: inRepo,
      envRoot,
      exists,
    });
    expect(resolved).toBe(envPath);
  });

  it("reads GSR_REFERENCE_ROOT from process.env when envRoot is not injected", () => {
    const inRepo = "/repo/multilang-schema-registry/shared/test";
    const envRoot = "/from-env/multilang-schema-registry";
    const envPath = "/from-env/multilang-schema-registry/shared/test";
    vi.stubEnv("GSR_REFERENCE_ROOT", envRoot);
    const exists = vi.fn((p: string) => p === envPath);
    const resolved = resolveFixtureRoot({
      inRepoCandidate: inRepo,
      exists,
    });
    expect(resolved).toBe(envPath);
  });

  it("throws when neither candidate exists and the env is set", () => {
    const inRepo = "/repo/multilang-schema-registry/shared/test";
    const envRoot = "/checkout/multilang-schema-registry";
    const exists = vi.fn(() => false);
    expect(() =>
      resolveFixtureRoot({
        inRepoCandidate: inRepo,
        envRoot,
        exists,
      }),
    ).toThrowError(/shared-fixture root not found/);
  });

  it("throws with a diagnostic naming both candidates when the env is set but neither exists", () => {
    const inRepo = "/repo/multilang-schema-registry/shared/test";
    const envRoot = "/checkout/multilang-schema-registry";
    let caught: unknown;
    try {
      resolveFixtureRoot({
        inRepoCandidate: inRepo,
        envRoot,
        exists: () => false,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    const message = (caught as Error).message;
    expect(message).toContain(inRepo);
    expect(message).toContain("shared/test");
    expect(message).toContain(envRoot);
  });

  it("throws with a hint about GSR_REFERENCE_ROOT when the env is unset and in-repo is missing", () => {
    vi.stubEnv("GSR_REFERENCE_ROOT", "");
    const inRepo = "/repo/multilang-schema-registry/shared/test";
    let caught: unknown;
    try {
      resolveFixtureRoot({
        inRepoCandidate: inRepo,
        envRoot: undefined,
        exists: () => false,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    const message = (caught as Error).message;
    expect(message).toContain("GSR_REFERENCE_ROOT is unset");
    expect(message).toContain(inRepo);
  });

  it("treats an empty-string GSR_REFERENCE_ROOT as unset (no path join)", () => {
    // `resolveFixtureRoot` must not try to resolve `"" + "/shared/test"`
    // — an empty env value is functionally the same as unset.
    vi.stubEnv("GSR_REFERENCE_ROOT", "");
    const inRepo = "/repo/multilang-schema-registry/shared/test";
    let caught: unknown;
    try {
      resolveFixtureRoot({
        inRepoCandidate: inRepo,
        envRoot: undefined,
        exists: () => false,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    const message = (caught as Error).message;
    expect(message).toContain("GSR_REFERENCE_ROOT is unset");
    // No `/shared/test` from the env fork appears in the diagnostic
    // because the env path was never constructed.
    expect(message).not.toMatch(/Tried .*GSR_REFERENCE_ROOT.* "\/shared\/test"/);
  });

  it("exports the in-repo candidate path pointing at shared/test", () => {
    // Sanity check: the derivation from `import.meta.url` climbs four
    // levels to `multilang-schema-registry/` and appends `shared/test`.
    // We only assert the tail — the leading path depends on the checkout.
    expect(IN_REPO_SHARED_TEST_PATH.endsWith("/shared/test")).toBe(true);
    expect(IN_REPO_SHARED_TEST_PATH).toContain(
      "multilang-schema-registry",
    );
  });
});

describe("listFixtures + manifest helpers (pure logic)", () => {
  // These paths and hashes exercise the enumeration / hashing helpers
  // without touching the real fixture tree — they operate against
  // fixture-root-relative synthetic directories written to a tmp dir.

  it("hashFile returns a hex sha256 for known bytes", () => {
    // Hash a tmp file with known contents. The sha of "hello" is a
    // well-known value; asserting it fixes any accidental swap to a
    // different hash algorithm or encoding.
    const { root, cleanup } = makeTempFixtureTree({
      "avro/test.avsc": "hello",
    });
    try {
      const digest = hashFile(join(root, "avro/test.avsc"));
      expect(digest).toBe(
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
      );
    } finally {
      cleanup();
    }
  });

  it("listFixtures enumerates only .avsc when kind='avsc'", () => {
    const { root, cleanup } = makeTempFixtureTree({
      "avro/none/test_v1.avsc": "{}",
      "avro/backward/user_v1.avsc": "{}",
      "protos/basic.proto": "syntax = \"proto3\";",
      "jsonschema/none/product_v1.schema.json": "{}",
      "jsonschema/none/product_v1.data.json": "{}",
    });
    try {
      const avros = listFixtures("avsc", root);
      const rels = avros.map((f) => f.relativePath);
      expect(rels).toEqual([
        "avro/backward/user_v1.avsc",
        "avro/none/test_v1.avsc",
      ]);
      // Absolute paths are absolute and end with the relative fragment.
      for (const f of avros) {
        expect(f.absolutePath.endsWith(f.relativePath)).toBe(true);
        expect(f.kind).toBe("avsc");
      }
    } finally {
      cleanup();
    }
  });

  it("listFixtures enumerates only .proto when kind='proto'", () => {
    const { root, cleanup } = makeTempFixtureTree({
      "protos/basic.proto": "syntax = \"proto3\";",
      "protos/google/type/money.proto": "syntax = \"proto3\";",
      "avro/none/test.avsc": "{}",
    });
    try {
      const protos = listFixtures("proto", root);
      const rels = protos.map((f) => f.relativePath);
      expect(rels).toEqual([
        "protos/basic.proto",
        "protos/google/type/money.proto",
      ]);
    } finally {
      cleanup();
    }
  });

  it("listFixtures enumerates only .schema.json (not .data.json) when kind='jsonschema'", () => {
    const { root, cleanup } = makeTempFixtureTree({
      "jsonschema/none/product_v1.schema.json": "{}",
      "jsonschema/none/product_v1.data.json": "{}",
      "jsonschema/full/event_v1.schema.json": "{}",
      "jsonschema/full/event_v1.data.json": "{}",
      "avro/none/test.avsc": "{}",
    });
    try {
      const jsons = listFixtures("jsonschema", root);
      const rels = jsons.map((f) => f.relativePath);
      // Only the two `.schema.json` files; `.data.json` companions are
      // deliberately excluded — they are inputs to the round-trip step,
      // not fixtures the sweep verifies as schemas.
      expect(rels).toEqual([
        "jsonschema/full/event_v1.schema.json",
        "jsonschema/none/product_v1.schema.json",
      ]);
    } finally {
      cleanup();
    }
  });

  it("computeFixtureManifest emits sorted entries with SHA-256 per file", () => {
    const { root, cleanup } = makeTempFixtureTree({
      "avro/none/test.avsc": "{\"type\":\"record\",\"name\":\"T\",\"fields\":[]}",
      "protos/basic.proto": "syntax = \"proto3\";",
      "jsonschema/none/p.schema.json": "{\"type\":\"object\"}",
      "jsonschema/none/p.data.json": "{}",
      "configs/ignored.properties": "not-a-fixture=1",
    });
    try {
      const manifest = computeFixtureManifest(root);
      expect(manifest.root).toBe("shared/test");
      const rels = manifest.entries.map((e) => e.relativePath);
      expect(rels).toEqual([
        "avro/none/test.avsc",
        "jsonschema/none/p.schema.json",
        "protos/basic.proto",
      ]);
      // Every SHA-256 is 64 hex chars.
      for (const e of manifest.entries) {
        expect(e.sha256).toMatch(/^[0-9a-f]{64}$/);
      }
      // The manifest sort is total: two calls produce the same shape.
      const again = computeFixtureManifest(root);
      expect(again).toEqual(manifest);
    } finally {
      cleanup();
    }
  });
});

/**
 * Create a fresh tmp directory populated with the given `path → contents`
 * mapping (paths are relative). Returns the root plus a `cleanup` that
 * removes the whole tree.
 */
function makeTempFixtureTree(files: Record<string, string>): {
  root: string;
  cleanup: () => void;
} {
  const root = mkdtempSync(join(tmpdir(), "gsr-fixture-root-"));
  for (const [rel, contents] of Object.entries(files)) {
    const full = join(root, rel);
    mkdirSync(dirname(full), { recursive: true });
    writeFileSync(full, contents, "utf8");
  }
  return {
    root,
    cleanup: () => {
      rmSync(root, { recursive: true, force: true });
    },
  };
}
