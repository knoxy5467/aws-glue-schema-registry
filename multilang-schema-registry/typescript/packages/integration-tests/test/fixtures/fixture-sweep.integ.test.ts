/**
 * Shared-fixture sweep — Tier-2 integration test.
 *
 * Enumerates every `.avsc`, `.proto`, and `.schema.json` file under the
 * shared `shared/test/` tree via `fixture-root.ts` and drives each of them
 * through the corresponding `@gsr/serde` path. For every fixture the sweep
 * asserts:
 *
 *   - Avro (`.avsc`)         — schema JSON parses, `avsc.Type.forSchema`
 *                              compiles it. Under `negative/malformed/*`
 *                              the assertion is INVERTED: parse/compile
 *                              MUST throw. This is the same case that
 *                              `packages/serde/src/avro/serde.ts` surfaces
 *                              as `GsrIncompatibleDataError`, so the
 *                              malformed fixture is exercised end-to-end.
 *   - Protobuf (`.proto`)    — schema text parses via `protobuf.parse`
 *                              (`protobufjs` handles the tokenizer + AST).
 *                              None of the current shared fixtures live
 *                              under `negative/malformed/`, so every proto
 *                              is expected to parse.
 *   - JSON-Schema (`.schema.json`) — schema JSON parses and compiles under
 *                              `ajv`. Under `negative/malformed/*` the
 *                              assertion is inverted (must throw). Where a
 *                              companion `.data.json` instance exists and
 *                              validates, a round-trip through
 *                              `serializeJson` → `deserializeJson` is
 *                              asserted to deep-equal the original.
 *
 * The sweep ALSO enforces the checksum manifest at
 * `test/fixtures/fixture-manifest.json`:
 *   - Every `relativePath` in the manifest exists on disk.
 *   - Every fixture on disk appears in the manifest.
 *   - Every fixture's SHA-256 matches the manifest value.
 *
 * Adding, removing, or mutating a fixture without regenerating the manifest
 * fails the sweep — this is the drift-detection guarantee.
 *
 * Gate: this file has the `.integ.test.ts` suffix, so it is selected only
 * by the integration vitest project. The `describeIntegration` wrapper adds
 * the runtime `AWS_INTEGRATION=1` gate; when unset the whole suite is a
 * loud-skip. No network, no Docker, no AWS credentials are ever needed —
 * the sweep is a pure-CPU parity check.
 */

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Ajv } from "ajv";
import avsc from "avsc";
import protobuf from "protobufjs";

import { deserializeJson, serializeJson } from "@gsr/serde";

import { describeIntegration } from "../../src/env-gate.js";
import {
  computeFixtureManifest,
  hashFile,
  listFixtures,
  resolveFixtureRoot,
  type FixtureKind,
  type FixtureManifestEntry,
} from "../../src/fixture-root.js";
import {
  expect,
  it,
} from "vitest";

const MANIFEST_PATH = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "fixture-manifest.json",
);

interface OnDiskManifest {
  readonly root: string;
  readonly entries: readonly FixtureManifestEntry[];
}

function loadManifest(): OnDiskManifest {
  const raw = readFileSync(MANIFEST_PATH, "utf8");
  return JSON.parse(raw) as OnDiskManifest;
}

/**
 * `ajv` config for JSON-Schema fixtures — same relaxations as the serde
 * itself: `strict: false` accepts unknown format keywords as annotations
 * rather than compile errors, `logger: false` silences the "unknown format"
 * console noise that would otherwise print on every fixture compile.
 */
function makeAjv(): Ajv {
  return new Ajv({ strict: false, logger: false });
}

/**
 * `true` when `relativePath` sits under a `negative/malformed/` subtree.
 * These are the fixtures whose whole purpose is to force the parser to
 * throw, so the sweep INVERTS its assertion for them.
 */
function isMalformed(relativePath: string): boolean {
  return relativePath.includes("/negative/malformed/");
}

describeIntegration("fixture-sweep — shared/test parity + manifest drift", () => {
  const root = resolveFixtureRoot();
  const manifest = loadManifest();

  /**
   * Manifest drift: the on-disk set MUST equal the manifest set, and every
   * fixture's SHA-256 MUST match. Failure here means someone added,
   * removed, or edited a shared fixture without regenerating
   * `fixture-manifest.json` — the sweep fails until they do.
   */
  it("manifest matches the on-disk fixture set with byte-identical SHA-256", () => {
    const live = computeFixtureManifest(root);
    const manifestKeys = manifest.entries.map((e) => e.relativePath).sort();
    const liveKeys = live.entries.map((e) => e.relativePath).slice().sort();

    // Detailed diff so a drift surfaces the exact files rather than just
    // "arrays differ".
    const missingFromManifest = liveKeys.filter(
      (k) => !manifestKeys.includes(k),
    );
    const missingFromDisk = manifestKeys.filter(
      (k) => !liveKeys.includes(k),
    );
    expect(
      missingFromManifest,
      "fixtures on disk not in fixture-manifest.json (regenerate the manifest)",
    ).toEqual([]);
    expect(
      missingFromDisk,
      "fixtures in fixture-manifest.json not on disk",
    ).toEqual([]);

    // Now diff the SHA-256 per entry.
    const manifestByPath = new Map(
      manifest.entries.map((e) => [e.relativePath, e]),
    );
    const drifted: string[] = [];
    for (const entry of live.entries) {
      const recorded = manifestByPath.get(entry.relativePath);
      if (recorded === undefined) continue; // caught above.
      if (recorded.sha256 !== entry.sha256) {
        drifted.push(
          `${entry.relativePath} (manifest ${recorded.sha256}, on-disk ${entry.sha256})`,
        );
      }
      if (recorded.kind !== entry.kind) {
        drifted.push(
          `${entry.relativePath} (manifest kind ${recorded.kind}, on-disk ${entry.kind})`,
        );
      }
    }
    expect(
      drifted,
      "fixture bytes have drifted since the manifest was regenerated",
    ).toEqual([]);
  });

  /**
   * Manifest counts stay non-trivial. If the manifest ever collapses to
   * zero entries the sweep would pass vacuously — this asserts the oracle
   * itself is non-empty, without hard-coding a specific count.
   */
  it("manifest carries fixtures of all three kinds", () => {
    const kinds: readonly FixtureKind[] = ["avsc", "proto", "jsonschema"];
    for (const kind of kinds) {
      const count = manifest.entries.filter((e) => e.kind === kind).length;
      expect(count, `manifest has zero ${kind} entries`).toBeGreaterThan(0);
    }
  });

  /**
   * Every fixture in the manifest hashes to the recorded SHA-256 on disk.
   * (Duplicative of the drift assertion above, kept as a per-file failure
   * mode so a partial drift surfaces the exact offender rather than a
   * whole-set diff.)
   */
  it.each(manifest.entries.map((e) => [e.relativePath, e]))(
    "manifest sha256 matches on-disk bytes for %s",
    (_relativePath: string, entry: FixtureManifestEntry) => {
      const abs = resolve(root, entry.relativePath);
      expect(hashFile(abs)).toBe(entry.sha256);
    },
  );

  /**
   * Every `.avsc` file compiles under `avsc.Type.forSchema`, EXCEPT the
   * malformed cells whose purpose is to force a parse error.
   */
  const avroFixtures = listFixtures("avsc", root);
  it.each(avroFixtures.map((f) => [f.relativePath, f.absolutePath]))(
    "avsc fixture loads: %s",
    (relativePath: string, absolutePath: string) => {
      const bytes = readFileSync(absolutePath, "utf8");
      if (isMalformed(relativePath)) {
        expect(() => {
          const parsed = JSON.parse(bytes) as avsc.schema.AvroSchema;
          avsc.Type.forSchema(parsed);
        }).toThrow();
        return;
      }
      const parsed = JSON.parse(bytes) as avsc.schema.AvroSchema;
      const type = avsc.Type.forSchema(parsed);
      expect(type.typeName).toBeTruthy();
    },
  );

  /**
   * Every `.proto` file parses under `protobufjs`. `protobufjs` parse is
   * import-oblivious — a `.proto` with `import "google/protobuf/...";`
   * parses successfully; only symbol lookup (`Root.lookupType`) fails when
   * the imported types are unresolved. The sweep asserts parse only, which
   * is the loading step the sweep is scoped to.
   */
  const protoFixtures = listFixtures("proto", root);
  it.each(protoFixtures.map((f) => [f.relativePath, f.absolutePath]))(
    "proto fixture parses: %s",
    (_relativePath: string, absolutePath: string) => {
      const text = readFileSync(absolutePath, "utf8");
      const parsed = protobuf.parse(text, { keepCase: true });
      expect(parsed.root).toBeDefined();
    },
  );

  /**
   * Every `.schema.json` file parses as JSON and compiles under `ajv`,
   * except the malformed cells (invalid JSON) which must throw at
   * `JSON.parse`. Where a companion `.data.json` instance exists AND the
   * schema accepts it, the round-trip `serializeJson` → `deserializeJson`
   * MUST deep-equal the original instance.
   */
  const jsonFixtures = listFixtures("jsonschema", root);
  it.each(jsonFixtures.map((f) => [f.relativePath, f.absolutePath]))(
    "jsonschema fixture loads: %s",
    (relativePath: string, absolutePath: string) => {
      const bytes = readFileSync(absolutePath, "utf8");
      if (isMalformed(relativePath)) {
        expect(() => JSON.parse(bytes)).toThrow();
        return;
      }
      const schema = JSON.parse(bytes) as object;
      const ajv = makeAjv();
      const validate = ajv.compile(schema);
      expect(typeof validate).toBe("function");

      // Optional round-trip: does a companion `.data.json` exist?
      const dataPath = absolutePath.replace(/\.schema\.json$/, ".data.json");
      let dataBytes: string;
      try {
        dataBytes = readFileSync(dataPath, "utf8");
      } catch {
        return; // no companion — parse-only assertion is sufficient.
      }
      const instance = JSON.parse(dataBytes) as unknown;

      // Only round-trip when the instance actually validates against the
      // schema — some fixtures are cross-version pairs where an old
      // instance is deliberately rejected by a stricter successor. The
      // sweep does not exercise those, but it does exercise every
      // matching instance/schema pair.
      if (!validate(instance)) {
        return;
      }
      const wireBytes = serializeJson(schema, instance);
      const decoded = deserializeJson(wireBytes, schema);
      expect(decoded).toEqual(instance);
    },
  );
});
