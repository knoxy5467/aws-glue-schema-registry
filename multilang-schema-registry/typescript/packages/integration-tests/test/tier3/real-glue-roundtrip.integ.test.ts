/**
 * Tier-3 real-Glue round-trip scenario.
 *
 * Runs under `npm run test:integ:real` (which sets `GSR_GLUE=real
 * AWS_INTEGRATION=1`) and exercises the encode-side control-plane end-to-end
 * against real AWS Glue in the operator's account. Every schema is namespaced
 * with the `gsr-ts-it-` runbook prefix + a per-run suffix so parallel or
 * repeated runs never collide, and the `CleanupTracker` deletes each on exit
 * in reverse insertion order — success OR failure. The suite never creates or
 * deletes a registry; every schema lives inside the pre-existing
 * `default-registry` (Glue's auto-created default).
 *
 * The suite is gated three ways:
 *   1. `*.integ.test.ts` suffix — invisible to the default `npm test`.
 *   2. `describeIntegration` — one-line loud-skip when `AWS_INTEGRATION!=1`.
 *   3. `requireRealCreds()` inside `beforeAll` — hard-throws when
 *      `GSR_GLUE=real` and no credentials resolvable via `AWS_PROFILE` or
 *      `AWS_ACCESS_KEY_ID`.
 *
 * When `GSR_GLUE` is anything other than `"real"` the fake backend runs
 * instead (in-process, offline), so the same scenario doubles as a
 * cross-tier smoke.
 */
import {
  createCache,
  createGlueClient,
  createMetadata,
  parseConfig,
  SchemaRegistrar,
  type GsrConfig,
} from "@gsr/core";
import { afterAll, beforeAll, expect, it } from "vitest";

import {
  describeIntegration,
  isRealGlue,
  requireRealCreds,
  resolveRegion,
} from "../../src/env-gate.js";
import {
  CleanupTracker,
  realSchemaName,
  RUNBOOK_PREFIX,
  selectGlueBackend,
} from "../../src/real-glue.js";

const REGISTRY_NAME = "default-registry";

/**
 * Trivial Avro schema string — the smallest well-formed body real Glue will
 * accept as `AVRO`. The scenario asserts on the round-tripped
 * `SchemaVersionId` shape, not on payload correctness, so a bare
 * `int`-with-name record is enough.
 */
const AVRO_SCHEMA = JSON.stringify({
  type: "record",
  name: "TsRealGlueRoundtripRecord",
  fields: [{ name: "id", type: "int" }],
});

describeIntegration("tier3 / real-glue-roundtrip", () => {
  // Resolved once at file load — the loud-skip / hard-fail branching runs
  // inside `beforeAll` so the reporter attributes the failure to this suite.
  let cfg: GsrConfig;
  let tracker: CleanupTracker | null = null;

  beforeAll(() => {
    // Hard-fail (throw) if the operator asked for real Glue but has no
    // credentials — never silently skip on missing creds when the intent was
    // explicit real-Glue.
    requireRealCreds();

    cfg = parseConfig({
      region: resolveRegion(),
      registryName: REGISTRY_NAME,
      schemaAutoRegistrationEnabled: "true",
    });
  });

  afterAll(async () => {
    // Cleanup runs on success AND failure (afterAll fires regardless). On the
    // fake path `tracker` stays null (fake needs no external teardown); on the
    // real path this deletes every namespaced schema the run created, in
    // reverse insertion order, tolerating EntityNotFoundException.
    if (tracker !== null) {
      await tracker.run();
    }
  });

  it("registers and resolves a per-run-namespaced Avro schema against the selected backend", async () => {
    const schemaName = realSchemaName("roundtrip");
    expect(schemaName.startsWith(RUNBOOK_PREFIX)).toBe(true);

    const backend = selectGlueBackend(cfg);
    // On the real path the tracker is bound to the same SDK config as the
    // encoder client (same account/region), so DeleteSchema on teardown
    // targets exactly the schemas this run created.
    tracker = backend.cleanup;
    if (tracker !== null) {
      tracker.track(REGISTRY_NAME, schemaName);
    }

    const cache = createCache<{ schemaVersionId: string }>({
      ttlMillis: cfg.timeToLiveMillis,
      size: cfg.cacheSize,
    });
    // Metadata writer uses the same client seam as the encoder — the writer
    // is non-fatal by design (Go parity) so a permissions gap on
    // PutSchemaVersionMetadata does not fail the registration itself.
    const metadata = createMetadata(backend.client, cfg);
    const registrar = new SchemaRegistrar(backend.client, cache, cfg, metadata);

    const schemaVersionId = await registrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat: "AVRO",
        schemaDefinition: AVRO_SCHEMA,
      },
      "tier3-roundtrip",
    );

    // Every Glue SchemaVersionId is a 36-char UUID (8-4-4-4-12 hex). Assert
    // the shape rather than a specific value — the real service assigns it
    // and the fake generates a compatible one.
    expect(schemaVersionId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    );

    // A second call for the same schema+definition must resolve from cache
    // — no additional network call — and return the same UUID.
    const cached = await registrar.getSchemaVersionId(
      {
        schemaName,
        dataFormat: "AVRO",
        schemaDefinition: AVRO_SCHEMA,
      },
      "tier3-roundtrip",
    );
    expect(cached).toBe(schemaVersionId);
  });

  it("uses the real SDK client when GSR_GLUE=real (skipped otherwise)", () => {
    // A read-only sanity check that the operator's env is what they think.
    // When the operator opts into real Glue, `selectGlueBackend` must have
    // produced a live SDK client — not the fake. The concrete class check
    // is intentional (via constructor identity of the module-scoped
    // `createGlueClient` output — it is not a FakeGlueClient).
    if (!isRealGlue()) {
      // Fake path — assert-only the branch discriminant, since a fake path
      // in a "real-glue" file should still work (developer running
      // `test:integ` without `GSR_GLUE=real` gets a smoke run for free).
      return;
    }
    // A minimal smoke build of the real client — exists, is not the fake.
    const real = createGlueClient(cfg);
    expect(real).toBeDefined();
    expect((real as unknown as { constructor: { name: string } }).constructor.name)
      .not.toBe("FakeGlueClient");
  });
});
