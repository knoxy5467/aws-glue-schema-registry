/**
 * Tier-2 error-path scenarios for `SchemaRegistrar.getSchemaVersionId`.
 *
 * `classifyGlueError` discriminates on the caught error's `.name` and drives
 * the registrar's branching: only `EntityNotFoundException` falls through to
 * auto-register, only `AlreadyExistsException` falls through to
 * `RegisterSchemaVersion` on the `CreateSchema` step, and everything else —
 * `ThrottlingException`, `AccessDeniedException`, `NetworkingError`, an
 * unnamed error — must surface as `GsrRegistrationError` wrapping the
 * original on `.cause`. These scenarios drive the "everything else" bucket
 * with named errors that mirror AWS SDK v3 shape names.
 *
 * Every assertion uses cross-package `instanceof` on the `@gsr/core` error
 * class rather than a `.name` string match — relies on the tsup-externalize
 * fix so a single class identity exists across the package boundary.
 */
import { beforeEach, expect, it, vi } from "vitest";

import {
  createCache,
  createMetadata,
  GsrRegistrationError,
  parseConfig,
  SchemaRegistrar,
  type CachedSchemaVersion,
  type GsrCache,
  type GsrConfig,
  type SchemaIdentity,
} from "@gsr/core";

import { describeIntegration } from "../../src/env-gate.js";
import {
  FakeGlueClient,
  makeAccessDeniedError,
  makeThrottlingError,
} from "../../src/fake-glue.js";

const IDENTITY: SchemaIdentity = {
  schemaName: "orders",
  dataFormat: "AVRO",
  schemaDefinition: '{"type":"record","name":"Order","fields":[]}',
};

const SCHEMA_VERSION_ID_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

function makeRig(overrides: Partial<GsrConfig> = {}): {
  fake: FakeGlueClient;
  registrar: SchemaRegistrar;
  cache: GsrCache<CachedSchemaVersion>;
} {
  const fake = new FakeGlueClient({ versionIds: [SCHEMA_VERSION_ID_A] });
  const cfg: GsrConfig = {
    ...parseConfig({}),
    schemaAutoRegistrationEnabled: true,
    ...overrides,
  };
  const cache = createCache<CachedSchemaVersion>();
  const metadata = createMetadata(fake, cfg);
  const sleep = vi.fn(async (_ms: number) => undefined);
  const registrar = new SchemaRegistrar(fake, cache, cfg, metadata, { sleep });
  return { fake, registrar, cache };
}

describeIntegration("tier2/error-paths", () => {
  let fake: FakeGlueClient;
  let registrar: SchemaRegistrar;
  let cache: GsrCache<CachedSchemaVersion>;

  beforeEach(() => {
    ({ fake, registrar, cache } = makeRig());
  });

  it("surfaces ThrottlingException on GetSchemaByDefinition as GsrRegistrationError with .cause preserved", async () => {
    const throttled = makeThrottlingError("Rate exceeded");
    fake.forceGetSchemaError = throttled;

    let caught: unknown;
    try {
      await registrar.getSchemaVersionId(IDENTITY, "orders");
    } catch (err) {
      caught = err;
    }

    expect(caught).toBeInstanceOf(GsrRegistrationError);
    expect((caught as GsrRegistrationError).cause).toBe(throttled);
    // Only the read call fired — the registrar bails before touching the
    // write path.
    expect(fake.callCounts.getSchemaByDefinition).toBe(1);
    expect(fake.callCounts.createSchema).toBe(0);
    expect(fake.callCounts.registerSchemaVersion).toBe(0);
    // The cache is untouched — a failing lookup must not poison the key.
    expect(cache.get("orders:AVRO")).toBeUndefined();
  });

  it("surfaces AccessDeniedException on GetSchemaByDefinition as GsrRegistrationError with .cause preserved", async () => {
    const denied = makeAccessDeniedError(
      "User is not authorized to perform: glue:GetSchemaByDefinition",
    );
    fake.forceGetSchemaError = denied;

    let caught: unknown;
    try {
      await registrar.getSchemaVersionId(IDENTITY, "orders");
    } catch (err) {
      caught = err;
    }

    expect(caught).toBeInstanceOf(GsrRegistrationError);
    expect((caught as GsrRegistrationError).cause).toBe(denied);
    expect(fake.callCounts.getSchemaByDefinition).toBe(1);
    expect(fake.callCounts.createSchema).toBe(0);
  });

  it("surfaces AccessDeniedException on CreateSchema as GsrRegistrationError with .cause preserved", async () => {
    // GetSchemaByDefinition returns EntityNotFound (fake default), so the
    // control flow enters the auto-register branch; CreateSchema then
    // trips the IAM deny.
    const denied = makeAccessDeniedError(
      "User is not authorized to perform: glue:CreateSchema",
    );
    fake.forceCreateError = denied;

    let caught: unknown;
    try {
      await registrar.getSchemaVersionId(IDENTITY, "orders");
    } catch (err) {
      caught = err;
    }

    expect(caught).toBeInstanceOf(GsrRegistrationError);
    expect((caught as GsrRegistrationError).cause).toBe(denied);
    expect(fake.callCounts.createSchema).toBe(1);
    expect(fake.callCounts.registerSchemaVersion).toBe(0);
    // A failing write must not populate the cache.
    expect(cache.get("orders:AVRO")).toBeUndefined();
  });

  it("does not retain the in-flight promise after a failing lookup — a second call is entitled to retry", async () => {
    fake.forceGetSchemaError = makeThrottlingError();

    await expect(
      registrar.getSchemaVersionId(IDENTITY, "orders"),
    ).rejects.toBeInstanceOf(GsrRegistrationError);

    // The registrar's in-flight `Map` deletes the entry on settle (success
    // OR failure). Prove that behaviour by unforcing the throttle and
    // re-resolving — the second call reaches the fake and succeeds.
    fake.forceGetSchemaError = null;
    const uuid = await registrar.getSchemaVersionId(IDENTITY, "orders");
    expect(uuid).toBe(SCHEMA_VERSION_ID_A);
    // Two GetSchemaByDefinition calls in total: one failed, one succeeded.
    expect(fake.callCounts.getSchemaByDefinition).toBe(2);
  });
});
