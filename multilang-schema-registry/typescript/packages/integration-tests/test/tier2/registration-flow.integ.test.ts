/**
 * Tier-2 fake-backend scenarios for `SchemaRegistrar.getSchemaVersionId`.
 *
 * Each scenario drives one control-flow branch of the register-on-serialize
 * path against an in-memory `FakeGlueClient` — no network, no Docker, no AWS
 * billing. The four branches covered here match the Go reference's
 * `fakeglue`-driven suite beat-for-beat:
 *
 *   1. Auto-register fall-through: `GetSchemaByDefinition` throws
 *      `EntityNotFoundException` on a first-encode → `CreateSchema` runs
 *      and yields the `AVAILABLE` schema-version UUID.
 *   2. Concurrent-producer race: `CreateSchema` throws
 *      `AlreadyExistsException` (a sibling producer won the race) →
 *      the registrar falls through to `RegisterSchemaVersion` and returns
 *      that call's UUID.
 *   3. Async register→poll convergence: `RegisterSchemaVersion` returns
 *      `PENDING`; `GetSchemaVersion` returns `PENDING` twice then
 *      `AVAILABLE` (the `forceRegisterPending`/`forcePendingCount=2`
 *      fake affordance). The sleep seam is instrumented so the poll
 *      interval is verified without real time passing.
 *   4. Cache-hit de-dup: two resolves against the same identity produce
 *      one `getSchemaByDefinition` call and no `createSchema` /
 *      `registerSchemaVersion` calls — the cache short-circuits Glue.
 *
 * The suite is gated by `describeIntegration`, so an integration run with
 * `AWS_INTEGRATION!=1` prints a loud one-line skip reason and never
 * constructs the fake, and the default `npm test` never selects this file
 * at all (the `.integ.test.ts` suffix is excluded by the default vitest
 * config).
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
  makeAlreadyExistsError,
  makeEntityNotFoundError,
} from "../../src/fake-glue.js";

/**
 * A representative Avro identity used across every scenario in this file.
 * The registrar is format-agnostic — the string body is forwarded verbatim
 * to the fake — so any well-formed record schema serves.
 */
const AVRO_ORDER_SCHEMA: SchemaIdentity = {
  schemaName: "orders",
  dataFormat: "AVRO",
  schemaDefinition: '{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}',
};

const SCHEMA_VERSION_ID_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const SCHEMA_VERSION_ID_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

/**
 * Wire a `SchemaRegistrar` against a fake Glue client and a bound metadata
 * writer, with an instrumented `sleep` seam so poll-interval assertions do
 * not depend on real time.
 */
function makeRig(overrides: Partial<GsrConfig> = {}): {
  fake: FakeGlueClient;
  registrar: SchemaRegistrar;
  cache: GsrCache<CachedSchemaVersion>;
  sleep: ReturnType<typeof vi.fn>;
} {
  const fake = new FakeGlueClient({
    versionIds: [SCHEMA_VERSION_ID_A, SCHEMA_VERSION_ID_B],
  });
  const cfg: GsrConfig = {
    ...parseConfig({}),
    schemaAutoRegistrationEnabled: true,
    ...overrides,
  };
  const cache = createCache<CachedSchemaVersion>();
  const metadata = createMetadata(fake, cfg);
  const sleep = vi.fn(async (_ms: number) => undefined);
  const registrar = new SchemaRegistrar(fake, cache, cfg, metadata, { sleep });
  return { fake, registrar, cache, sleep };
}

describeIntegration("tier2/registration-flow", () => {
  let fake: FakeGlueClient;
  let registrar: SchemaRegistrar;
  let cache: GsrCache<CachedSchemaVersion>;
  let sleep: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    ({ fake, registrar, cache, sleep } = makeRig());
  });

  it("auto-registers on first encode when GetSchemaByDefinition throws EntityNotFoundException", async () => {
    // The store is empty and the fake's default behavior is already
    // "throw EntityNotFoundException on miss"; assert explicitly by setting
    // the Force* affordance so the intent is visible in the scenario.
    fake.forceGetSchemaError = makeEntityNotFoundError(
      "no schema version found: schemaName=orders",
    );

    const uuid = await registrar.getSchemaVersionId(AVRO_ORDER_SCHEMA, "orders");

    expect(uuid).toBe(SCHEMA_VERSION_ID_A);
    expect(fake.callCounts.getSchemaByDefinition).toBe(1);
    expect(fake.callCounts.createSchema).toBe(1);
    expect(fake.callCounts.registerSchemaVersion).toBe(0);
    // Metadata flush ran on the write path — transport-key entry is required.
    expect(fake.callCounts.putSchemaVersionMetadata).toBeGreaterThanOrEqual(1);
    // Cache was populated with the resolved UUID.
    expect(cache.get("orders:AVRO")).toEqual({ schemaVersionId: SCHEMA_VERSION_ID_A });
  });

  it("falls through to RegisterSchemaVersion on a concurrent-producer race (CreateSchema throws AlreadyExistsException)", async () => {
    // GetSchemaByDefinition → not found; CreateSchema → race (sibling
    // registered first); registrar falls through to RegisterSchemaVersion.
    fake.forceGetSchemaError = makeEntityNotFoundError();
    fake.forceCreateError = makeAlreadyExistsError(
      "schema already exists: schemaName=orders",
    );

    const uuid = await registrar.getSchemaVersionId(AVRO_ORDER_SCHEMA, "orders");

    // RegisterSchemaVersion's UUID (the first seeded UUID — `createSchema`
    // threw before allocating one).
    expect(uuid).toBe(SCHEMA_VERSION_ID_A);
    expect(fake.callCounts.getSchemaByDefinition).toBe(1);
    expect(fake.callCounts.createSchema).toBe(1);
    expect(fake.callCounts.registerSchemaVersion).toBe(1);
    expect(cache.get("orders:AVRO")).toEqual({ schemaVersionId: SCHEMA_VERSION_ID_A });
  });

  it("polls to AVAILABLE via GetSchemaVersion after two PENDING responses (forceRegisterPending + forcePendingCount=2)", async () => {
    // Force the register→poll branch: GetSchemaByDefinition throws
    // not-found, CreateSchema throws already-exists so we take the
    // registerSchemaVersion path, and forceRegisterPending makes the new
    // version return PENDING twice before AVAILABLE.
    fake.forceGetSchemaError = makeEntityNotFoundError();
    fake.forceCreateError = makeAlreadyExistsError();
    fake.forceRegisterPending = true;
    fake.forcePendingCount = 2;

    const uuid = await registrar.getSchemaVersionId(AVRO_ORDER_SCHEMA, "orders");

    expect(uuid).toBe(SCHEMA_VERSION_ID_A);
    expect(fake.callCounts.registerSchemaVersion).toBe(1);
    // 2 PENDING polls + 1 AVAILABLE poll = 3 `getSchemaVersion` calls.
    expect(fake.callCounts.getSchemaVersion).toBe(3);
    // Sleep-before-each-poll: three polls → three sleeps at 3000 ms each.
    expect(sleep).toHaveBeenCalledTimes(3);
    for (const [ms] of sleep.mock.calls) {
      expect(ms).toBe(3_000);
    }
    expect(cache.get("orders:AVRO")).toEqual({ schemaVersionId: SCHEMA_VERSION_ID_A });
  });

  it("de-dupes a repeated resolve via the cache — the second call touches no Glue method", async () => {
    // First resolve: happy-path auto-register fall-through.
    fake.forceGetSchemaError = makeEntityNotFoundError();
    const first = await registrar.getSchemaVersionId(AVRO_ORDER_SCHEMA, "orders");
    expect(first).toBe(SCHEMA_VERSION_ID_A);

    const countsAfterFirst = { ...fake.callCounts };

    // Second resolve: cache hit — no Glue traffic, no metadata flush.
    const second = await registrar.getSchemaVersionId(AVRO_ORDER_SCHEMA, "orders");
    expect(second).toBe(SCHEMA_VERSION_ID_A);

    expect(fake.callCounts.getSchemaByDefinition).toBe(countsAfterFirst.getSchemaByDefinition);
    expect(fake.callCounts.createSchema).toBe(countsAfterFirst.createSchema);
    expect(fake.callCounts.registerSchemaVersion).toBe(countsAfterFirst.registerSchemaVersion);
    expect(fake.callCounts.putSchemaVersionMetadata).toBe(
      countsAfterFirst.putSchemaVersionMetadata,
    );
  });

  it("de-dupes concurrent first-resolves onto one Glue call (in-flight singleflight)", async () => {
    fake.forceGetSchemaError = makeEntityNotFoundError();

    // Fire N concurrent resolves for the same identity before any of them
    // settle. The in-flight map should collapse them onto one Glue call.
    const [a, b, c] = await Promise.all([
      registrar.getSchemaVersionId(AVRO_ORDER_SCHEMA, "orders"),
      registrar.getSchemaVersionId(AVRO_ORDER_SCHEMA, "orders"),
      registrar.getSchemaVersionId(AVRO_ORDER_SCHEMA, "orders"),
    ]);

    expect(a).toBe(SCHEMA_VERSION_ID_A);
    expect(b).toBe(SCHEMA_VERSION_ID_A);
    expect(c).toBe(SCHEMA_VERSION_ID_A);
    expect(fake.callCounts.getSchemaByDefinition).toBe(1);
    expect(fake.callCounts.createSchema).toBe(1);
  });

  it("surfaces a poll-time SDK rejection as GsrRegistrationError (instanceof, .cause preserved)", async () => {
    // Poll-loop failure: RegisterSchemaVersion goes pending, then the
    // GetSchemaVersion call itself rejects. The registrar must wrap and
    // preserve `.cause`.
    fake.forceGetSchemaError = makeEntityNotFoundError();
    fake.forceCreateError = makeAlreadyExistsError();
    fake.forceRegisterPending = true;
    fake.forcePendingCount = 1;
    const pollError = new Error("connection reset");
    pollError.name = "NetworkingError";
    fake.forceGetVersionError = pollError;

    let caught: unknown;
    try {
      await registrar.getSchemaVersionId(AVRO_ORDER_SCHEMA, "orders");
    } catch (err) {
      caught = err;
    }

    // Cross-package instanceof — relies on the tsup-externalize fix so a
    // single class identity exists across the @gsr/core → @gsr/serde →
    // @gsr/integration-tests boundary.
    expect(caught).toBeInstanceOf(GsrRegistrationError);
    expect((caught as GsrRegistrationError).cause).toBe(pollError);
  });
});
