/**
 * Tier-1 unit test for the fake Glue backend's own state machine.
 *
 * Runs offline under the default `npm test`; no network, no Docker, no AWS.
 * Exercises: definition→version indexing, call counters, each `Force*`
 * affordance, the async poll-to-`AVAILABLE` convergence, and the
 * `.name`-discriminated error factories.
 */
import { describe, expect, it, beforeEach } from "vitest";
import { classifyGlueError, type GlueClient } from "@gsr/core";

import {
  FakeGlueClient,
  GLUE_ERROR_NAMES,
  makeAccessDeniedError,
  makeAlreadyExistsError,
  makeEntityNotFoundError,
  makeThrottlingError,
} from "./fake-glue.js";

describe("FakeGlueClient — GlueClient interface conformance", () => {
  it("satisfies the @gsr/core GlueClient interface (compile-checked structural typing)", () => {
    // Assigning to a `GlueClient`-typed local is a compile-time proof that
    // every seam method exists with the right signature. If a method drops
    // or drifts, tsc fails this file at build time.
    const seam: GlueClient = new FakeGlueClient();
    expect(typeof seam.getSchemaByDefinition).toBe("function");
    expect(typeof seam.getSchemaVersion).toBe("function");
    expect(typeof seam.createSchema).toBe("function");
    expect(typeof seam.registerSchemaVersion).toBe("function");
    expect(typeof seam.putSchemaVersionMetadata).toBe("function");
    expect(typeof seam.querySchemaVersionMetadata).toBe("function");
    expect(typeof seam.getTags).toBe("function");
  });
});

describe("FakeGlueClient — call counters", () => {
  let fake: FakeGlueClient;
  beforeEach(() => {
    fake = new FakeGlueClient();
  });

  it("starts every counter at zero", () => {
    expect(fake.callCounts).toEqual({
      getSchemaByDefinition: 0,
      getSchemaVersion: 0,
      createSchema: 0,
      registerSchemaVersion: 0,
      putSchemaVersionMetadata: 0,
      querySchemaVersionMetadata: 0,
      getTags: 0,
    });
  });

  it("returns a snapshot copy, not a live reference", () => {
    const snap = fake.callCounts;
    // Snapshotting and then advancing a counter must not mutate the snapshot.
    void fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    expect(snap.createSchema).toBe(0);
  });

  it("increments each counter exactly once per call, including on failure", async () => {
    fake.forceGetSchemaError = makeEntityNotFoundError();
    await expect(
      fake.getSchemaByDefinition({
        SchemaId: { RegistryName: "r", SchemaName: "s" },
        SchemaDefinition: "{}",
      }),
    ).rejects.toThrow();
    expect(fake.callCounts.getSchemaByDefinition).toBe(1);

    await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    expect(fake.callCounts.createSchema).toBe(1);
  });
});

describe("FakeGlueClient — reset()", () => {
  it("clears store, counters, and Force* fields", async () => {
    const fake = new FakeGlueClient();
    fake.forceCreateError = makeAlreadyExistsError();
    fake.forceGetSchemaError = makeEntityNotFoundError();
    fake.forceGetVersionError = makeThrottlingError();
    fake.forceRegisterPending = true;
    fake.forcePendingCount = 3;

    // Stage some state via a create.
    fake.forceCreateError = null; // let create through
    await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    expect(fake.callCounts.createSchema).toBe(1);

    fake.reset();

    expect(fake.callCounts).toEqual({
      getSchemaByDefinition: 0,
      getSchemaVersion: 0,
      createSchema: 0,
      registerSchemaVersion: 0,
      putSchemaVersionMetadata: 0,
      querySchemaVersionMetadata: 0,
      getTags: 0,
    });
    expect(fake.forceCreateError).toBeNull();
    expect(fake.forceGetSchemaError).toBeNull();
    expect(fake.forceGetVersionError).toBeNull();
    expect(fake.forceRegisterPending).toBe(false);
    expect(fake.forcePendingCount).toBe(0);

    // After reset, a lookup for the same schema must now not find it.
    await expect(
      fake.getSchemaByDefinition({
        SchemaId: { RegistryName: "r", SchemaName: "s" },
        SchemaDefinition: "{}",
      }),
    ).rejects.toMatchObject({ name: GLUE_ERROR_NAMES.EntityNotFound });
  });
});

describe("FakeGlueClient — definition→version indexing", () => {
  it("createSchema stores the definition; getSchemaByDefinition returns the same versionId", async () => {
    const fake = new FakeGlueClient({
      versionIds: ["11111111-1111-1111-1111-111111111111"],
    });
    const created = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    expect(created.SchemaVersionId).toBe(
      "11111111-1111-1111-1111-111111111111",
    );

    const got = await fake.getSchemaByDefinition({
      SchemaId: { RegistryName: "r", SchemaName: "s" },
      SchemaDefinition: "{}",
    });
    expect(got.SchemaVersionId).toBe("11111111-1111-1111-1111-111111111111");
    expect(got.Status).toBe("AVAILABLE");
  });

  it("unknown definition throws EntityNotFoundException classifying as entity-not-found", async () => {
    const fake = new FakeGlueClient();
    let caught: unknown;
    try {
      await fake.getSchemaByDefinition({
        SchemaId: { RegistryName: "r", SchemaName: "s" },
        SchemaDefinition: "{}",
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).name).toBe(GLUE_ERROR_NAMES.EntityNotFound);
    expect(classifyGlueError(caught)).toBe("entity-not-found");
  });

  it("createSchema for an existing definition throws AlreadyExistsException", async () => {
    const fake = new FakeGlueClient();
    await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    let caught: unknown;
    try {
      await fake.createSchema({
        RegistryId: { RegistryName: "r" },
        SchemaName: "s",
        DataFormat: "AVRO",
        SchemaDefinition: "{}",
      });
    } catch (err) {
      caught = err;
    }
    expect((caught as Error).name).toBe(GLUE_ERROR_NAMES.AlreadyExists);
    expect(classifyGlueError(caught)).toBe("already-exists");
  });

  it("distinguishes different registry/name/definition triples in the key", async () => {
    const fake = new FakeGlueClient();
    await fake.createSchema({
      RegistryId: { RegistryName: "r1" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    // Same schema name + definition, DIFFERENT registry → separate key.
    await fake.createSchema({
      RegistryId: { RegistryName: "r2" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    const a = await fake.getSchemaByDefinition({
      SchemaId: { RegistryName: "r1", SchemaName: "s" },
      SchemaDefinition: "{}",
    });
    const b = await fake.getSchemaByDefinition({
      SchemaId: { RegistryName: "r2", SchemaName: "s" },
      SchemaDefinition: "{}",
    });
    expect(a.SchemaVersionId).not.toBe(b.SchemaVersionId);
  });
});

describe("FakeGlueClient — registerSchemaVersion", () => {
  it("registers a new version and stores it", async () => {
    const fake = new FakeGlueClient();
    // Seed one version via create so the (registry, name) pair exists.
    await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: '{"v":1}',
    });
    const reg = await fake.registerSchemaVersion({
      SchemaId: { RegistryName: "r", SchemaName: "s" },
      SchemaDefinition: '{"v":2}',
    });
    expect(typeof reg.SchemaVersionId).toBe("string");
    expect(reg.Status).toBe("AVAILABLE");

    // A follow-on GetSchemaByDefinition on the new definition returns the
    // register-produced versionId.
    const got = await fake.getSchemaByDefinition({
      SchemaId: { RegistryName: "r", SchemaName: "s" },
      SchemaDefinition: '{"v":2}',
    });
    expect(got.SchemaVersionId).toBe(reg.SchemaVersionId);
  });

  it("is idempotent on an identical (name, definition) — returns the existing versionId", async () => {
    const fake = new FakeGlueClient({
      versionIds: ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"],
    });
    await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    const first = await fake.registerSchemaVersion({
      SchemaId: { RegistryName: "r", SchemaName: "s" },
      SchemaDefinition: "{}",
    });
    const second = await fake.registerSchemaVersion({
      SchemaId: { RegistryName: "r", SchemaName: "s" },
      SchemaDefinition: "{}",
    });
    expect(first.SchemaVersionId).toBe("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    expect(second.SchemaVersionId).toBe(first.SchemaVersionId);
  });
});

describe("FakeGlueClient — Force* affordances", () => {
  it("forceGetSchemaError makes getSchemaByDefinition throw the exact configured error", async () => {
    const fake = new FakeGlueClient();
    // Even after seeding the definition — force wins.
    await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    fake.forceGetSchemaError = makeThrottlingError("rate limited");
    await expect(
      fake.getSchemaByDefinition({
        SchemaId: { RegistryName: "r", SchemaName: "s" },
        SchemaDefinition: "{}",
      }),
    ).rejects.toMatchObject({
      name: GLUE_ERROR_NAMES.Throttling,
      message: "rate limited",
    });
  });

  it("forceCreateError makes createSchema throw the configured error even for a fresh definition", async () => {
    const fake = new FakeGlueClient();
    fake.forceCreateError = makeAlreadyExistsError();
    await expect(
      fake.createSchema({
        RegistryId: { RegistryName: "r" },
        SchemaName: "s",
        DataFormat: "AVRO",
        SchemaDefinition: "{}",
      }),
    ).rejects.toMatchObject({ name: GLUE_ERROR_NAMES.AlreadyExists });
    // Store must NOT have advanced — a forced throw is a pure error path.
    fake.forceCreateError = null;
    await expect(
      fake.getSchemaByDefinition({
        SchemaId: { RegistryName: "r", SchemaName: "s" },
        SchemaDefinition: "{}",
      }),
    ).rejects.toMatchObject({ name: GLUE_ERROR_NAMES.EntityNotFound });
  });

  it("forceGetVersionError makes getSchemaVersion throw the configured error", async () => {
    const fake = new FakeGlueClient();
    const created = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    fake.forceGetVersionError = makeAccessDeniedError();
    await expect(
      fake.getSchemaVersion({ SchemaVersionId: created.SchemaVersionId! }),
    ).rejects.toMatchObject({ name: GLUE_ERROR_NAMES.AccessDenied });
  });
});

describe("FakeGlueClient — poll-to-AVAILABLE convergence", () => {
  it("forceRegisterPending + forcePendingCount=2 makes getSchemaVersion return PENDING twice, then AVAILABLE", async () => {
    const fake = new FakeGlueClient();
    fake.forceRegisterPending = true;
    fake.forcePendingCount = 2;

    const created = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    // The create-response reports the pending state via `SchemaVersionStatus`
    // (the `CreateSchemaResponse`-specific first-version status field, not
    // the `Status` used elsewhere on the seam).
    expect(created.SchemaVersionStatus).toBe("PENDING");

    const id = created.SchemaVersionId!;
    const first = await fake.getSchemaVersion({ SchemaVersionId: id });
    expect(first.Status).toBe("PENDING");
    const second = await fake.getSchemaVersion({ SchemaVersionId: id });
    expect(second.Status).toBe("PENDING");
    const third = await fake.getSchemaVersion({ SchemaVersionId: id });
    expect(third.Status).toBe("AVAILABLE");
  });

  it("forcePendingCount=0 with forceRegisterPending=true yields AVAILABLE on the first poll", async () => {
    const fake = new FakeGlueClient();
    fake.forceRegisterPending = true;
    fake.forcePendingCount = 0;
    const created = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    expect(created.SchemaVersionStatus).toBe("AVAILABLE");
    const poll = await fake.getSchemaVersion({
      SchemaVersionId: created.SchemaVersionId!,
    });
    expect(poll.Status).toBe("AVAILABLE");
  });
});

describe("FakeGlueClient — metadata write/read", () => {
  it("putSchemaVersionMetadata stores the pair and querySchemaVersionMetadata flattens it", async () => {
    const fake = new FakeGlueClient();
    const created = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    const id = created.SchemaVersionId!;
    await fake.putSchemaVersionMetadata({
      SchemaVersionId: id,
      MetadataKeyValue: { MetadataKey: "k1", MetadataValue: "v1" },
    });
    await fake.putSchemaVersionMetadata({
      SchemaVersionId: id,
      MetadataKeyValue: { MetadataKey: "k2", MetadataValue: "v2" },
    });
    const q = await fake.querySchemaVersionMetadata({ SchemaVersionId: id });
    expect(q.MetadataInfoMap).toEqual({
      k1: { MetadataValue: "v1" },
      k2: { MetadataValue: "v2" },
    });
  });

  it("re-writing an existing metadata key throws AlreadyExistsException (matches real Glue)", async () => {
    const fake = new FakeGlueClient();
    const created = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    const id = created.SchemaVersionId!;
    await fake.putSchemaVersionMetadata({
      SchemaVersionId: id,
      MetadataKeyValue: { MetadataKey: "k", MetadataValue: "v1" },
    });
    await expect(
      fake.putSchemaVersionMetadata({
        SchemaVersionId: id,
        MetadataKeyValue: { MetadataKey: "k", MetadataValue: "v2" },
      }),
    ).rejects.toMatchObject({ name: GLUE_ERROR_NAMES.AlreadyExists });
  });
});

describe("FakeGlueClient — tags", () => {
  it("createSchema tags round-trip through getTags via the synthesised schema ARN", async () => {
    const fake = new FakeGlueClient();
    const created = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
      Tags: { env: "prod", owner: "alice" },
    });
    const arn = `arn:aws:glue:us-east-2:000000000000:schema/r/s`;
    const tags = await fake.getTags({ ResourceArn: arn });
    expect(tags.Tags).toEqual({ env: "prod", owner: "alice" });

    // Sanity: peekVersion sees the same tags.
    const peek = fake.peekVersion(created.SchemaVersionId!);
    expect(peek?.tags).toEqual({ env: "prod", owner: "alice" });
  });

  it("getTags for an unknown ARN returns an empty map", async () => {
    const fake = new FakeGlueClient();
    const tags = await fake.getTags({
      ResourceArn: "arn:aws:glue:us-east-2:000000000000:schema/nope/nope",
    });
    expect(tags.Tags).toEqual({});
  });
});

describe("FakeGlueClient — named-error factories", () => {
  it("each factory produces an Error whose .name matches the expected exception and classifyGlueError agrees", () => {
    const notFound = makeEntityNotFoundError();
    expect(notFound).toBeInstanceOf(Error);
    expect(notFound.name).toBe(GLUE_ERROR_NAMES.EntityNotFound);
    expect(classifyGlueError(notFound)).toBe("entity-not-found");

    const already = makeAlreadyExistsError();
    expect(already.name).toBe(GLUE_ERROR_NAMES.AlreadyExists);
    expect(classifyGlueError(already)).toBe("already-exists");

    const throttled = makeThrottlingError();
    expect(throttled.name).toBe(GLUE_ERROR_NAMES.Throttling);
    // Throttling is not on the classifier's branching path — it maps to
    // `other` so the registrar propagates it as `GsrRegistrationError`.
    expect(classifyGlueError(throttled)).toBe("other");

    const denied = makeAccessDeniedError();
    expect(denied.name).toBe(GLUE_ERROR_NAMES.AccessDenied);
    expect(classifyGlueError(denied)).toBe("other");
  });

  it("carries a caller-supplied message when provided", () => {
    const e = makeEntityNotFoundError("custom reason");
    expect(e.message).toBe("custom reason");
  });
});

describe("FakeGlueClient — deterministic UUID generation", () => {
  it("seeded UUIDs are handed out in order; then falls back to a monotonic default", async () => {
    const fake = new FakeGlueClient({
      versionIds: [
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
      ],
    });
    const a = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s1",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    const b = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s2",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    const c = await fake.createSchema({
      RegistryId: { RegistryName: "r" },
      SchemaName: "s3",
      DataFormat: "AVRO",
      SchemaDefinition: "{}",
    });
    expect(a.SchemaVersionId).toBe("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    expect(b.SchemaVersionId).toBe("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    // Third one exhausts the seed list; the generator produces a
    // deterministic id from the internal counter.
    expect(c.SchemaVersionId).toMatch(/^00000000-0000-0000-0000-[0-9a-f]{12}$/);
  });
});
