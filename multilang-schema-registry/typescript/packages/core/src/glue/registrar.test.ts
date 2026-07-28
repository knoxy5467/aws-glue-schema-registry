import { describe, expect, it, vi } from "vitest";

import { createCache, type GsrCache } from "../cache/lru-cache.js";
import { parseConfig, type GsrConfig } from "../config/config.js";
import {
  GsrAutoRegistrationDisabledError,
  GsrRegistrationError,
} from "../errors/gsr-errors.js";
import type { GlueClient } from "./client-seam.js";
import type { MetadataWriter } from "./metadata.js";
import {
  SchemaRegistrar,
  type CachedSchemaVersion,
  type SchemaIdentity,
} from "./registrar.js";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const AVRO_SCHEMA: SchemaIdentity = {
  schemaName: "test-schema",
  dataFormat: "AVRO",
  schemaDefinition: '{"type":"record","name":"R","fields":[]}',
};

const UUID_1 = "11111111-1111-1111-1111-111111111111";
const UUID_2 = "22222222-2222-2222-2222-222222222222";

/**
 * Build a `GlueClient` fake where every method is a vitest spy resolving to
 * an empty `$metadata`-only response. Individual tests override the specific
 * calls they exercise.
 */
function makeGlueClient(): GlueClient & {
  [K in keyof GlueClient]: ReturnType<typeof vi.fn>;
} {
  return {
    getSchemaByDefinition: vi.fn(async () => ({ $metadata: {} })),
    getSchemaVersion: vi.fn(async () => ({ $metadata: {} })),
    createSchema: vi.fn(async () => ({ $metadata: {} })),
    registerSchemaVersion: vi.fn(async () => ({ $metadata: {} })),
    putSchemaVersionMetadata: vi.fn(async () => ({ $metadata: {} })),
    querySchemaVersionMetadata: vi.fn(async () => ({ $metadata: {} })),
    getTags: vi.fn(async () => ({ $metadata: {} })),
  };
}

function makeMetadataWriter(): MetadataWriter & {
  putSchemaVersionMetadataBatch: ReturnType<typeof vi.fn>;
} {
  return {
    putSchemaVersionMetadataBatch: vi.fn(async () => undefined),
  };
}

/** A minimally-populated Glue-typed error — `.name` is what `classifyGlueError` reads. */
function glueError(name: string, message = name): Error {
  const err = new Error(message);
  err.name = name;
  return err;
}

/**
 * A `sleep` seam that resolves synchronously — tests never wait real time.
 * The vitest spy exposes call args for assertions on the poll interval.
 */
function makeSleep(): (ms: number) => Promise<void> {
  return vi.fn(async () => undefined);
}

/**
 * Build a `SchemaRegistrar` wired against a caller-supplied fake `GlueClient`
 * and metadata writer, using the real `createCache` from the cache module so
 * cache behavior is end-to-end covered.
 */
function makeRegistrar(
  client: GlueClient,
  metadata: MetadataWriter,
  overrides: Partial<GsrConfig> = {},
  sleep: (ms: number) => Promise<void> = makeSleep(),
): {
  registrar: SchemaRegistrar;
  cache: GsrCache<CachedSchemaVersion>;
  cfg: GsrConfig;
  sleep: (ms: number) => Promise<void>;
} {
  const cfg: GsrConfig = { ...parseConfig({}), ...overrides };
  const cache = createCache<CachedSchemaVersion>();
  const registrar = new SchemaRegistrar(client, cache, cfg, metadata, {
    sleep,
  });
  return { registrar, cache, cfg, sleep };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SchemaRegistrar — cache fast path", () => {
  it("returns the cached UUID with no Glue call and no metadata flush on a hit", async () => {
    const client = makeGlueClient();
    const metadata = makeMetadataWriter();
    const { registrar, cache } = makeRegistrar(client, metadata);

    // Pre-populate the cache directly to isolate the fast-path behavior.
    cache.set("test-schema:AVRO", { schemaVersionId: UUID_1 });

    const got = await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");

    expect(got).toBe(UUID_1);
    expect(client.getSchemaByDefinition).not.toHaveBeenCalled();
    expect(client.createSchema).not.toHaveBeenCalled();
    expect(client.registerSchemaVersion).not.toHaveBeenCalled();
    expect(metadata.putSchemaVersionMetadataBatch).not.toHaveBeenCalled();
  });

  it("keys the cache by schemaName + dataFormat — same name under two formats is disjoint", async () => {
    const client = makeGlueClient();
    const metadata = makeMetadataWriter();
    const { registrar, cache } = makeRegistrar(client, metadata);

    cache.set("test-schema:AVRO", { schemaVersionId: UUID_1 });
    cache.set("test-schema:PROTOBUF", { schemaVersionId: UUID_2 });

    const gotAvro = await registrar.getSchemaVersionId(AVRO_SCHEMA, "t");
    const gotProto = await registrar.getSchemaVersionId(
      { ...AVRO_SCHEMA, dataFormat: "PROTOBUF" },
      "t",
    );

    expect(gotAvro).toBe(UUID_1);
    expect(gotProto).toBe(UUID_2);
    expect(client.getSchemaByDefinition).not.toHaveBeenCalled();
  });
});

describe("SchemaRegistrar — GetSchemaByDefinition happy path", () => {
  it("returns the UUID and caches it when the definition is AVAILABLE", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "AVAILABLE",
    });
    const metadata = makeMetadataWriter();
    const { registrar, cache } = makeRegistrar(client, metadata);

    const got = await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");

    expect(got).toBe(UUID_1);
    expect(cache.get("test-schema:AVRO")).toEqual({ schemaVersionId: UUID_1 });
    expect(client.createSchema).not.toHaveBeenCalled();
    // Existing-schema fast path does not flush metadata (Java parity — flush
    // fires only on the mutating branches).
    expect(metadata.putSchemaVersionMetadataBatch).not.toHaveBeenCalled();
  });

  it("hits the cache on the second call and issues no further Glue call", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "AVAILABLE",
    });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata);

    const first = await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");
    const second = await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");

    expect(first).toBe(UUID_1);
    expect(second).toBe(UUID_1);
    expect(client.getSchemaByDefinition).toHaveBeenCalledTimes(1);
    expect(metadata.putSchemaVersionMetadataBatch).not.toHaveBeenCalled();
  });

  it("forwards RegistryName and SchemaDefinition on GetSchemaByDefinition", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "AVAILABLE",
    });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      registryName: "my-registry",
    });

    await registrar.getSchemaVersionId(AVRO_SCHEMA, "t");

    expect(client.getSchemaByDefinition).toHaveBeenCalledWith({
      SchemaId: {
        RegistryName: "my-registry",
        SchemaName: "test-schema",
      },
      SchemaDefinition: AVRO_SCHEMA.schemaDefinition,
    });
  });
});

describe("SchemaRegistrar — error discrimination", () => {
  it("propagates a non-EntityNotFound Glue error as GsrRegistrationError, and never calls CreateSchema", async () => {
    const client = makeGlueClient();
    const denied = glueError("AccessDeniedException", "you shall not pass");
    client.getSchemaByDefinition.mockRejectedValueOnce(denied);
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    await expect(
      registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
    ).rejects.toBeInstanceOf(GsrRegistrationError);

    expect(client.createSchema).not.toHaveBeenCalled();
  });

  it("preserves the original SDK error on GsrRegistrationError.cause (raw, not double-wrapped)", async () => {
    const client = makeGlueClient();
    const denied = glueError("AccessDeniedException");
    client.getSchemaByDefinition.mockRejectedValueOnce(denied);
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    let caught: unknown;
    try {
      await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");
    } catch (err) {
      caught = err;
    }

    expect(caught).toBeInstanceOf(GsrRegistrationError);
    expect((caught as GsrRegistrationError).cause).toBe(denied);
  });

  it("classifies on the outer error only — an already-wrapped registration error would NOT be treated as entity-not-found", async () => {
    // Sanity check for the classifyGlueError contract: it inspects only the
    // outer error's `.name` and does not unwrap. If a caller (or an over-eager retry layer) hands us a
    // GsrRegistrationError whose cause happens to be EntityNotFoundException,
    // the outer error's `.name` is "GsrRegistrationError" — that must
    // propagate as-is and NOT trigger the auto-register fall-through.
    const client = makeGlueClient();
    const wrapped = new GsrRegistrationError("wrapped", {
      cause: glueError("EntityNotFoundException"),
    });
    client.getSchemaByDefinition.mockRejectedValueOnce(wrapped);
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    await expect(
      registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
    ).rejects.toBeInstanceOf(GsrRegistrationError);

    expect(client.createSchema).not.toHaveBeenCalled();
  });

  it("throws GsrAutoRegistrationDisabledError on EntityNotFoundException when auto-register is disabled", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: false,
    });

    await expect(
      registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
    ).rejects.toBeInstanceOf(GsrAutoRegistrationDisabledError);

    expect(client.createSchema).not.toHaveBeenCalled();
    expect(client.registerSchemaVersion).not.toHaveBeenCalled();
  });
});

describe("SchemaRegistrar — CreateSchema fall-through (compat carry + tags + metadata)", () => {
  it("calls CreateSchema with Compatibility from cfg, then flushes metadata, caches, and returns the UUID", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
    });
    const metadata = makeMetadataWriter();
    const { registrar, cache } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
      compatibility: "FULL_ALL",
      description: "test description",
    });

    const got = await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");

    expect(got).toBe(UUID_1);
    expect(cache.get("test-schema:AVRO")).toEqual({ schemaVersionId: UUID_1 });
    expect(client.createSchema).toHaveBeenCalledTimes(1);
    const [input] = client.createSchema.mock.calls[0]!;
    expect(input).toMatchObject({
      RegistryId: { RegistryName: "default-registry" },
      SchemaName: "test-schema",
      DataFormat: "AVRO",
      SchemaDefinition: AVRO_SCHEMA.schemaDefinition,
      Compatibility: "FULL_ALL",
      Description: "test description",
    });
    expect(metadata.putSchemaVersionMetadataBatch).toHaveBeenCalledWith(
      UUID_1,
      "topic-1",
    );
  });

  it("passes configured tags inline on CreateSchema.Tags", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
    });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
      tags: { env: "test", team: "gsr" },
    });

    await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");

    const [input] = client.createSchema.mock.calls[0]!;
    expect(input.Tags).toEqual({ env: "test", team: "gsr" });
  });

  it("omits Tags when configured tags are empty (minimal request payload)", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
    });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");

    const [input] = client.createSchema.mock.calls[0]!;
    expect(input.Tags).toBeUndefined();
  });

  it("wraps a non-AlreadyExists CreateSchema error in GsrRegistrationError and does NOT poll", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    const invalid = glueError("InvalidInputException", "bad shape");
    client.createSchema.mockRejectedValueOnce(invalid);
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    let caught: unknown;
    try {
      await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");
    } catch (err) {
      caught = err;
    }

    expect(caught).toBeInstanceOf(GsrRegistrationError);
    expect((caught as GsrRegistrationError).cause).toBe(invalid);
    expect(client.registerSchemaVersion).not.toHaveBeenCalled();
    expect(client.getSchemaVersion).not.toHaveBeenCalled();
    expect(metadata.putSchemaVersionMetadataBatch).not.toHaveBeenCalled();
  });

  it("throws GsrRegistrationError when CreateSchema returns no SchemaVersionId", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockResolvedValueOnce({
      $metadata: {},
      // SchemaVersionId omitted
    });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    await expect(
      registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
    ).rejects.toBeInstanceOf(GsrRegistrationError);
    expect(metadata.putSchemaVersionMetadataBatch).not.toHaveBeenCalled();
  });
});

describe("SchemaRegistrar — RegisterSchemaVersion race + poll", () => {
  it("falls back to RegisterSchemaVersion on AlreadyExistsException from CreateSchema, and returns AVAILABLE immediately without polling", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockRejectedValueOnce(
      glueError("AlreadyExistsException"),
    );
    client.registerSchemaVersion.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "AVAILABLE",
    });
    const metadata = makeMetadataWriter();
    const sleep = makeSleep();
    const { registrar, cache } = makeRegistrar(
      client,
      metadata,
      { schemaAutoRegistrationEnabled: true },
      sleep,
    );

    const got = await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");

    expect(got).toBe(UUID_1);
    expect(client.registerSchemaVersion).toHaveBeenCalledWith({
      SchemaId: { RegistryName: "default-registry", SchemaName: "test-schema" },
      SchemaDefinition: AVRO_SCHEMA.schemaDefinition,
    });
    expect(client.getSchemaVersion).not.toHaveBeenCalled();
    expect(sleep).not.toHaveBeenCalled();
    expect(metadata.putSchemaVersionMetadataBatch).toHaveBeenCalledWith(
      UUID_1,
      "topic-1",
    );
    expect(cache.get("test-schema:AVRO")).toEqual({ schemaVersionId: UUID_1 });
  });

  it("polls GetSchemaVersion after a PENDING RegisterSchemaVersion response and sleeps 3s before each poll", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockRejectedValueOnce(
      glueError("AlreadyExistsException"),
    );
    client.registerSchemaVersion.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "PENDING",
    });
    // First two polls still PENDING, third one AVAILABLE.
    client.getSchemaVersion
      .mockResolvedValueOnce({ $metadata: {}, Status: "PENDING" })
      .mockResolvedValueOnce({ $metadata: {}, Status: "PENDING" })
      .mockResolvedValueOnce({ $metadata: {}, Status: "AVAILABLE" });
    const metadata = makeMetadataWriter();
    const sleep = makeSleep();
    const { registrar } = makeRegistrar(
      client,
      metadata,
      { schemaAutoRegistrationEnabled: true },
      sleep,
    );

    const got = await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");

    expect(got).toBe(UUID_1);
    expect(client.getSchemaVersion).toHaveBeenCalledTimes(3);
    // Sleep-before-poll: 3 polls → 3 sleeps, all at 3000ms.
    expect(sleep).toHaveBeenCalledTimes(3);
    expect(vi.mocked(sleep).mock.calls).toEqual([[3000], [3000], [3000]]);
    // Metadata flushes AFTER the poll succeeds (Java parity line 281).
    expect(metadata.putSchemaVersionMetadataBatch).toHaveBeenCalledWith(
      UUID_1,
      "topic-1",
    );
  });

  it("throws GsrRegistrationError on a FAILURE poll response and does NOT flush metadata", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockRejectedValueOnce(
      glueError("AlreadyExistsException"),
    );
    client.registerSchemaVersion.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "PENDING",
    });
    client.getSchemaVersion.mockResolvedValueOnce({
      $metadata: {},
      Status: "FAILURE",
    });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    await expect(
      registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
    ).rejects.toBeInstanceOf(GsrRegistrationError);
    expect(metadata.putSchemaVersionMetadataBatch).not.toHaveBeenCalled();
  });

  it("throws GsrRegistrationError on a DELETING poll response", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockRejectedValueOnce(
      glueError("AlreadyExistsException"),
    );
    client.registerSchemaVersion.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "PENDING",
    });
    client.getSchemaVersion.mockResolvedValueOnce({
      $metadata: {},
      Status: "DELETING",
    });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    await expect(
      registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
    ).rejects.toBeInstanceOf(GsrRegistrationError);
  });

  it("throws GsrRegistrationError after 10 exhausted PENDING poll attempts", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockRejectedValueOnce(
      glueError("AlreadyExistsException"),
    );
    client.registerSchemaVersion.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "PENDING",
    });
    // Always PENDING.
    client.getSchemaVersion.mockResolvedValue({
      $metadata: {},
      Status: "PENDING",
    });
    const metadata = makeMetadataWriter();
    const sleep = makeSleep();
    const { registrar } = makeRegistrar(
      client,
      metadata,
      { schemaAutoRegistrationEnabled: true },
      sleep,
    );

    await expect(
      registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
    ).rejects.toBeInstanceOf(GsrRegistrationError);
    expect(client.getSchemaVersion).toHaveBeenCalledTimes(10);
    expect(sleep).toHaveBeenCalledTimes(10);
    expect(metadata.putSchemaVersionMetadataBatch).not.toHaveBeenCalled();
  });

  it("wraps a poll-time GetSchemaVersion rejection in GsrRegistrationError", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockRejectedValueOnce(
      glueError("AlreadyExistsException"),
    );
    client.registerSchemaVersion.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "PENDING",
    });
    const pollErr = glueError("InternalServiceException", "boom");
    client.getSchemaVersion.mockRejectedValueOnce(pollErr);
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    let caught: unknown;
    try {
      await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");
    } catch (err) {
      caught = err;
    }

    expect(caught).toBeInstanceOf(GsrRegistrationError);
    expect((caught as GsrRegistrationError).cause).toBe(pollErr);
  });

  it("wraps a RegisterSchemaVersion rejection in GsrRegistrationError", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockRejectedValueOnce(
      glueError("AlreadyExistsException"),
    );
    const regErr = glueError("InternalServiceException", "boom");
    client.registerSchemaVersion.mockRejectedValueOnce(regErr);
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    let caught: unknown;
    try {
      await registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1");
    } catch (err) {
      caught = err;
    }

    expect(caught).toBeInstanceOf(GsrRegistrationError);
    expect((caught as GsrRegistrationError).cause).toBe(regErr);
  });

  it("throws GsrRegistrationError when RegisterSchemaVersion returns no SchemaVersionId", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition.mockRejectedValueOnce(
      glueError("EntityNotFoundException"),
    );
    client.createSchema.mockRejectedValueOnce(
      glueError("AlreadyExistsException"),
    );
    client.registerSchemaVersion.mockResolvedValueOnce({
      $metadata: {},
      Status: "AVAILABLE",
      // SchemaVersionId omitted.
    });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    await expect(
      registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
    ).rejects.toBeInstanceOf(GsrRegistrationError);
  });
});

describe("SchemaRegistrar — concurrent first-resolve de-duplication", () => {
  it("collapses N concurrent lookups for the same key onto one GetSchemaByDefinition", async () => {
    const client = makeGlueClient();
    // Gate GetSchemaByDefinition so callers actually race — one long promise
    // resolves for all N concurrent callers.
    let releaseGet!: (v: {
      $metadata: object;
      SchemaVersionId: string;
      Status: string;
    }) => void;
    client.getSchemaByDefinition.mockReturnValueOnce(
      new Promise((resolve) => {
        releaseGet = resolve;
      }),
    );
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata);

    const pending = Promise.all(
      Array.from({ length: 5 }, () =>
        registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
      ),
    );

    // Let the microtask queue drain so all five callers reach the in-flight
    // check before we release the underlying Glue call.
    await Promise.resolve();
    releaseGet({
      $metadata: {},
      SchemaVersionId: UUID_1,
      Status: "AVAILABLE",
    });

    const results = await pending;
    expect(results).toEqual([UUID_1, UUID_1, UUID_1, UUID_1, UUID_1]);
    expect(client.getSchemaByDefinition).toHaveBeenCalledTimes(1);
  });

  it("does NOT poison the key on failure — a retry after a rejection issues a fresh GetSchemaByDefinition", async () => {
    const client = makeGlueClient();
    client.getSchemaByDefinition
      .mockRejectedValueOnce(glueError("AccessDeniedException"))
      .mockResolvedValueOnce({
        $metadata: {},
        SchemaVersionId: UUID_1,
        Status: "AVAILABLE",
      });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata);

    await expect(
      registrar.getSchemaVersionId(AVRO_SCHEMA, "t"),
    ).rejects.toBeInstanceOf(GsrRegistrationError);

    // The in-flight entry MUST have been deleted after the first failure so
    // the retry actually re-issues the Glue call rather than latching onto a
    // dead rejected promise.
    const retried = await registrar.getSchemaVersionId(AVRO_SCHEMA, "t");
    expect(retried).toBe(UUID_1);
    expect(client.getSchemaByDefinition).toHaveBeenCalledTimes(2);
  });

  it("de-dups the CreateSchema fall-through — one create call for N concurrent misses", async () => {
    const client = makeGlueClient();
    let releaseGet!: (e: Error) => void;
    client.getSchemaByDefinition.mockReturnValueOnce(
      new Promise((_, reject) => {
        releaseGet = reject;
      }),
    );
    client.createSchema.mockResolvedValueOnce({
      $metadata: {},
      SchemaVersionId: UUID_1,
    });
    const metadata = makeMetadataWriter();
    const { registrar } = makeRegistrar(client, metadata, {
      schemaAutoRegistrationEnabled: true,
    });

    const pending = Promise.all(
      Array.from({ length: 4 }, () =>
        registrar.getSchemaVersionId(AVRO_SCHEMA, "topic-1"),
      ),
    );
    await Promise.resolve();
    releaseGet(glueError("EntityNotFoundException"));

    const results = await pending;
    expect(results).toEqual([UUID_1, UUID_1, UUID_1, UUID_1]);
    expect(client.getSchemaByDefinition).toHaveBeenCalledTimes(1);
    expect(client.createSchema).toHaveBeenCalledTimes(1);
    // Metadata is flushed exactly once — on the single winning resolve.
    expect(metadata.putSchemaVersionMetadataBatch).toHaveBeenCalledTimes(1);
  });
});
