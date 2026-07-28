import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { parseConfig } from "../config/config.js";
import type { GlueClient } from "./client-seam.js";
import {
  TRANSPORT_METADATA_KEY,
  createMetadata,
} from "./metadata.js";

/**
 * Build a minimal hand-rolled {@link GlueClient} where each seam method is a
 * vitest spy. Individual tests wire the behaviour of the calls they exercise
 * (typically `putSchemaVersionMetadata`, `querySchemaVersionMetadata`,
 * `getTags`) and leave the rest as trivially-resolving spies. This keeps the
 * assertion surface focused on what the metadata module actually calls
 * without dragging in the concrete SDK client.
 */
function makeFakeClient(): GlueClient {
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

const SCHEMA_VERSION_ID = "11111111-2222-3333-4444-555555555555";

let warnSpy: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
  warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {
    // Swallow warnings; individual tests inspect calls on the spy when
    // asserting the log-and-continue contract.
  });
});

afterEach(() => {
  warnSpy.mockRestore();
});

describe("createMetadata — write path", () => {
  it("merges transport-first, configured wins on collision — customer TRANSPORT_METADATA_KEY overrides per-call transportName", async () => {
    const client = makeFakeClient();
    const cfg = parseConfig({
      [`metadata.${TRANSPORT_METADATA_KEY}`]: "customer-transport",
      "metadata.other": "value",
    });

    const md = createMetadata(client, cfg);
    await md.putSchemaVersionMetadataBatch(SCHEMA_VERSION_ID, "kafka-topic");

    const calls = (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>)
      .mock.calls;
    // Two entries total: configured wins over the auto-injected transport,
    // so the count is `len(configured)` (2), NOT `len(configured)+1`.
    expect(calls).toHaveLength(2);

    const pairs = calls.map(([input]) => ({
      key: input.MetadataKeyValue.MetadataKey,
      value: input.MetadataKeyValue.MetadataValue,
      schemaVersionId: input.SchemaVersionId,
    }));
    // Every call carries the schema-version id.
    for (const p of pairs) {
      expect(p.schemaVersionId).toBe(SCHEMA_VERSION_ID);
    }
    // Order is not pinned by the contract; compare as a set.
    expect(pairs.map(({ schemaVersionId: _, ...rest }) => rest).sort((a, b) =>
      a.key.localeCompare(b.key),
    )).toEqual(
      [
        { key: TRANSPORT_METADATA_KEY, value: "customer-transport" },
        { key: "other", value: "value" },
      ].sort((a, b) => a.key.localeCompare(b.key)),
    );
  });

  it("stamps the transport entry when configured metadata is empty (len == 1, transport only)", async () => {
    const client = makeFakeClient();
    const cfg = parseConfig({});

    const md = createMetadata(client, cfg);
    await md.putSchemaVersionMetadataBatch(SCHEMA_VERSION_ID, "kafka-topic");

    const calls = (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>)
      .mock.calls;
    expect(calls).toHaveLength(1);
    expect(calls[0]![0]).toEqual({
      SchemaVersionId: SCHEMA_VERSION_ID,
      MetadataKeyValue: {
        MetadataKey: TRANSPORT_METADATA_KEY,
        MetadataValue: "kafka-topic",
      },
    });
  });

  it("ALWAYS injects the transport entry, even when transportName is an empty string", async () => {
    const client = makeFakeClient();
    const cfg = parseConfig({});

    const md = createMetadata(client, cfg);
    await md.putSchemaVersionMetadataBatch(SCHEMA_VERSION_ID, "");

    const calls = (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>)
      .mock.calls;
    expect(calls).toHaveLength(1);
    expect(calls[0]![0].MetadataKeyValue.MetadataKey).toBe(
      TRANSPORT_METADATA_KEY,
    );
    // The value IS the empty string — the entry is present with a "" value,
    // not skipped.
    expect(calls[0]![0].MetadataKeyValue.MetadataValue).toBe("");
  });

  it("writes each configured entry plus the transport entry (len + 1)", async () => {
    const client = makeFakeClient();
    const cfg = parseConfig({
      "metadata.commit": "abc123",
      "metadata.env": "beta",
    });

    const md = createMetadata(client, cfg);
    await md.putSchemaVersionMetadataBatch(SCHEMA_VERSION_ID, "kafka-topic");

    const calls = (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>)
      .mock.calls;
    expect(calls).toHaveLength(3);

    const pairs = calls.map(([input]) => ({
      key: input.MetadataKeyValue.MetadataKey,
      value: input.MetadataKeyValue.MetadataValue,
    }));
    const set = pairs.sort((a, b) => a.key.localeCompare(b.key));
    expect(set).toEqual(
      [
        { key: TRANSPORT_METADATA_KEY, value: "kafka-topic" },
        { key: "commit", value: "abc123" },
        { key: "env", value: "beta" },
      ].sort((a, b) => a.key.localeCompare(b.key)),
    );
  });

  it("iterates sequentially — no next call fires until the current call settles", async () => {
    const client = makeFakeClient();
    const cfg = parseConfig({
      "metadata.a": "1",
      "metadata.b": "2",
    });

    // Gate each put on a per-call deferred so the test can prove ordering:
    // if the loop were parallel, all three deferreds would be created before
    // any resolves. The loop is sequential, so we must resolve each in turn.
    const resolvers: Array<() => void> = [];
    let inFlight = 0;
    let maxInFlight = 0;
    (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>)
      .mockImplementation(async () => {
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        await new Promise<void>((resolve) => resolvers.push(resolve));
        inFlight--;
        return { $metadata: {} };
      });

    const md = createMetadata(client, cfg);
    const done = md.putSchemaVersionMetadataBatch(SCHEMA_VERSION_ID, "t");

    // Yield the microtask queue enough for the first call to enter the mock,
    // then drain each pending call one at a time.
    for (let i = 0; i < 3; i++) {
      // Wait until exactly one call has entered the mock and is awaiting
      // its resolver.
      while (resolvers.length === i) {
        await new Promise((r) => setImmediate(r));
      }
      resolvers[i]!();
    }

    await done;

    // If the loop had been parallel the first check would have observed
    // more than one concurrent call in flight.
    expect(maxInFlight).toBe(1);
    expect(
      (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>).mock.calls,
    ).toHaveLength(3);
  });

  it("logs per-entry failure via console.warn with a `gsr:` prefix and continues to the next entry", async () => {
    const client = makeFakeClient();
    const cfg = parseConfig({
      "metadata.commit": "abc123",
      "metadata.env": "beta",
    });

    // Reject on the "env" entry, resolve everything else.
    (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>)
      .mockImplementation(async (input) => {
        if (input.MetadataKeyValue.MetadataKey === "env") {
          throw new Error("simulated put failure");
        }
        return { $metadata: {} };
      });

    const md = createMetadata(client, cfg);
    await expect(
      md.putSchemaVersionMetadataBatch(SCHEMA_VERSION_ID, "kafka-topic"),
    ).resolves.toBeUndefined();

    // All three entries are attempted despite the middle rejection.
    expect(
      (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>).mock.calls,
    ).toHaveLength(3);

    // Exactly one warning was emitted, prefixed with `gsr:`, mentioning the
    // failing key and value.
    expect(warnSpy).toHaveBeenCalledTimes(1);
    const [firstArg, secondArg] = warnSpy.mock.calls[0]!;
    expect(String(firstArg)).toMatch(/^gsr:/);
    expect(String(firstArg)).toContain("env");
    expect(String(firstArg)).toContain("beta");
    // The underlying error is forwarded as a second console.warn argument for
    // debuggability.
    expect(secondArg).toBeInstanceOf(Error);
  });

  it("treats AlreadyExistsException as success — no warn, no throw", async () => {
    const client = makeFakeClient();
    const cfg = parseConfig({ "metadata.commit": "abc123" });

    (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>)
      .mockImplementation(async (input) => {
        if (input.MetadataKeyValue.MetadataKey === "commit") {
          throw Object.assign(new Error("already stored"), {
            name: "AlreadyExistsException",
          });
        }
        return { $metadata: {} };
      });

    const md = createMetadata(client, cfg);
    await expect(
      md.putSchemaVersionMetadataBatch(SCHEMA_VERSION_ID, "kafka-topic"),
    ).resolves.toBeUndefined();

    expect(warnSpy).not.toHaveBeenCalled();
  });

  it("resolves without throwing even when ALL entries fail", async () => {
    const client = makeFakeClient();
    const cfg = parseConfig({
      "metadata.commit": "abc123",
      "metadata.env": "beta",
    });

    (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>)
      .mockRejectedValue(new Error("everything is on fire"));

    const md = createMetadata(client, cfg);
    await expect(
      md.putSchemaVersionMetadataBatch(SCHEMA_VERSION_ID, "kafka-topic"),
    ).resolves.toBeUndefined();

    // All three merged entries are attempted, and each fires a warning.
    expect(
      (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>).mock.calls,
    ).toHaveLength(3);
    expect(warnSpy).toHaveBeenCalledTimes(3);
    for (const [firstArg] of warnSpy.mock.calls) {
      expect(String(firstArg)).toMatch(/^gsr:/);
    }
  });

  it("threads the schema-version id verbatim to every underlying put call", async () => {
    const client = makeFakeClient();
    const cfg = parseConfig({ "metadata.k": "v" });

    const md = createMetadata(client, cfg);
    await md.putSchemaVersionMetadataBatch("abc-vid", "t");

    const calls = (client.putSchemaVersionMetadata as ReturnType<typeof vi.fn>)
      .mock.calls;
    for (const [input] of calls) {
      expect(input.SchemaVersionId).toBe("abc-vid");
    }
  });
});

describe("createMetadata — read path", () => {
  it("flattens the Glue MetadataInfoMap into a plain Record<string, string>", async () => {
    const client = makeFakeClient();
    (client.querySchemaVersionMetadata as ReturnType<typeof vi.fn>).mockResolvedValue({
      MetadataInfoMap: {
        [TRANSPORT_METADATA_KEY]: { MetadataValue: "kafka-topic" },
        commit: { MetadataValue: "abc123" },
      },
    });

    const md = createMetadata(client, parseConfig({}));
    const result = await md.querySchemaVersionMetadata(SCHEMA_VERSION_ID);

    expect(result).toEqual({
      [TRANSPORT_METADATA_KEY]: "kafka-topic",
      commit: "abc123",
    });
    expect(
      (client.querySchemaVersionMetadata as ReturnType<typeof vi.fn>).mock.calls,
    ).toHaveLength(1);
    expect(
      (client.querySchemaVersionMetadata as ReturnType<typeof vi.fn>).mock.calls[0]![0],
    ).toEqual({ SchemaVersionId: SCHEMA_VERSION_ID });
  });

  it("skips entries whose MetadataValue is undefined", async () => {
    const client = makeFakeClient();
    (client.querySchemaVersionMetadata as ReturnType<typeof vi.fn>).mockResolvedValue({
      MetadataInfoMap: {
        keep: { MetadataValue: "yes" },
        drop: {},
      },
    });

    const md = createMetadata(client, parseConfig({}));
    const result = await md.querySchemaVersionMetadata(SCHEMA_VERSION_ID);

    expect(result).toEqual({ keep: "yes" });
  });

  it("returns an empty map when the response has no MetadataInfoMap", async () => {
    const client = makeFakeClient();
    (client.querySchemaVersionMetadata as ReturnType<typeof vi.fn>).mockResolvedValue({});

    const md = createMetadata(client, parseConfig({}));
    const result = await md.querySchemaVersionMetadata(SCHEMA_VERSION_ID);

    expect(result).toEqual({});
  });

  it("propagates a rejection from querySchemaVersionMetadata to the caller (read path is NOT non-fatal)", async () => {
    const client = makeFakeClient();
    (client.querySchemaVersionMetadata as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error("boom"),
    );

    const md = createMetadata(client, parseConfig({}));
    await expect(md.querySchemaVersionMetadata(SCHEMA_VERSION_ID)).rejects.toThrow(
      "boom",
    );
  });

  it("getSchemaTags forwards the ARN to GetTags and returns the tags map", async () => {
    const client = makeFakeClient();
    (client.getTags as ReturnType<typeof vi.fn>).mockResolvedValue({
      Tags: { env: "prod", team: "gsr" },
    });

    const md = createMetadata(client, parseConfig({}));
    const arn =
      "arn:aws:glue:us-east-2:123456789012:schema/default-registry/orders";
    const result = await md.getSchemaTags(arn);

    expect(result).toEqual({ env: "prod", team: "gsr" });
    expect(
      (client.getTags as ReturnType<typeof vi.fn>).mock.calls,
    ).toHaveLength(1);
    expect((client.getTags as ReturnType<typeof vi.fn>).mock.calls[0]![0]).toEqual({
      ResourceArn: arn,
    });
  });

  it("getSchemaTags returns an empty map when GetTags omits Tags", async () => {
    const client = makeFakeClient();
    (client.getTags as ReturnType<typeof vi.fn>).mockResolvedValue({});

    const md = createMetadata(client, parseConfig({}));
    const result = await md.getSchemaTags(
      "arn:aws:glue:us-east-2:123456789012:schema/default-registry/orders",
    );

    expect(result).toEqual({});
  });

  it("propagates a rejection from GetTags to the caller", async () => {
    const client = makeFakeClient();
    (client.getTags as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error("access denied"),
    );

    const md = createMetadata(client, parseConfig({}));
    await expect(
      md.getSchemaTags(
        "arn:aws:glue:us-east-2:123456789012:schema/default-registry/orders",
      ),
    ).rejects.toThrow("access denied");
  });
});

describe("createMetadata — surface", () => {
  it("exposes exactly the three declared operations", () => {
    const md = createMetadata(makeFakeClient(), parseConfig({}));
    expect(typeof md.putSchemaVersionMetadataBatch).toBe("function");
    expect(typeof md.querySchemaVersionMetadata).toBe("function");
    expect(typeof md.getSchemaTags).toBe("function");
  });

  it("exports the canonical transport metadata key", () => {
    expect(TRANSPORT_METADATA_KEY).toBe("x-amz-meta-transport");
  });
});
