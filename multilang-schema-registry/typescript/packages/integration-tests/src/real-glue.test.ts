import { DeleteSchemaCommand } from "@aws-sdk/client-glue";
import { parseConfig } from "@gsr/core";
import type { GlueClient, GsrConfig } from "@gsr/core";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
  type Mock,
} from "vitest";

import { FakeGlueClient } from "./fake-glue.js";
import {
  CleanupTracker,
  RUNBOOK_PREFIX,
  perRunSuffix,
  realSchemaName,
  selectGlueBackend,
  type CleanupClient,
} from "./real-glue.js";

/**
 * Tier-1 unit coverage for the real-Glue selector + CleanupTracker. Every
 * case is offline: the "real" branch is exercised via the `buildReal`
 * override so no SDK client construction ever calls out to AWS or reads a
 * credential chain.
 */

beforeEach(() => {
  vi.stubEnv("AWS_INTEGRATION", "");
  vi.stubEnv("GSR_GLUE", "");
  vi.stubEnv("AWS_REGION", "");
  vi.stubEnv("AWS_PROFILE", "");
  vi.stubEnv("AWS_ACCESS_KEY_ID", "");
});

afterEach(() => {
  vi.unstubAllEnvs();
});

/**
 * A validated `GsrConfig` built through the real parser so the selector sees
 * exactly the shape a production caller would hand it. Empty map → all
 * defaults, which is enough for the selector's purposes (it only inspects
 * region + a handful of fields when building the SDK client).
 */
function testConfig(): GsrConfig {
  return parseConfig({});
}

/**
 * Build a stubbed cleanup client whose `send` records every command it
 * receives and can be programmed to throw on a given call index.
 */
function stubCleanupClient(): {
  client: CleanupClient;
  send: Mock;
  deletedNames(): string[];
} {
  const send = vi.fn().mockResolvedValue({ $metadata: {} });
  const client: CleanupClient = {
    send: (cmd) => send(cmd),
  };
  return {
    client,
    send,
    deletedNames(): string[] {
      const names: string[] = [];
      for (const call of send.mock.calls) {
        const cmd = call[0] as DeleteSchemaCommand;
        const name = cmd.input.SchemaId?.SchemaName;
        if (name !== undefined) names.push(name);
      }
      return names;
    },
  };
}

describe("perRunSuffix", () => {
  it("matches the epoch-seconds and hex shape", () => {
    const s = perRunSuffix();
    // 10 digits (Unix epoch seconds — good through year 2286) - 4 hex chars.
    expect(s).toMatch(/^\d{10}-[0-9a-f]{4}$/);
  });

  it("returns distinct values across successive calls", () => {
    const seen = new Set<string>();
    for (let i = 0; i < 32; i++) {
      seen.add(perRunSuffix());
    }
    // 16 random bits collide 1-in-65k pairs; 32 draws is well below that
    // birthday-collision threshold. Occasional collisions here would signal
    // the RNG source failed, not test-flakiness.
    expect(seen.size).toBe(32);
  });
});

describe("realSchemaName", () => {
  it("prefixes with the runbook prefix and includes the label + suffix", () => {
    const name = realSchemaName("roundtrip");
    expect(name.startsWith(RUNBOOK_PREFIX)).toBe(true);
    expect(name).toMatch(/^gsr-ts-it-roundtrip-\d{10}-[0-9a-f]{4}$/);
  });

  it("uses the exact runbook prefix constant (single source of truth)", () => {
    expect(RUNBOOK_PREFIX).toBe("gsr-ts-it-");
  });
});

describe("selectGlueBackend", () => {
  it("returns a FakeGlueClient when GSR_GLUE is unset", () => {
    const picked = selectGlueBackend(testConfig());
    expect(picked.isReal).toBe(false);
    expect(picked.client).toBeInstanceOf(FakeGlueClient);
    expect(picked.cleanup).toBeNull();
  });

  it("returns a FakeGlueClient when GSR_GLUE is 'fake' or any non-real value", () => {
    for (const v of ["fake", "REAL", "rael", "", "yes"]) {
      vi.stubEnv("GSR_GLUE", v);
      const picked = selectGlueBackend(testConfig());
      expect(picked.isReal).toBe(false);
      expect(picked.client).toBeInstanceOf(FakeGlueClient);
      expect(picked.cleanup).toBeNull();
    }
  });

  it("takes the real branch and builds a CleanupTracker when GSR_GLUE=real", () => {
    vi.stubEnv("GSR_GLUE", "real");
    const stubReal: GlueClient & CleanupClient = {
      getSchemaByDefinition: vi.fn(),
      getSchemaVersion: vi.fn(),
      createSchema: vi.fn(),
      registerSchemaVersion: vi.fn(),
      putSchemaVersionMetadata: vi.fn(),
      querySchemaVersionMetadata: vi.fn(),
      getTags: vi.fn(),
      // CleanupClient
      send: vi.fn().mockResolvedValue({ $metadata: {} }),
    };
    const buildReal = vi.fn((_cfg: GsrConfig) => stubReal as GlueClient);
    const picked = selectGlueBackend(testConfig(), { buildReal });
    expect(picked.isReal).toBe(true);
    expect(picked.client).toBe(stubReal);
    expect(buildReal).toHaveBeenCalledTimes(1);
    // The tracker is bound to the real client's send (via the test-mode
    // override) so a smoke run does not touch AWS.
    expect(picked.cleanup).toBeInstanceOf(CleanupTracker);
  });

  it("honors overrideBackend regardless of GSR_GLUE", () => {
    vi.stubEnv("GSR_GLUE", "real");
    const override: GlueClient = new FakeGlueClient();
    const picked = selectGlueBackend(testConfig(), { overrideBackend: override });
    expect(picked.isReal).toBe(false);
    expect(picked.client).toBe(override);
    expect(picked.cleanup).toBeNull();
  });
});

describe("CleanupTracker", () => {
  it("tracks (registry, schemaName) tuples in insertion order", () => {
    const stub = stubCleanupClient();
    const t = new CleanupTracker(stub.client);
    t.track("default-registry", "schema-a");
    t.track("default-registry", "schema-b");
    t.track("default-registry", "schema-c");
    expect(t.schemas.map((s) => s.schemaName)).toEqual([
      "schema-a",
      "schema-b",
      "schema-c",
    ]);
    expect(stub.send).not.toHaveBeenCalled();
  });

  it("is idempotent — repeat track on the same (registry, name) is a no-op", () => {
    const stub = stubCleanupClient();
    const t = new CleanupTracker(stub.client);
    t.track("default-registry", "schema-a");
    t.track("default-registry", "schema-a");
    t.track("default-registry", "schema-b");
    expect(t.schemas.map((s) => s.schemaName)).toEqual([
      "schema-a",
      "schema-b",
    ]);
  });

  it("distinguishes same schemaName across different registries", () => {
    const stub = stubCleanupClient();
    const t = new CleanupTracker(stub.client);
    t.track("default-registry", "schema-a");
    t.track("other-registry", "schema-a");
    expect(t.schemas).toHaveLength(2);
  });

  it("run() deletes tracked schemas in REVERSE insertion order (success path)", async () => {
    const stub = stubCleanupClient();
    const t = new CleanupTracker(stub.client);
    t.track("default-registry", "schema-a");
    t.track("default-registry", "schema-b");
    t.track("default-registry", "schema-c");

    await t.run();

    expect(stub.deletedNames()).toEqual(["schema-c", "schema-b", "schema-a"]);
  });

  it("run() issues DeleteSchemaCommand with the correct (registryName, schemaName)", async () => {
    const stub = stubCleanupClient();
    const t = new CleanupTracker(stub.client);
    t.track("default-registry", "schema-only");

    await t.run();

    expect(stub.send).toHaveBeenCalledTimes(1);
    const cmd = stub.send.mock.calls[0]![0] as DeleteSchemaCommand;
    expect(cmd).toBeInstanceOf(DeleteSchemaCommand);
    expect(cmd.input.SchemaId?.RegistryName).toBe("default-registry");
    expect(cmd.input.SchemaId?.SchemaName).toBe("schema-only");
  });

  it("run() NEVER issues CreateRegistry / DeleteRegistry — only DeleteSchemaCommand", async () => {
    const stub = stubCleanupClient();
    const t = new CleanupTracker(stub.client);
    t.track("default-registry", "schema-a");
    t.track("default-registry", "schema-b");
    await t.run();

    // The only command type any send() call has ever received is
    // DeleteSchemaCommand; no CreateRegistry / DeleteRegistry / other
    // control-plane call shows up.
    for (const call of stub.send.mock.calls) {
      expect(call[0]).toBeInstanceOf(DeleteSchemaCommand);
    }
  });

  it("run() still deletes remaining schemas after a mid-run failure (failure path)", async () => {
    const stub = stubCleanupClient();
    let call = 0;
    stub.send.mockImplementation(() => {
      call += 1;
      if (call === 2) {
        return Promise.reject(new Error("boom"));
      }
      return Promise.resolve({ $metadata: {} });
    });

    const t = new CleanupTracker(stub.client);
    t.track("default-registry", "schema-a");
    t.track("default-registry", "schema-b");
    t.track("default-registry", "schema-c");

    let caught: unknown;
    try {
      await t.run();
    } catch (err) {
      caught = err;
    }

    // Even with a failure on the middle delete, ALL three schemas were
    // attempted in reverse insertion order.
    expect(stub.deletedNames()).toEqual(["schema-c", "schema-b", "schema-a"]);
    expect(caught).toBeInstanceOf(AggregateError);
    expect((caught as AggregateError).errors).toHaveLength(1);
    expect((caught as AggregateError).errors[0]).toBeInstanceOf(Error);
  });

  it("run() suppresses EntityNotFoundException (already-deleted / race case)", async () => {
    const stub = stubCleanupClient();
    stub.send.mockImplementation(() => {
      const err = new Error("no such schema");
      err.name = "EntityNotFoundException";
      return Promise.reject(err);
    });

    const t = new CleanupTracker(stub.client);
    t.track("default-registry", "schema-a");
    t.track("default-registry", "schema-b");

    // No throw despite every delete failing with EntityNotFoundException —
    // that class of error is expected during teardown races.
    await expect(t.run()).resolves.toBeUndefined();
    expect(stub.send).toHaveBeenCalledTimes(2);
  });

  it("run() re-throws non-not-found errors, one per failed delete, aggregated", async () => {
    const stub = stubCleanupClient();
    stub.send.mockImplementation(() => {
      const err = new Error("boom");
      err.name = "ThrottlingException";
      return Promise.reject(err);
    });

    const t = new CleanupTracker(stub.client);
    t.track("default-registry", "schema-a");
    t.track("default-registry", "schema-b");
    t.track("default-registry", "schema-c");

    let caught: unknown;
    try {
      await t.run();
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(AggregateError);
    const agg = caught as AggregateError;
    expect(agg.errors).toHaveLength(3);
    // Every error message carries the scenario-level tag so the aggregate is
    // scannable.
    for (const e of agg.errors) {
      expect((e as Error).message).toMatch(/DeleteSchema default-registry/);
    }
  });

  it("run() is a no-op when nothing was tracked", async () => {
    const stub = stubCleanupClient();
    const t = new CleanupTracker(stub.client);
    await expect(t.run()).resolves.toBeUndefined();
    expect(stub.send).not.toHaveBeenCalled();
  });

  it("run() delivers cleanup on BOTH the success and failure legs of a try/finally", async () => {
    // Prove the "cleanup on exit — success or failure" contract by
    // simulating the caller pattern: a scenario runs (or throws), then the
    // caller's `finally` invokes `run()`. The tracker's job is to delete
    // whatever was tracked before the throw; whether the scenario body
    // succeeded or failed is not the tracker's concern.
    async function scenario(t: CleanupTracker, willThrow: boolean): Promise<void> {
      t.track("default-registry", "schema-a");
      t.track("default-registry", "schema-b");
      if (willThrow) {
        throw new Error("scenario failed halfway");
      }
    }

    // Success leg.
    {
      const stub = stubCleanupClient();
      const t = new CleanupTracker(stub.client);
      try {
        await scenario(t, false);
      } finally {
        await t.run();
      }
      expect(stub.deletedNames()).toEqual(["schema-b", "schema-a"]);
    }

    // Failure leg.
    {
      const stub = stubCleanupClient();
      const t = new CleanupTracker(stub.client);
      let thrown: unknown;
      try {
        try {
          await scenario(t, true);
        } finally {
          await t.run();
        }
      } catch (err) {
        thrown = err;
      }
      expect((thrown as Error).message).toBe("scenario failed halfway");
      // Cleanup fired despite the scenario throw.
      expect(stub.deletedNames()).toEqual(["schema-b", "schema-a"]);
    }
  });
});
