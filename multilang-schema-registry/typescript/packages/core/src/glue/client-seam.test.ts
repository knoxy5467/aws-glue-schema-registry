import {
  CreateSchemaCommand,
  GetSchemaByDefinitionCommand,
  GetSchemaVersionCommand,
  GetTagsCommand,
  GlueClient as SdkGlueClient,
  PutSchemaVersionMetadataCommand,
  QuerySchemaVersionMetadataCommand,
  RegisterSchemaVersionCommand,
} from "@aws-sdk/client-glue";
import { mockClient } from "aws-sdk-client-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { parseConfig } from "../config/config.js";
import { createGlueClient, type GlueClient } from "./client-seam.js";

// One mock instance per test — `.reset()` in `afterEach` restores a clean
// history and behavior for the next case, keeping the tests independent.
const sdkMock = mockClient(SdkGlueClient);

beforeEach(() => {
  sdkMock.reset();
});

afterEach(() => {
  sdkMock.reset();
});

describe("GlueClient seam — shape", () => {
  it("exposes exactly the seven declared operations, and neither createRegistry nor tagResource", () => {
    const seam = createGlueClient(parseConfig({}));

    const expected = [
      "getSchemaByDefinition",
      "getSchemaVersion",
      "createSchema",
      "registerSchemaVersion",
      "putSchemaVersionMetadata",
      "querySchemaVersionMetadata",
      "getTags",
    ].sort();

    expect(Object.keys(seam).sort()).toEqual(expected);

    for (const op of expected) {
      expect(typeof (seam as unknown as Record<string, unknown>)[op]).toBe(
        "function",
      );
    }

    // Explicit negatives — the Go-parity omissions.
    expect(
      (seam as unknown as Record<string, unknown>).createRegistry,
    ).toBeUndefined();
    expect(
      (seam as unknown as Record<string, unknown>).tagResource,
    ).toBeUndefined();
  });

  it("accepts a hand-rolled plain object satisfying the interface (structural typing)", () => {
    // Compile-time proof that the interface is a structural contract, not a
    // class — a bare object that implements the seven methods is a valid
    // `GlueClient`. If any method were missing or mistyped, TS would fail
    // this assignment.
    const fake: GlueClient = {
      getSchemaByDefinition: async () => ({ $metadata: {} }),
      getSchemaVersion: async () => ({ $metadata: {} }),
      createSchema: async () => ({ $metadata: {} }),
      registerSchemaVersion: async () => ({ $metadata: {} }),
      putSchemaVersionMetadata: async () => ({ $metadata: {} }),
      querySchemaVersionMetadata: async () => ({ $metadata: {} }),
      getTags: async () => ({ $metadata: {} }),
    };

    expect(typeof fake.getSchemaByDefinition).toBe("function");
  });
});

describe("createGlueClient — SDK command adaptation", () => {
  it("routes getSchemaByDefinition through GetSchemaByDefinitionCommand", async () => {
    sdkMock.on(GetSchemaByDefinitionCommand).resolves({
      SchemaVersionId: "11111111-1111-1111-1111-111111111111",
      Status: "AVAILABLE",
    });

    const seam = createGlueClient(parseConfig({}));
    const out = await seam.getSchemaByDefinition({
      SchemaId: { SchemaName: "s", RegistryName: "default-registry" },
      SchemaDefinition: "{}",
    });

    expect(out.SchemaVersionId).toBe("11111111-1111-1111-1111-111111111111");
    expect(out.Status).toBe("AVAILABLE");
    expect(sdkMock.commandCalls(GetSchemaByDefinitionCommand)).toHaveLength(1);
    const [call] = sdkMock.commandCalls(GetSchemaByDefinitionCommand);
    expect(call!.args[0].input).toEqual({
      SchemaId: { SchemaName: "s", RegistryName: "default-registry" },
      SchemaDefinition: "{}",
    });
  });

  it("routes getSchemaVersion through GetSchemaVersionCommand", async () => {
    sdkMock.on(GetSchemaVersionCommand).resolves({
      SchemaVersionId: "22222222-2222-2222-2222-222222222222",
      Status: "AVAILABLE",
    });

    const seam = createGlueClient(parseConfig({}));
    const out = await seam.getSchemaVersion({
      SchemaVersionId: "22222222-2222-2222-2222-222222222222",
    });

    expect(out.SchemaVersionId).toBe("22222222-2222-2222-2222-222222222222");
    expect(sdkMock.commandCalls(GetSchemaVersionCommand)).toHaveLength(1);
  });

  it("routes createSchema through CreateSchemaCommand and forwards the full input", async () => {
    sdkMock.on(CreateSchemaCommand).resolves({
      SchemaVersionId: "33333333-3333-3333-3333-333333333333",
    });

    const seam = createGlueClient(parseConfig({}));
    const input = {
      RegistryId: { RegistryName: "default-registry" },
      SchemaName: "orders",
      DataFormat: "AVRO" as const,
      SchemaDefinition: '{"type":"record","name":"O","fields":[]}',
      Compatibility: "BACKWARD" as const,
      Description: "DEFAULT-DESCRIPTION-us-east-2-default-registry",
      Tags: { env: "prod" },
    };
    const out = await seam.createSchema(input);

    expect(out.SchemaVersionId).toBe("33333333-3333-3333-3333-333333333333");
    expect(sdkMock.commandCalls(CreateSchemaCommand)).toHaveLength(1);
    const [call] = sdkMock.commandCalls(CreateSchemaCommand);
    expect(call!.args[0].input).toEqual(input);
  });

  it("routes registerSchemaVersion through RegisterSchemaVersionCommand", async () => {
    sdkMock.on(RegisterSchemaVersionCommand).resolves({
      SchemaVersionId: "44444444-4444-4444-4444-444444444444",
      Status: "PENDING",
    });

    const seam = createGlueClient(parseConfig({}));
    const out = await seam.registerSchemaVersion({
      SchemaId: { SchemaName: "s", RegistryName: "default-registry" },
      SchemaDefinition: "{}",
    });

    expect(out.SchemaVersionId).toBe("44444444-4444-4444-4444-444444444444");
    expect(out.Status).toBe("PENDING");
    expect(sdkMock.commandCalls(RegisterSchemaVersionCommand)).toHaveLength(1);
  });

  it("routes putSchemaVersionMetadata through PutSchemaVersionMetadataCommand", async () => {
    sdkMock.on(PutSchemaVersionMetadataCommand).resolves({});

    const seam = createGlueClient(parseConfig({}));
    await seam.putSchemaVersionMetadata({
      SchemaVersionId: "55555555-5555-5555-5555-555555555555",
      MetadataKeyValue: { MetadataKey: "k", MetadataValue: "v" },
    });

    expect(sdkMock.commandCalls(PutSchemaVersionMetadataCommand)).toHaveLength(
      1,
    );
    const [call] = sdkMock.commandCalls(PutSchemaVersionMetadataCommand);
    expect(call!.args[0].input.SchemaVersionId).toBe(
      "55555555-5555-5555-5555-555555555555",
    );
    expect(call!.args[0].input.MetadataKeyValue).toEqual({
      MetadataKey: "k",
      MetadataValue: "v",
    });
  });

  it("routes querySchemaVersionMetadata through QuerySchemaVersionMetadataCommand", async () => {
    sdkMock.on(QuerySchemaVersionMetadataCommand).resolves({
      MetadataInfoMap: {
        "x-amz-meta-transport": { MetadataValue: "topic-1" },
      },
    });

    const seam = createGlueClient(parseConfig({}));
    const out = await seam.querySchemaVersionMetadata({
      SchemaVersionId: "66666666-6666-6666-6666-666666666666",
    });

    expect(out.MetadataInfoMap?.["x-amz-meta-transport"]?.MetadataValue).toBe(
      "topic-1",
    );
    expect(sdkMock.commandCalls(QuerySchemaVersionMetadataCommand)).toHaveLength(
      1,
    );
  });

  it("routes getTags through GetTagsCommand", async () => {
    sdkMock.on(GetTagsCommand).resolves({ Tags: { env: "prod" } });

    const seam = createGlueClient(parseConfig({}));
    const out = await seam.getTags({
      ResourceArn: "arn:aws:glue:us-east-2:123456789012:schema/default-registry/s",
    });

    expect(out.Tags).toEqual({ env: "prod" });
    expect(sdkMock.commandCalls(GetTagsCommand)).toHaveLength(1);
  });

  it("propagates SDK rejections back through the seam method", async () => {
    const boom = new Error("Glue exploded");
    sdkMock.on(GetSchemaByDefinitionCommand).rejects(boom);

    const seam = createGlueClient(parseConfig({}));

    await expect(
      seam.getSchemaByDefinition({
        SchemaId: { SchemaName: "s", RegistryName: "default-registry" },
        SchemaDefinition: "{}",
      }),
    ).rejects.toThrow("Glue exploded");
  });
});

describe("createGlueClient — mock substitution", () => {
  it("is fully substitutable by aws-sdk-client-mock: only mocked commands respond, unmocked send returns undefined", async () => {
    sdkMock.on(GetSchemaByDefinitionCommand).resolves({
      SchemaVersionId: "77777777-7777-7777-7777-777777777777",
      Status: "AVAILABLE",
    });

    const seam = createGlueClient(parseConfig({}));

    const mocked = await seam.getSchemaByDefinition({
      SchemaId: { SchemaName: "s", RegistryName: "default-registry" },
      SchemaDefinition: "{}",
    });
    expect(mocked.SchemaVersionId).toBe(
      "77777777-7777-7777-7777-777777777777",
    );

    // Unconfigured commands resolve to `undefined` under aws-sdk-client-mock;
    // this proves the underlying SDK `send` is fully stubbed rather than
    // hitting the wire when the seam is exercised in a test environment.
    const unmocked = (await seam.getSchemaVersion({
      SchemaVersionId: "77777777-7777-7777-7777-777777777777",
    })) as unknown;
    expect(unmocked).toBeUndefined();
  });
});
