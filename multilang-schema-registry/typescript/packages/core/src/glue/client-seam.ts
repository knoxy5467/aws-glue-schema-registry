/**
 * Narrow Glue client seam for the `@gsr/core` control-plane surface.
 *
 * `GlueClient` declares exactly the seven `@aws-sdk/client-glue` v3 operations
 * the client calls, one-to-one with Go's `GlueClient` interface
 * (`core/client_interface.go`), which enumerates the Glue calls the Java
 * SerDe makes:
 *
 *   - `GetSchemaByDefinition`      (definition → schema-version lookup)
 *   - `GetSchemaVersion`           (version-id → schema; poll target)
 *   - `CreateSchema`               (first-registration path; tags + description ride inline)
 *   - `RegisterSchemaVersion`      (concurrent-producer race fallback after `CreateSchema` `AlreadyExistsException`)
 *   - `PutSchemaVersionMetadata`   (metadata-write path)
 *   - `QuerySchemaVersionMetadata` (metadata-read path)
 *   - `GetTags`                    (tags-read path)
 *
 * `CreateRegistry` and `TagResource` are deliberately omitted — Go's seam
 * omits them too (Java never calls `createRegistry` or `tagResource`;
 * registries are assumed to exist, and tags ride inline on `CreateSchema`).
 * This preserves the auto-creation posture around `default-registry`.
 *
 * `createGlueClient(cfg)` builds a concrete SDK `GlueClient` from
 * `buildGlueClientConfig(cfg)` and adapts SDK v3's command/`send` shape to
 * the seam methods, so every downstream module (registrar, metadata reader)
 * talks to the same narrow surface — the concrete SDK client in production,
 * an `aws-sdk-client-mock`-driven stub or hand-rolled fake in tests.
 */
import {
  CreateSchemaCommand,
  type CreateSchemaCommandInput,
  type CreateSchemaCommandOutput,
  GetSchemaByDefinitionCommand,
  type GetSchemaByDefinitionCommandInput,
  type GetSchemaByDefinitionCommandOutput,
  GetSchemaVersionCommand,
  type GetSchemaVersionCommandInput,
  type GetSchemaVersionCommandOutput,
  GetTagsCommand,
  type GetTagsCommandInput,
  type GetTagsCommandOutput,
  GlueClient as SdkGlueClient,
  PutSchemaVersionMetadataCommand,
  type PutSchemaVersionMetadataCommandInput,
  type PutSchemaVersionMetadataCommandOutput,
  QuerySchemaVersionMetadataCommand,
  type QuerySchemaVersionMetadataCommandInput,
  type QuerySchemaVersionMetadataCommandOutput,
  RegisterSchemaVersionCommand,
  type RegisterSchemaVersionCommandInput,
  type RegisterSchemaVersionCommandOutput,
} from "@aws-sdk/client-glue";

import { buildGlueClientConfig } from "../config/client-options.js";
import type { GsrConfig } from "../config/config.js";

/**
 * Narrow contract over exactly the Glue operations the client calls.
 *
 * Every downstream control-plane consumer (registrar, metadata read/write)
 * depends on this interface, never on the concrete SDK class — so tests can
 * substitute a stub built with `aws-sdk-client-mock` (which wraps the
 * concrete SDK client) or a hand-rolled fake that implements this seam
 * directly. Neither `createRegistry` nor `tagResource` is declared here;
 * see the module-level notes for the rationale.
 */
export interface GlueClient {
  getSchemaByDefinition(
    input: GetSchemaByDefinitionCommandInput,
  ): Promise<GetSchemaByDefinitionCommandOutput>;
  getSchemaVersion(
    input: GetSchemaVersionCommandInput,
  ): Promise<GetSchemaVersionCommandOutput>;
  createSchema(
    input: CreateSchemaCommandInput,
  ): Promise<CreateSchemaCommandOutput>;
  registerSchemaVersion(
    input: RegisterSchemaVersionCommandInput,
  ): Promise<RegisterSchemaVersionCommandOutput>;
  putSchemaVersionMetadata(
    input: PutSchemaVersionMetadataCommandInput,
  ): Promise<PutSchemaVersionMetadataCommandOutput>;
  querySchemaVersionMetadata(
    input: QuerySchemaVersionMetadataCommandInput,
  ): Promise<QuerySchemaVersionMetadataCommandOutput>;
  getTags(input: GetTagsCommandInput): Promise<GetTagsCommandOutput>;
}

/**
 * Build a concrete {@link GlueClient} backed by an `@aws-sdk/client-glue`
 * v3 client configured via {@link buildGlueClientConfig}.
 *
 * Each seam method builds the matching SDK `Command` and awaits
 * `client.send(command)`. `aws-sdk-client-mock` intercepts the underlying
 * `send()` method on the SDK client, so tests can substitute behavior for
 * any subset of these calls without touching the seam. Alternatively,
 * consumers may pass a hand-rolled object that implements {@link GlueClient}
 * directly — the interface is the substitution boundary.
 */
export function createGlueClient(cfg: GsrConfig): GlueClient {
  const sdk = new SdkGlueClient(buildGlueClientConfig(cfg));

  return {
    getSchemaByDefinition: (input) =>
      sdk.send(new GetSchemaByDefinitionCommand(input)),
    getSchemaVersion: (input) => sdk.send(new GetSchemaVersionCommand(input)),
    createSchema: (input) => sdk.send(new CreateSchemaCommand(input)),
    registerSchemaVersion: (input) =>
      sdk.send(new RegisterSchemaVersionCommand(input)),
    putSchemaVersionMetadata: (input) =>
      sdk.send(new PutSchemaVersionMetadataCommand(input)),
    querySchemaVersionMetadata: (input) =>
      sdk.send(new QuerySchemaVersionMetadataCommand(input)),
    getTags: (input) => sdk.send(new GetTagsCommand(input)),
  };
}
