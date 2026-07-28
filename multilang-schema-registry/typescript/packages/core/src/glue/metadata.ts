/**
 * Schema-version metadata: non-fatal write path + query/tags read path.
 *
 * Mirrors the Go reference `core/metadata.go` (write) and
 * `core/metadata_query.go` (read). The write path is **non-fatal**: per-entry
 * `PutSchemaVersionMetadata` failures are logged and never propagated to the
 * caller (metadata is best-effort telemetry, not a serialization contract).
 * The read path exposes `QuerySchemaVersionMetadata` (flattened to a plain
 * `Record<string, string>`) and `GetTags` for a resolved schema ARN.
 *
 * Merge order for the write batch is transport-entry first, configured
 * metadata second, so a caller who configures
 * `metadata.x-amz-meta-transport=<value>` overrides the per-call
 * `transportName` — Java parity (`AWSSchemaRegistryClient.java:419-422`) and
 * Go parity (`core/metadata.go` `putSchemaVersionMetadataBatch`).
 *
 * The transport entry is ALWAYS injected, including when `transportName === ""`.
 *
 * Logging uses the platform `console.warn` with a `gsr:` prefix — mirroring
 * Go's stdlib `log.Printf` with the same prefix — so no new logging
 * dependency is introduced.
 */
import type { GlueClient } from "./client-seam.js";
import type { GsrConfig } from "../config/config.js";
import { classifyGlueError } from "../errors/gsr-errors.js";

/**
 * Canonical metadata key stamped on every registered/registered-again schema
 * version by the client. Mirrors Go `TransportMetadataKey` in
 * `core/metadata.go` and the Java constant in
 * `AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY`.
 */
export const TRANSPORT_METADATA_KEY = "x-amz-meta-transport";

/**
 * Non-fatal write surface for schema-version metadata.
 *
 * The batch write iterates the merged (transport + configured) map
 * sequentially and calls `PutSchemaVersionMetadata` once per entry. Failures
 * on individual calls are logged and skipped; `AlreadyExists` is treated as
 * success (idempotent write). The returned `Promise` resolves regardless of
 * partial or total failure — the write path never throws to the caller.
 */
export interface MetadataWriter {
  /**
   * Write each merged `(key, value)` pair under `schemaVersionId` via
   * `PutSchemaVersionMetadata`.
   *
   * Merge order: `{ [TRANSPORT_METADATA_KEY]: transportName }` is set first,
   * then the configured metadata overlays on top — so a configured entry
   * keyed at `TRANSPORT_METADATA_KEY` wins over the per-call `transportName`
   * (Java/Go collision parity). The transport entry is ALWAYS injected,
   * even when `transportName === ""`.
   *
   * The loop is deliberately sequential (deterministic ordering; low
   * cardinality — typically 1-10 entries — makes parallelism unnecessary).
   * Per-entry rejections are logged via `console.warn` with a `gsr:` prefix
   * and skipped. `AlreadyExists` rejections are silently treated as success.
   * The returned Promise resolves whether zero, some, or all entries failed.
   */
  putSchemaVersionMetadataBatch(
    schemaVersionId: string,
    transportName: string,
  ): Promise<void>;
}

/**
 * Read surface for schema-version metadata and schema-ARN tags.
 */
export interface MetadataReader {
  /**
   * Fetch the metadata map stored on a schema version and flatten the Glue
   * `MetadataInfoMap` into a plain `Record<string, string>` by reading each
   * entry's `MetadataValue`. Mirrors Go `QuerySchemaVersionMetadata`
   * (`core/metadata_query.go`) and Java
   * `AWSSchemaRegistryClient.querySchemaVersionMetadata`.
   */
  querySchemaVersionMetadata(
    schemaVersionId: string,
  ): Promise<Record<string, string>>;

  /**
   * Fetch the tags map on a schema resource identified by its ARN. Mirrors
   * Java `AWSSchemaRegistryClient.querySchemaTags` (which the Go reference
   * transcribes as `QuerySchemaTags`). Unlike the Go signature — which takes
   * a schema definition + name and resolves the ARN via
   * `GetSchemaByDefinition` internally — this TS surface takes the ARN
   * directly, because the caller already knows the schema-version UUID's
   * parent schema ARN by the time it reaches this call and threading an
   * already-resolved ARN keeps the read path a single Glue call.
   */
  getSchemaTags(resourceArn: string): Promise<Record<string, string>>;
}

/**
 * Build a bound `MetadataWriter & MetadataReader` that talks to the supplied
 * {@link GlueClient} seam using the caller's `GsrConfig` for the write-side
 * configured-metadata map. Pure — no network I/O beyond the underlying seam
 * calls; the returned object holds references only, no cached state.
 */
export function createMetadata(
  client: GlueClient,
  cfg: GsrConfig,
): MetadataWriter & MetadataReader {
  return {
    async putSchemaVersionMetadataBatch(
      schemaVersionId: string,
      transportName: string,
    ): Promise<void> {
      // Merge order: transport first, configured second. When the caller
      // configures a `metadata.x-amz-meta-transport=<value>` entry, that
      // configured value replaces the per-call `transportName` (Java/Go
      // collision parity). The transport entry is ALWAYS injected — the
      // initial assignment is unconditional, even when `transportName === ""`.
      const merged: Record<string, string> = {
        [TRANSPORT_METADATA_KEY]: transportName,
      };
      for (const [k, v] of Object.entries(cfg.metadata)) {
        merged[k] = v;
      }

      for (const [key, value] of Object.entries(merged)) {
        try {
          await client.putSchemaVersionMetadata({
            SchemaVersionId: schemaVersionId,
            MetadataKeyValue: { MetadataKey: key, MetadataValue: value },
          });
        } catch (err) {
          // `AlreadyExists` on a metadata key is an idempotent-write signal —
          // the key/value is already stored on this schema version, so treat
          // it as success and move on without logging (Java swallow-and-
          // continue at `AWSSchemaRegistryClient.java:425`).
          if (classifyGlueError(err) === "already-exists") {
            continue;
          }
          // Log-and-continue for everything else. The write path is
          // best-effort — a failure here MUST NOT propagate to the caller
          // (metadata is telemetry, not a serialization contract).
          console.warn(
            `gsr: put schema version metadata key=${key} value=${value}:`,
            err,
          );
        }
      }
    },

    async querySchemaVersionMetadata(
      schemaVersionId: string,
    ): Promise<Record<string, string>> {
      const resp = await client.querySchemaVersionMetadata({
        SchemaVersionId: schemaVersionId,
      });
      const out: Record<string, string> = {};
      const infoMap = resp.MetadataInfoMap;
      if (infoMap) {
        for (const [k, info] of Object.entries(infoMap)) {
          if (info?.MetadataValue !== undefined) {
            out[k] = info.MetadataValue;
          }
        }
      }
      return out;
    },

    async getSchemaTags(
      resourceArn: string,
    ): Promise<Record<string, string>> {
      const resp = await client.getTags({ ResourceArn: resourceArn });
      return resp.Tags ?? {};
    },
  };
}
