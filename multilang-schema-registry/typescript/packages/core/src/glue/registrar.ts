/**
 * Register-on-serialize auto-registration path.
 *
 * `SchemaRegistrar.getSchemaVersionId` resolves the Glue schema-version UUID
 * that the wire header will carry. Transcribes the Go reference
 * `core/encoder.go` (`getSchemaVersionIdByDefinition`, `fetchSchemaVersionID`,
 * `createSchema`, `registerSchemaVersion`, `waitForSchemaEvolutionCheck`)
 * behavior beat-for-beat:
 *
 *   1. Cache fast path on `${schemaName}:${dataFormat}`; hit → return, no
 *      metadata flush (a cached hit means the schema-version metadata was
 *      already written on the miss branch that produced the entry).
 *   2. Concurrent-first-resolve de-duplication onto one in-flight `Promise`
 *      per cache key, the JS-idiomatic equivalent of Go's
 *      `singleflight.Group`. The in-flight entry is deleted when the promise
 *      settles (success OR failure), so a failing lookup does not poison the
 *      key. The cache is re-checked inside the deduped section for the case
 *      where a sibling call raced ahead and populated it.
 *   3. `GetSchemaByDefinition`. `SchemaVersionId` + `Status === "AVAILABLE"`
 *      → cache + return.
 *   4. Error discrimination via `classifyGlueError`. Only `entity-not-found`
 *      falls through to auto-register; everything else (`already-exists` at
 *      this stage is anomalous and treated as `other`; `AccessDenied`,
 *      `Throttling`, `InvalidInput`, network) propagates as
 *      `GsrRegistrationError` wrapping the original on `.cause`. Grounded in
 *      Go's `errors.As(&notFound)` gate.
 *   5. Auto-register gate. `schemaAutoRegistrationEnabled === false` on an
 *      unknown schema → `GsrAutoRegistrationDisabledError` (Go's bug-1 fix:
 *      honor the flag).
 *   6. `CreateSchema` with `Compatibility=cfg.compatibility` — the
 *      compatibility-mode carry — inline `Tags` from config, and the
 *      resolved description. `AlreadyExistsException` from `CreateSchema`
 *      (concurrent-producer race) → fall through to step 7. Any other error
 *      → `GsrRegistrationError`. Success → non-fatal metadata flush + cache
 *      + return the UUID.
 *   7. `RegisterSchemaVersion` fallback. If the returned `Status` is not
 *      `AVAILABLE`, poll `GetSchemaVersion` up to 10 attempts, sleeping 3s
 *      **before each** attempt (Go `schemaEvolutionMaxAttempts=10`,
 *      `schemaEvolutionMaxWaitInterval=3s`, sleep-before-poll). `AVAILABLE`
 *      → non-fatal metadata flush + cache + return. `FAILURE` / `DELETING` /
 *      any unexpected status / poll exhaustion / a poll-time SDK rejection
 *      → `GsrRegistrationError`.
 *
 * The `sleep` option is the Tier-1 seam that lets tests drive the poll loop
 * without real time (Go's `sleepFn` field). This module holds no state
 * beyond the in-flight map — the cache and Glue client are the shared state
 * the caller owns.
 *
 * This module MUST remain transport-agnostic: no Kafka, no serde-layer
 * imports, no encode/serde byte-path touches.
 */
import type { Compatibility, DataFormat } from "@aws-sdk/client-glue";
import type { GsrCache } from "../cache/lru-cache.js";
import type { GsrConfig } from "../config/config.js";
import {
  classifyGlueError,
  GsrAutoRegistrationDisabledError,
  GsrRegistrationError,
} from "../errors/gsr-errors.js";
import type { GlueClient } from "./client-seam.js";
import type { MetadataWriter } from "./metadata.js";

/**
 * Max `GetSchemaVersion` poll attempts before the poll loop gives up.
 * Mirrors Go `schemaEvolutionMaxAttempts` / Java
 * `MAX_SCHEMA_EVOLUTION_CHECK_RETRIES`.
 */
const MAX_POLL_ATTEMPTS = 10;

/**
 * Sleep duration in milliseconds before each `GetSchemaVersion` poll. Mirrors
 * Go `schemaEvolutionMaxWaitInterval` (`3 * time.Second`) / Java
 * `MAX_SCHEMA_WAIT_INTERVAL_SECONDS`.
 */
const POLL_INTERVAL_MS = 3_000;

/**
 * Cached entry shape. Only the schema-version UUID is retained — the
 * cache-hit fast path needs nothing else. Kept as an object (rather than a
 * bare `string`) so downstream by-definition consumers may attach further
 * per-schema fields (data format, additional info) without a cache reshape.
 */
export interface CachedSchemaVersion {
  schemaVersionId: string;
}

/**
 * Callable identity for a schema being resolved. `schemaDefinition` is the
 * schema body forwarded verbatim to `CreateSchema` / `RegisterSchemaVersion`
 * / `GetSchemaByDefinition`; `schemaName` and `dataFormat` compose the cache
 * key. `dataFormat` rides through to `CreateSchema.DataFormat` — the
 * registrar is format-agnostic and does not inspect the definition string.
 */
export interface SchemaIdentity {
  schemaName: string;
  dataFormat: string;
  schemaDefinition: string;
}

/**
 * Construction-time options for the registrar. All options are Tier-1
 * determinism seams; production callers should not set them.
 */
export interface SchemaRegistrarOptions {
  /**
   * Sleep function used before each `GetSchemaVersion` poll call. Tests
   * inject a synchronous or mocked variant to drive the poll loop without
   * real time. Default: real `setTimeout`-backed `Promise` resolver.
   */
  sleep?: (ms: number) => Promise<void>;
}

/**
 * Default `sleep` — a `setTimeout`-backed `Promise` resolver.
 */
function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Build the cache key for a schema. Format `<schemaName>:<dataFormat>`, byte-
 * identical to the Go reference's `fmt.Sprintf("%s:%s", schemaName,
 * dataFormat)`. Composing the key on two fields lets the same schema name
 * coexist under two data formats — no collision, no cache-poisoning across
 * formats.
 */
function cacheKeyFor(schema: SchemaIdentity): string {
  return `${schema.schemaName}:${schema.dataFormat}`;
}

/**
 * Resolves the Glue schema-version UUID for a given schema on serialize.
 * Cache-hits skip Glue entirely; cache-misses fan out through the
 * `GetSchemaByDefinition` → `CreateSchema` → `RegisterSchemaVersion` +
 * poll fall-through path documented on the module.
 *
 * Instances are safe to share across concurrent serialize callers: the
 * in-flight map de-duplicates concurrent first-resolves onto one Glue call,
 * and both the cache and the underlying Glue seam are the caller's shared
 * state.
 */
export class SchemaRegistrar {
  private readonly client: GlueClient;
  private readonly cache: GsrCache<CachedSchemaVersion>;
  private readonly cfg: GsrConfig;
  private readonly metadata: MetadataWriter;
  private readonly sleep: (ms: number) => Promise<void>;

  /**
   * The in-flight de-dup map. Concurrent first-resolves for the same cache
   * key collapse onto one entry. Every entry is deleted when the resolve
   * settles (via `finally` inside the promise executor), so a failing
   * lookup does not poison the key for subsequent callers.
   */
  private readonly inFlight = new Map<string, Promise<string>>();

  constructor(
    client: GlueClient,
    cache: GsrCache<CachedSchemaVersion>,
    cfg: GsrConfig,
    metadata: MetadataWriter,
    opts?: SchemaRegistrarOptions,
  ) {
    this.client = client;
    this.cache = cache;
    this.cfg = cfg;
    this.metadata = metadata;
    this.sleep = opts?.sleep ?? defaultSleep;
  }

  /**
   * Resolve the schema-version UUID for `schema`, registering the schema on
   * first miss when auto-registration is enabled. `transportName` is
   * threaded per-call for the metadata flush and is intentionally NOT
   * stashed on the instance — an instance may serve concurrent callers with
   * different transports (Go's Option A over Option B trade-off).
   *
   * On a cache hit the returned UUID is the previously-resolved value and
   * no Glue call or metadata flush fires. On a cache miss the full
   * fall-through path runs under an in-flight promise shared by any
   * concurrent callers on the same cache key.
   */
  async getSchemaVersionId(
    schema: SchemaIdentity,
    transportName: string,
  ): Promise<string> {
    const key = cacheKeyFor(schema);

    // 1. Fast path — cache hit skips Glue entirely.
    const cached = this.cache.get(key);
    if (cached !== undefined) {
      return cached.schemaVersionId;
    }

    // 2. In-flight de-dup — collapse concurrent first-resolves onto one
    //    Promise. Note that we deliberately return the same promise
    //    reference to every caller so they all resolve with the same value
    //    (or reject with the same error).
    const already = this.inFlight.get(key);
    if (already !== undefined) {
      return already;
    }

    const pending = this.resolveUncached(schema, transportName, key).finally(
      () => {
        // Delete on settle — success OR failure. A failed lookup must not
        // poison the key; the next caller is entitled to retry.
        this.inFlight.delete(key);
      },
    );
    this.inFlight.set(key, pending);
    return pending;
  }

  /**
   * The Glue-talking body of `getSchemaVersionId`. Runs inside the in-flight
   * de-dup wrapper; the cache is re-checked here first for the case where a
   * sibling call raced ahead of us and populated it between the outer
   * cache-miss and the in-flight `set`.
   */
  private async resolveUncached(
    schema: SchemaIdentity,
    transportName: string,
    key: string,
  ): Promise<string> {
    // Re-check the cache inside the deduped section. A sibling call may
    // have populated it in the time between the outer miss and setting the
    // in-flight entry.
    const cached = this.cache.get(key);
    if (cached !== undefined) {
      return cached.schemaVersionId;
    }

    // 3. GetSchemaByDefinition — cheap read that skips the register write
    //    when the schema is already known to Glue.
    let getResp;
    try {
      getResp = await this.client.getSchemaByDefinition({
        SchemaId: {
          RegistryName: this.cfg.registryName,
          SchemaName: schema.schemaName,
        },
        SchemaDefinition: schema.schemaDefinition,
      });
    } catch (err) {
      // 4. Error discrimination on the raw seam error. Classify FIRST, then
      //    wrap — `classifyGlueError` looks at the outer error's `.name`
      //    and does not unwrap.
      if (classifyGlueError(err) === "entity-not-found") {
        // Fall through to the auto-register gate below.
        getResp = undefined;
      } else {
        throw new GsrRegistrationError(
          `failed to get schema by definition: schemaName=${schema.schemaName}`,
          { cause: err },
        );
      }
    }

    if (
      getResp !== undefined &&
      getResp.SchemaVersionId &&
      getResp.Status === "AVAILABLE"
    ) {
      const uuid = getResp.SchemaVersionId;
      this.cache.set(key, { schemaVersionId: uuid });
      return uuid;
    }

    // 5. Auto-register gate. If disabled, the caller opted out of any
    //    write, so bail with the typed error even though the schema is not
    //    yet known to Glue.
    if (!this.cfg.schemaAutoRegistrationEnabled) {
      throw new GsrAutoRegistrationDisabledError(
        `schema auto-registration is disabled: schemaName=${schema.schemaName}`,
      );
    }

    // 6. CreateSchema — first-registration write with the compat-mode carry
    //    and inline tags/description.
    let createResp;
    try {
      createResp = await this.client.createSchema({
        RegistryId: { RegistryName: this.cfg.registryName },
        SchemaName: schema.schemaName,
        DataFormat: schema.dataFormat as DataFormat,
        SchemaDefinition: schema.schemaDefinition,
        Compatibility: this.cfg.compatibility as Compatibility,
        Description: this.cfg.description,
        Tags: this.buildTags(),
      });
    } catch (err) {
      if (classifyGlueError(err) === "already-exists") {
        // 7. Concurrent-producer race — fall through to
        //    RegisterSchemaVersion + poll.
        return this.registerAndPoll(schema, transportName, key);
      }
      throw new GsrRegistrationError(
        `failed to create schema: schemaName=${schema.schemaName}`,
        { cause: err },
      );
    }

    if (!createResp.SchemaVersionId) {
      throw new GsrRegistrationError(
        `create schema returned no SchemaVersionId: schemaName=${schema.schemaName}`,
      );
    }

    const uuid = createResp.SchemaVersionId;
    // Non-fatal metadata flush (write path never throws to the caller —
    // `MetadataWriter.putSchemaVersionMetadataBatch` swallows partial or
    // total failure and logs).
    await this.metadata.putSchemaVersionMetadataBatch(uuid, transportName);
    this.cache.set(key, { schemaVersionId: uuid });
    return uuid;
  }

  /**
   * Register a new schema version for a schema that already exists in Glue
   * (concurrent-producer race), then poll `GetSchemaVersion` until the
   * version reports `AVAILABLE` (or fail with `GsrRegistrationError`).
   * Metadata flushes after the poll succeeds.
   */
  private async registerAndPoll(
    schema: SchemaIdentity,
    transportName: string,
    key: string,
  ): Promise<string> {
    let regResp;
    try {
      regResp = await this.client.registerSchemaVersion({
        SchemaId: {
          RegistryName: this.cfg.registryName,
          SchemaName: schema.schemaName,
        },
        SchemaDefinition: schema.schemaDefinition,
      });
    } catch (err) {
      throw new GsrRegistrationError(
        `failed to register schema version: schemaName=${schema.schemaName}`,
        { cause: err },
      );
    }

    if (!regResp.SchemaVersionId) {
      throw new GsrRegistrationError(
        `register schema version returned no SchemaVersionId: schemaName=${schema.schemaName}`,
      );
    }

    const uuid = regResp.SchemaVersionId;

    if (regResp.Status !== "AVAILABLE") {
      await this.pollUntilAvailable(uuid);
    }

    // Non-fatal metadata flush — fires only after AVAILABLE is confirmed.
    await this.metadata.putSchemaVersionMetadataBatch(uuid, transportName);
    this.cache.set(key, { schemaVersionId: uuid });
    return uuid;
  }

  /**
   * Poll `GetSchemaVersion` up to `MAX_POLL_ATTEMPTS` times, sleeping
   * `POLL_INTERVAL_MS` BEFORE each call (Go / Java parity: the sleep
   * precedes the poll). Returns on `AVAILABLE`; throws
   * `GsrRegistrationError` on `FAILURE` / `DELETING` / any unexpected
   * status / poll exhaustion / a poll-time SDK rejection.
   */
  private async pollUntilAvailable(schemaVersionId: string): Promise<void> {
    let lastStatus: string | undefined;

    for (let i = 0; i < MAX_POLL_ATTEMPTS; i++) {
      // Sleep BEFORE the poll — matches Java `Thread.sleep` at the top of
      // the do-while body / Go's `sleepFn` call before the poll.
      await this.sleep(POLL_INTERVAL_MS);

      let pollResp;
      try {
        pollResp = await this.client.getSchemaVersion({
          SchemaVersionId: schemaVersionId,
        });
      } catch (err) {
        throw new GsrRegistrationError(
          `schema evolution check failed: schemaVersionId=${schemaVersionId}`,
          { cause: err },
        );
      }

      lastStatus = pollResp.Status;
      if (lastStatus === "AVAILABLE") {
        return;
      }
      if (lastStatus !== "PENDING") {
        // FAILURE, DELETING, or any unexpected status: bail immediately.
        throw new GsrRegistrationError(
          `schema evolution check failed: schemaVersionId=${schemaVersionId} status=${String(
            lastStatus,
          )}`,
        );
      }
      // PENDING → keep polling.
    }

    throw new GsrRegistrationError(
      `schema evolution check retries exhausted: schemaVersionId=${schemaVersionId} status=${String(
        lastStatus,
      )}`,
    );
  }

  /**
   * Return the configured `Tags` map to pass on `CreateSchema.Tags`, or
   * `undefined` when the config carries no tags. The Glue SDK treats an
   * empty map and an omitted field the same, but omitting the field keeps
   * the request payload minimal (Go parity: `Tags: nil` when empty).
   */
  private buildTags(): Record<string, string> | undefined {
    if (Object.keys(this.cfg.tags).length === 0) {
      return undefined;
    }
    return this.cfg.tags;
  }
}
