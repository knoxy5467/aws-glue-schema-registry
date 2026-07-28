/**
 * In-memory `GlueClient` fake for Tier-2 tests.
 *
 * `FakeGlueClient` implements the `@gsr/core` `GlueClient` seam entirely in
 * process — no network, no Docker, no AWS. It maintains a definition→versionId
 * lookup and a versionId→stored-schema map, mirrors Go's `fakeglue.Fake` beat
 * for beat, and exposes:
 *
 *   - Per-method `callCounts` for de-dup / cache-hit assertions.
 *   - `Force*` affordances that let a scenario simulate any of the four Glue
 *     failure modes the classifier discriminates (`EntityNotFoundException`,
 *     `AlreadyExistsException`, `ThrottlingException`, `AccessDeniedException`)
 *     as well as the async register→poll convergence.
 *   - Named-error factory helpers producing `.name`-discriminated errors that
 *     `@gsr/core.classifyGlueError` reads exactly the way it reads
 *     AWS SDK v3 service errors (via the outer error's `.name`).
 *
 * The fake is a Tier-2 substitute; where a scenario needs call-order or
 * argument-matcher assertions it may layer `aws-sdk-client-mock` against the
 * concrete SDK client instead. Both mechanisms are permitted; a scenario
 * picks whichever expresses its assertion most directly.
 */
import type {
  CreateSchemaCommandInput,
  CreateSchemaCommandOutput,
  GetSchemaByDefinitionCommandInput,
  GetSchemaByDefinitionCommandOutput,
  GetSchemaVersionCommandInput,
  GetSchemaVersionCommandOutput,
  GetTagsCommandInput,
  GetTagsCommandOutput,
  PutSchemaVersionMetadataCommandInput,
  PutSchemaVersionMetadataCommandOutput,
  QuerySchemaVersionMetadataCommandInput,
  QuerySchemaVersionMetadataCommandOutput,
  RegisterSchemaVersionCommandInput,
  RegisterSchemaVersionCommandOutput,
} from "@aws-sdk/client-glue";
import type { GlueClient } from "@gsr/core";

/**
 * The seven method names the fake tracks call counts for. Matches
 * `@gsr/core` `GlueClient` one-to-one.
 */
export type FakeGlueMethodName =
  | "getSchemaByDefinition"
  | "getSchemaVersion"
  | "createSchema"
  | "registerSchemaVersion"
  | "putSchemaVersionMetadata"
  | "querySchemaVersionMetadata"
  | "getTags";

/**
 * Zero-initialised call-count map. Kept as a top-level constant so a fresh
 * counter can be produced on construction and on `reset()` without listing
 * every method name in-line each time.
 */
const ZERO_CALL_COUNTS: Readonly<Record<FakeGlueMethodName, number>> =
  Object.freeze({
    getSchemaByDefinition: 0,
    getSchemaVersion: 0,
    createSchema: 0,
    registerSchemaVersion: 0,
    putSchemaVersionMetadata: 0,
    querySchemaVersionMetadata: 0,
    getTags: 0,
  });

/**
 * Named-error `.name` values the fake can raise on the `Force*` fields and
 * that consumers may throw from their own paths. Wired to
 * `@gsr/core.classifyGlueError`, which discriminates on `err.name` exactly
 * the way AWS SDK v3 service errors expose it.
 */
export const GLUE_ERROR_NAMES = {
  EntityNotFound: "EntityNotFoundException",
  AlreadyExists: "AlreadyExistsException",
  Throttling: "ThrottlingException",
  AccessDenied: "AccessDeniedException",
} as const;

/**
 * Build an `Error` whose `.name` matches an AWS SDK v3 Glue service error so
 * `classifyGlueError` (and any `.name`-based comparison) treats it as the
 * real thing. AWS SDK v3 always sets `.name` on service errors to the exact
 * Smithy shape name; setting `.name` on a plain `Error` reproduces that
 * discriminant faithfully without pulling the SDK into this file's runtime
 * dependency graph.
 */
function makeNamedError(name: string, message: string): Error {
  const err = new Error(message);
  err.name = name;
  return err;
}

/**
 * `EntityNotFoundException` factory — the auto-register fall-through signal.
 * Raised by `GetSchemaByDefinition` when the definition is not yet known to
 * Glue; the registrar branches on this to run `CreateSchema`.
 */
export function makeEntityNotFoundError(message = "entity not found"): Error {
  return makeNamedError(GLUE_ERROR_NAMES.EntityNotFound, message);
}

/**
 * `AlreadyExistsException` factory — the concurrent-producer race signal.
 * Raised by `CreateSchema` when a sibling producer registered the same
 * schema first; the registrar falls through to `RegisterSchemaVersion`.
 */
export function makeAlreadyExistsError(message = "already exists"): Error {
  return makeNamedError(GLUE_ERROR_NAMES.AlreadyExists, message);
}

/**
 * `ThrottlingException` factory. Not on the register-flow branching path
 * (surfaces as `other` in `classifyGlueError`) but exposed here so scenarios
 * can drive backoff/retry surfacing.
 */
export function makeThrottlingError(message = "rate exceeded"): Error {
  return makeNamedError(GLUE_ERROR_NAMES.Throttling, message);
}

/**
 * `AccessDeniedException` factory. Surfaces IAM-denied paths as a client
 * error; classifies as `other` and therefore propagates as
 * `GsrRegistrationError` when caught on the register flow.
 */
export function makeAccessDeniedError(message = "access denied"): Error {
  return makeNamedError(GLUE_ERROR_NAMES.AccessDenied, message);
}

/**
 * Options accepted at construction time. Every field is a Tier-2
 * determinism seam — none is required for a bare `new FakeGlueClient()`.
 */
export interface FakeGlueOptions {
  /**
   * `SchemaVersionId` UUIDs the fake will hand out on `CreateSchema` /
   * `RegisterSchemaVersion`, in order. Callers who assert on specific UUIDs
   * supply their own list; when omitted, the fake generates deterministic
   * UUIDs (`00000000-0000-0000-0000-<12-hex>` where the low bytes are a
   * monotonic counter) so the test output is reproducible without depending
   * on `crypto.randomUUID`.
   */
  versionIds?: string[];
}

/**
 * Internal shape stored per registered schema version. `SchemaDefinition` +
 * `DataFormat` are what a caller sends on `RegisterSchemaVersion` /
 * `CreateSchema`; the fake retains them so `GetSchemaVersion` can return
 * them and so a duplicate-definition lookup on `GetSchemaByDefinition`
 * resolves without re-registering.
 */
interface StoredSchemaVersion {
  schemaVersionId: string;
  schemaName: string;
  registryName: string;
  schemaDefinition: string;
  dataFormat: string;
  compatibility?: string;
  description?: string;
  tags: Record<string, string>;
  metadata: Record<string, string>;
  /** Poll-count remaining before this version flips `AVAILABLE`. */
  pendingCount: number;
}

/**
 * Compose the definition-lookup key used by `GetSchemaByDefinition` and
 * (internally) by `CreateSchema`. Registry + schema name + full definition
 * so two schemas with different names but identical bodies never collide,
 * and so a caller who repeats the same (name, definition) lookup gets the
 * previously registered version back.
 */
function definitionKey(
  registryName: string | undefined,
  schemaName: string | undefined,
  schemaDefinition: string | undefined,
): string {
  return `${registryName ?? ""}|${schemaName ?? ""}|${schemaDefinition ?? ""}`;
}

/**
 * In-memory Glue fake. Implements the full `GlueClient` seam interface so
 * TypeScript compile-checks any drift; a scenario constructs a bare
 * `new FakeGlueClient()`, tweaks the `Force*` fields per test, and hands
 * the instance to the registrar/metadata reader.
 *
 * Not thread-safe (Node is single-threaded per event loop; concurrent
 * `Promise` fan-out on one instance is fine because the fake's writes are
 * atomic within a microtask).
 */
export class FakeGlueClient implements GlueClient {
  /**
   * Definition→versionId lookup keyed by
   * `${registryName}|${schemaName}|${schemaDefinition}`.
   */
  private readonly definitionToVersion = new Map<string, string>();

  /** versionId→stored-schema map. */
  private readonly versionToSchema = new Map<string, StoredSchemaVersion>();

  /** Per-method call counter (public via {@link callCounts}). */
  private readonly counts: Record<FakeGlueMethodName, number> = {
    ...ZERO_CALL_COUNTS,
  };

  /** Preseeded UUIDs from the constructor options, popped left-to-right. */
  private readonly seededVersionIds: string[];

  /** Monotonic counter for the fallback UUID generator. */
  private nextGeneratedId = 1;

  // --- Force* affordances -------------------------------------------------

  /**
   * When set, `getSchemaByDefinition` throws this error instead of consulting
   * the store. Set to a `makeEntityNotFoundError()` (or any classifier-typed
   * error) to drive control-flow branches.
   */
  public forceGetSchemaError: Error | null = null;

  /**
   * When set, `createSchema` throws this error. Set to
   * `makeAlreadyExistsError()` to drive the concurrent-producer race path.
   */
  public forceCreateError: Error | null = null;

  /**
   * When set, `getSchemaVersion` throws this error. Drives poll-path
   * failure.
   */
  public forceGetVersionError: Error | null = null;

  /**
   * When true, a newly created/registered schema version starts in
   * `PENDING` state and requires {@link forcePendingCount} `GetSchemaVersion`
   * calls before flipping to `AVAILABLE`. When false, versions are
   * `AVAILABLE` immediately.
   */
  public forceRegisterPending = false;

  /**
   * Number of `PENDING` responses `GetSchemaVersion` returns before the
   * version flips to `AVAILABLE`. Only consulted when
   * {@link forceRegisterPending} is true. `0` means "flip on the first
   * poll" (already `AVAILABLE`); larger values force N `PENDING` polls
   * then one `AVAILABLE`.
   */
  public forcePendingCount = 0;

  constructor(opts: FakeGlueOptions = {}) {
    this.seededVersionIds = opts.versionIds ? [...opts.versionIds] : [];
  }

  /**
   * Per-method call counter. Read-only view — mutations happen inside the
   * seam methods only. Returned as a shallow copy so a caller who snapshots
   * this value at two points can diff them without racing the internal
   * counter.
   */
  get callCounts(): Readonly<Record<FakeGlueMethodName, number>> {
    return { ...this.counts };
  }

  /**
   * Reset counters, `Force*` fields, and the schema store to the pristine
   * post-construction state. Useful in `beforeEach`.
   */
  reset(): void {
    this.definitionToVersion.clear();
    this.versionToSchema.clear();
    for (const k of Object.keys(this.counts) as FakeGlueMethodName[]) {
      this.counts[k] = 0;
    }
    this.forceGetSchemaError = null;
    this.forceCreateError = null;
    this.forceGetVersionError = null;
    this.forceRegisterPending = false;
    this.forcePendingCount = 0;
    this.nextGeneratedId = 1;
  }

  /**
   * Directly read a stored version — for test-side assertions that a
   * scenario wrote what it thought it wrote. Not part of the `GlueClient`
   * seam.
   */
  peekVersion(schemaVersionId: string): StoredSchemaVersion | undefined {
    return this.versionToSchema.get(schemaVersionId);
  }

  // --- GlueClient seam methods -------------------------------------------

  async getSchemaByDefinition(
    input: GetSchemaByDefinitionCommandInput,
  ): Promise<GetSchemaByDefinitionCommandOutput> {
    this.counts.getSchemaByDefinition += 1;
    if (this.forceGetSchemaError) {
      throw this.forceGetSchemaError;
    }

    const key = definitionKey(
      input.SchemaId?.RegistryName,
      input.SchemaId?.SchemaName,
      input.SchemaDefinition,
    );
    const versionId = this.definitionToVersion.get(key);
    if (versionId === undefined) {
      // Not-found is expressed as a typed error (matching real Glue and
      // `classifyGlueError`), not as a null response — the registrar
      // branches on `EntityNotFoundException`, so returning `undefined`
      // would silently break the auto-register flow.
      throw makeEntityNotFoundError(
        `no schema version found for schemaName=${
          input.SchemaId?.SchemaName ?? ""
        }`,
      );
    }
    const stored = this.versionToSchema.get(versionId);
    return {
      $metadata: {},
      SchemaVersionId: versionId,
      Status: this.statusFor(stored),
      SchemaArn: this.schemaArnFor(stored),
      DataFormat: stored?.dataFormat as
        | GetSchemaByDefinitionCommandOutput["DataFormat"]
        | undefined,
    };
  }

  async getSchemaVersion(
    input: GetSchemaVersionCommandInput,
  ): Promise<GetSchemaVersionCommandOutput> {
    this.counts.getSchemaVersion += 1;
    if (this.forceGetVersionError) {
      throw this.forceGetVersionError;
    }

    const versionId = input.SchemaVersionId;
    if (versionId === undefined) {
      throw makeEntityNotFoundError("SchemaVersionId is required");
    }
    const stored = this.versionToSchema.get(versionId);
    if (stored === undefined) {
      throw makeEntityNotFoundError(
        `no schema version found: schemaVersionId=${versionId}`,
      );
    }

    // Poll-loop convergence: each `getSchemaVersion` call decrements
    // `pendingCount` until it hits zero, at which point the caller sees
    // `AVAILABLE`. Modelled as a countdown so `forcePendingCount=2` matches
    // "return PENDING twice, then AVAILABLE" — Go parity with the async
    // poll-to-AVAILABLE loop.
    let status: "AVAILABLE" | "PENDING";
    if (stored.pendingCount > 0) {
      status = "PENDING";
      stored.pendingCount -= 1;
    } else {
      status = "AVAILABLE";
    }

    return {
      $metadata: {},
      SchemaVersionId: versionId,
      Status: status,
      SchemaArn: this.schemaArnFor(stored),
      SchemaDefinition: stored.schemaDefinition,
      DataFormat: stored.dataFormat as
        | GetSchemaVersionCommandOutput["DataFormat"]
        | undefined,
    };
  }

  async createSchema(
    input: CreateSchemaCommandInput,
  ): Promise<CreateSchemaCommandOutput> {
    this.counts.createSchema += 1;
    if (this.forceCreateError) {
      throw this.forceCreateError;
    }

    const registryName = input.RegistryId?.RegistryName;
    const schemaName = input.SchemaName;
    const schemaDefinition = input.SchemaDefinition;
    const dataFormat = input.DataFormat;

    const key = definitionKey(registryName, schemaName, schemaDefinition);
    // Idempotency without `Force*` guidance: real Glue rejects a duplicate
    // `CreateSchema` with `AlreadyExistsException`, so the fake does the
    // same when the schema name already exists in the store. A scenario
    // that wants to short-circuit this can set `forceCreateError` to a
    // named error directly.
    if (this.definitionToVersion.has(key)) {
      throw makeAlreadyExistsError(
        `schema already exists: schemaName=${schemaName ?? ""}`,
      );
    }

    const versionId = this.nextVersionId();
    const stored: StoredSchemaVersion = {
      schemaVersionId: versionId,
      schemaName: schemaName ?? "",
      registryName: registryName ?? "",
      schemaDefinition: schemaDefinition ?? "",
      dataFormat: (dataFormat as string | undefined) ?? "",
      compatibility: input.Compatibility as string | undefined,
      description: input.Description,
      tags: { ...(input.Tags ?? {}) },
      metadata: {},
      pendingCount: this.forceRegisterPending ? this.forcePendingCount : 0,
    };
    this.definitionToVersion.set(key, versionId);
    this.versionToSchema.set(versionId, stored);

    return {
      $metadata: {},
      SchemaVersionId: versionId,
      SchemaName: schemaName,
      SchemaArn: this.schemaArnFor(stored),
      DataFormat: dataFormat,
      Compatibility: input.Compatibility,
      Description: input.Description,
      Tags: input.Tags,
      // `CreateSchemaResponse` exposes the first version's status via
      // `SchemaVersionStatus` (not the generic `Status` used on
      // `RegisterSchemaVersion` / `GetSchemaByDefinition`).
      SchemaVersionStatus: this.statusFor(stored),
    };
  }

  async registerSchemaVersion(
    input: RegisterSchemaVersionCommandInput,
  ): Promise<RegisterSchemaVersionCommandOutput> {
    this.counts.registerSchemaVersion += 1;

    const registryName = input.SchemaId?.RegistryName;
    const schemaName = input.SchemaId?.SchemaName;
    const schemaDefinition = input.SchemaDefinition;

    const key = definitionKey(registryName, schemaName, schemaDefinition);
    // If the exact (name, definition) is already registered, real Glue
    // returns the existing SchemaVersionId with `AVAILABLE` (register is
    // idempotent on identical definitions). The fake mirrors that.
    const existing = this.definitionToVersion.get(key);
    if (existing !== undefined) {
      const stored = this.versionToSchema.get(existing);
      return {
        $metadata: {},
        SchemaVersionId: existing,
        Status: this.statusFor(stored),
      };
    }

    const versionId = this.nextVersionId();
    // Register-under-existing-schema does not carry DataFormat/Compatibility
    // on the input; carry over the fields from any prior version of the
    // same schema when available, else record an empty format.
    const priorFormat = this.findPriorDataFormat(registryName, schemaName);
    const stored: StoredSchemaVersion = {
      schemaVersionId: versionId,
      schemaName: schemaName ?? "",
      registryName: registryName ?? "",
      schemaDefinition: schemaDefinition ?? "",
      dataFormat: priorFormat ?? "",
      metadata: {},
      tags: {},
      pendingCount: this.forceRegisterPending ? this.forcePendingCount : 0,
    };
    this.definitionToVersion.set(key, versionId);
    this.versionToSchema.set(versionId, stored);

    return {
      $metadata: {},
      SchemaVersionId: versionId,
      Status: this.statusFor(stored),
    };
  }

  async putSchemaVersionMetadata(
    input: PutSchemaVersionMetadataCommandInput,
  ): Promise<PutSchemaVersionMetadataCommandOutput> {
    this.counts.putSchemaVersionMetadata += 1;

    const versionId = input.SchemaVersionId;
    const kv = input.MetadataKeyValue;
    if (versionId === undefined) {
      throw makeEntityNotFoundError("SchemaVersionId is required");
    }
    const stored = this.versionToSchema.get(versionId);
    if (stored === undefined) {
      throw makeEntityNotFoundError(
        `no schema version found: schemaVersionId=${versionId}`,
      );
    }
    if (kv?.MetadataKey === undefined) {
      return { $metadata: {}, SchemaVersionId: versionId };
    }
    // Duplicate-key writes surface `AlreadyExistsException` — the real Glue
    // behaviour the writer path treats as an idempotent success.
    if (Object.prototype.hasOwnProperty.call(stored.metadata, kv.MetadataKey)) {
      throw makeAlreadyExistsError(
        `metadata key already set: schemaVersionId=${versionId} key=${kv.MetadataKey}`,
      );
    }
    stored.metadata[kv.MetadataKey] = kv.MetadataValue ?? "";
    return {
      $metadata: {},
      SchemaVersionId: versionId,
      MetadataKey: kv.MetadataKey,
      MetadataValue: kv.MetadataValue,
    };
  }

  async querySchemaVersionMetadata(
    input: QuerySchemaVersionMetadataCommandInput,
  ): Promise<QuerySchemaVersionMetadataCommandOutput> {
    this.counts.querySchemaVersionMetadata += 1;

    const versionId = input.SchemaVersionId;
    if (versionId === undefined) {
      throw makeEntityNotFoundError("SchemaVersionId is required");
    }
    const stored = this.versionToSchema.get(versionId);
    if (stored === undefined) {
      throw makeEntityNotFoundError(
        `no schema version found: schemaVersionId=${versionId}`,
      );
    }
    // Flatten into the SDK's `MetadataInfoMap` shape — the reader consumes
    // `info.MetadataValue` off each entry.
    const infoMap: Record<string, { MetadataValue: string }> = {};
    for (const [k, v] of Object.entries(stored.metadata)) {
      infoMap[k] = { MetadataValue: v };
    }
    return {
      $metadata: {},
      SchemaVersionId: versionId,
      MetadataInfoMap: infoMap,
    };
  }

  async getTags(input: GetTagsCommandInput): Promise<GetTagsCommandOutput> {
    this.counts.getTags += 1;

    const arn = input.ResourceArn;
    if (arn === undefined) {
      return { $metadata: {}, Tags: {} };
    }
    // The fake tracks tags on the primary (first) version of a schema (Glue
    // stores tags on the schema resource, not per version); scan by ARN.
    for (const stored of this.versionToSchema.values()) {
      if (this.schemaArnFor(stored) === arn) {
        return { $metadata: {}, Tags: { ...stored.tags } };
      }
    }
    return { $metadata: {}, Tags: {} };
  }

  // --- Internal helpers --------------------------------------------------

  /**
   * Report `AVAILABLE` when the version has no remaining pending polls,
   * else `PENDING`. Matches Glue's status vocabulary; the seven other
   * statuses (`DELETING` / `FAILURE` / etc.) are not modelled here because
   * a scenario that needs one drives it explicitly via `Force*`.
   */
  private statusFor(
    stored: StoredSchemaVersion | undefined,
  ): "AVAILABLE" | "PENDING" | undefined {
    if (stored === undefined) return undefined;
    return stored.pendingCount > 0 ? "PENDING" : "AVAILABLE";
  }

  /**
   * Synthesise a schema ARN in the Glue-idiomatic shape so tag lookup by
   * ARN can round-trip. Not a real ARN — the fake ignores account/region
   * and derives them from the registry name.
   */
  private schemaArnFor(stored: StoredSchemaVersion | undefined): string {
    if (stored === undefined) return "";
    return `arn:aws:glue:us-east-2:000000000000:schema/${stored.registryName}/${stored.schemaName}`;
  }

  /**
   * Return the `DataFormat` recorded on the earliest version of a given
   * `(registryName, schemaName)` pair, or `undefined` if none exists.
   * `RegisterSchemaVersion` does not carry a `DataFormat` on its input, so
   * a follow-on register needs the format from a prior version to answer a
   * later `getSchemaVersion` call faithfully.
   */
  private findPriorDataFormat(
    registryName: string | undefined,
    schemaName: string | undefined,
  ): string | undefined {
    const rn = registryName ?? "";
    const sn = schemaName ?? "";
    for (const stored of this.versionToSchema.values()) {
      if (stored.registryName === rn && stored.schemaName === sn) {
        return stored.dataFormat;
      }
    }
    return undefined;
  }

  /**
   * Hand out the next `SchemaVersionId`. Seeded UUIDs from the constructor
   * are consumed first; when exhausted, generate a deterministic UUID from
   * a monotonic counter so tests stay reproducible without depending on
   * `crypto.randomUUID`.
   */
  private nextVersionId(): string {
    const seeded = this.seededVersionIds.shift();
    if (seeded !== undefined) {
      return seeded;
    }
    const n = this.nextGeneratedId++;
    const hex = n.toString(16).padStart(12, "0");
    return `00000000-0000-0000-0000-${hex}`;
  }
}
