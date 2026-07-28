/**
 * Reusable TTL + size LRU cache wrapper for the `@gsr/core` Glue-integration
 * layer. Mirrors the Go reference `core/cache.go` `LRUCacheWrapper`: TTL is
 * computed on `set`, re-evaluated on `get`, and the LRU-oldest entry is
 * evicted when inserting at capacity.
 *
 * The wrapper is **key-scheme-agnostic** (keys are opaque strings). Callers
 * instantiate a separate `createCache` per logical cache — this keeps the
 * definition-cache key space (`name:format` composites) and the deferred
 * by-id key space (schema-version UUIDs) in structurally independent
 * backing stores that cannot collide.
 *
 * The `now` option is the deterministic-clock seam that mirrors Go's
 * injected `Clock`: tests advance it (or use fake timers) to drive TTL
 * eviction without real sleeps.
 */
import { LRUCache } from "lru-cache";

import {
  DEFAULT_CACHE_SIZE,
  DEFAULT_CACHE_TTL_MILLIS,
} from "../config/config.js";

/**
 * Minimal cache surface the Glue registrar (and, later, the by-id serde
 * consumer) depend on. The four operations are the strict subset of Go's
 * `Cache` interface that the client actually calls: `Get`, `Set`, `Remove`,
 * and `Close`/`Purge` (mapped to `clear`).
 *
 * `V extends {}` mirrors the `lru-cache` value constraint — `null` and
 * `undefined` collide with the "not-present" sentinel `get` returns for a
 * miss, so they are structurally unstorable.
 */
export interface GsrCache<V extends {}> {
  /** Look up a key. Returns `undefined` on miss or once the entry has expired. */
  get(key: string): V | undefined;
  /** Insert (or overwrite) a key. Resets the per-entry TTL clock. */
  set(key: string, value: V): void;
  /** Delete a key. Returns `true` iff the key was present. */
  remove(key: string): boolean;
  /** Drop every entry (Go `Cache.Close`/`Purge` semantics). */
  clear(): void;
}

/**
 * Construction-time options. All three are optional; a `≤0`, non-finite, or
 * non-integer value on `ttlMillis`/`size` coerces to the default (24h / 200,
 * mirroring Java Caffeine and Go defaults). Values are otherwise passed to
 * `lru-cache` untouched.
 */
export interface CacheOptions {
  /** Per-entry lifetime in milliseconds. Default: `DEFAULT_CACHE_TTL_MILLIS` (24h). */
  ttlMillis?: number;
  /** Hard entry-count cap; LRU-oldest is evicted on insert at capacity. Default: `DEFAULT_CACHE_SIZE` (200). */
  size?: number;
  /**
   * Clock seam for deterministic TTL tests. When supplied, `lru-cache` reads
   * the current time through it instead of `performance.now()` / `Date.now()`.
   * Advance it (or use fake timers) to age entries without real sleeps.
   */
  now?: () => number;
}

/**
 * Build a fresh `GsrCache<V>` backed by `lru-cache`. Each call returns an
 * independent backing store — callers deliberately instantiate one per
 * logical cache so key spaces stay structurally disjoint.
 */
export function createCache<V extends {}>(opts?: CacheOptions): GsrCache<V> {
  const ttl = coercePositiveInteger(opts?.ttlMillis, DEFAULT_CACHE_TTL_MILLIS);
  const max = coercePositiveInteger(opts?.size, DEFAULT_CACHE_SIZE);
  const now = opts?.now;

  const inner = new LRUCache<string, V>({
    max,
    ttl,
    // `perf.now()` is the internal clock `lru-cache` reads for TTL tracking.
    // Wiring the injected `now` through it — with ttlResolution=0 so the
    // library re-reads the clock on every check instead of caching it for
    // the default 1 ms — is what makes Tier-1 TTL tests deterministic when
    // the caller advances the clock in synchronous jumps.
    ...(now ? { perf: { now }, ttlResolution: 0 } : {}),
  });

  return {
    get: (key) => inner.get(key),
    set: (key, value) => {
      inner.set(key, value);
    },
    remove: (key) => inner.delete(key),
    clear: () => {
      inner.clear();
    },
  };
}

/** Return `value` iff it is a positive-integer number; otherwise the fallback. */
function coercePositiveInteger(value: number | undefined, fallback: number): number {
  return typeof value === "number" && Number.isInteger(value) && value > 0
    ? value
    : fallback;
}
