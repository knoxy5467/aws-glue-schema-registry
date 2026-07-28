import { describe, expect, it } from "vitest";

import {
  DEFAULT_CACHE_SIZE,
  DEFAULT_CACHE_TTL_MILLIS,
} from "../config/config.js";

import { createCache, type CacheOptions } from "./lru-cache.js";

/**
 * Helper — build a `now` seam driven by an in-scope wall value. The returned
 * `advance` fn moves the clock forward synchronously; the cache reads the new
 * value on the next `get`/`set`, yielding deterministic TTL tests without
 * touching real wall-clock time.
 *
 * Baseline defaults to `1` (not `0`) because `lru-cache` treats a start-time
 * of `0` as "no TTL tracked" — matching the library's own convention that a
 * falsy start disables staleness.
 */
function fakeClock(startMillis = 1): { now: () => number; advance: (ms: number) => void } {
  let wall = startMillis;
  return {
    now: () => wall,
    advance: (ms) => {
      wall += ms;
    },
  };
}

describe("createCache — API surface", () => {
  it("get/set/remove/clear behave per the GsrCache<V> contract", () => {
    const cache = createCache<string>({ ttlMillis: 60_000, size: 4 });

    expect(cache.get("a")).toBeUndefined();

    cache.set("a", "one");
    cache.set("b", "two");
    expect(cache.get("a")).toBe("one");
    expect(cache.get("b")).toBe("two");

    // remove returns true only on a hit; subsequent get is a miss.
    expect(cache.remove("a")).toBe(true);
    expect(cache.remove("a")).toBe(false);
    expect(cache.get("a")).toBeUndefined();

    // clear drops everything.
    cache.clear();
    expect(cache.get("b")).toBeUndefined();
  });

  it("overwrites the value when the same key is set twice", () => {
    const cache = createCache<number>({ ttlMillis: 60_000, size: 4 });
    cache.set("k", 1);
    cache.set("k", 2);
    expect(cache.get("k")).toBe(2);
  });
});

describe("createCache — TTL eviction driven by the injected clock", () => {
  it("returns undefined once the clock advances past ttlMillis", () => {
    const clock = fakeClock(1_000);
    const cache = createCache<string>({ ttlMillis: 100, size: 4, now: clock.now });

    cache.set("k", "v");
    expect(cache.get("k")).toBe("v");

    // Advance to the exact TTL boundary — still fresh.
    clock.advance(100);
    expect(cache.get("k")).toBe("v");

    // Advance one tick past the TTL — the entry is stale and reads as a miss.
    clock.advance(1);
    expect(cache.get("k")).toBeUndefined();
  });

  it("resets the TTL clock when the same key is set again", () => {
    const clock = fakeClock();
    const cache = createCache<string>({ ttlMillis: 100, size: 4, now: clock.now });

    cache.set("k", "v1");
    clock.advance(80);
    cache.set("k", "v2"); // re-set resets the age
    clock.advance(80);    // total 160ms since first set, but 80ms since re-set
    expect(cache.get("k")).toBe("v2");
  });

  it("does not affect other, still-fresh entries", () => {
    const clock = fakeClock();
    const cache = createCache<string>({ ttlMillis: 100, size: 4, now: clock.now });

    cache.set("stale", "x");
    clock.advance(50);
    cache.set("fresh", "y");
    clock.advance(60); // stale is 110ms old (expired); fresh is 60ms old (alive)

    expect(cache.get("stale")).toBeUndefined();
    expect(cache.get("fresh")).toBe("y");
  });
});

describe("createCache — hard size cap (LRU-oldest eviction on insert at capacity)", () => {
  it("evicts the least-recently-used entry when inserting beyond size", () => {
    const cache = createCache<string>({ ttlMillis: 60_000, size: 2 });

    cache.set("a", "1");
    cache.set("b", "2");
    cache.set("c", "3"); // capacity=2 → oldest (a) is evicted

    expect(cache.get("a")).toBeUndefined();
    expect(cache.get("b")).toBe("2");
    expect(cache.get("c")).toBe("3");
  });

  it("refreshes recency on get so a recently-read entry survives eviction", () => {
    const cache = createCache<string>({ ttlMillis: 60_000, size: 2 });

    cache.set("a", "1");
    cache.set("b", "2");
    // Touch 'a' — it is now the most-recently-used, so the next insert
    // evicts 'b' instead of 'a'.
    cache.get("a");
    cache.set("c", "3");

    expect(cache.get("a")).toBe("1");
    expect(cache.get("b")).toBeUndefined();
    expect(cache.get("c")).toBe("3");
  });
});

describe("createCache — defaults coerced from missing/invalid options", () => {
  it("falls back to DEFAULT_CACHE_TTL_MILLIS / DEFAULT_CACHE_SIZE when opts is omitted", () => {
    const clock = fakeClock();
    // Use the clock-injected form to prove defaults engage — building without
    // any options must not throw or emit an unbounded-cache warning either.
    const cache = createCache<string>({ now: clock.now });

    cache.set("k", "v");
    // At 24h - 1ms, still fresh.
    clock.advance(DEFAULT_CACHE_TTL_MILLIS - 1);
    expect(cache.get("k")).toBe("v");

    // 1ms past the default TTL, the entry expires.
    clock.advance(2);
    expect(cache.get("k")).toBeUndefined();
  });

  it("also accepts createCache() with no argument at all", () => {
    const cache = createCache<number>();
    cache.set("a", 1);
    expect(cache.get("a")).toBe(1);
  });

  it("coerces each of the invalid ttlMillis / size shapes back to defaults", () => {
    const clock = fakeClock();
    const invalid: CacheOptions[] = [
      { ttlMillis: 0, size: 0 },
      { ttlMillis: -5, size: -3 },
      { ttlMillis: Number.NaN, size: Number.NaN },
      { ttlMillis: Number.POSITIVE_INFINITY, size: Number.POSITIVE_INFINITY },
      { ttlMillis: 1.5, size: 2.5 }, // non-integer
    ];

    for (const shape of invalid) {
      const cache = createCache<string>({ ...shape, now: clock.now });
      // Fresh clock per iteration so an earlier expiry does not bleed in.
      cache.set("k", "v");
      expect(cache.get("k")).toBe("v");
    }

    // And the default size actually caps growth at DEFAULT_CACHE_SIZE.
    const capped = createCache<number>({ size: 0 });
    for (let i = 0; i <= DEFAULT_CACHE_SIZE; i++) {
      capped.set(`k${i}`, i);
    }
    // The very first insert (`k0`) is the LRU-oldest and must have been evicted.
    expect(capped.get("k0")).toBeUndefined();
    expect(capped.get(`k${DEFAULT_CACHE_SIZE}`)).toBe(DEFAULT_CACHE_SIZE);
  });
});

describe("createCache — instance isolation for disjoint key namespaces", () => {
  it("independent instances do not share a backing store", () => {
    // Simulates the spec's separate-instance rule: definition cache and the
    // deferred by-id cache are structurally independent, so overlapping key
    // strings across instances never collide.
    const byDefinition = createCache<string>({ ttlMillis: 60_000, size: 4 });
    const byId = createCache<string>({ ttlMillis: 60_000, size: 4 });

    byDefinition.set("shared-key", "definition-value");
    byId.set("shared-key", "id-value");

    expect(byDefinition.get("shared-key")).toBe("definition-value");
    expect(byId.get("shared-key")).toBe("id-value");

    byDefinition.clear();
    expect(byDefinition.get("shared-key")).toBeUndefined();
    // The other instance is untouched.
    expect(byId.get("shared-key")).toBe("id-value");
  });
});

describe("createCache — generic value type", () => {
  it("carries the caller's V through the get/set surface", () => {
    interface SchemaVersion {
      schemaVersionId: string;
    }
    const cache = createCache<SchemaVersion>({ ttlMillis: 60_000, size: 4 });
    cache.set("avro-schema:AVRO", { schemaVersionId: "uuid-1" });

    const hit = cache.get("avro-schema:AVRO");
    expect(hit?.schemaVersionId).toBe("uuid-1");
  });
});
