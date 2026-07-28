/**
 * Tier-1 unit coverage for `kafka-broker.ts`.
 *
 * The three tri-state branches (empty-string skip, external reuse, container
 * launch) are driven through the injectable `RuntimeDeps` seam so no Docker
 * socket is ever opened and no `testcontainers` module graph is loaded.
 *
 * `pickFreeTcpPort` is exercised for real — binding to `127.0.0.1:0` is a
 * pure Node syscall with no external prereq and gives us a genuine free-port
 * confirmation.
 */

import { describe, expect, it } from "vitest";

import {
  EMPTY_STRING_SKIP_REASON,
  isSkip,
  pickFreeTcpPort,
  resolveBroker,
  type BrokerHandle,
  type RuntimeDeps,
} from "./kafka-broker.js";

describe("resolveBroker — empty-string no-Docker signal", () => {
  it("returns a skip result with the canonical reason string", async () => {
    const deps: RuntimeDeps = {
      env: { KAFKA_BROKER: "" },
      // Neither seam should be invoked when the empty-string signal is set.
      probeContainerRuntime: async () => {
        throw new Error("probe should not have been called");
      },
      startContainerKafka: async () => {
        throw new Error("start should not have been called");
      },
    };
    const resolved = await resolveBroker(deps);
    expect(isSkip(resolved)).toBe(true);
    if (isSkip(resolved)) {
      expect(resolved.skip).toBe(EMPTY_STRING_SKIP_REASON);
    }
  });
});

describe("resolveBroker — external reuse", () => {
  it("returns the provided bootstrap verbatim without probing Docker", async () => {
    let probed = false;
    let started = false;
    const deps: RuntimeDeps = {
      env: { KAFKA_BROKER: "kafka.internal:9092" },
      probeContainerRuntime: async () => {
        probed = true;
      },
      startContainerKafka: async () => {
        started = true;
        return { bootstrap: "unused", stop: async () => undefined };
      },
    };
    const resolved = await resolveBroker(deps);
    expect(isSkip(resolved)).toBe(false);
    if (!isSkip(resolved)) {
      expect(resolved.bootstrap).toBe("kafka.internal:9092");
      // stop() must exist and be a no-op — every caller unconditionally
      // awaits `stop()` on teardown; a missing method would blow up
      // suites that reused an external broker.
      await expect(resolved.stop()).resolves.toBeUndefined();
    }
    expect(probed).toBe(false);
    expect(started).toBe(false);
  });

  it("preserves a bootstrap with commas (multi-broker string) verbatim", async () => {
    const deps: RuntimeDeps = {
      env: { KAFKA_BROKER: "a:9092,b:9092,c:9092" },
    };
    const resolved = await resolveBroker(deps);
    expect(isSkip(resolved)).toBe(false);
    if (!isSkip(resolved)) {
      expect(resolved.bootstrap).toBe("a:9092,b:9092,c:9092");
    }
  });
});

describe("resolveBroker — container path (KAFKA_BROKER unset)", () => {
  it("invokes probe then start, and returns the container's bootstrap", async () => {
    let probeCalls = 0;
    let startCalls = 0;
    const containerStop = async (): Promise<void> => undefined;
    const deps: RuntimeDeps = {
      env: {},
      probeContainerRuntime: async () => {
        probeCalls++;
      },
      startContainerKafka: async () => {
        startCalls++;
        return { bootstrap: "localhost:34567", stop: containerStop };
      },
    };
    const resolved = await resolveBroker(deps);
    expect(isSkip(resolved)).toBe(false);
    if (!isSkip(resolved)) {
      expect(resolved.bootstrap).toBe("localhost:34567");
    }
    expect(probeCalls).toBe(1);
    expect(startCalls).toBe(1);
  });

  it("loud-skips when the container runtime probe throws", async () => {
    const deps: RuntimeDeps = {
      env: {},
      probeContainerRuntime: async () => {
        throw new Error("Cannot connect to Docker daemon");
      },
      startContainerKafka: async () => {
        throw new Error("start should not have been called after probe fail");
      },
    };
    const resolved = await resolveBroker(deps);
    expect(isSkip(resolved)).toBe(true);
    if (isSkip(resolved)) {
      expect(resolved.skip).toContain("Docker unavailable");
      expect(resolved.skip).toContain("Cannot connect to Docker daemon");
    }
  });

  it("stop() from the container path forwards to the returned handle", async () => {
    let containerStopped = false;
    const deps: RuntimeDeps = {
      env: {},
      probeContainerRuntime: async () => undefined,
      startContainerKafka: async () => ({
        bootstrap: "localhost:12345",
        stop: async () => {
          containerStopped = true;
        },
      }),
    };
    const resolved = await resolveBroker(deps);
    if (!isSkip(resolved)) {
      await resolved.stop();
    }
    expect(containerStopped).toBe(true);
  });

  it("skips with a message body that carries a stringified non-Error probe rejection", async () => {
    // Some downstream libraries reject with a plain string (not an Error);
    // the skip reason must still include the payload so an operator has a
    // clue what went wrong.
    const deps: RuntimeDeps = {
      env: {},
      probeContainerRuntime: async () => {
        // eslint-disable-next-line @typescript-eslint/no-throw-literal
        throw "docker-desktop is off";
      },
    };
    const resolved = await resolveBroker(deps);
    expect(isSkip(resolved)).toBe(true);
    if (isSkip(resolved)) {
      expect(resolved.skip).toContain("docker-desktop is off");
    }
  });
});

describe("pickFreeTcpPort", () => {
  it("returns a positive integer within the ephemeral port range", async () => {
    const port = await pickFreeTcpPort();
    expect(Number.isInteger(port)).toBe(true);
    expect(port).toBeGreaterThan(0);
    expect(port).toBeLessThanOrEqual(65_535);
  });

  it("yields two distinct ports across two calls (best-effort)", async () => {
    // Not a guarantee — the OS could rebind the same port after close — but
    // a repeated match on the very next call would be an outlier. Assert
    // that at least one of five calls differs from the first.
    const first = await pickFreeTcpPort();
    const seen: number[] = [first];
    for (let i = 0; i < 4; i++) {
      seen.push(await pickFreeTcpPort());
    }
    const distinct = new Set(seen);
    expect(distinct.size).toBeGreaterThan(1);
  });
});

describe("BrokerHandle contract", () => {
  it("stop() from the external-reuse branch is idempotent", async () => {
    const deps: RuntimeDeps = { env: { KAFKA_BROKER: "x:9092" } };
    const resolved = await resolveBroker(deps);
    expect(isSkip(resolved)).toBe(false);
    if (!isSkip(resolved)) {
      const handle: BrokerHandle = resolved;
      await handle.stop();
      // A second stop() must resolve, not throw — matches the "always
      // safe to unconditionally await teardown" contract.
      await expect(handle.stop()).resolves.toBeUndefined();
    }
  });
});
