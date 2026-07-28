/**
 * Shared Kafka broker-discovery module for the interop test tier.
 *
 * The interop suites need exactly one Kafka broker per run. Three shapes
 * are supported, discriminated by the `KAFKA_BROKER` env var:
 *
 *   - `KAFKA_BROKER=<host:port>` — reuse an externally-managed broker.
 *     Fastest path; used by CI hosts that already have Kafka running.
 *
 *   - `KAFKA_BROKER=""` (empty string) — the explicit no-Docker signal.
 *     `resolveBroker` returns `{ skip: <reason> }` without probing Docker,
 *     so operators can suppress the container path deterministically.
 *
 *   - `KAFKA_BROKER` unset — attempt to start a testcontainers KRaft Kafka
 *     broker. When Docker is unavailable the resolver returns
 *     `{ skip: <reason> }` (never fails); when the container starts, its
 *     host-mapped port is returned as the bootstrap.
 *
 * The discovery logic used to live in `test/tier2/kafka-roundtrip.integ.test.ts`
 * as private inline functions with no importable export. The interop tier
 * (three separate suites) needs the same tri-state behavior in exactly one
 * place; this module is that place. The Tier-2 file is left intact — the
 * discovery contract is re-hosted here rather than extracted out of that
 * file.
 *
 * ## Injectable runtime seam
 *
 * The `resolveBroker` function is intentionally accepts a `RuntimeDeps` bag
 * so unit tests can drive the tri-state branches without a real Docker
 * daemon: the container path is stubbed, the external-reuse path is pure,
 * and the empty-string no-Docker path is a synchronous return. Production
 * callers omit `RuntimeDeps` and get the default (real testcontainers) wire.
 */

import { createServer } from "node:net";

/**
 * A resolved broker. `bootstrap` is either a caller-provided `host:port` or
 * the host-mapped port of a testcontainers-started KRaft Kafka. `stop()`
 * is a cleanup hook: a no-op when we reused an external broker, and a
 * container-stop when we started one.
 */
export interface BrokerHandle {
  readonly bootstrap: string;
  stop(): Promise<void>;
}

/**
 * Discriminated-union return from `resolveBroker`. Loud-skip results are
 * carried as `{ skip: <reason> }` so callers can emit a machine-parseable
 * `SKIP <suite> — <reason>` line and gracefully short-circuit the suite.
 */
export type BrokerResolution = BrokerHandle | { readonly skip: string };

/**
 * Reason string for the explicit empty-string signal. Extracted so both the
 * production code and tests reference the exact same wording, and so a
 * later log-scanner can pattern-match on it.
 */
export const EMPTY_STRING_SKIP_REASON =
  "KAFKA_BROKER=<empty> (explicit no-Docker signal)";

/**
 * Injectable dependency bag. Every field is optional; when omitted the
 * default production wire is used.
 *
 * `env` is a snapshot of the process environment — kept as a parameter so
 * tests never have to mutate `process.env`. `probeContainerRuntime` and
 * `startContainerKafka` are the two live seams: the first checks whether
 * Docker is reachable, the second actually launches a KRaft container.
 * Both are async because the underlying `testcontainers` calls are async.
 */
export interface RuntimeDeps {
  readonly env?: NodeJS.ProcessEnv;
  /**
   * Probe: returns nothing on success; throws with a human-readable reason
   * when the container runtime is unreachable.
   */
  readonly probeContainerRuntime?: () => Promise<void>;
  /**
   * Boot a single-node KRaft Kafka broker and return a `BrokerHandle`
   * pointing at its host-mapped bootstrap.
   */
  readonly startContainerKafka?: () => Promise<BrokerHandle>;
}

/**
 * Resolve a Kafka broker per the tri-state protocol above.
 *
 * The result is a discriminated union: either a `BrokerHandle` (which the
 * caller MUST `stop()` on teardown, even if the broker was external — the
 * external-broker `stop` is a no-op but keeping the same shape means suite
 * teardown code doesn't have to branch), or `{ skip: <reason> }` (the
 * suite must loud-skip; it MUST NOT proceed to construct a producer or
 * consumer against a nonexistent broker).
 */
export async function resolveBroker(
  deps: RuntimeDeps = {},
): Promise<BrokerResolution> {
  const env = deps.env ?? process.env;
  const raw = env["KAFKA_BROKER"];
  if (raw !== undefined && raw === "") {
    return { skip: EMPTY_STRING_SKIP_REASON };
  }
  if (raw !== undefined && raw !== "") {
    return {
      bootstrap: raw,
      stop: async () => undefined,
    };
  }
  // Unset — try testcontainers, but probe the runtime first so a missing
  // Docker daemon skips loudly (never fails).
  const probe = deps.probeContainerRuntime ?? defaultProbeContainerRuntime;
  try {
    await probe();
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    return { skip: `Docker unavailable: ${reason}` };
  }
  const start = deps.startContainerKafka ?? defaultStartContainerKafka;
  return await start();
}

/**
 * Default probe: call `getContainerRuntimeClient` from testcontainers.
 * Dynamic import so a Docker-free default test run never pulls in the
 * testcontainers module graph at collection time.
 */
async function defaultProbeContainerRuntime(): Promise<void> {
  const { getContainerRuntimeClient } = await import("testcontainers");
  await getContainerRuntimeClient();
}

/**
 * Default container-Kafka launcher. Boots a single-node KRaft-mode Kafka
 * broker via testcontainers using `apache/kafka:3.7.0` (KRaft built in;
 * no ZooKeeper needed) with two host-visible listeners: an EXTERNAL listener
 * pinned to a random free host port (whose advertised address names that
 * exact port so kafkajs from the test host connects on the FIRST bootstrap
 * attempt), and an INTERNAL listener for inter-broker traffic.
 *
 * Pinning the host port BEFORE the container boots is load-bearing — the
 * alternative (start, read mapped port, rewrite advertised via
 * `kafka-configs.sh --alter`) fails because a client connecting for the
 * FIRST bootstrap sees the pre-alter advertised address.
 *
 * The implementation is a re-host of the Tier-2 kafka-roundtrip file's inline
 * logic; that file is left intact (its inline discovery has no importable
 * export; we do not edit it).
 */
async function defaultStartContainerKafka(): Promise<BrokerHandle> {
  const { GenericContainer, Wait } = await import("testcontainers");

  const externalHostPort = await pickFreeTcpPort();
  const EXTERNAL_CONTAINER_PORT = 19_092;
  const INTERNAL_PORT = 9_092;
  const CONTROLLER_PORT = 9_093;

  const container = await new GenericContainer("apache/kafka:3.7.0")
    .withExposedPorts({
      container: EXTERNAL_CONTAINER_PORT,
      host: externalHostPort,
    })
    .withEnvironment({
      KAFKA_NODE_ID: "1",
      KAFKA_PROCESS_ROLES: "broker,controller",
      KAFKA_LISTENERS: `INTERNAL://:${INTERNAL_PORT},EXTERNAL://:${EXTERNAL_CONTAINER_PORT},CONTROLLER://:${CONTROLLER_PORT}`,
      KAFKA_ADVERTISED_LISTENERS: `INTERNAL://localhost:${INTERNAL_PORT},EXTERNAL://localhost:${externalHostPort}`,
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL",
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
      KAFKA_CONTROLLER_QUORUM_VOTERS: `1@localhost:${CONTROLLER_PORT}`,
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
        "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1",
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0",
      KAFKA_LOG_DIRS: "/tmp/kraft-combined-logs",
      CLUSTER_ID: "5L6g3nShT-eMCtK--X86sw",
    })
    .withStartupTimeout(120_000)
    .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/i))
    .start();

  return {
    bootstrap: `localhost:${externalHostPort}`,
    stop: async () => {
      await container.stop();
    },
  };
}

/**
 * Return a free ephemeral TCP port on the host by binding a `net.Server` to
 * port 0 and reading the OS-assigned port. Close it immediately so the
 * caller can bind. There is a tiny TOCTOU race between close and rebind,
 * but Linux's default `SO_REUSEADDR` handling makes practical collisions
 * effectively impossible for a test that binds within milliseconds. Kept
 * exported so tests that want a free port for their own uses can reach it
 * without duplicating the helper.
 */
export async function pickFreeTcpPort(): Promise<number> {
  return await new Promise<number>((resolveP, rejectP) => {
    const server = createServer();
    server.on("error", rejectP);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (addr === null || typeof addr === "string") {
        server.close();
        rejectP(new Error(`unexpected server.address(): ${String(addr)}`));
        return;
      }
      const port = addr.port;
      server.close((err) => {
        if (err !== undefined && err !== null) {
          rejectP(err);
        } else {
          resolveP(port);
        }
      });
    });
  });
}

/**
 * Type guard: narrows a `BrokerResolution` to the skip branch. Kept alongside
 * the union so callers reference one shared narrower and don't hand-roll
 * `'skip' in resolved` in each suite.
 */
export function isSkip(
  resolution: BrokerResolution,
): resolution is { readonly skip: string } {
  return "skip" in resolution;
}
