/**
 * Tier-2 Kafka round-trip scenario.
 *
 * Exercises the full producer→broker→consumer path with a GSR-encoded Avro
 * payload: the producer builds a wire message via `GsrSerializer`, publishes
 * it through `kafkajs`, and the consumer reads it back and decodes it via
 * `GsrDeserializer`. The decoded record must equal the input — proving the
 * TS wire codec survives an actual Kafka broker without any transport
 * pre/post-processing (the byte-format contract is Kafka-agnostic, but a
 * transport round-trip is the belt-and-braces guarantee).
 *
 * Broker discovery follows the tri-state `KAFKA_BROKER` protocol codified in
 * the integration env matrix:
 *   - `KAFKA_BROKER` unset → try to start a testcontainers Kafka broker;
 *     if Docker is unavailable, loud-skip the suite (do NOT fail).
 *   - `KAFKA_BROKER=` (empty string) → the explicit no-Docker signal;
 *     loud-skip the suite.
 *   - `KAFKA_BROKER=<non-empty>` → reuse the external broker at that
 *     bootstrap.
 *
 * The suite is gated by `describeIntegration`, so an integration run with
 * `AWS_INTEGRATION!=1` prints a loud one-line skip reason and never
 * constructs the producer/consumer; the default `npm test` never selects
 * this file at all.
 */
import { afterAll, beforeAll, expect, it } from "vitest";

import { GsrDeserializer, GsrSerializer, DataFormat } from "@gsr/serde";

import { describeIntegration } from "../../src/env-gate.js";

/**
 * Emit the same loud-skip line shape `describeIntegration` uses for a
 * secondary gate that fires AFTER the AWS_INTEGRATION suite gate — e.g.
 * Docker is absent or `KAFKA_BROKER=""`. Registered directly with vitest's
 * console-warn sink so an operator scanning the reporter output sees a
 * single unified `SKIP <name> — <reason>` line per skipped suite.
 */
function warnSkip(name: string, reason: string): void {
  // eslint-disable-next-line no-console
  console.warn(`SKIP ${name} — ${reason}`);
}

const SUITE_NAME = "tier2/kafka-roundtrip";

/**
 * Broker-discovery result. `bootstrap` is either a caller-provided broker
 * ("host:port") or the mapped-port bootstrap of a testcontainers-started
 * container; `stop` is a cleanup hook that shuts the container down (a
 * no-op when we reused an external broker).
 */
interface BrokerHandle {
  bootstrap: string;
  stop(): Promise<void>;
}

/**
 * Discriminate the three `KAFKA_BROKER` states and decide whether to reuse
 * an external broker, start a container, or loud-skip.
 */
async function resolveBroker(): Promise<BrokerHandle | { skip: string }> {
  const raw = process.env["KAFKA_BROKER"];
  if (raw !== undefined && raw === "") {
    return { skip: "KAFKA_BROKER=<empty> (explicit no-Docker signal)" };
  }
  if (raw !== undefined && raw !== "") {
    return {
      bootstrap: raw,
      stop: async () => undefined,
    };
  }
  // Unset — try testcontainers. Probe Docker availability first so a
  // missing daemon skips loudly (not fails) per the tri-state protocol.
  try {
    // Dynamic imports so a Docker-free default test run never pulls in the
    // testcontainers module graph at collection time.
    const { getContainerRuntimeClient } = await import("testcontainers");
    await getContainerRuntimeClient();
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    return { skip: `Docker unavailable: ${reason}` };
  }
  return startTestcontainersKafka();
}

/**
 * Boot a single-node KRaft-mode Kafka broker via testcontainers and return
 * the host-mapped bootstrap. Uses `apache/kafka:3.7.0` (KRaft mode built in;
 * no ZooKeeper needed) with two host-visible listeners: an EXTERNAL listener
 * pinned to a random free host port, whose advertised address is `host:port`
 * so kafkajs from the test host connects successfully, and an INTERNAL
 * listener for inter-broker traffic. Pinning the host port BEFORE the
 * container boots lets us set `advertised.listeners` correctly at start
 * time — the alternative (start, read mapped port, rewrite advertised via
 * `kafka-configs.sh --alter`) fails because a client connecting for the
 * FIRST bootstrap sees the pre-alter advertised address.
 */
async function startTestcontainersKafka(): Promise<BrokerHandle> {
  const { GenericContainer, Wait } = await import("testcontainers");

  // Pin an ephemeral host port up front so the container's
  // `advertised.listeners` can name it verbatim at boot.
  const externalHostPort = await pickFreeTcpPort();
  const EXTERNAL_CONTAINER_PORT = 19092;
  const INTERNAL_PORT = 9092;
  const CONTROLLER_PORT = 9093;

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
 * Return a free ephemeral TCP port on the host by binding a server to
 * port 0 and reading the OS-assigned port. Close it immediately so the
 * caller can bind. There is a tiny TOCTOU race between close and rebind,
 * but Linux's default `SO_REUSEADDR` handling makes practical collisions
 * effectively impossible for a test that binds within milliseconds.
 */
async function pickFreeTcpPort(): Promise<number> {
  const net = await import("node:net");
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (addr === null || typeof addr === "string") {
        server.close();
        reject(new Error(`unexpected server.address(): ${String(addr)}`));
        return;
      }
      const port = addr.port;
      server.close((err) => {
        if (err) reject(err);
        else resolve(port);
      });
    });
  });
}

const SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

const AVRO_ORDER_SCHEMA = {
  type: "record",
  name: "Order",
  namespace: "example.orders",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "double" },
  ],
} as const;

const AVRO_ORDER_RECORD = { id: "ord-42", amount: 19.99 };

describeIntegration(SUITE_NAME, () => {
  // Broker discovery happens once per suite. When the discovery loud-skips
  // (Docker absent or explicit no-Docker signal), every test in the suite
  // short-circuits — the shape mirrors `describe.skip` semantics but keeps
  // the vitest reporter output uniform for both gates.
  let broker: BrokerHandle | null = null;
  let skipReason: string | null = null;

  beforeAll(async () => {
    const resolved = await resolveBroker();
    if ("skip" in resolved) {
      skipReason = resolved.skip;
      warnSkip(SUITE_NAME, resolved.skip);
      return;
    }
    broker = resolved;
  }, 180_000);

  afterAll(async () => {
    if (broker) {
      await broker.stop();
      broker = null;
    }
  });

  it("round-trips a GSR-encoded Avro record via Kafka producer/consumer", async () => {
    if (skipReason !== null) {
      // Short-circuit: broker discovery already emitted its loud skip in
      // beforeAll; the test itself is a no-op so `npm run test:integ`
      // stays green when Docker/`KAFKA_BROKER` are absent.
      return;
    }
    if (broker === null) {
      throw new Error(
        "broker discovery neither returned a bootstrap nor a skip reason",
      );
    }

    const { Kafka, logLevel } = await import("kafkajs");
    const clientId = "gsr-tier2-roundtrip";
    const topic = `gsr-tier2-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
    const kafka = new Kafka({
      clientId,
      brokers: [broker.bootstrap],
      logLevel: logLevel.NOTHING,
    });

    const admin = kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
        waitForLeaders: true,
      });
    } finally {
      await admin.disconnect();
    }

    // ---- Produce: serialize via GsrSerializer, publish as the record value.
    const serializer = new GsrSerializer();
    const wire = serializer.serialize({
      format: DataFormat.AVRO,
      schemaVersionId: SCHEMA_VERSION_ID,
      topic,
      schema: AVRO_ORDER_SCHEMA,
      data: AVRO_ORDER_RECORD,
    });

    const producer = kafka.producer();
    await producer.connect();
    try {
      await producer.send({
        topic,
        messages: [{ value: wire }],
      });
    } finally {
      await producer.disconnect();
    }

    // ---- Consume: pull the record back, decode via GsrDeserializer.
    const groupId = `${clientId}-consumer-${Date.now()}`;
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    try {
      await consumer.subscribe({ topic, fromBeginning: true });

      const received = await new Promise<Buffer>((resolve, reject) => {
        const timeoutHandle = setTimeout(
          () => reject(new Error("timed out waiting for Kafka message")),
          30_000,
        );
        void consumer
          .run({
            eachMessage: async ({ message }) => {
              if (message.value) {
                clearTimeout(timeoutHandle);
                resolve(Buffer.from(message.value));
              }
            },
          })
          .catch((err) => {
            clearTimeout(timeoutHandle);
            reject(err instanceof Error ? err : new Error(String(err)));
          });
      });

      const deserializer = new GsrDeserializer();
      const decoded = deserializer.deserialize({
        format: DataFormat.AVRO,
        data: received,
        schema: AVRO_ORDER_SCHEMA,
      });
      expect(decoded).toEqual(AVRO_ORDER_RECORD);
    } finally {
      await consumer.disconnect();
    }
  }, 120_000);
});
