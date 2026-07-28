/**
 * Tier-1 unit coverage for `java-sidecar.ts`.
 *
 * Every case here drives the launcher through the injectable command/HTTP
 * seams — no JVM is spawned, no TCP socket is opened, and the tests remain
 * OS-portable. The seams themselves are exercised end-to-end so the
 * launcher's PORT-scrape / health-poll / HTTP-client wiring is covered
 * without ever leaving the vitest process.
 */

import { Buffer } from "node:buffer";
import type { SpawnOptions } from "node:child_process";
import { EventEmitter } from "node:events";
import { Readable } from "node:stream";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  DEFAULT_JAR_RELATIVE_PATH,
  isJvmAvailable,
  resolveJarPath,
  startSidecar,
  type Commander,
  type HttpJsonPoster,
  type SidecarChild,
} from "./java-sidecar.js";

/**
 * Build a fake `SidecarChild` whose stdout emits `stdout` synchronously as
 * soon as a `data` listener attaches. The child's `kill()` records the
 * signal on the returned instance so the test can assert the launcher's
 * `stop()` sent `SIGTERM`.
 *
 * The fake mirrors the shape of the Go reference test's `fakeCommandRun`
 * (stdout is a pipe, stderr is a pipe, `Start()` writes the canned bytes)
 * but leans on Node's `Readable.from` so we don't have to hand-roll a pull
 * stream.
 */
interface FakeChild extends SidecarChild {
  readonly killSignals: string[];
}

function makeFakeChild(stdoutBytes: string): FakeChild {
  const stdoutStream = Readable.from([Buffer.from(stdoutBytes, "utf8")]);
  const stderrStream = Readable.from([]);
  const killSignals: string[] = [];
  const exitEmitter = new EventEmitter();
  return {
    stdout: stdoutStream,
    stderr: stderrStream,
    pid: 12345,
    kill(signal) {
      killSignals.push(String(signal ?? "SIGTERM"));
      return true;
    },
    on(event, listener) {
      exitEmitter.on(event, listener);
      return this as unknown as FakeChild;
    },
    killSignals,
  };
}

/**
 * Build a fake `Commander` that captures the spawn arguments and yields a
 * pre-built `FakeChild`. Kept as a factory so each test gets its own
 * capture slot.
 */
function makeCapturingCommander(child: FakeChild): {
  commander: Commander;
  calls: Array<{ command: string; args: readonly string[]; options: SpawnOptions }>;
} {
  const calls: Array<{
    command: string;
    args: readonly string[];
    options: SpawnOptions;
  }> = [];
  return {
    commander: {
      spawn(command, args, options) {
        calls.push({ command, args: [...args], options });
        return child;
      },
    },
    calls,
  };
}

/**
 * Minimal in-memory `HttpJsonPoster`. Answers `/health` with 200 by
 * default (the launcher's health-poll waits for this) and stubs
 * `/encode`/`/decode`/`/kafka-produce`/`/kafka-consume` with the shape the
 * launcher expects — a 20-byte "framed" buffer for encode, a base64
 * payload for decode, and a per-endpoint envelope for the Kafka calls.
 *
 * The record of URLs and bodies is exposed so tests can assert the
 * launcher shipped the right JSON.
 */
interface FakePoster extends HttpJsonPoster {
  readonly requests: Array<{ url: string; body: unknown }>;
  readonly gets: string[];
  healthStatus: number;
}

function makeFakePoster(): FakePoster {
  const requests: Array<{ url: string; body: unknown }> = [];
  const gets: string[] = [];
  const poster: FakePoster = {
    requests,
    gets,
    healthStatus: 200,
    async postJson(url, body) {
      requests.push({ url, body });
      if (url.endsWith("/encode")) {
        const b = body as { payload: string };
        // Prepend an 18-byte stub header so the caller can pretend it got a
        // framed response (matches the Go reference test's mini server).
        const raw = Buffer.from(b.payload, "base64");
        const framed = Buffer.concat([Buffer.alloc(18), raw]);
        return { bytes: framed.toString("base64") };
      }
      if (url.endsWith("/decode")) {
        const b = body as { bytes: string };
        const raw = Buffer.from(b.bytes, "base64");
        const payload = raw.length >= 18 ? raw.subarray(18) : Buffer.alloc(0);
        return {
          payload: payload.toString("base64"),
          schemaVersionId: "00000000-0000-0000-0000-000000000000",
          schemaName: "fake-schema",
          schemaDefinition: "{}",
          dataFormat: "AVRO",
        };
      }
      if (url.endsWith("/kafka-produce")) {
        return {
          schemaVersionId: "00000000-0000-0000-0000-000000000000",
          bytes: Buffer.from([1, 2, 3]).toString("base64"),
          offset: 0,
          partition: 0,
        };
      }
      if (url.endsWith("/kafka-consume")) {
        return {
          schemaVersionId: "00000000-0000-0000-0000-000000000000",
          dataFormat: "AVRO",
          schemaDefinition: "{}",
          schemaArn: "arn:aws:glue:us-east-2:0:schema/default-registry/fake",
          record: { fields: { id: "ord-42" } },
        };
      }
      throw new Error(`fake poster: unhandled URL ${url}`);
    },
    async getStatus(url) {
      gets.push(url);
      return { status: this.healthStatus };
    },
  };
  return poster;
}

/**
 * Build a jar path that `existsSync` will accept. The launcher's
 * `startSidecar` calls `existsSync(jarPath)` after `resolveJarPath` returns;
 * the tests point at a real temp file (created in `beforeEach`) so that
 * check passes without a real Maven build.
 */
let stubJarPath: string;
beforeEach(async () => {
  const os = await import("node:os");
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "java-sidecar-test-"));
  stubJarPath = path.join(dir, "fake-sidecar.jar");
  await fs.writeFile(stubJarPath, "not a real jar", "utf8");
});

afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe("resolveJarPath", () => {
  it("returns the explicit jarPath verbatim when provided", () => {
    // A caller-provided path is returned as-is — the walk isn't invoked.
    const explicit = "/some/where/sidecar.jar";
    expect(resolveJarPath({ jarPath: explicit })).toBe(explicit);
  });

  it("walks upward until an existing candidate is found", () => {
    const start = "/a/b/c/d";
    const hit = `/a/b/${DEFAULT_JAR_RELATIVE_PATH}`;
    const seen: string[] = [];
    const exists = (p: string): boolean => {
      seen.push(p);
      return p === hit;
    };
    expect(resolveJarPath({ startDir: start, exists })).toBe(hit);
    // Confirms the walk climbed at least one level before landing.
    expect(seen.length).toBeGreaterThanOrEqual(2);
  });

  it("throws with a diagnostic when the walk exhausts", () => {
    // Every probe returns false → the walker gives up after maxDepth iters.
    const start = "/deep/nested/path";
    expect(() =>
      resolveJarPath({
        startDir: start,
        exists: () => false,
        maxDepth: 3,
      }),
    ).toThrowError(/could not locate.*java-interop-sidecar\.jar/);
  });

  it("respects maxDepth (does not walk beyond the bound)", () => {
    const seen: string[] = [];
    try {
      resolveJarPath({
        startDir: "/very/deeply/nested/starting/dir",
        exists: (p) => {
          seen.push(p);
          return false;
        },
        maxDepth: 4,
      });
    } catch {
      // expected — the walk exhausts
    }
    expect(seen.length).toBeLessThanOrEqual(4);
  });
});

describe("isJvmAvailable", () => {
  it("returns true for the bare `java` sentinel by default", () => {
    // The default probe treats bare `java` as PATH-resolvable without
    // shelling out, so the availability check remains cheap.
    expect(
      isJvmAvailable({ env: {}, probe: (c) => c === "java" }),
    ).toBe(true);
  });

  it("uses the explicit javaBinary option when set", () => {
    let probed: string | undefined;
    const ok = isJvmAvailable({
      javaBinary: "/opt/jdk17/bin/java",
      env: { GSR_INTEROP_JAVA: "/should/be/ignored" },
      probe: (c) => {
        probed = c;
        return true;
      },
    });
    expect(ok).toBe(true);
    expect(probed).toBe("/opt/jdk17/bin/java");
  });

  it("falls back to GSR_INTEROP_JAVA env when no explicit binary is given", () => {
    let probed: string | undefined;
    isJvmAvailable({
      env: { GSR_INTEROP_JAVA: "/hermetic/jdk/bin/java" },
      probe: (c) => {
        probed = c;
        return true;
      },
    });
    expect(probed).toBe("/hermetic/jdk/bin/java");
  });

  it("returns false when the probe rejects the resolved binary", () => {
    expect(
      isJvmAvailable({
        javaBinary: "/no/such/binary",
        env: {},
        probe: () => false,
      }),
    ).toBe(false);
  });
});

describe("startSidecar", () => {
  it("spawns java with the expected args (jar, --port=0) and drives /health poll", async () => {
    const child = makeFakeChild(`starting up\nPORT: 40404\nready\n`);
    const { commander, calls } = makeCapturingCommander(child);
    const poster = makeFakePoster();

    const sc = await startSidecar({
      jarPath: stubJarPath,
      javaBinary: "fake-java",
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    try {
      expect(calls).toHaveLength(1);
      expect(calls[0]!.command).toBe("fake-java");
      expect(calls[0]!.args).toEqual(["-jar", stubJarPath, "--port=0"]);
      expect(sc.baseUrl()).toBe("http://127.0.0.1:40404");
      // The health poll must have hit `/health` at least once.
      expect(poster.gets.some((u) => u.endsWith("/health"))).toBe(true);
    } finally {
      await sc.stop();
    }
  });

  it("throws when the child stdout closes before PORT: line", async () => {
    const child = makeFakeChild(`no port line here\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();

    await expect(
      startSidecar({
        jarPath: stubJarPath,
        javaBinary: "fake-java",
        commander,
        httpPoster: poster,
        startTimeoutMs: 500,
      }),
    ).rejects.toThrow(/PORT: line/);
  });

  it("surfaces a health-poll timeout with a diagnostic message", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();
    poster.healthStatus = 500;

    await expect(
      startSidecar({
        jarPath: stubJarPath,
        javaBinary: "fake-java",
        commander,
        httpPoster: poster,
        startTimeoutMs: 300,
      }),
    ).rejects.toThrow(/never became healthy/);
  });

  it("throws when the jarPath does not exist on disk", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();

    await expect(
      startSidecar({
        jarPath: "/does/not/exist/java-interop-sidecar.jar",
        commander,
        httpPoster: poster,
      }),
    ).rejects.toThrow(/jar not found/);
  });

  it("stop() sends SIGTERM and is idempotent", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();

    const sc = await startSidecar({
      jarPath: stubJarPath,
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    await sc.stop();
    await sc.stop();
    expect(child.killSignals).toEqual(["SIGTERM"]);
  });

  it("rejects an invalid PORT: line", async () => {
    const child = makeFakeChild(`PORT: 999999\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();

    await expect(
      startSidecar({
        jarPath: stubJarPath,
        commander,
        httpPoster: poster,
        startTimeoutMs: 500,
      }),
    ).rejects.toThrow(/invalid port/);
  });
});

describe("Sidecar HTTP client methods", () => {
  it("encode() ships base64 payload and returns decoded framed bytes", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();

    const sc = await startSidecar({
      jarPath: stubJarPath,
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    try {
      const framed = await sc.encode({
        format: "AVRO",
        schema: `{"type":"string"}`,
        schemaName: "s",
        schemaVersionId: "00000000-0000-0000-0000-000000000000",
        payload: Buffer.from("hi", "utf8"),
      });
      // 18-byte stub header + "hi"
      expect(framed.length).toBe(20);
      const req = poster.requests.find((r) => r.url.endsWith("/encode"));
      expect(req).toBeDefined();
      const body = req!.body as { payload: string; compression: string };
      // Payload is base64-encoded — that IS the HTTP-contract shape.
      expect(Buffer.from(body.payload, "base64").toString("utf8")).toBe(
        "hi",
      );
      expect(body.compression).toBe("NONE");
    } finally {
      await sc.stop();
    }
  });

  it("encode() normalizes compression case and rejects unknown tokens", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();
    const sc = await startSidecar({
      jarPath: stubJarPath,
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    try {
      // Lower-case token is normalized to the sidecar vocabulary.
      await sc.encode({
        format: "AVRO",
        schema: "{}",
        schemaName: "s",
        schemaVersionId: "00000000-0000-0000-0000-000000000000",
        payload: Buffer.alloc(0),
        compression: "zlib",
      });
      const req = poster.requests.find((r) => r.url.endsWith("/encode"));
      const body = req!.body as { compression: string };
      expect(body.compression).toBe("ZLIB");

      // GZIP is not on the sidecar contract — reject locally.
      await expect(
        sc.encode({
          format: "AVRO",
          schema: "{}",
          schemaName: "s",
          schemaVersionId: "00000000-0000-0000-0000-000000000000",
          payload: Buffer.alloc(0),
          compression: "GZIP",
        }),
      ).rejects.toThrow(/unsupported compression/);
    } finally {
      await sc.stop();
    }
  });

  it("decode() round-trips base64 framed bytes to payload + metadata", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();
    const sc = await startSidecar({
      jarPath: stubJarPath,
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    try {
      // Round-trip a 20-byte "framed" buffer whose last two bytes are the
      // payload — the fake poster strips the first 18 stub-header bytes.
      const framed = Buffer.concat([Buffer.alloc(18), Buffer.from("hi", "utf8")]);
      const resp = await sc.decode(framed);
      expect(resp.payload.toString("utf8")).toBe("hi");
      expect(resp.schemaName).toBe("fake-schema");
      expect(resp.schemaVersionId).toBe(
        "00000000-0000-0000-0000-000000000000",
      );
      expect(resp.dataFormat).toBe("AVRO");
    } finally {
      await sc.stop();
    }
  });

  it("kafkaProduce() ships the envelope and includes optional region/compat only when non-empty", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();
    const sc = await startSidecar({
      jarPath: stubJarPath,
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    try {
      const resp = await sc.kafkaProduce({
        format: "AVRO",
        schema: `{"type":"record","name":"X","fields":[]}`,
        schemaName: "sn",
        record: { fields: { id: "ord-42" } },
        bootstrap: "localhost:9092",
        topic: "t1",
      });
      expect(resp.offset).toBe(0);
      expect(resp.partition).toBe(0);
      const req = poster.requests.find((r) =>
        r.url.endsWith("/kafka-produce"),
      );
      const body = req!.body as Record<string, unknown>;
      // Optional fields NOT set → NOT present on the wire (matches contract).
      expect(body["region"]).toBeUndefined();
      expect(body["compatibility"]).toBeUndefined();
      expect(body["compression"]).toBe("NONE");
    } finally {
      await sc.stop();
    }
  });

  it("kafkaProduce() includes region + compatibility when provided", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();
    const sc = await startSidecar({
      jarPath: stubJarPath,
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    try {
      await sc.kafkaProduce({
        format: "AVRO",
        schema: "{}",
        schemaName: "sn",
        record: { fields: {} },
        bootstrap: "localhost:9092",
        topic: "t1",
        region: "us-east-2",
        compatibility: "BACKWARD",
      });
      const req = poster.requests.find((r) =>
        r.url.endsWith("/kafka-produce"),
      );
      const body = req!.body as Record<string, unknown>;
      expect(body["region"]).toBe("us-east-2");
      expect(body["compatibility"]).toBe("BACKWARD");
    } finally {
      await sc.stop();
    }
  });

  it("kafkaConsume() throws when format is empty", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();
    const sc = await startSidecar({
      jarPath: stubJarPath,
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    try {
      await expect(
        sc.kafkaConsume({
          bootstrap: "localhost:9092",
          topic: "t1",
          format: "",
        }),
      ).rejects.toThrow(/format must be set/);
    } finally {
      await sc.stop();
    }
  });

  it("kafkaConsume() ships the envelope and returns the record", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();
    const sc = await startSidecar({
      jarPath: stubJarPath,
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    try {
      const resp = await sc.kafkaConsume({
        bootstrap: "localhost:9092",
        topic: "t1",
        format: "AVRO",
        groupId: "g1",
        timeoutMs: 15_000,
      });
      expect(resp.dataFormat).toBe("AVRO");
      expect(resp.record).toEqual({ fields: { id: "ord-42" } });
      const req = poster.requests.find((r) =>
        r.url.endsWith("/kafka-consume"),
      );
      const body = req!.body as Record<string, unknown>;
      expect(body["groupId"]).toBe("g1");
      expect(body["timeoutMs"]).toBe(15_000);
    } finally {
      await sc.stop();
    }
  });

  it("decode() throws when the sidecar returns a malformed payload envelope", async () => {
    const child = makeFakeChild(`PORT: 40404\n`);
    const { commander } = makeCapturingCommander(child);
    const poster = makeFakePoster();
    // Corrupt the decode response — the missing schemaVersionId field must
    // surface as an explicit client-side error rather than an undefined bug.
    const original = poster.postJson.bind(poster);
    poster.postJson = async (url, body, signal) => {
      if (url.endsWith("/decode")) {
        return { payload: "aGVsbG8=" };
      }
      return original(url, body, signal);
    };
    const sc = await startSidecar({
      jarPath: stubJarPath,
      commander,
      httpPoster: poster,
      startTimeoutMs: 2_000,
    });
    try {
      await expect(
        sc.decode(Buffer.from([1, 2, 3])),
      ).rejects.toThrow(/response missing/);
    } finally {
      await sc.stop();
    }
  });
});
