/**
 * TypeScript launcher and HTTP client for the Java interop sidecar.
 *
 * The sidecar is a JVM process wrapping the real Java `aws-glue-schema-registry`
 * client library and exposing a small HTTP surface (`/encode`, `/decode`,
 * `/kafka-produce`, `/kafka-consume`, `/health`). This module is the TS analog
 * of the Go `pkg/javasidecar` package: it spawns the JVM with
 * `java -jar <jar> --port=0`, scrapes the `PORT: <n>` header line off stdout,
 * polls `/health` until it returns 200, and returns a `Sidecar` handle whose
 * methods proxy through to the sidecar over HTTP.
 *
 * Local mode only (the Kafka-in-the-loop interop path requires the JVM to
 * reach a testcontainers Kafka broker on its host-mapped port, which is only
 * reachable when the JVM runs on the host — matching the Go reference's
 * `test-integ-interop-real` target). Container mode is intentionally not
 * implemented here.
 *
 * ## Injectable command seam
 *
 * `startSidecar` accepts an optional `commander` in its options. Production
 * paths omit it and get `realCommander`, which shells out via `child_process`.
 * The Tier-1 unit test injects a fake commander that returns scripted stdout
 * without spawning any JVM, so the launcher's PORT-scrape / health-poll /
 * HTTP-client logic is fully exercised offline.
 *
 * ## HTTP contract
 *
 * The request/response shapes and endpoint names below MUST match the
 * sidecar HTTP contract exactly. The Java-side server (authored under the
 * separate Maven sidecar module) conforms to the same contract; both agree
 * only by both conforming to the contract — there is no shared source between
 * the two sides.
 */

import { Buffer } from "node:buffer";
import type { ChildProcess, SpawnOptions } from "node:child_process";
import { spawn as nodeSpawn } from "node:child_process";
import { existsSync } from "node:fs";
import { request as httpRequest } from "node:http";
import type { RequestOptions } from "node:http";
import { dirname, resolve as pathResolve } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Human-readable path fragment for the built fat JAR, relative to the
 * `integration-tests` package root. Matches the Maven layout the vendored
 * sidecar module produces (mirror of the Go reference).
 */
export const DEFAULT_JAR_RELATIVE_PATH =
  "java-interop/target/java-interop-sidecar.jar";

/**
 * Options for `resolveJarPath`. Every field is injectable so the Tier-1
 * unit test can drive the walk without touching the real filesystem.
 */
export interface ResolveJarPathOptions {
  /**
   * Explicit override. When present, the resolver returns it verbatim (does
   * not check for existence — that responsibility belongs to the caller,
   * which produces a more useful error message).
   */
  readonly jarPath?: string;
  /**
   * Starting directory for the upward walk. Defaults to this module's own
   * directory so the walk is deterministic regardless of the operator's cwd.
   */
  readonly startDir?: string;
  /**
   * Existence predicate. Defaults to `fs.existsSync`; unit tests inject a
   * stub that reports which candidate paths were probed.
   */
  readonly exists?: (path: string) => boolean;
  /**
   * Upper bound on how far the walk climbs before giving up. The Go
   * reference caps at 8; we keep the same bound.
   */
  readonly maxDepth?: number;
}

/**
 * Walk upward from `startDir` looking for `<parent>/<DEFAULT_JAR_RELATIVE_PATH>`.
 * Returns the first hit, or throws when the walk exhausts.
 *
 * The walk mirrors the Go reference's `defaultJarPath`: bounded to 8 levels
 * so we never hit `/` on a malformed workspace and hang searching filesystem
 * root.
 */
export function resolveJarPath(opts?: ResolveJarPathOptions): string {
  if (opts?.jarPath !== undefined && opts.jarPath !== "") {
    return opts.jarPath;
  }
  const exists = opts?.exists ?? existsSync;
  const maxDepth = opts?.maxDepth ?? 8;
  const startDir =
    opts?.startDir ?? dirname(fileURLToPath(import.meta.url));

  let dir = startDir;
  const tried: string[] = [];
  for (let i = 0; i < maxDepth; i++) {
    const candidate = pathResolve(dir, DEFAULT_JAR_RELATIVE_PATH);
    tried.push(candidate);
    if (exists(candidate)) {
      return candidate;
    }
    const parent = dirname(dir);
    if (parent === dir) {
      break;
    }
    dir = parent;
  }
  throw new Error(
    `java-sidecar: could not locate ${DEFAULT_JAR_RELATIVE_PATH} starting from ` +
      `${startDir} (tried ${tried.length} candidate path(s), max depth ${maxDepth})`,
  );
}

/**
 * Options for `isJvmAvailable`. `javaBinary` overrides the resolved binary;
 * `probe` overrides the on-disk / PATH check. Both are used by the unit test
 * to drive the branches without touching the host's real JDK install.
 */
export interface IsJvmAvailableOptions {
  /** Explicit path to a `java` binary. */
  readonly javaBinary?: string;
  /**
   * Env accessor. Defaults to `process.env`; the unit test injects a plain
   * object so `GSR_INTEROP_JAVA` lookups are isolated from the host shell.
   */
  readonly env?: NodeJS.ProcessEnv;
  /**
   * Predicate that reports whether a binary is invocable. Defaults to a
   * `fs.existsSync` probe; unit tests inject a stub.
   */
  readonly probe?: (candidate: string) => boolean;
}

/**
 * Best-effort check that a JVM is reachable so the interop gate can loud-skip
 * cleanly when it isn't. Precedence:
 *   1. explicit `javaBinary` opt
 *   2. `GSR_INTEROP_JAVA` env (mirrors the Go reference's hermetic-build seam)
 *   3. plain `java`
 *
 * A resolved binary is considered available iff `probe(binary)` returns true.
 * The default probe accepts either an absolute path that exists on disk OR
 * the sentinel bare name `java` (which we cannot statically verify without
 * shelling PATH — an availability check that shells out defeats the point of
 * a probe).
 */
export function isJvmAvailable(opts?: IsJvmAvailableOptions): boolean {
  const env = opts?.env ?? process.env;
  const explicit = opts?.javaBinary;
  const envJava = env["GSR_INTEROP_JAVA"];
  const candidate =
    explicit !== undefined && explicit !== ""
      ? explicit
      : envJava !== undefined && envJava !== ""
        ? envJava
        : "java";
  const probe = opts?.probe ?? defaultJvmProbe;
  return probe(candidate);
}

/**
 * The default JVM probe: accept an absolute path that resolves on-disk;
 * accept the bare `java` sentinel (we assume PATH resolution when neither
 * `javaBinary` nor `GSR_INTEROP_JAVA` is set); reject anything else that
 * doesn't exist. Kept as a stand-alone function so unit tests can invoke it
 * directly without going through `isJvmAvailable`.
 */
function defaultJvmProbe(candidate: string): boolean {
  if (candidate === "java") {
    return true;
  }
  return existsSync(candidate);
}

/**
 * Per-endpoint request shapes. Match the sidecar HTTP contract verbatim —
 * every field name and its wire type is part of the contract.
 */
export interface EncodeRequest {
  readonly format: string;
  readonly schema: string;
  readonly schemaName: string;
  readonly schemaVersionId: string;
  readonly payload: Buffer;
  readonly compression?: string;
}

export interface DecodeResponse {
  readonly payload: Buffer;
  readonly schemaVersionId: string;
  readonly schemaName: string;
  readonly schemaDefinition: string;
  readonly dataFormat: string;
}

/**
 * Per-format envelope for `KafkaProduceRequest.record` /
 * `KafkaConsumeResponse.record`:
 *
 *  - `AVRO`     : `{"fields": {...}}`
 *  - `JSON`     : `{"schema": "...", "payload": "..."}`
 *  - `PROTOBUF` : `{"messageTypeFullName": "...", "fieldsJson": "..."}`
 *
 * The envelope is opaque to this launcher; the sidecar reconstructs the
 * typed Java record from it.
 */
export type RecordEnvelope = Record<string, unknown>;

export interface KafkaProduceRequest {
  readonly format: string;
  readonly schema: string;
  readonly schemaName: string;
  readonly record: RecordEnvelope;
  readonly compression?: string;
  readonly bootstrap: string;
  readonly topic: string;
  /** Optional; sidecar falls back to `AWS_REGION` / `us-east-2`. */
  readonly region?: string;
  /** Optional; sidecar defaults to `NONE` when absent. */
  readonly compatibility?: string;
}

export interface KafkaProduceResponse {
  readonly schemaVersionId: string;
  /** Framed wire bytes actually shipped to Kafka. */
  readonly bytes: Buffer;
  readonly offset: number;
  readonly partition: number;
}

export interface KafkaConsumeRequest {
  readonly bootstrap: string;
  readonly topic: string;
  /** `AVRO | JSON | PROTOBUF` — drives the envelope shape returned by the sidecar. */
  readonly format: string;
  readonly groupId?: string;
  readonly region?: string;
  readonly timeoutMs?: number;
}

export interface KafkaConsumeResponse {
  readonly schemaVersionId: string;
  readonly dataFormat: string;
  readonly schemaDefinition: string;
  readonly schemaArn: string;
  readonly record: RecordEnvelope;
}

/**
 * Handle for a running sidecar. `baseUrl()` is exposed so tests that need to
 * hit `/health` or additional endpoints directly can do so without recreating
 * the URL from the port.
 */
export interface Sidecar {
  baseUrl(): string;
  encode(req: EncodeRequest): Promise<Buffer>;
  decode(framed: Buffer): Promise<DecodeResponse>;
  kafkaProduce(req: KafkaProduceRequest): Promise<KafkaProduceResponse>;
  kafkaConsume(req: KafkaConsumeRequest): Promise<KafkaConsumeResponse>;
  stop(): Promise<void>;
}

/**
 * Sanctioned compression tokens on the sidecar HTTP contract. The Java side
 * accepts only these two; a typo like `"GZIP"` would surface as an opaque 500
 * from the sidecar. Reject client-side with a specific error message so the
 * problem is diagnosable from the TS side.
 */
const SIDECAR_COMPRESSION_TOKENS = new Set(["NONE", "ZLIB"]);

/**
 * Normalize a caller-supplied compression string to the sidecar's vocabulary.
 * Empty / undefined maps to `"NONE"`. Case-folds to upper so `"none"` and
 * `"None"` are accepted. Unknown values throw synchronously.
 */
function compressionOrDefault(c?: string): string {
  if (c === undefined || c === "") {
    return "NONE";
  }
  const up = c.toUpperCase();
  if (!SIDECAR_COMPRESSION_TOKENS.has(up)) {
    throw new Error(
      `java-sidecar: unsupported compression "${c}" (want NONE or ZLIB)`,
    );
  }
  return up;
}

/**
 * Minimal shape for the child process object we depend on. Kept narrow so
 * the unit test's fake doesn't have to satisfy the full `ChildProcess`
 * surface (which is large and mostly irrelevant here).
 */
export interface SidecarChild {
  readonly stdout: NodeJS.ReadableStream | null;
  readonly stderr: NodeJS.ReadableStream | null;
  readonly pid: number | undefined;
  kill(signal?: NodeJS.Signals | number): boolean;
  on(event: "exit", listener: (code: number | null) => void): this;
}

/**
 * Injectable exec seam. Production wiring resolves to a wrapper around
 * `child_process.spawn`; the unit test wires a fake that captures the args
 * and yields a `SidecarChild` whose stdout is a canned `PORT:` line.
 */
export interface Commander {
  spawn(
    command: string,
    args: readonly string[],
    options: SpawnOptions,
  ): SidecarChild;
}

/**
 * Production commander that delegates to `child_process.spawn`. Detaches the
 * child (`detached: true`) so `SIGTERM` to the launcher process does not
 * propagate to the JVM automatically — we want the launcher to send SIGTERM
 * explicitly, giving the sidecar's shutdown hook a chance to drain in-flight
 * requests.
 */
export const realCommander: Commander = {
  spawn(command, args, options) {
    const child: ChildProcess = nodeSpawn(command, [...args], options);
    return child as SidecarChild;
  },
};

/**
 * Injectable HTTP request seam. Production wires to `node:http`'s
 * `request` API; unit tests inject a stub that answers in-process without
 * opening a socket.
 *
 * The seam accepts a fully-formed URL + JSON body and returns the parsed
 * JSON response. It intentionally does not expose streaming or non-JSON
 * request bodies — every sidecar endpoint speaks `application/json`.
 */
export interface HttpJsonPoster {
  postJson(
    url: string,
    body: unknown,
    signal: AbortSignal,
  ): Promise<unknown>;
  /** GETs a URL and returns the HTTP status code and body text. */
  getStatus(url: string, signal: AbortSignal): Promise<{ status: number }>;
}

/**
 * Production HTTP client backed by `node:http`. Chose the low-level `http`
 * module (rather than `fetch`) so the launcher runs identically on Node 18
 * and Node 20+ without stumbling on the experimental `fetch` warning some
 * older Node minors still emit under `--experimental-fetch`.
 */
export const realHttpPoster: HttpJsonPoster = {
  async postJson(url, body, signal) {
    const payload = Buffer.from(JSON.stringify(body), "utf8");
    return await new Promise((resolveP, rejectP) => {
      const opts: RequestOptions = {
        ...urlToRequestOptions(url),
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": String(payload.length),
        },
      };
      const req = httpRequest(opts, (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => {
          const raw = Buffer.concat(chunks).toString("utf8");
          if (
            res.statusCode !== undefined &&
            res.statusCode >= 400
          ) {
            rejectP(
              new Error(
                `java-sidecar: POST ${url} -> ${res.statusCode}: ${raw.trim()}`,
              ),
            );
            return;
          }
          try {
            resolveP(raw === "" ? {} : JSON.parse(raw));
          } catch (err) {
            rejectP(err);
          }
        });
      });
      req.on("error", rejectP);
      signal.addEventListener("abort", () => {
        req.destroy(new Error("aborted"));
      });
      req.write(payload);
      req.end();
    });
  },
  async getStatus(url, signal) {
    return await new Promise((resolveP, rejectP) => {
      const opts: RequestOptions = {
        ...urlToRequestOptions(url),
        method: "GET",
      };
      const req = httpRequest(opts, (res) => {
        // Drain body to free the socket, then report the status.
        res.on("data", () => undefined);
        res.on("end", () => resolveP({ status: res.statusCode ?? 0 }));
      });
      req.on("error", rejectP);
      signal.addEventListener("abort", () => {
        req.destroy(new Error("aborted"));
      });
      req.end();
    });
  },
};

/**
 * Convert `http://host:port/path` into the option bag `http.request` wants.
 * `URL.parse` handles ports and paths for us; we surface only the fields the
 * request needs.
 */
function urlToRequestOptions(url: string): RequestOptions {
  const parsed = new URL(url);
  const port =
    parsed.port !== ""
      ? Number(parsed.port)
      : parsed.protocol === "https:"
        ? 443
        : 80;
  return {
    protocol: parsed.protocol,
    hostname: parsed.hostname,
    port,
    path: `${parsed.pathname}${parsed.search}`,
  };
}

/**
 * Options bag for `startSidecar`. Every runtime-facing seam is injectable so
 * the Tier-1 unit test can drive the whole lifecycle without a JVM, a real
 * TCP socket, or a live HTTP server.
 */
export interface SidecarOptions {
  /** Absolute path to the built fat JAR. Defaults to `resolveJarPath()`. */
  readonly jarPath?: string;
  /**
   * The binary to invoke. Defaults to `GSR_INTEROP_JAVA` (if set), else
   * `java`. Same precedence as the Go reference.
   */
  readonly javaBinary?: string;
  /**
   * Milliseconds to wait for the `PORT:` line, and separately for `/health`
   * to return 200. Defaults to 30_000 for each phase.
   */
  readonly startTimeoutMs?: number;
  /**
   * Optional commander override; production paths omit this and get
   * `realCommander`.
   */
  readonly commander?: Commander;
  /**
   * Optional HTTP poster override; production paths omit this and get
   * `realHttpPoster`.
   */
  readonly httpPoster?: HttpJsonPoster;
  /**
   * Optional `AbortSignal` — cancels the health-poll wait and any in-flight
   * HTTP call. Not tied to the child's lifetime; use `stop()` for that.
   */
  readonly signal?: AbortSignal;
}

/**
 * Spawn a sidecar and wait for it to become healthy. Returns once
 * `GET /health` returns 200. Callers MUST `await sidecar.stop()` on teardown
 * or a `SIGTERM`-ed JVM will be left behind on early failures.
 *
 * Local mode only. Container mode (Testcontainers-driven Docker) is a
 * separate concern the interop gate covers, not this module.
 */
export async function startSidecar(
  opts: SidecarOptions = {},
): Promise<Sidecar> {
  const jarPath = opts.jarPath ?? resolveJarPath();
  if (!existsSync(jarPath)) {
    throw new Error(
      `java-sidecar: jar not found at ${jarPath} (build it via mvn -DskipTests package)`,
    );
  }
  const envJava = process.env["GSR_INTEROP_JAVA"];
  const javaBinary =
    opts.javaBinary !== undefined && opts.javaBinary !== ""
      ? opts.javaBinary
      : envJava !== undefined && envJava !== ""
        ? envJava
        : "java";
  const timeoutMs = opts.startTimeoutMs ?? 30_000;
  const commander = opts.commander ?? realCommander;
  const httpPoster = opts.httpPoster ?? realHttpPoster;

  const args = ["-jar", jarPath, "--port=0"] as const;
  const child = commander.spawn(javaBinary, args, {
    // `pipe` on both so we can scrape stdout for `PORT:` and forward stderr.
    stdio: ["ignore", "pipe", "pipe"],
    detached: true,
  });

  // Route stderr to the process's stderr so operator can debug a hung JVM;
  // avoid `child.stderr.pipe(process.stderr)` because that would end the
  // process's stderr when the child closes.
  if (child.stderr !== null) {
    child.stderr.on("data", (chunk: Buffer) => {
      process.stderr.write(chunk);
    });
  }

  let stopped = false;
  const stop = async (): Promise<void> => {
    if (stopped) {
      return;
    }
    stopped = true;
    try {
      child.kill("SIGTERM");
    } catch {
      // The child may have already exited; the kill throws in that case,
      // which is fine — we still want stop() to be idempotent.
    }
  };

  let port: number;
  try {
    port = await readPortLine(child, timeoutMs);
  } catch (err) {
    await stop();
    throw err;
  }
  const baseUrl = `http://127.0.0.1:${port}`;

  const controller = new AbortController();
  const externalSignal = opts.signal;
  if (externalSignal !== undefined) {
    if (externalSignal.aborted) {
      controller.abort();
    } else {
      externalSignal.addEventListener("abort", () => controller.abort());
    }
  }

  try {
    await waitHealthy(httpPoster, baseUrl, timeoutMs, controller.signal);
  } catch (err) {
    await stop();
    throw new Error(
      `java-sidecar: never became healthy at ${baseUrl}: ${errorMessage(err)}`,
    );
  }

  return buildSidecarHandle(baseUrl, httpPoster, stop, controller.signal);
}

/**
 * Assemble the `Sidecar` handle. Kept out of `startSidecar` so the top-level
 * function stays legible; the returned object is a thin layer over the JSON
 * POST helpers with the base64 encoding rules baked in.
 */
function buildSidecarHandle(
  baseUrl: string,
  httpPoster: HttpJsonPoster,
  stop: () => Promise<void>,
  signal: AbortSignal,
): Sidecar {
  return {
    baseUrl: () => baseUrl,
    async encode(req) {
      const body = {
        format: req.format,
        schema: req.schema,
        schemaName: req.schemaName,
        schemaVersionId: req.schemaVersionId,
        payload: req.payload.toString("base64"),
        compression: compressionOrDefault(req.compression),
      };
      const raw = (await httpPoster.postJson(
        `${baseUrl}/encode`,
        body,
        signal,
      )) as { bytes?: string };
      if (typeof raw.bytes !== "string") {
        throw new Error(
          `java-sidecar: /encode response missing "bytes" field`,
        );
      }
      return Buffer.from(raw.bytes, "base64");
    },
    async decode(framed) {
      const raw = (await httpPoster.postJson(
        `${baseUrl}/decode`,
        { bytes: framed.toString("base64") },
        signal,
      )) as {
        payload?: string;
        schemaVersionId?: string;
        schemaName?: string;
        schemaDefinition?: string;
        dataFormat?: string;
      };
      if (
        typeof raw.payload !== "string" ||
        typeof raw.schemaVersionId !== "string" ||
        typeof raw.schemaName !== "string" ||
        typeof raw.schemaDefinition !== "string" ||
        typeof raw.dataFormat !== "string"
      ) {
        throw new Error(
          `java-sidecar: /decode response missing one of payload/schemaVersionId/schemaName/schemaDefinition/dataFormat`,
        );
      }
      return {
        payload: Buffer.from(raw.payload, "base64"),
        schemaVersionId: raw.schemaVersionId,
        schemaName: raw.schemaName,
        schemaDefinition: raw.schemaDefinition,
        dataFormat: raw.dataFormat,
      };
    },
    async kafkaProduce(req) {
      const body: Record<string, unknown> = {
        format: req.format,
        schema: req.schema,
        schemaName: req.schemaName,
        record: req.record,
        compression: compressionOrDefault(req.compression),
        bootstrap: req.bootstrap,
        topic: req.topic,
      };
      if (req.region !== undefined && req.region !== "") {
        body["region"] = req.region;
      }
      if (req.compatibility !== undefined && req.compatibility !== "") {
        body["compatibility"] = req.compatibility;
      }
      const raw = (await httpPoster.postJson(
        `${baseUrl}/kafka-produce`,
        body,
        signal,
      )) as {
        schemaVersionId?: string;
        bytes?: string;
        offset?: number;
        partition?: number;
      };
      if (
        typeof raw.schemaVersionId !== "string" ||
        typeof raw.bytes !== "string" ||
        typeof raw.offset !== "number" ||
        typeof raw.partition !== "number"
      ) {
        throw new Error(
          `java-sidecar: /kafka-produce response missing one of schemaVersionId/bytes/offset/partition`,
        );
      }
      return {
        schemaVersionId: raw.schemaVersionId,
        bytes: Buffer.from(raw.bytes, "base64"),
        offset: raw.offset,
        partition: raw.partition,
      };
    },
    async kafkaConsume(req) {
      if (req.format === "") {
        throw new Error(
          `java-sidecar: KafkaConsumeRequest.format must be set (AVRO|JSON|PROTOBUF)`,
        );
      }
      const body: Record<string, unknown> = {
        bootstrap: req.bootstrap,
        topic: req.topic,
        format: req.format,
      };
      if (req.groupId !== undefined && req.groupId !== "") {
        body["groupId"] = req.groupId;
      }
      if (req.region !== undefined && req.region !== "") {
        body["region"] = req.region;
      }
      if (req.timeoutMs !== undefined && req.timeoutMs > 0) {
        body["timeoutMs"] = req.timeoutMs;
      }
      const raw = (await httpPoster.postJson(
        `${baseUrl}/kafka-consume`,
        body,
        signal,
      )) as {
        schemaVersionId?: string;
        dataFormat?: string;
        schemaDefinition?: string;
        schemaArn?: string;
        record?: RecordEnvelope;
      };
      if (
        typeof raw.schemaVersionId !== "string" ||
        typeof raw.dataFormat !== "string" ||
        typeof raw.schemaDefinition !== "string" ||
        typeof raw.schemaArn !== "string" ||
        raw.record === undefined
      ) {
        throw new Error(
          `java-sidecar: /kafka-consume response missing one of schemaVersionId/dataFormat/schemaDefinition/schemaArn/record`,
        );
      }
      return {
        schemaVersionId: raw.schemaVersionId,
        dataFormat: raw.dataFormat,
        schemaDefinition: raw.schemaDefinition,
        schemaArn: raw.schemaArn,
        record: raw.record,
      };
    },
    stop,
  };
}

/**
 * Read from the child's stdout until the first `PORT: <n>` line, or throw
 * on timeout / stream close. Consumes any lines before `PORT:` (the sidecar
 * may log startup lines above the port line).
 *
 * After extracting the port, the reader keeps draining stdout so a full
 * pipe never backpressures the JVM. Draining is done via a `data` handler
 * rather than piping to `process.stdout` so operator logs stay quiet in
 * tests unless something is explicitly wrong.
 */
async function readPortLine(
  child: SidecarChild,
  timeoutMs: number,
): Promise<number> {
  const stdout = child.stdout;
  if (stdout === null) {
    throw new Error(
      "java-sidecar: child stdout is null; cannot read PORT: line",
    );
  }
  return await new Promise<number>((resolveP, rejectP) => {
    const timer = setTimeout(() => {
      cleanup();
      rejectP(
        new Error(
          `java-sidecar: timed out after ${timeoutMs}ms waiting for PORT: line`,
        ),
      );
    }, timeoutMs);
    let buffer = "";
    let resolved = false;
    const onData = (chunk: Buffer | string): void => {
      if (resolved) {
        return;
      }
      buffer += typeof chunk === "string" ? chunk : chunk.toString("utf8");
      let newlineIdx = buffer.indexOf("\n");
      while (newlineIdx >= 0) {
        const line = buffer.slice(0, newlineIdx).trimEnd();
        buffer = buffer.slice(newlineIdx + 1);
        const match = /^PORT:\s*(\d+)$/.exec(line);
        if (match !== null) {
          const port = Number(match[1]);
          if (!Number.isInteger(port) || port <= 0 || port > 65_535) {
            cleanup();
            rejectP(
              new Error(
                `java-sidecar: invalid port in "${line}" (got ${port})`,
              ),
            );
            return;
          }
          resolved = true;
          cleanup();
          // Keep the stream drained so the JVM never blocks on a full pipe.
          stdout.on("data", () => undefined);
          resolveP(port);
          return;
        }
        newlineIdx = buffer.indexOf("\n");
      }
    };
    const onEnd = (): void => {
      if (resolved) {
        return;
      }
      cleanup();
      rejectP(
        new Error(
          "java-sidecar: jvm exited before printing PORT: line",
        ),
      );
    };
    const onError = (err: Error): void => {
      if (resolved) {
        return;
      }
      cleanup();
      rejectP(err);
    };
    const cleanup = (): void => {
      clearTimeout(timer);
      stdout.off("data", onData);
      stdout.off("end", onEnd);
      stdout.off("error", onError);
    };
    stdout.on("data", onData);
    stdout.on("end", onEnd);
    stdout.on("error", onError);
  });
}

/**
 * Poll `GET /health` every 100 ms until it returns 200 or the deadline is
 * hit. Kept as a wall-clock loop (rather than an exponential backoff) so the
 * launcher startup latency is a simple flat number bounded by the deadline;
 * the sidecar's cold-start is dominated by JVM boot, not by the poll rate.
 */
async function waitHealthy(
  httpPoster: HttpJsonPoster,
  baseUrl: string,
  timeoutMs: number,
  signal: AbortSignal,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    if (signal.aborted) {
      throw new Error("java-sidecar: /health poll aborted");
    }
    try {
      const { status } = await httpPoster.getStatus(
        `${baseUrl}/health`,
        signal,
      );
      if (status === 200) {
        return;
      }
      lastError = new Error(`status=${status}`);
    } catch (err) {
      lastError = err;
    }
    await sleep(100, signal);
  }
  throw new Error(
    lastError !== undefined
      ? `timed out (last error: ${errorMessage(lastError)})`
      : "timed out",
  );
}

/**
 * Promise-flavored `setTimeout` that resolves early on abort. Kept private
 * so the launcher's polling loop reads linearly without a nested `Promise`
 * construction at each iteration site.
 */
async function sleep(ms: number, signal: AbortSignal): Promise<void> {
  await new Promise<void>((resolveP) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolveP();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      resolveP();
    };
    signal.addEventListener("abort", onAbort);
  });
}

/**
 * Best-effort extraction of a human-readable error message. Errors thrown
 * from `postJson` may or may not be `Error` instances (JSON parse failure
 * yields a `SyntaxError`); the sidecar-side rejection path may pass through
 * a plain string. Handle both.
 */
function errorMessage(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}
