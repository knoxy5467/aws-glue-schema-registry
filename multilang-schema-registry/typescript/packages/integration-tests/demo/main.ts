/**
 * Entry point for the narrated cross-language interop demo.
 *
 * The demo runs the 21-scenario catalogue (18 matrix cells from
 * `scenarios.ts` + 3 non-matrix drivers from `scenarios-extra.ts`) against
 * real AWS Glue Schema Registry, a real testcontainers Kafka broker, and a
 * real Java sidecar JVM. It prints a narrated log so an operator can watch
 * the wire-format round-trip byte-by-byte and derives a summary table +
 * process exit code from the collected {@link ScenarioResult} rows.
 *
 * ## Startup ordering
 *
 *   1. Resolve region — `resolveRegion()` from the shared env-gate.
 *   2. `requireInterop()` — the three-gate loud-skip discipline (AWS-
 *      integration, real-Glue, JVM/JAR/partner). A skip returns cleanly
 *      with exit code 0 after emitting one machine-parseable `SKIP` line.
 *   3. `requireRealCreds()` — hard throw when `GSR_GLUE=real` and no
 *      credentials resolve. Mirrors the Go reference's Makefile hard-fail.
 *   4. Best-effort account id — read `AWS_ACCOUNT_ID` from the environment
 *      so the banner names the target account; falls back to
 *      `(unresolved)` when unset. Never throws.
 *   5. `printBanner()` — one banner block naming account / region /
 *      registry before any AWS call so the operator confirms the target.
 *   6. `resolveBroker()` — testcontainers Kafka OR an external
 *      `KAFKA_BROKER=<host:port>` OR a loud-skip when Docker is
 *      unavailable / explicitly disabled.
 *   7. `startSidecar()` — spawn the Java sidecar JVM and wait for
 *      `/health`.
 *   8. `parseConfig()` — validate the config surface (region, registry,
 *      compatibility, auto-registration flag) into a typed `GsrConfig`.
 *   9. `selectGlueBackend(cfg)` — real `@aws-sdk/client-glue` client +
 *      matched `CleanupTracker` (reverse-order `DeleteSchema` on exit).
 *  10. Build `cache`, `metadata`, `SchemaRegistrar` — the SAME cache
 *      instance is passed to the registrar AND to scenario 19's deps bag,
 *      so scenario 19's `cache.get(key)` / `cache.remove(key)`
 *      observations see the exact rows the registrar wrote (the shared-
 *      cache wiring the cache-hit scenario depends on).
 *
 * ## Dispatch
 *
 * The 18 matrix scenarios from `scenarios.ts` and the 3 non-matrix
 * scenarios from `scenarios-extra.ts` are concatenated in order and
 * dispatched sequentially — each returns exactly one `ScenarioResult`.
 * A scenario body never throws to abort the run (a caught error becomes
 * `pass: false` with `error` populated), so cleanup always fires.
 *
 * ## Teardown
 *
 * Runs on EVERY exit path (success, per-scenario failure aggregate, and
 * unhandled throw at startup after the try wraps) via a `try/finally`
 * plus a `SIGINT`/`SIGTERM` handler. Order is sidecar → broker → schemas:
 * a JVM hang during shutdown does not delay the reverse-order
 * `DeleteSchema` calls, and a Kafka broker running under testcontainers
 * cannot linger past the container's own `stop()`.
 *
 * ## Exit code
 *
 * Exit 0 iff every scenario returned `pass: true`; otherwise 1. A gate
 * loud-skip is a clean exit 0 (mirrors the vitest suite behaviour — a
 * skipped Tier-3 run is not a failure).
 *
 * ## Reuse discipline
 *
 * This module is a wiring shim. It composes the harness modules
 * (`env-gate`, `interop-gate`, `kafka-broker`, `java-sidecar`,
 * `real-glue`), `@gsr/core` (config + cache + registrar + metadata +
 * Glue seam), and `@gsr/serde` (only reachable via the scenario driver
 * modules), then dispatches into `scenarios.ts` / `scenarios-extra.ts`.
 * It re-implements no wire framing, compression, Glue call, sidecar HTTP,
 * or Kafka client.
 */

import {
  createCache,
  createMetadata,
  parseConfig,
  SchemaRegistrar,
  type GsrConfig,
} from "@gsr/core";
import { Kafka, logLevel as KafkaLogLevel } from "kafkajs";

// Import the vitest-free runtime helpers — importing from `env-gate.js`
// would transitively pull vitest into the vite-node process, tripping
// vitest's internal-state guard at module load.
import {
  requireRealCreds,
  resolveRegion,
} from "../src/env-gate.runtime.js";
import { requireInterop } from "../src/interop-gate.js";
import {
  isSkip,
  resolveBroker,
  type BrokerHandle,
} from "../src/kafka-broker.js";
import {
  selectGlueBackend,
  type CleanupTracker,
} from "../src/real-glue.js";
import {
  startSidecar,
  type Sidecar,
} from "../src/java-sidecar.js";

import { createDemoSchemaNamer } from "./demo-fixtures.js";
import {
  printBanner,
  printStage,
  type NarratorSink,
} from "./narrator.js";
import {
  REGISTRY_NAME,
  SCENARIOS_1_TO_18,
  type ScenarioContext,
  type ScenarioResult,
} from "./scenarios.js";
import {
  runScenario19Cache,
  runScenario20AutoRegister,
  runScenario21WriterRegisters,
  type ExtraScenarioDeps,
} from "./scenarios-extra.js";

/**
 * Suite name embedded in the interop gate's `SKIP <name> — <reason>`
 * line so a log scanner distinguishes this demo's skip from a vitest
 * interop suite's skip.
 */
const SUITE_NAME = "demo-interop";

/**
 * Placeholder account id printed in the banner when no environment hint
 * is available. Best-effort account resolution is a nice-to-have; the
 * demo does not add a `@aws-sdk/client-sts` dependency solely for it.
 */
const UNRESOLVED_ACCOUNT_ID = "(unresolved)";

/**
 * Total scenarios in the demo catalogue — matches the "scenario N of 21"
 * numbering the driver modules narrate. Kept as a constant so the summary
 * total row cannot drift from the driver-side count.
 */
const TOTAL_SCENARIOS = 21;

/**
 * Sidecar startup budget. Kept generous so a cold JVM on a CI host does
 * not flake the whole demo; a value this large is fine because the same
 * per-scenario Kafka consume timeouts already gate the wall-clock.
 */
const SIDECAR_START_TIMEOUT_MS = 120_000;

/**
 * Bag of resources the demo builds during startup and tears down at exit.
 * Kept as a mutable object so a mid-startup failure still leaves partial
 * state for the finally block — e.g. a sidecar that started but a
 * broker that then failed still gets `sidecar.stop()`'d.
 */
interface Runtime {
  sidecar: Sidecar | null;
  broker: BrokerHandle | null;
  cleanup: CleanupTracker | null;
}

/**
 * Entry point. Runs the whole demo; resolves with a numeric exit code the
 * outer wrapper hands to `process.exit`. Never throws — every error path
 * is turned into an exit code + narrated diagnostic, so a partial run
 * still fires the cleanup path.
 */
export async function main(io: NarratorSink = defaultSink()): Promise<number> {
  // --- Step 1: region ---------------------------------------------------
  const region = resolveRegion();

  // --- Step 2: interop gate --------------------------------------------
  // The gate emits one `SKIP <suite> — <reason>` line itself on skip; we
  // return exit 0 to mirror the vitest-style clean skip.
  const gate = requireInterop({ suiteName: SUITE_NAME });
  if (gate.skipped) {
    return 0;
  }

  // --- Step 3: real-creds hard-fail ------------------------------------
  // Throws when GSR_GLUE=real and no credentials resolve. The throw is
  // intentional — a silent skip here would ship a false green.
  requireRealCreds();

  // --- Step 4: best-effort account id ----------------------------------
  const accountId = resolveAccountId();

  // --- Step 5: banner --------------------------------------------------
  printBanner(io, {
    accountId,
    region,
    registry: REGISTRY_NAME,
  });

  // Track the resources we own so the finally block can tear each down
  // even if a later startup step throws.
  const runtime: Runtime = {
    sidecar: null,
    broker: null,
    cleanup: null,
  };

  // Wire SIGINT/SIGTERM to a graceful teardown. Ctrl-C mid-run must not
  // leak the JVM or a testcontainers Kafka container; the handler flips
  // a shared flag and lets the finally block do the work — no direct
  // process.exit inside the handler because a mid-flight kafkajs
  // consumer.disconnect would then race the interpreter shutdown.
  let signalled: NodeJS.Signals | null = null;
  const onSignal = (sig: NodeJS.Signals): void => {
    signalled = sig;
    // Do not exit here — allow the current scenario to observe the
    // AbortSignal-style flag via the shared runtime and unwind cleanly.
    printStage(io, "shutdown", `received ${sig} — draining and cleaning up`);
  };
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);

  let exitCode = 0;
  try {
    // --- Step 6: broker ------------------------------------------------
    const resolved = await resolveBroker();
    if (isSkip(resolved)) {
      // eslint-disable-next-line no-console
      console.warn(`SKIP ${SUITE_NAME} — ${resolved.skip}`);
      return 0;
    }
    runtime.broker = resolved;
    printStage(io, "broker", `bootstrap=${resolved.bootstrap}`);

    // --- Step 7: sidecar ----------------------------------------------
    runtime.sidecar = await startSidecar({
      startTimeoutMs: SIDECAR_START_TIMEOUT_MS,
    });
    printStage(io, "sidecar", `ready at ${runtime.sidecar.baseUrl()}`);

    // --- Step 8: config ------------------------------------------------
    const cfg: GsrConfig = parseConfig({
      region,
      "registry.name": REGISTRY_NAME,
      // BACKWARD lets a v1 record decode under a registered v2 reader
      // — the cross-version cells (rows 1-12) depend on this.
      compatibility: "BACKWARD",
      // Scenario 20 asserts the flag is on; scenarios 3/4/7/8/11/12/13/
      // 15/17 need it so first-write registrations do not fault.
      schemaAutoRegistrationEnabled: "true",
    });

    // --- Step 9: Glue backend + cleanup tracker -----------------------
    const backend = selectGlueBackend(cfg);
    if (!backend.isReal || backend.cleanup === null) {
      // requireRealCreds + requireInterop should have gated this out;
      // guard defensively so a mis-set env never silently exercises the
      // fake backend against a real AWS region.
      throw new Error(
        "demo-interop: selectGlueBackend returned the fake backend — " +
          "the demo only runs against real Glue (set GSR_GLUE=real).",
      );
    }
    runtime.cleanup = backend.cleanup;

    // --- Step 10: cache + metadata + registrar (shared instance) ------
    // Build ONE cache instance. It is passed to the SchemaRegistrar AND
    // handed to scenario 19 via its deps bag so `cache.get(key)` /
    // `cache.remove(key)` observations see the exact rows the registrar
    // wrote — otherwise the cache seam scenario would be measuring a
    // private cache no one else in the demo writes to.
    const cache = createCache<{ schemaVersionId: string }>({
      ttlMillis: cfg.timeToLiveMillis,
      size: cfg.cacheSize,
    });
    const metadata = createMetadata(backend.client, cfg);
    const registrar = new SchemaRegistrar(
      backend.client,
      cache,
      cfg,
      metadata,
    );

    // Per-run suffix threaded into topic names (see scenarios.ts /
    // scenarios-extra.ts). Not the same as `perRunSuffix()` from
    // real-glue.ts — that one is used inside `realSchemaName(label)` to
    // per-run-namespace SCHEMA names; this suffix is for TOPIC names and
    // consumer group ids only.
    const runSuffix = makeShortRunSuffix();
    const namer = createDemoSchemaNamer();

    // The shared kafkajs client used by scenario 21 (writer-registers).
    // Scenarios 1-18 already build their own per-call `Kafka` clients
    // inside `produceOne` / `consumeOne` — the interop test suites'
    // pattern verbatim. Scenario 21 accepts a `Kafka` on its deps bag
    // because its produce/consume are inlined; wiring one client here
    // means the demo owns exactly one long-lived kafka handle plus the
    // per-call short-lived ones the matrix drivers use.
    const kafka = new Kafka({
      clientId: "gsr-ts-demo-main",
      brokers: [runtime.broker.bootstrap],
      logLevel: KafkaLogLevel.NOTHING,
    });

    // Build the two shared contexts. The extras bag is a superset of the
    // matrix bag (adds `cache`, `kafka`, `tracker`) so a scenario body
    // needs only one shape of dep injection.
    const ctx: ScenarioContext = {
      io,
      sidecar: runtime.sidecar,
      broker: runtime.broker,
      registrar,
      glueClient: backend.client,
      cleanup: runtime.cleanup,
      namer,
      region,
      cfg,
      runSuffix,
    };
    const extraDeps: ExtraScenarioDeps = {
      io,
      namer,
      cfg,
      glueClient: backend.client,
      cache,
      registrar,
      tracker: runtime.cleanup,
      broker: runtime.broker,
      sidecar: runtime.sidecar,
      kafka,
    };

    // Dispatch all 21 scenarios in catalogue order. Each returns a
    // ScenarioResult — a caught error inside a scenario becomes
    // `pass:false + error` so we never lose the tracker's later-scenario
    // schemas or the broker/sidecar teardown to a mid-run throw.
    const results: ScenarioResult[] = [];
    for (const scenario of SCENARIOS_1_TO_18) {
      if (signalled !== null) {
        printStage(io, "shutdown", `aborting before scenario ${results.length + 1}: ${signalled}`);
        break;
      }
      results.push(await scenario(ctx));
    }
    if (signalled === null) {
      results.push(await runScenario19Cache(extraDeps));
    }
    if (signalled === null) {
      results.push(await runScenario20AutoRegister(extraDeps));
    }
    if (signalled === null) {
      results.push(await runScenario21WriterRegisters(extraDeps));
    }

    // Summary + exit code derived exclusively from `results`. Never
    // recompute PASS/FAIL — the operator's on-screen check and the
    // summary row must agree by construction.
    printSummary(io, results, signalled !== null);
    exitCode = results.every((r) => r.pass) && signalled === null ? 0 : 1;
  } catch (err) {
    // A startup-path throw (e.g. `requireRealCreds`) or a rare uncaught
    // in scenario dispatch reaches here. Narrate the message and set a
    // non-zero exit; the finally block still tears down.
    const msg = err instanceof Error ? err.message : String(err);
    printStage(io, "error", msg);
    exitCode = 1;
  } finally {
    // Teardown order: sidecar → broker → schemas. A JVM hang during
    // shutdown does NOT delay the DeleteSchema calls (the tracker is
    // last), and any per-step failure is narrated but does not skip
    // subsequent teardown.
    process.off("SIGINT", onSignal);
    process.off("SIGTERM", onSignal);
    if (runtime.sidecar !== null) {
      try {
        await runtime.sidecar.stop();
      } catch (err) {
        printStage(io, "teardown", `sidecar.stop() failed: ${errText(err)}`);
      }
    }
    if (runtime.broker !== null) {
      try {
        await runtime.broker.stop();
      } catch (err) {
        printStage(io, "teardown", `broker.stop() failed: ${errText(err)}`);
      }
    }
    if (runtime.cleanup !== null) {
      try {
        await runtime.cleanup.run();
      } catch (err) {
        // AggregateError message already lists per-schema failures; we
        // narrate the top-level message so a leaked schema is visible.
        printStage(io, "teardown", `cleanup.run() failed: ${errText(err)}`);
      }
    }
  }

  return exitCode;
}

/**
 * Print the fixed-width summary table: one row per scenario plus a
 * totals footer. The `pass` column reads directly from
 * `ScenarioResult.pass` (which itself came from the on-screen
 * `printEqualityCheck` calls), so the summary can never disagree with
 * the narrated PASS/FAIL an operator saw scroll past.
 */
function printSummary(
  io: NarratorSink,
  results: readonly ScenarioResult[],
  aborted: boolean,
): void {
  io.write("\n");
  io.write("=".repeat(80) + "\n");
  io.write("  Summary\n");
  io.write("=".repeat(80) + "\n");
  for (const r of results) {
    const flag = r.pass ? "PASS" : "FAIL";
    const err = r.error !== undefined ? ` — ${r.error}` : "";
    io.write(
      `  [${flag}] scenario ${padIndex(r.index)}: ${r.format}/${r.compression} ${r.direction}  ${r.title}${err}\n`,
    );
  }
  const passed = results.filter((r) => r.pass).length;
  io.write("-".repeat(80) + "\n");
  const suffix = aborted ? "  (RUN ABORTED)" : "";
  io.write(`  ${passed} / ${TOTAL_SCENARIOS} PASS  (${results.length} scenarios ran)${suffix}\n`);
  io.write("=".repeat(80) + "\n");
}

/**
 * Right-align a scenario index in a two-character column so the summary
 * lines up on the terminal without a tabular library.
 */
function padIndex(n: number): string {
  return n < 10 ? ` ${n}` : String(n);
}

/**
 * Extract a human message from an arbitrary thrown value.
 */
function errText(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Best-effort account id resolution. Reads `AWS_ACCOUNT_ID` from the
 * environment; falls back to `(unresolved)` when unset. The demo does
 * not add a `@aws-sdk/client-sts` dependency solely to print a banner
 * line — an operator who wants the exact account can set the env var
 * or read it from `aws sts get-caller-identity` before invocation.
 */
function resolveAccountId(): string {
  const raw = process.env["AWS_ACCOUNT_ID"];
  if (raw === undefined || raw === "") {
    return UNRESOLVED_ACCOUNT_ID;
  }
  return raw;
}

/**
 * Short random suffix for topic + group-id namespacing. Matches the
 * interop test suites' `makeShortRunSuffix` shape — `<epochMs-base36><4
 * hex>` — so a leaked topic on the broker is scannable by wall-clock.
 */
function makeShortRunSuffix(): string {
  return `${Date.now().toString(36)}${Math.random().toString(16).slice(2, 6)}`;
}

/**
 * Default narrator sink — forwards to `process.stdout`. Broken out so
 * an outer wrapper (or a diagnostic tool) can inject a memory buffer
 * without shipping a copy of the write logic.
 */
function defaultSink(): NarratorSink {
  return {
    write(s: string): void {
      process.stdout.write(s);
    },
  };
}

// ---------------------------------------------------------------------------
// Node entry — invoke `main()` when this module is the process entry point.
// Guarded so importing this file (e.g. for a future diagnostic tool that
// wants to reuse `resolveAccountId`) does not run the demo on import.
//
// vite-node keeps its OWN binary in `process.argv[1]`, so the classic
// `import.meta.url === file://${process.argv[1]}` shape does not work here.
// Match `import.meta.url` against this file's stable suffix instead, and
// use the presence of `VITEST` as a reliable "we're under the test runner,
// do not auto-run" signal. Same pattern as `bench/report.ts`.
// ---------------------------------------------------------------------------

const invokedDirectly = ((): boolean => {
  const metaUrl = import.meta.url;
  if (typeof metaUrl !== "string") return false;
  if (process.env.VITEST) return false;
  return metaUrl.endsWith("/packages/integration-tests/demo/main.ts");
})();
if (invokedDirectly) {
  void main().then(
    (code) => {
      process.exit(code);
    },
    (err: unknown) => {
      const msg = err instanceof Error ? err.message : String(err);
      // eslint-disable-next-line no-console
      console.error(`demo-interop: unhandled error — ${msg}`);
      process.exit(1);
    },
  );
}
