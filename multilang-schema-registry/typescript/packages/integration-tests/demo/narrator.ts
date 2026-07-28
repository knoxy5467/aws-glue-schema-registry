/**
 * Narration formatting helpers for the demo-interop TypeScript program.
 *
 * Pure functions with no I/O beyond writing to an injectable `NarratorSink`.
 * A production caller passes a sink that forwards to `process.stdout`; unit
 * tests pass an in-memory string sink and assert on the captured output.
 *
 * The frozen GSR wire format is:
 *
 *   byte 0      — header byte (0x03)
 *   byte 1      — compression byte (0x00 NONE, 0x05 ZLIB, else unknown)
 *   bytes 2-17  — schema-version UUID (16 bytes, big-endian)
 *   bytes 18+   — serialized payload body
 *
 * `printHexDump` walks the message in that order, emitting a `[wire-format]`
 * stage line per segment with a hex + printable-ASCII gutter so an operator
 * can read the compatibility claim without reading test source. Mirrors the
 * Go reference (`integration-tests/cmd/demo-interop/narrator.go`) so a
 * reviewer sees the same shape across languages.
 */

/**
 * Sink surface — a single `write(s)` method. Kept minimal so callers can
 * pass `process.stdout` directly (its `.write` is compatible) OR an
 * in-memory buffer in a Tier-1 unit test.
 */
export interface NarratorSink {
  write(s: string): void;
}

/**
 * Options for `printBanner`. The demo prints these three values verbatim at
 * startup so an operator can confirm the resolved region/account before the
 * run bills real AWS.
 */
export interface BannerOptions {
  readonly accountId: string;
  readonly region: string;
  readonly registry: string;
}

/**
 * Width in characters of the horizontal rules drawn by `printBanner` /
 * `printSectionHeader`. Chosen to fit inside an 80-column terminal so
 * copy-pasted demo output stays legible in a review comment or CR body.
 */
const SEPARATOR_WIDTH = 80;

/**
 * Wire-format constants. Duplicated as locals (rather than imported from
 * `@gsr/core`) so the narrator stays a pure formatting module with no
 * cross-package dependency — the format is frozen by contract, and the
 * verification-pointer test asserts these values line up with the header
 * bytes the serde codec emits.
 */
const HEADER_BYTE_INDEX = 0;
const COMPRESSION_BYTE_INDEX = 1;
const UUID_START_INDEX = 2;
const UUID_END_INDEX = 18;
const UUID_BYTE_LENGTH = 16;
const HEX_ASCII_CAP = 32;

/**
 * Render the banner block at the top of a demo run: heavy rule, three
 * labelled fields, heavy rule. The banner is legible on its own — an
 * operator scanning stderr can confirm the account/region/registry
 * targeted by the run.
 */
export function printBanner(io: NarratorSink, opts: BannerOptions): void {
  const rule = "=".repeat(SEPARATOR_WIDTH);
  io.write(rule + "\n");
  io.write("  GSR TypeScript Client — Java↔TS Cross-Language Interop Demo\n");
  io.write(`  Account:  ${opts.accountId}\n`);
  io.write(`  Region:   ${opts.region}\n`);
  io.write(`  Registry: ${opts.registry}\n`);
  io.write(rule + "\n");
  io.write("\n");
}

/**
 * Render a scenario section header: a light horizontal rule, the title, and
 * a matching rule. Callers pass a fully-formed title that already includes
 * the "scenario N of 21" numbering; the narrator does not know the catalogue
 * shape.
 */
export function printSectionHeader(io: NarratorSink, title: string): void {
  const rule = "─".repeat(SEPARATOR_WIDTH);
  io.write(rule + "\n");
  io.write(`  ${title}\n`);
  io.write(rule + "\n");
  io.write("\n");
}

/**
 * Render a tagged single-line stage message: `[<tag>] <message>`. Used both
 * by `printHexDump` (with tag `wire-format`) and by scenario bodies for
 * ad-hoc narration lines (e.g. `[glue] created schema-version-id ...`).
 */
export function printStage(
  io: NarratorSink,
  tag: string,
  message: string,
): void {
  io.write(`[${tag}] ${message}\n`);
}

/**
 * Options for `printScenarioIntro`. `what` is a one-line summary of the step
 * about to run; `proves` is a one-line summary of the GSR concept this
 * scenario exercises (why an operator would care). Both are printed after
 * the section header and before the first `[glue]` / `[schema]` line, so a
 * reviewer scanning demo output sees the intent before the mechanics.
 */
export interface ScenarioIntro {
  readonly what: string;
  readonly proves: string;
}

/**
 * Narrate what a scenario is about to do and what it demonstrates before
 * any schema registration or wire round-trip fires. Mirrors the Go
 * reference's per-section "What this proves" narration so the TS demo
 * meets the same verbosity bar. Output shape:
 *
 *     [scenario] What: <one-liner>
 *     [scenario] Proves: <one-liner>
 *
 * followed by a blank line.
 */
export function printScenarioIntro(
  io: NarratorSink,
  intro: ScenarioIntro,
): void {
  printStage(io, "scenario", `What:   ${intro.what}`);
  printStage(io, "scenario", `Proves: ${intro.proves}`);
  io.write("\n");
}

/**
 * Pretty-print a multi-line schema body under a `[schema] <label>` header,
 * with each line indented four spaces (matches the Go reference's
 * `Schema body:` block shape). A `oneLine` inline dump loses the shape of
 * an Avro / JSON-Schema / Proto3 body — the whole point of the narrated
 * demo is that an operator can read the schema without opening the source,
 * so the multi-line variant is the primary form.
 *
 * A trailing newline separates the block from the next stage line.
 */
export function printSchemaBody(
  io: NarratorSink,
  label: string,
  body: string,
): void {
  printStage(io, "schema", label);
  const lines = body.split("\n");
  for (const line of lines) {
    io.write(`    ${line}\n`);
  }
  io.write("\n");
}

/**
 * Narrate the record value that is about to be serialized (Direction: writer)
 * or that the receiver just decoded (Direction: reader). Kept as its own
 * helper so scenarios can emit a uniform `[record] <label>` line before
 * either the encode step or the equality check — mirrors the Go reference's
 * `Record: {...}` narration.
 */
export function printRecord(
  io: NarratorSink,
  label: string,
  record: unknown,
): void {
  printStage(io, "record", `${label}: ${renderRecord(record)}`);
}

/**
 * Emit a final `[verdict] PASS/FAIL — <detail>` line at the end of a
 * scenario, mirroring the Go reference's verdict tag. Callers pass in the
 * pre-decided pass/fail bit (usually the AND of the scenario's individual
 * `printEqualityCheck` returns) and a human-readable one-liner explaining
 * what was checked. The verdict is redundant with the earlier equality
 * lines — that redundancy is the point: an operator scanning the tail of
 * a scenario section sees the outcome without having to scroll up.
 */
export function printVerdict(
  io: NarratorSink,
  pass: boolean,
  detail: string,
): void {
  const flag = pass ? "PASS" : "FAIL";
  printStage(io, "verdict", `${flag} — ${detail}`);
  io.write("\n");
}

/**
 * Decompose a wire message into its four labelled segments (header,
 * compression byte, schema-version UUID, payload) and emit each as a
 * `[wire-format]` stage line with hex and an ASCII gutter. Truncated inputs
 * produce a diagnostic stage line and stop early — narration must not
 * throw, so a partially-formed hex dump is preferable to a stack trace
 * mid-scenario.
 */
export function printHexDump(
  io: NarratorSink,
  label: string,
  wire: Uint8Array,
): void {
  io.write(`  ${label} (${wire.length} bytes total):\n`);
  if (wire.length === 0) {
    io.write("    (empty)\n");
    return;
  }

  const headerByte = wire[HEADER_BYTE_INDEX] as number;
  printStage(
    io,
    "wire-format",
    `Header byte: 0x${byteHex(headerByte)} (GSR wire format)  ` +
      formatHexLine(wire.subarray(HEADER_BYTE_INDEX, HEADER_BYTE_INDEX + 1)),
  );

  if (wire.length < 2) {
    return;
  }

  const compressionByte = wire[COMPRESSION_BYTE_INDEX] as number;
  printStage(
    io,
    "wire-format",
    `Compression byte: 0x${byteHex(compressionByte)} (${compressionName(compressionByte)})  ` +
      formatHexLine(
        wire.subarray(COMPRESSION_BYTE_INDEX, COMPRESSION_BYTE_INDEX + 1),
      ),
  );

  if (wire.length < UUID_END_INDEX) {
    printStage(
      io,
      "wire-format",
      "(truncated — expected ≥18 bytes for schema-version UUID)",
    );
    return;
  }

  const uuidBytes = wire.subarray(UUID_START_INDEX, UUID_END_INDEX);
  printStage(
    io,
    "wire-format",
    `Schema version UUID: ${formatUuid(uuidBytes)}  ${formatHexLine(uuidBytes)}`,
  );

  const body = wire.subarray(UUID_END_INDEX);
  if (body.length > 0) {
    printStage(
      io,
      "wire-format",
      `Payload body: ${body.length} bytes  ${formatHexLine(body)}`,
    );
  } else {
    printStage(io, "wire-format", "Payload body: (empty)");
  }
  io.write("\n");
}

/**
 * Print an expected-vs-actual equality line with a check/cross indicator
 * and return the boolean it printed, so scenario bodies derive PASS/FAIL
 * from exactly the comparison the operator sees. A silent second
 * comparison for the ScenarioResult would risk divergent narration and
 * verdict — this contract eliminates that possibility.
 *
 * Equality is decided by JSON-serialization on `expected` and `actual` so
 * plain-object records compare structurally without a runtime deep-equal
 * dependency. `undefined` and `null` are normalized to the string
 * `"undefined"` / `"null"` so both scenarios print human-readable output.
 */
export function printEqualityCheck(
  io: NarratorSink,
  label: string,
  expected: unknown,
  actual: unknown,
): boolean {
  const equal = valuesEqual(expected, actual);
  const indicator = equal ? "✓" : "✗";
  io.write(
    `  ${label}: expected=${renderValue(expected)} actual=${renderValue(actual)}  ${indicator}\n`,
  );
  return equal;
}

/**
 * Format a 16-byte view as the canonical lowercase UUID string
 * (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`). Inputs whose length is not 16
 * produce a diagnostic string rather than a throw — narration must be
 * total.
 */
export function formatUuid(bytes: Uint8Array): string {
  if (bytes.length !== UUID_BYTE_LENGTH) {
    return `(invalid length ${bytes.length})`;
  }
  const hex = toHexString(bytes, "");
  return (
    hex.slice(0, 8) +
    "-" +
    hex.slice(8, 12) +
    "-" +
    hex.slice(12, 16) +
    "-" +
    hex.slice(16, 20) +
    "-" +
    hex.slice(20, 32)
  );
}

/**
 * Map a compression byte to its human-readable name. `0x00` → `NONE`,
 * `0x05` → `ZLIB`, everything else → `unknown(0xNN)`.
 */
export function compressionName(b: number): string {
  if (b === 0x00) {
    return "NONE";
  }
  if (b === 0x05) {
    return "ZLIB";
  }
  return `unknown(0x${byteHex(b)})`;
}

/**
 * Render a subarray as a hex + ASCII gutter, capped at HEX_ASCII_CAP bytes
 * for readability. Longer inputs are truncated and annotated with the
 * remaining count so hex dumps of large payloads stay one line each.
 */
function formatHexLine(bytes: Uint8Array): string {
  const truncated = bytes.length > HEX_ASCII_CAP;
  const display = truncated ? bytes.subarray(0, HEX_ASCII_CAP) : bytes;
  const hex = toHexString(display, " ").toUpperCase();
  const ascii = toPrintableAscii(display);
  const suffix = truncated ? ` ...+${bytes.length - HEX_ASCII_CAP} bytes` : "";
  return `    ${hex}  |${ascii}|${suffix}`;
}

/**
 * Convert a byte to a zero-padded two-hex-digit lowercase string.
 */
function byteHex(b: number): string {
  return (b & 0xff).toString(16).padStart(2, "0");
}

/**
 * Concatenate bytes as hex with the given separator between each byte.
 */
function toHexString(bytes: Uint8Array, separator: string): string {
  const parts: string[] = [];
  for (let i = 0; i < bytes.length; i += 1) {
    parts.push(byteHex(bytes[i] as number));
  }
  return parts.join(separator);
}

/**
 * Return an ASCII printable representation of `bytes`, replacing every
 * non-printable byte (outside 0x20..0x7E) with `.`.
 */
function toPrintableAscii(bytes: Uint8Array): string {
  const chars: string[] = [];
  for (let i = 0; i < bytes.length; i += 1) {
    const v = bytes[i] as number;
    if (v >= 0x20 && v <= 0x7e) {
      chars.push(String.fromCharCode(v));
    } else {
      chars.push(".");
    }
  }
  return chars.join("");
}

/**
 * Structural equality via JSON canonicalization. Sufficient for the demo's
 * comparison surface (schema records, UUID strings, arrays of primitives)
 * and avoids adding a runtime deep-equal dep.
 */
function valuesEqual(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

/**
 * Render an arbitrary value for the equality-check line. Objects and
 * arrays are JSON-stringified; primitives use `String(...)`; `undefined`
 * and `null` are printed as their literal names.
 */
function renderValue(v: unknown): string {
  if (v === undefined) {
    return "undefined";
  }
  if (v === null) {
    return "null";
  }
  if (typeof v === "string") {
    return JSON.stringify(v);
  }
  if (typeof v === "object") {
    return JSON.stringify(v);
  }
  return String(v);
}

/**
 * Render a record for the `[record] <label>: ...` line. Objects go through
 * `JSON.stringify` so an operator sees the actual field/value shape;
 * primitives fall through to `String(...)` so a stringified scalar is not
 * quoted twice.
 */
function renderRecord(v: unknown): string {
  if (v === undefined) return "undefined";
  if (v === null) return "null";
  if (typeof v === "object") return JSON.stringify(v);
  if (typeof v === "string") return JSON.stringify(v);
  return String(v);
}
