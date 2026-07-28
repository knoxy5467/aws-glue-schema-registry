/**
 * Tier-1 offline unit coverage for the narrator formatting helpers.
 *
 * Every case drives the helpers against an in-memory string sink so the run
 * touches no network, no JVM, no Kafka, no Glue. The captured text is
 * asserted directly, so the hex-dump segmentation contract and the
 * equality-check return value are checked without any process-level I/O.
 */

import { describe, expect, it } from "vitest";

import {
  compressionName,
  formatUuid,
  printBanner,
  printEqualityCheck,
  printHexDump,
  printRecord,
  printSchemaBody,
  printScenarioIntro,
  printSectionHeader,
  printStage,
  printVerdict,
  type NarratorSink,
} from "./narrator.js";

/**
 * In-memory string sink: appends every `write` to an internal buffer and
 * exposes the concatenated result via `toString()`. Used everywhere in
 * this suite so no case writes to real stdout.
 */
class BufferSink implements NarratorSink {
  private readonly parts: string[] = [];

  write(s: string): void {
    this.parts.push(s);
  }

  toString(): string {
    return this.parts.join("");
  }
}

/**
 * Build a well-formed 20-byte wire message: 0x03 header, given compression
 * byte, a fixed 16-byte UUID, and a two-byte payload. Used by hex-dump
 * cases so every case starts from a known-good frame and mutates one
 * dimension at a time.
 */
function makeWire(compressionByte: number): Uint8Array {
  const buf = new Uint8Array(20);
  buf[0] = 0x03;
  buf[1] = compressionByte;
  // UUID bytes 01..10 → "01020304-0506-0708-090a-0b0c0d0e0f10".
  for (let i = 0; i < 16; i += 1) {
    buf[2 + i] = i + 1;
  }
  buf[18] = 0x41; // 'A'
  buf[19] = 0x42; // 'B'
  return buf;
}

describe("printBanner", () => {
  it("prints a labelled banner block with all three fields", () => {
    const sink = new BufferSink();
    printBanner(sink, {
      accountId: "123456789012",
      region: "us-east-2",
      registry: "default-registry",
    });
    const out = sink.toString();
    expect(out).toContain("Account:  123456789012");
    expect(out).toContain("Region:   us-east-2");
    expect(out).toContain("Registry: default-registry");
    // Two heavy rules bracketing the field block.
    expect(out.match(/={80}/g)?.length ?? 0).toBe(2);
  });
});

describe("printSectionHeader", () => {
  it("wraps the title between two light rules of separator width", () => {
    const sink = new BufferSink();
    printSectionHeader(sink, "scenario 1 of 21 — AVRO Java→TS NONE");
    const out = sink.toString();
    expect(out).toContain("scenario 1 of 21 — AVRO Java→TS NONE");
    expect(out.match(/─{80}/g)?.length ?? 0).toBe(2);
  });
});

describe("printStage", () => {
  it("prints a bracketed tag followed by the message", () => {
    const sink = new BufferSink();
    printStage(sink, "glue", "created schema-version-id abcdef");
    expect(sink.toString()).toBe("[glue] created schema-version-id abcdef\n");
  });
});

describe("printHexDump", () => {
  it("segments a NONE-compressed message into four wire-format lines", () => {
    const sink = new BufferSink();
    printHexDump(sink, "produced wire", makeWire(0x00));
    const out = sink.toString();
    expect(out).toContain("produced wire (20 bytes total)");
    // One [wire-format] line per segment: header, compression, UUID, payload.
    const wireLines = out
      .split("\n")
      .filter((line) => line.startsWith("[wire-format]"));
    expect(wireLines).toHaveLength(4);
    expect(wireLines[0]).toContain("Header byte: 0x03");
    expect(wireLines[1]).toContain("Compression byte: 0x00 (NONE)");
    expect(wireLines[2]).toContain(
      "Schema version UUID: 01020304-0506-0708-090a-0b0c0d0e0f10",
    );
    expect(wireLines[3]).toContain("Payload body: 2 bytes");
    // Payload gutter should show the ASCII characters for 'A' and 'B'.
    expect(wireLines[3]).toContain("|AB|");
  });

  it("labels a ZLIB compression byte as ZLIB and preserves segmentation", () => {
    const sink = new BufferSink();
    printHexDump(sink, "produced wire", makeWire(0x05));
    const wireLines = sink
      .toString()
      .split("\n")
      .filter((line) => line.startsWith("[wire-format]"));
    expect(wireLines).toHaveLength(4);
    expect(wireLines[1]).toContain("Compression byte: 0x05 (ZLIB)");
  });

  it("labels an unknown compression byte with its hex value", () => {
    const sink = new BufferSink();
    printHexDump(sink, "produced wire", makeWire(0x42));
    expect(sink.toString()).toContain("Compression byte: 0x42 (unknown(0x42))");
  });

  it("prints '(empty)' for a zero-length wire message", () => {
    const sink = new BufferSink();
    printHexDump(sink, "produced wire", new Uint8Array(0));
    expect(sink.toString()).toContain("(empty)");
  });

  it("emits a truncation diagnostic when fewer than 18 bytes are present", () => {
    const sink = new BufferSink();
    const short = new Uint8Array([0x03, 0x00, 0xaa, 0xbb]);
    printHexDump(sink, "short wire", short);
    const out = sink.toString();
    expect(out).toContain("Header byte: 0x03");
    expect(out).toContain("Compression byte: 0x00");
    expect(out).toContain("(truncated — expected ≥18 bytes for schema-version UUID)");
    expect(out).not.toContain("Schema version UUID:");
  });

  it("prints 'Payload body: (empty)' when the wire is exactly 18 bytes", () => {
    const sink = new BufferSink();
    const header = makeWire(0x00).subarray(0, 18);
    printHexDump(sink, "header only", header);
    expect(sink.toString()).toContain("Payload body: (empty)");
  });

  it("truncates payload gutters longer than 32 bytes and annotates the remainder", () => {
    const sink = new BufferSink();
    // 18-byte header + 40-byte payload → payload gutter capped at 32, +8 shown.
    const wire = new Uint8Array(18 + 40);
    wire[0] = 0x03;
    wire[1] = 0x00;
    for (let i = 0; i < 16; i += 1) {
      wire[2 + i] = i + 1;
    }
    for (let i = 0; i < 40; i += 1) {
      wire[18 + i] = 0x20 + (i % 0x40);
    }
    printHexDump(sink, "wide wire", wire);
    expect(sink.toString()).toContain("...+8 bytes");
  });
});

describe("printEqualityCheck", () => {
  it("returns true and prints a check indicator for matching values", () => {
    const sink = new BufferSink();
    const equal = printEqualityCheck(sink, "fields", { name: "acme" }, {
      name: "acme",
    });
    expect(equal).toBe(true);
    expect(sink.toString()).toContain("✓");
    expect(sink.toString()).toContain('expected={"name":"acme"}');
    expect(sink.toString()).toContain('actual={"name":"acme"}');
  });

  it("returns false and prints a cross indicator for mismatching values", () => {
    const sink = new BufferSink();
    const equal = printEqualityCheck(sink, "fields", { name: "acme" }, {
      name: "widgetco",
    });
    expect(equal).toBe(false);
    expect(sink.toString()).toContain("✗");
  });

  it("returns exactly the boolean it prints (identity-of-signal contract)", () => {
    const sink = new BufferSink();
    // Two calls, one true, one false — the returned booleans must match the
    // indicator in the emitted line, since scenario PASS/FAIL derives from it.
    const eqTrue = printEqualityCheck(sink, "a", 1, 1);
    const eqFalse = printEqualityCheck(sink, "b", 1, 2);
    const lines = sink.toString().trim().split("\n");
    expect(lines).toHaveLength(2);
    expect(eqTrue).toBe(true);
    expect(lines[0]?.endsWith("✓")).toBe(true);
    expect(eqFalse).toBe(false);
    expect(lines[1]?.endsWith("✗")).toBe(true);
  });

  it("handles undefined and null explicitly in the rendered line", () => {
    const sink = new BufferSink();
    const equal = printEqualityCheck(sink, "nullish", undefined, null);
    // JSON.stringify(undefined) === undefined; JSON.stringify(null) === "null".
    // They are not JSON-equal → false, and both literals render in the line.
    expect(equal).toBe(false);
    const out = sink.toString();
    expect(out).toContain("expected=undefined");
    expect(out).toContain("actual=null");
  });
});

describe("formatUuid", () => {
  it("formats 16 bytes as a canonical lowercase UUID string", () => {
    const bytes = new Uint8Array([
      0x01, 0x02, 0x03, 0x04,
      0x05, 0x06,
      0x07, 0x08,
      0x09, 0x0a,
      0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    ]);
    expect(formatUuid(bytes)).toBe("01020304-0506-0708-090a-0b0c0d0e0f10");
  });

  it("returns a diagnostic string for a wrong-length input", () => {
    expect(formatUuid(new Uint8Array([0x01, 0x02, 0x03]))).toBe(
      "(invalid length 3)",
    );
    expect(formatUuid(new Uint8Array(0))).toBe("(invalid length 0)");
  });
});

describe("compressionName", () => {
  it("maps 0x00 to NONE", () => {
    expect(compressionName(0x00)).toBe("NONE");
  });

  it("maps 0x05 to ZLIB", () => {
    expect(compressionName(0x05)).toBe("ZLIB");
  });

  it("renders any other byte as unknown(0xNN) with two-digit lowercase hex", () => {
    expect(compressionName(0x01)).toBe("unknown(0x01)");
    expect(compressionName(0xff)).toBe("unknown(0xff)");
    expect(compressionName(0x2a)).toBe("unknown(0x2a)");
  });
});

describe("printScenarioIntro", () => {
  it("prints two [scenario] tag lines (What + Proves) and a trailing blank line", () => {
    const sink = new BufferSink();
    printScenarioIntro(sink, {
      what: "encode and round-trip",
      proves: "wire-format parity",
    });
    const out = sink.toString();
    const lines = out.split("\n");
    // Expected shape: "[scenario] What: ...", "[scenario] Proves: ...", ""
    expect(lines[0]).toBe("[scenario] What:   encode and round-trip");
    expect(lines[1]).toBe("[scenario] Proves: wire-format parity");
    // Trailing blank line = empty string element after the final \n.
    expect(lines[2]).toBe("");
  });
});

describe("printSchemaBody", () => {
  it("prints a [schema] header then each body line indented four spaces", () => {
    const sink = new BufferSink();
    printSchemaBody(sink, "v1 body", '{\n  "type": "record"\n}');
    const out = sink.toString();
    expect(out).toContain("[schema] v1 body\n");
    expect(out).toContain("    {\n");
    expect(out).toContain('    "type": "record"');
    expect(out).toContain("    }\n");
    // A trailing blank line separates the block from the next stage line.
    expect(out.endsWith("\n\n")).toBe(true);
  });

  it("handles a single-line schema body (no embedded newline)", () => {
    const sink = new BufferSink();
    printSchemaBody(sink, "compact", 'syntax="proto3";');
    const out = sink.toString();
    expect(out).toContain("[schema] compact\n");
    expect(out).toContain('    syntax="proto3";\n');
  });
});

describe("printRecord", () => {
  it("renders an object record as a [record] line with JSON-stringified value", () => {
    const sink = new BufferSink();
    printRecord(sink, "encoded", { id: "42", name: "acme", age: 7 });
    expect(sink.toString()).toBe(
      '[record] encoded: {"id":"42","name":"acme","age":7}\n',
    );
  });

  it("renders undefined and null as their literal tokens", () => {
    const sink = new BufferSink();
    printRecord(sink, "u", undefined);
    printRecord(sink, "n", null);
    const out = sink.toString();
    expect(out).toContain("[record] u: undefined");
    expect(out).toContain("[record] n: null");
  });
});

describe("printVerdict", () => {
  it("prints [verdict] PASS — <detail> and a trailing blank line for pass", () => {
    const sink = new BufferSink();
    printVerdict(sink, true, "round-trip succeeded");
    const out = sink.toString();
    expect(out).toBe("[verdict] PASS — round-trip succeeded\n\n");
  });

  it("prints [verdict] FAIL — <detail> and a trailing blank line for fail", () => {
    const sink = new BufferSink();
    printVerdict(sink, false, "record mismatch");
    const out = sink.toString();
    expect(out).toBe("[verdict] FAIL — record mismatch\n\n");
  });
});
