import { describe, expect, it } from "vitest";
import {
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
} from "./errors.js";

describe("wire/errors", () => {
  describe("GsrIncompatibleDataError", () => {
    const err = new GsrIncompatibleDataError("short data");

    it("extends Error", () => {
      expect(err).toBeInstanceOf(Error);
      expect(err).toBeInstanceOf(GsrIncompatibleDataError);
    });

    it("carries a distinct name", () => {
      expect(err.name).toBe("GsrIncompatibleDataError");
    });

    it("carries the caller-supplied message", () => {
      expect(err.message).toBe("short data");
    });

    it("captures a stack trace", () => {
      expect(typeof err.stack).toBe("string");
    });
  });

  describe("GsrMessageTypeNotFoundError", () => {
    const err = new GsrMessageTypeNotFoundError("missing.Message");

    it("extends Error", () => {
      expect(err).toBeInstanceOf(Error);
      expect(err).toBeInstanceOf(GsrMessageTypeNotFoundError);
    });

    it("carries a distinct name", () => {
      expect(err.name).toBe("GsrMessageTypeNotFoundError");
    });

    it("carries the caller-supplied message", () => {
      expect(err.message).toBe("missing.Message");
    });
  });

  it("keeps the two error classes distinct — cross-instanceof is false", () => {
    const incompatible = new GsrIncompatibleDataError("a");
    const notFound = new GsrMessageTypeNotFoundError("b");
    expect(incompatible).not.toBeInstanceOf(GsrMessageTypeNotFoundError);
    expect(notFound).not.toBeInstanceOf(GsrIncompatibleDataError);
    expect(incompatible.name).not.toBe(notFound.name);
  });
});
