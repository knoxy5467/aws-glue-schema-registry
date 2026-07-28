import { describe, expect, it } from "vitest";

import {
  CompatibilityMode,
  DEFAULT_COMPATIBILITY_MODE,
  GsrInvalidCompatibilityModeError,
  isCompatibilityMode,
  parseCompatibilityMode,
} from "./compatibility.js";

describe("evolution/compatibility", () => {
  describe("CompatibilityMode enum", () => {
    it("enumerates all eight case-exact Glue modes", () => {
      // Sorted for a stable comparison against the Java canonical set and
      // the Go reference `validCompatibilities`.
      expect(Object.values(CompatibilityMode).sort()).toEqual([
        "BACKWARD",
        "BACKWARD_ALL",
        "DISABLED",
        "FORWARD",
        "FORWARD_ALL",
        "FULL",
        "FULL_ALL",
        "NONE",
      ]);
    });

    it("uses identical key and value for each member (wire-carried form)", () => {
      for (const [key, value] of Object.entries(CompatibilityMode)) {
        expect(value).toBe(key);
      }
    });
  });

  describe("DEFAULT_COMPATIBILITY_MODE", () => {
    it("is BACKWARD (Java canonical / Go reference default)", () => {
      expect(DEFAULT_COMPATIBILITY_MODE).toBe(CompatibilityMode.BACKWARD);
      expect(DEFAULT_COMPATIBILITY_MODE).toBe("BACKWARD");
    });
  });

  describe("isCompatibilityMode", () => {
    it.each([
      "NONE",
      "DISABLED",
      "BACKWARD",
      "BACKWARD_ALL",
      "FORWARD",
      "FORWARD_ALL",
      "FULL",
      "FULL_ALL",
    ])("returns true for %s (case-exact)", (mode) => {
      expect(isCompatibilityMode(mode)).toBe(true);
    });

    it.each([
      "backward",
      "Backward",
      "BackWard",
      "backward_all",
      "FORWARDS",
      "FULL_",
      " BACKWARD",
      "BACKWARD ",
      "",
      "NON",
      "UNKNOWN",
    ])("returns false for %j (rejected)", (mode) => {
      expect(isCompatibilityMode(mode)).toBe(false);
    });
  });

  describe("parseCompatibilityMode", () => {
    it.each([
      ["NONE", CompatibilityMode.NONE],
      ["DISABLED", CompatibilityMode.DISABLED],
      ["BACKWARD", CompatibilityMode.BACKWARD],
      ["BACKWARD_ALL", CompatibilityMode.BACKWARD_ALL],
      ["FORWARD", CompatibilityMode.FORWARD],
      ["FORWARD_ALL", CompatibilityMode.FORWARD_ALL],
      ["FULL", CompatibilityMode.FULL],
      ["FULL_ALL", CompatibilityMode.FULL_ALL],
    ])("returns the enum member for %s", (input, expected) => {
      expect(parseCompatibilityMode(input)).toBe(expected);
    });

    it.each(["backward", "FORWARDS", ""])(
      "throws GsrInvalidCompatibilityModeError for %j",
      (input) => {
        expect(() => parseCompatibilityMode(input)).toThrow(
          GsrInvalidCompatibilityModeError,
        );
      },
    );

    it("names the rejected value in the thrown message", () => {
      let thrown: unknown;
      try {
        parseCompatibilityMode("backward");
      } catch (err) {
        thrown = err;
      }
      expect(thrown).toBeInstanceOf(GsrInvalidCompatibilityModeError);
      const message = (thrown as Error).message;
      expect(message).toContain("backward");
      expect(message.toLowerCase()).toContain("compatibility");
    });
  });

  describe("GsrInvalidCompatibilityModeError", () => {
    const err = new GsrInvalidCompatibilityModeError("invalid: \"nope\"");

    it("extends Error", () => {
      expect(err).toBeInstanceOf(Error);
      expect(err).toBeInstanceOf(GsrInvalidCompatibilityModeError);
    });

    it("carries a distinct name", () => {
      expect(err.name).toBe("GsrInvalidCompatibilityModeError");
    });

    it("carries the caller-supplied message", () => {
      expect(err.message).toBe("invalid: \"nope\"");
    });

    it("captures a stack trace", () => {
      expect(typeof err.stack).toBe("string");
    });
  });
});
