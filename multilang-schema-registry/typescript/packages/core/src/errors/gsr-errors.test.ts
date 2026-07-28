import { describe, expect, it } from "vitest";

import {
  GsrAutoRegistrationDisabledError,
  GsrError,
  GsrInvalidAvroRecordTypeError,
  GsrInvalidCacheSizeError,
  GsrInvalidCacheTtlError,
  GsrInvalidCompatibilityError,
  GsrInvalidCompressionTypeError,
  GsrInvalidProtobufMessageTypeError,
  GsrRegistrationError,
  classifyGlueError,
} from "./gsr-errors.js";

/**
 * The Glue-integration error taxonomy is small and declarative — these
 * tests pin the umbrella hierarchy, each subclass's `.name`, the `.cause`
 * plumbing on `GsrRegistrationError`, and the `classifyGlueError`
 * discriminant that the registrar branches on.
 */
describe("errors/gsr-errors", () => {
  describe("GsrError umbrella", () => {
    it("is a subclass of the platform Error", () => {
      const err = new GsrError("boom");
      expect(err).toBeInstanceOf(Error);
      expect(err.message).toBe("boom");
      expect(err.name).toBe("GsrError");
    });

    it("accepts a cause on the standard options bag", () => {
      const inner = new Error("inner");
      const err = new GsrError("outer", { cause: inner });
      expect(err.cause).toBe(inner);
    });
  });

  describe("umbrella hierarchy", () => {
    const specimens: ReadonlyArray<
      readonly [string, new (msg: string) => GsrError]
    > = [
      ["GsrAutoRegistrationDisabledError", GsrAutoRegistrationDisabledError],
      ["GsrInvalidCompressionTypeError", GsrInvalidCompressionTypeError],
      ["GsrInvalidCompatibilityError", GsrInvalidCompatibilityError],
      ["GsrInvalidCacheTtlError", GsrInvalidCacheTtlError],
      ["GsrInvalidCacheSizeError", GsrInvalidCacheSizeError],
      ["GsrInvalidAvroRecordTypeError", GsrInvalidAvroRecordTypeError],
      ["GsrInvalidProtobufMessageTypeError", GsrInvalidProtobufMessageTypeError],
      ["GsrRegistrationError", GsrRegistrationError],
    ];

    it.each(specimens)("%s extends GsrError and sets its own name", (name, Cls) => {
      const err = new Cls(`${name} test`);
      expect(err).toBeInstanceOf(GsrError);
      expect(err).toBeInstanceOf(Error);
      expect(err.name).toBe(name);
      expect(err.message).toBe(`${name} test`);
    });

    it("each subclass is distinguishable via instanceof", () => {
      const auto = new GsrAutoRegistrationDisabledError("x");
      const compat = new GsrInvalidCompatibilityError("x");
      expect(auto).toBeInstanceOf(GsrAutoRegistrationDisabledError);
      expect(auto).not.toBeInstanceOf(GsrInvalidCompatibilityError);
      expect(compat).toBeInstanceOf(GsrInvalidCompatibilityError);
      expect(compat).not.toBeInstanceOf(GsrAutoRegistrationDisabledError);
    });
  });

  describe("GsrRegistrationError.cause plumbing", () => {
    it("preserves an Error cause verbatim", () => {
      const inner = new Error("glue exploded");
      const err = new GsrRegistrationError("wrapping", { cause: inner });
      expect(err.cause).toBe(inner);
      expect(err).toBeInstanceOf(GsrError);
    });

    it("preserves a non-Error cause verbatim", () => {
      const inner = { code: "Throttling" };
      const err = new GsrRegistrationError("wrapping", { cause: inner });
      expect(err.cause).toBe(inner);
    });

    it("omits cause when the options bag is absent", () => {
      const err = new GsrRegistrationError("no wrap");
      expect(err.cause).toBeUndefined();
    });
  });

  describe("classifyGlueError", () => {
    it("returns 'entity-not-found' for the EntityNotFoundException shape", () => {
      const err = Object.assign(new Error("no such schema"), {
        name: "EntityNotFoundException",
      });
      expect(classifyGlueError(err)).toBe("entity-not-found");
    });

    it("returns 'already-exists' for the AlreadyExistsException shape", () => {
      const err = Object.assign(new Error("race"), {
        name: "AlreadyExistsException",
      });
      expect(classifyGlueError(err)).toBe("already-exists");
    });

    it("returns 'other' for a Glue error whose name is neither of the two", () => {
      const err = Object.assign(new Error("denied"), {
        name: "AccessDeniedException",
      });
      expect(classifyGlueError(err)).toBe("other");
    });

    it("returns 'other' for a plain Error without a Glue-typed name", () => {
      expect(classifyGlueError(new Error("network"))).toBe("other");
    });

    it("returns 'other' for null, undefined, and primitives", () => {
      expect(classifyGlueError(null)).toBe("other");
      expect(classifyGlueError(undefined)).toBe("other");
      expect(classifyGlueError("EntityNotFoundException")).toBe("other");
      expect(classifyGlueError(42)).toBe("other");
    });

    it("also matches a plain object carrying the Glue name (SDK metadata-only shape)", () => {
      expect(classifyGlueError({ name: "EntityNotFoundException" })).toBe(
        "entity-not-found",
      );
      expect(classifyGlueError({ name: "AlreadyExistsException" })).toBe(
        "already-exists",
      );
    });
  });
});
