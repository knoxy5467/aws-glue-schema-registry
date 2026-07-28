import { describe, expect, it } from "vitest";

import {
  DefaultSchemaNameStrategy,
  RecordNameStrategy,
  type SchemaNameStrategy,
} from "./schema-name-strategy.js";

describe("serde/facade/schema-name-strategy", () => {
  describe("DefaultSchemaNameStrategy", () => {
    const strategy = new DefaultSchemaNameStrategy();

    it("returns the topic verbatim regardless of the record", () => {
      expect(strategy.getSchemaName({ any: "record" }, "orders")).toBe(
        "orders",
      );
      expect(strategy.getSchemaName(null, "orders")).toBe("orders");
      expect(strategy.getSchemaName(undefined, "orders")).toBe("orders");
      expect(strategy.getSchemaName("plain-string", "orders")).toBe("orders");
    });

    it("preserves an empty topic (caller is trusted to pass a valid topic)", () => {
      expect(strategy.getSchemaName({ any: "record" }, "")).toBe("");
    });

    it("conforms to the SchemaNameStrategy interface", () => {
      const asInterface: SchemaNameStrategy = strategy;
      expect(asInterface.getSchemaName({}, "topic-a")).toBe("topic-a");
    });

    it("performs no I/O — repeated calls are pure and deterministic", () => {
      const first = strategy.getSchemaName({ n: 1 }, "orders");
      const second = strategy.getSchemaName({ n: 2 }, "orders");
      expect(first).toBe("orders");
      expect(second).toBe("orders");
    });
  });

  describe("RecordNameStrategy", () => {
    const strategy = new RecordNameStrategy();

    describe("Protobuf records", () => {
      it("derives the name from a protobufjs $type.fullName", () => {
        const message = {
          value: "hello",
          $type: { fullName: ".google.protobuf.StringValue" },
        };
        expect(strategy.getSchemaName(message, "orders")).toBe(
          "orders-google.protobuf.StringValue",
        );
      });

      it("accepts a fullName that does not start with a dot", () => {
        const message = {
          value: 42,
          $type: { fullName: "example.Order" },
        };
        expect(strategy.getSchemaName(message, "orders")).toBe(
          "orders-example.Order",
        );
      });

      it("falls back to later channels when $type is missing fullName", () => {
        const message = { $type: {}, $id: "https://example/id" };
        expect(strategy.getSchemaName(message, "orders")).toBe(
          "orders-https://example/id",
        );
      });

      it("ignores a non-object $type", () => {
        const message = { $type: "not-a-type", title: "Fallback" };
        expect(strategy.getSchemaName(message, "orders")).toBe(
          "orders-Fallback",
        );
      });
    });

    describe("JSON-Schema records", () => {
      it("prefers $id over title", () => {
        const doc = {
          $id: "https://schemas.example.com/product.json",
          title: "Product",
        };
        expect(strategy.getSchemaName(doc, "products")).toBe(
          "products-https://schemas.example.com/product.json",
        );
      });

      it("uses title when $id is absent", () => {
        expect(strategy.getSchemaName({ title: "Product" }, "products")).toBe(
          "products-Product",
        );
      });

      it("skips empty $id and empty title", () => {
        expect(strategy.getSchemaName({ $id: "", title: "" }, "topic")).toBe(
          "topic",
        );
      });

      it("ignores non-string $id/title", () => {
        expect(
          strategy.getSchemaName({ $id: 123, title: null }, "topic"),
        ).toBe("topic");
      });
    });

    describe("Avro specific records", () => {
      it("reads getSchema().fullName when available", () => {
        const record = {
          data: "hello",
          count: 7,
          getSchema() {
            return { fullName: "com.example.TestV1", name: "TestV1" };
          },
        };
        expect(strategy.getSchemaName(record, "orders")).toBe(
          "orders-com.example.TestV1",
        );
      });

      it("falls back to getSchema().name when fullName is absent", () => {
        const record = {
          getSchema() {
            return { name: "TestV1" };
          },
        };
        expect(strategy.getSchemaName(record, "orders")).toBe(
          "orders-TestV1",
        );
      });

      it("returns the topic when getSchema throws", () => {
        const record = {
          getSchema() {
            throw new Error("boom");
          },
        };
        expect(strategy.getSchemaName(record, "orders")).toBe("orders");
      });

      it("returns the topic when getSchema yields no name", () => {
        const record = {
          getSchema() {
            return {};
          },
        };
        expect(strategy.getSchemaName(record, "orders")).toBe("orders");
      });
    });

    describe("Constructor-name fallback", () => {
      it("uses a class-name constructor when no format identifier is present", () => {
        class OrderPojo {
          id = 1;
        }
        expect(strategy.getSchemaName(new OrderPojo(), "orders")).toBe(
          "orders-OrderPojo",
        );
      });

      it("ignores plain Object literal constructors", () => {
        expect(strategy.getSchemaName({ id: 1 }, "orders")).toBe("orders");
      });

      it("ignores plain Array constructors", () => {
        expect(strategy.getSchemaName([1, 2, 3], "orders")).toBe("orders");
      });
    });

    describe("Fallback and edge cases", () => {
      it("returns the topic for null and undefined", () => {
        expect(strategy.getSchemaName(null, "orders")).toBe("orders");
        expect(strategy.getSchemaName(undefined, "orders")).toBe("orders");
      });

      it("returns the topic for primitives", () => {
        expect(strategy.getSchemaName("string-payload", "orders")).toBe(
          "orders",
        );
        expect(strategy.getSchemaName(42, "orders")).toBe("orders");
        expect(strategy.getSchemaName(true, "orders")).toBe("orders");
      });

      it("prefers protobuf > json-schema > avro > constructor", () => {
        class Ambiguous {
          $type = { fullName: "example.Winner" };
          $id = "https://example/loser";
          getSchema() {
            return { fullName: "com.example.AlsoLoser" };
          }
        }
        expect(strategy.getSchemaName(new Ambiguous(), "topic")).toBe(
          "topic-example.Winner",
        );
      });

      it("conforms to the SchemaNameStrategy interface", () => {
        const asInterface: SchemaNameStrategy = strategy;
        expect(
          asInterface.getSchemaName({ title: "Widget" }, "products"),
        ).toBe("products-Widget");
      });

      it("performs no I/O — repeated calls are pure and deterministic", () => {
        const record = { $type: { fullName: "example.Order" } };
        const first = strategy.getSchemaName(record, "orders");
        const second = strategy.getSchemaName(record, "orders");
        expect(first).toBe("orders-example.Order");
        expect(second).toBe("orders-example.Order");
      });
    });
  });
});
