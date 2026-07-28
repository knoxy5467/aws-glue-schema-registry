/*
 * Tiny request/response helpers for the JDK HttpServer.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

final class HttpUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpUtil() {}

    static String requireString(JsonNode req, String field) {
        if (req == null || !req.hasNonNull(field)) {
            throw new IllegalArgumentException("missing required field: " + field);
        }
        JsonNode node = req.get(field);
        if (!node.isTextual()) {
            throw new IllegalArgumentException(
                    "field " + field + " must be a JSON string (got " + node.getNodeType() + ")");
        }
        return node.asText();
    }

    /**
     * Returns the textual value of an optional string field, or the supplied
     * default when the field is absent / explicitly null. Fails when the
     * field is present but is not a JSON string — this surfaces caller typos
     * (e.g. region: 5 instead of region: "us-east-1") as 400 errors rather
     * than silently coercing to a useless default.
     */
    static String optionalString(JsonNode req, String field, String defaultValue) {
        if (req == null || !req.hasNonNull(field)) {
            return defaultValue;
        }
        JsonNode node = req.get(field);
        if (!node.isTextual()) {
            throw new IllegalArgumentException(
                    "field " + field + " must be a JSON string (got " + node.getNodeType() + ")");
        }
        return node.asText();
    }

    /**
     * Returns the integer value of an optional int field, or the supplied
     * default when the field is absent / explicitly null. Fails when the
     * field is present but is not a JSON integer.
     */
    static int optionalInt(JsonNode req, String field, int defaultValue) {
        if (req == null || !req.hasNonNull(field)) {
            return defaultValue;
        }
        JsonNode node = req.get(field);
        if (!node.isIntegralNumber()) {
            throw new IllegalArgumentException(
                    "field " + field + " must be a JSON integer (got " + node.getNodeType() + ")");
        }
        return node.asInt();
    }

    static void writeJson(HttpExchange exchange, int status, ObjectNode body) throws IOException {
        byte[] payload = MAPPER.writeValueAsBytes(body);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, payload.length);
        exchange.getResponseBody().write(payload);
        exchange.close();
    }

    static void writeError(HttpExchange exchange, int status, String message) throws IOException {
        ObjectNode err = MAPPER.createObjectNode();
        err.put("error", message);
        writeJson(exchange, status, err);
    }
}
