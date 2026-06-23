/*
 * Tiny request/response helpers for the JDK HttpServer.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

final class HttpUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpUtil() {}

    static String requireString(JsonNode req, String field) {
        if (req == null || !req.hasNonNull(field)) {
            throw new IllegalArgumentException("missing required field: " + field);
        }
        return req.get(field).asText();
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
