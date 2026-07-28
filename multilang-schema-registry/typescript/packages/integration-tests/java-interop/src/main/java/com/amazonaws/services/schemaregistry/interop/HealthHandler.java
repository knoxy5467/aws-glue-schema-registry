/*
 * GET /health — flat 200 OK with the literal body "ok".
 *
 * The TS launcher polls this after parsing PORT: <n> from stdout so the
 * test runner only proceeds once the JVM is actually ready to serve.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public final class HealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }
}
