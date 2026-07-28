/*
 * Java sidecar entry point for the TypeScript client's cross-language
 * interop tier.
 *
 * Launches a JDK-builtin HttpServer on a port chosen by --port (0 = ephemeral),
 * prints "PORT: <n>" on the first stdout line so the TS launcher can scrape it,
 * and shuts down on SIGTERM.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public final class SidecarMain {

    private SidecarMain() {}

    public static void main(String[] args) throws Exception {
        int port = 0;
        for (String arg : args) {
            if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            }
        }

        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        InMemorySchemaStore store = new InMemorySchemaStore();
        EncodeHandler encodeHandler = new EncodeHandler(store);
        DecodeHandler decodeHandler = new DecodeHandler(store);

        // Wire-format-only endpoints (in-memory schema store, no AWS calls).
        // Kept for fast byte-level parity tests that don't need Kafka.
        server.createContext("/encode", encodeHandler);
        server.createContext("/decode", decodeHandler);
        // Kafka-in-the-loop endpoints (real AWS Glue + real Kafka). The
        // producer-side endpoint registers schemas with the configured
        // account; cleanup is the test harness's responsibility.
        server.createContext("/kafka-produce", new KafkaProduceHandler());
        server.createContext("/kafka-consume", new KafkaConsumeHandler());
        server.createContext("/health", new HealthHandler());

        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();

        // Stdout contract: first non-log line is "PORT: <n>". The launcher
        // reads it before any further output to discover the chosen port.
        System.out.println("PORT: " + server.getAddress().getPort());
        System.out.flush();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));

        // Block forever; shutdown hook handles SIGTERM.
        Thread.currentThread().join();
    }
}
