/*
 * In-memory schemaVersionId -> Schema map.
 *
 * The wire-format endpoints never call AWS Glue. Tests pass the
 * schemaVersionId UUID in the encode request; that UUID is what ends up
 * in the GSR header. Decode lookups go against this store using the UUID
 * parsed out of the header.
 *
 * Concurrent get/put because the JDK HttpServer is multithreaded.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.amazonaws.services.schemaregistry.common.Schema;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemorySchemaStore {
    private final ConcurrentHashMap<UUID, Schema> byVersion = new ConcurrentHashMap<>();

    public void put(UUID schemaVersionId, Schema schema) {
        byVersion.put(schemaVersionId, schema);
    }

    public Schema get(UUID schemaVersionId) {
        return byVersion.get(schemaVersionId);
    }
}
