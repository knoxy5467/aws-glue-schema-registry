/*
 * POST /decode handler.
 *
 * Request body (JSON):
 *   { "bytes": "<base64 of GSR-framed bytes>" }
 *
 * Behavior:
 *   - Uses the real Java GlueSchemaRegistryDeserializerDataParser to
 *     extract the schemaVersionId UUID and the decompressed payload from
 *     the 18-byte-header + body frame.
 *   - Looks the UUID up in the in-memory store to return schema metadata
 *     (so the Go test can assert schemaName + schemaDefinition).
 *   - If the UUID is unknown (decode was never preceded by a matching
 *     encode), schemaName + schemaDefinition come back as null and the
 *     test is expected to handle that — useful for negative cases.
 *
 * Response body (JSON):
 *   {
 *     "payload":          "<base64 of decompressed raw payload bytes>",
 *     "schemaVersionId":  "<UUID>",
 *     "schemaName":       "<name or null>",
 *     "schemaDefinition": "<def or null>",
 *     "dataFormat":       "<format or null>"
 *   }
 */
package com.amazonaws.services.schemaregistry.interop;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

public final class DecodeHandler implements HttpHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final InMemorySchemaStore store;

    public DecodeHandler(InMemorySchemaStore store) {
        this.store = store;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            HttpUtil.writeError(exchange, 405, "method not allowed");
            return;
        }
        try {
            JsonNode req = MAPPER.readTree(exchange.getRequestBody());
            byte[] framed = Base64.getDecoder().decode(HttpUtil.requireString(req, "bytes"));

            GlueSchemaRegistryDeserializerDataParser parser =
                    GlueSchemaRegistryDeserializerDataParser.getInstance();
            ByteBuffer buf = ByteBuffer.wrap(framed);
            UUID schemaVersionId = parser.getSchemaVersionId(buf);
            byte[] plain = parser.getPlainData(ByteBuffer.wrap(framed));

            Schema schema = store.get(schemaVersionId);

            ObjectNode resp = MAPPER.createObjectNode();
            resp.put("payload", Base64.getEncoder().encodeToString(plain));
            resp.put("schemaVersionId", schemaVersionId.toString());
            if (schema != null) {
                resp.put("schemaName", schema.getSchemaName());
                resp.put("schemaDefinition", schema.getSchemaDefinition());
                resp.put("dataFormat", schema.getDataFormat());
            } else {
                resp.putNull("schemaName");
                resp.putNull("schemaDefinition");
                resp.putNull("dataFormat");
            }
            HttpUtil.writeJson(exchange, 200, resp);
        } catch (IllegalArgumentException e) {
            HttpUtil.writeError(exchange, 400, e.getMessage());
        } catch (Exception e) {
            HttpUtil.writeError(exchange, 500, "decode failed: " + e.getClass().getSimpleName()
                    + ": " + (e.getMessage() == null ? "" : e.getMessage()));
        }
    }
}
