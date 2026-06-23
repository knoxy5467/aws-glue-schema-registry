/*
 * POST /encode handler.
 *
 * Request body (JSON):
 *   {
 *     "format":           "AVRO" | "JSON" | "PROTOBUF",
 *     "schema":           "<schema definition string>",
 *     "schemaName":       "<schema name>",
 *     "schemaVersionId":  "<UUID>",
 *     "payload":          "<base64 of raw format-encoded bytes>",
 *     "compression":      "NONE" | "ZLIB"
 *   }
 *
 * Behavior:
 *   - Decodes `payload` as base64.
 *   - Constructs a real GSR SerializationDataEncoder configured for the
 *     requested compression and writes the 18-byte header + payload using
 *     the schemaVersionId from the request. This exercises the real Java
 *     wire-format layer.
 *   - Records (schemaVersionId -> Schema) in the in-memory store so a
 *     subsequent /decode against the same UUID can return schema metadata.
 *
 * Response body (JSON):
 *   { "bytes": "<base64 of GSR-framed bytes>" }
 *
 * Why the format-encoded payload is provided pre-serialized (not the raw
 * Java object): the sidecar's contract is the wire-format layer, NOT
 * format-specific serialization. The Go side already does the format
 * encoding; the sidecar's job is to prove the framing layer is byte-for-byte
 * identical across languages. Adding a JSON-or-Avro-or-Protobuf record
 * serializer here would force a second round of cross-language object-shape
 * negotiation, which is not what the harness is for.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.SerializationDataEncoder;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class EncodeHandler implements HttpHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final InMemorySchemaStore store;

    public EncodeHandler(InMemorySchemaStore store) {
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
            String format = HttpUtil.requireString(req, "format");
            String schemaDef = HttpUtil.requireString(req, "schema");
            String schemaName = HttpUtil.requireString(req, "schemaName");
            UUID schemaVersionId = UUID.fromString(HttpUtil.requireString(req, "schemaVersionId"));
            byte[] payload = Base64.getDecoder().decode(HttpUtil.requireString(req, "payload"));
            String compression = req.hasNonNull("compression")
                    ? req.get("compression").asText("NONE")
                    : "NONE";

            store.put(schemaVersionId, new Schema(schemaDef, format, schemaName));

            Map<String, Object> configMap = new HashMap<>();
            configMap.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
            configMap.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compression);
            GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(configMap);
            SerializationDataEncoder encoder = new SerializationDataEncoder(config);

            byte[] framed = encoder.write(payload, schemaVersionId);

            ObjectNode resp = MAPPER.createObjectNode();
            resp.put("bytes", Base64.getEncoder().encodeToString(framed));
            HttpUtil.writeJson(exchange, 200, resp);
        } catch (IllegalArgumentException e) {
            HttpUtil.writeError(exchange, 400, e.getMessage());
        } catch (Exception e) {
            HttpUtil.writeError(exchange, 500, "encode failed: " + e.getClass().getSimpleName()
                    + ": " + (e.getMessage() == null ? "" : e.getMessage()));
        }
    }
}
