/*
 * POST /kafka-consume — Java consumer in a cross-language interop test.
 *
 * Request body (JSON):
 *   {
 *     "bootstrap":  "<kafka bootstrap servers>",
 *     "topic":      "<kafka topic name>",
 *     "groupId":    "<consumer group id>",      // optional; defaults to random
 *     "region":     "<aws region>",             // optional
 *     "timeoutMs":  <int>                       // optional poll budget, default 30000
 *   }
 *
 * What the handler does:
 *   1. Polls Kafka for ONE record (or times out).
 *   2. Parses the GSR header via the real Java deserializer-data-parser:
 *      extracts schemaVersionId UUID and the decompressed payload bytes.
 *   3. Calls AWSSchemaRegistryClient.getSchemaVersionResponse(UUID) against
 *      real AWS Glue to resolve the schema — this is the real-interop
 *      signal that the producer (other language) registered the schema
 *      we're now looking up.
 *
 * Response body (JSON):
 *   {
 *     "payload":          "<base64 of decompressed payload>",
 *     "schemaVersionId":  "<UUID>",
 *     "schemaDefinition": "<string>",
 *     "dataFormat":       "AVRO" | "JSON" | "PROTOBUF",
 *     "schemaArn":        "<arn>"
 *   }
 *
 * On timeout returns 404 with { "error": "no record within <ms>ms" }.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public final class KafkaConsumeHandler implements HttpHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_TIMEOUT_MS = 30_000;

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            HttpUtil.writeError(exchange, 405, "method not allowed");
            return;
        }
        try {
            JsonNode req = MAPPER.readTree(exchange.getRequestBody());
            String bootstrap = HttpUtil.requireString(req, "bootstrap");
            String topic = HttpUtil.requireString(req, "topic");
            String groupId = req.hasNonNull("groupId")
                    ? req.get("groupId").asText()
                    : "gsr-interop-" + UUID.randomUUID();
            String region = req.hasNonNull("region") ? req.get("region").asText(null) : null;
            int timeoutMs = req.hasNonNull("timeoutMs") ? req.get("timeoutMs").asInt(DEFAULT_TIMEOUT_MS) : DEFAULT_TIMEOUT_MS;

            // 1. Poll Kafka.
            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrap);
            props.put("group.id", groupId);
            props.put("key.deserializer", ByteArrayDeserializer.class.getName());
            props.put("value.deserializer", ByteArrayDeserializer.class.getName());
            props.put("auto.offset.reset", "earliest");
            props.put("enable.auto.commit", "false");

            byte[] framed;
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(topic));
                long deadline = System.currentTimeMillis() + timeoutMs;
                framed = null;
                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                    if (!records.isEmpty()) {
                        ConsumerRecord<byte[], byte[]> rec = records.iterator().next();
                        framed = rec.value();
                        break;
                    }
                }
            }
            if (framed == null) {
                HttpUtil.writeError(exchange, 404, "no record within " + timeoutMs + "ms");
                return;
            }

            // 2. Parse the GSR header + decompress.
            GlueSchemaRegistryDeserializerDataParser parser = GlueSchemaRegistryDeserializerDataParser.getInstance();
            UUID schemaVersionId = parser.getSchemaVersionId(ByteBuffer.wrap(framed));
            byte[] plain = parser.getPlainData(ByteBuffer.wrap(framed));

            // 3. Resolve the schema via real Glue — the cross-language signal.
            AWSSchemaRegistryClient client = RealGlueClient.get(region);
            GetSchemaVersionResponse schemaResp = client.getSchemaVersionResponse(schemaVersionId.toString());

            ObjectNode resp = MAPPER.createObjectNode();
            resp.put("payload", Base64.getEncoder().encodeToString(plain));
            resp.put("schemaVersionId", schemaVersionId.toString());
            resp.put("schemaDefinition", schemaResp.schemaDefinition());
            resp.put("dataFormat", schemaResp.dataFormat().toString());
            resp.put("schemaArn", schemaResp.schemaArn());
            HttpUtil.writeJson(exchange, 200, resp);
        } catch (IllegalArgumentException e) {
            HttpUtil.writeError(exchange, 400, e.getMessage());
        } catch (Exception e) {
            HttpUtil.writeError(exchange, 500, "kafka-consume failed: " + e.getClass().getSimpleName()
                    + ": " + (e.getMessage() == null ? "" : e.getMessage()));
        }
    }
}
