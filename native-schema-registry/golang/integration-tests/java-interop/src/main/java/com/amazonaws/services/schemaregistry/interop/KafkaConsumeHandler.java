/*
 * POST /kafka-consume — Java consumer in a cross-language interop test.
 *
 * Phase 4.6.5: drives the FULL Kafka customer API — exact same library
 * entry point a real Java GSR Kafka consumer would use.
 *
 * Request body (JSON):
 *   {
 *     "bootstrap": "<kafka bootstrap servers>",
 *     "topic":     "<kafka topic name>",
 *     "format":    "AVRO" | "JSON" | "PROTOBUF",   // needed to re-encode the typed record
 *     "groupId":   "<optional consumer group>",
 *     "region":    "<aws region, optional>",
 *     "timeoutMs": 30000                            // optional
 *   }
 *
 * Behavior:
 *   1. Plain KafkaConsumer<byte[],byte[]> polls one record.
 *   2. GlueSchemaRegistryKafkaDeserializer.deserialize(topic, bytes) →
 *      typed Java record. This is the symmetric call to the producer side.
 *   3. RecordCodec.recordToEnvelope serializes back to the per-format JSON
 *      envelope the Go test asserts against.
 *
 * Response body (JSON):
 *   { "schemaVersionId", "dataFormat", "schemaDefinition", "schemaArn",
 *     "record": <per-format envelope> }
 *
 * On poll timeout: 404 with { "error": "no record within <ms>ms" }.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
            String format = HttpUtil.requireString(req, "format");
            String groupId = req.hasNonNull("groupId")
                    ? req.get("groupId").asText()
                    : "gsr-interop-" + UUID.randomUUID();
            String region = req.hasNonNull("region") ? req.get("region").asText(null) : null;
            int timeoutMs = req.hasNonNull("timeoutMs") ? req.get("timeoutMs").asInt(DEFAULT_TIMEOUT_MS) : DEFAULT_TIMEOUT_MS;

            // 1. Poll Kafka for one framed message.
            Properties consumerProps = new Properties();
            consumerProps.put("bootstrap.servers", bootstrap);
            consumerProps.put("group.id", groupId);
            consumerProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
            consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
            consumerProps.put("auto.offset.reset", "earliest");
            consumerProps.put("enable.auto.commit", "false");

            byte[] framed;
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
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

            // 2. Hand to the real GSR Kafka deserializer. Configure with the
            // same property surface a real Kafka consumer would supply.
            Map<String, Object> gsrConfigs = new HashMap<>();
            gsrConfigs.put(AWSSchemaRegistryConstants.AWS_REGION,
                    region != null ? region : RealGlueClient.defaultRegion());
            if ("PROTOBUF".equals(format)) {
                gsrConfigs.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, "DYNAMIC_MESSAGE");
            }
            if ("AVRO".equals(format)) {
                // Generic record path so we get a GenericRecord back rather
                // than the deserializer trying to load a generated SpecificRecord
                // class that doesn't exist on the sidecar classpath.
                gsrConfigs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, "GENERIC_RECORD");
            }

            GlueSchemaRegistryKafkaDeserializer kafkaDeserializer =
                    new GlueSchemaRegistryKafkaDeserializer(gsrConfigs);
            Object javaRecord = kafkaDeserializer.deserialize(topic, framed);

            // 3. Resolve schema metadata for the response (so the test
            // can sanity-check the registered schema definition matches
            // what the producer side claims).
            UUID schemaVersionId = GlueSchemaRegistryDeserializerDataParser.getInstance()
                    .getSchemaVersionId(ByteBuffer.wrap(framed));
            AWSSchemaRegistryClient glueClient = RealGlueClient.get(region);
            GetSchemaVersionResponse schemaResp =
                    glueClient.getSchemaVersionResponse(schemaVersionId.toString());

            ObjectNode resp = MAPPER.createObjectNode();
            resp.put("schemaVersionId", schemaVersionId.toString());
            resp.put("dataFormat", schemaResp.dataFormat().toString());
            resp.put("schemaDefinition", schemaResp.schemaDefinition());
            resp.put("schemaArn", schemaResp.schemaArn());
            resp.set("record", RecordCodec.recordToEnvelope(format, javaRecord));
            HttpUtil.writeJson(exchange, 200, resp);
        } catch (IllegalArgumentException e) {
            HttpUtil.writeError(exchange, 400, e.getMessage());
        } catch (Exception e) {
            HttpUtil.writeError(exchange, 500, "kafka-consume failed: " + e.getClass().getSimpleName()
                    + ": " + (e.getMessage() == null ? "" : e.getMessage()));
        }
    }
}
