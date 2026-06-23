/*
 * POST /kafka-produce — Java producer in a cross-language interop test.
 *
 * Phase 4.6.5: drives the FULL Kafka customer API — same library entry
 * point a real Java GSR Kafka producer would use — so the test exercises
 * the format-specific serializer (including PROTOBUF's message-index
 * prefix) and the Glue register/lookup, not just the wire-format envelope.
 *
 * Request body (JSON):
 *   {
 *     "format":      "AVRO" | "JSON" | "PROTOBUF",
 *     "schema":      "<schema definition string>",
 *     "schemaName":  "<schema name>",
 *     "record":      <per-format JSON envelope; see RecordCodec>
 *     "compression": "NONE" | "ZLIB",
 *     "bootstrap":   "<kafka bootstrap servers>",
 *     "topic":       "<kafka topic name>",
 *     "region":      "<aws region, optional>"
 *   }
 *
 * Behavior:
 *   1. RecordCodec.buildRecord rebuilds the typed Java record
 *      (GenericRecord / JsonDataWithSchema / DynamicMessage) from the
 *      envelope.
 *   2. GlueSchemaRegistryKafkaSerializer.serialize(topic, record) → byte[]
 *      registers the schema with real Glue (auto-registration on) and
 *      returns the framed bytes — same call path a real Java customer hits.
 *   3. Plain KafkaProducer<byte[], byte[]> ships one record to Kafka.
 *
 * Response body (JSON):
 *   { "schemaVersionId", "bytes", "offset", "partition" }
 *
 * The schemaVersionId is parsed back out of the framed bytes' GSR header
 * because GlueSchemaRegistryKafkaSerializer doesn't expose the registered
 * UUID directly through its Serializer interface — the header round-trip
 * is the cheapest reliable way to surface it for the test's assertions.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public final class KafkaProduceHandler implements HttpHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
            String compression = req.hasNonNull("compression") ? req.get("compression").asText("NONE") : "NONE";
            String bootstrap = HttpUtil.requireString(req, "bootstrap");
            String topic = HttpUtil.requireString(req, "topic");
            String region = req.hasNonNull("region") ? req.get("region").asText(null) : null;
            JsonNode recordEnv = req.get("record");
            if (recordEnv == null || !recordEnv.isObject()) {
                throw new IllegalArgumentException("missing required field: record (object)");
            }

            Object javaRecord = RecordCodec.buildRecord(format, schemaDef, recordEnv);

            // Configure the GSR Kafka serializer with the same property keys
            // a real Kafka customer would set on the Producer config.
            Map<String, Object> gsrConfigs = new HashMap<>();
            gsrConfigs.put(AWSSchemaRegistryConstants.AWS_REGION,
                    region != null ? region : RealGlueClient.defaultRegion());
            gsrConfigs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, schemaName);
            gsrConfigs.put(AWSSchemaRegistryConstants.DATA_FORMAT, format);
            gsrConfigs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compression);
            gsrConfigs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "NONE");
            gsrConfigs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
            if ("PROTOBUF".equals(format)) {
                gsrConfigs.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, "DYNAMIC_MESSAGE");
            }

            GlueSchemaRegistryKafkaSerializer kafkaSerializer = new GlueSchemaRegistryKafkaSerializer(gsrConfigs);
            byte[] framed = kafkaSerializer.serialize(topic, javaRecord);
            if (framed == null) {
                throw new IllegalStateException(
                        "GlueSchemaRegistryKafkaSerializer.serialize returned null for non-null record"
                        + " (format=" + format + ", topic=" + topic + ")");
            }

            // Plain bytes producer — Kafka is just transport here.
            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrap);
            props.put("key.serializer", ByteArraySerializer.class.getName());
            props.put("value.serializer", ByteArraySerializer.class.getName());
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("request.timeout.ms", "10000");
            RecordMetadata md;
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, framed);
                md = producer.send(record).get();
            }

            // Recover the registered UUID from the GSR header for the test's
            // assertions.
            UUID schemaVersionId =
                    GlueSchemaRegistryDeserializerDataParser.getInstance().getSchemaVersionId(ByteBuffer.wrap(framed));

            ObjectNode resp = MAPPER.createObjectNode();
            resp.put("schemaVersionId", schemaVersionId.toString());
            resp.put("bytes", Base64.getEncoder().encodeToString(framed));
            resp.put("offset", md.offset());
            resp.put("partition", md.partition());
            HttpUtil.writeJson(exchange, 200, resp);
        } catch (IllegalArgumentException e) {
            HttpUtil.writeError(exchange, 400, e.getMessage());
        } catch (Exception e) {
            HttpUtil.writeError(exchange, 500, "kafka-produce failed: " + e.getClass().getSimpleName()
                    + ": " + (e.getMessage() == null ? "" : e.getMessage()));
        }
    }
}
