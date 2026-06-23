/*
 * POST /kafka-produce — Java producer in a cross-language interop test.
 *
 * Request body (JSON):
 *   {
 *     "format":      "AVRO" | "JSON" | "PROTOBUF",
 *     "schema":      "<schema definition string>",
 *     "schemaName":  "<schema name>",        // must match the gsr-go-it-* prefix
 *     "payload":     "<base64 of pre-encoded record bytes>",
 *     "compression": "NONE" | "ZLIB",
 *     "bootstrap":   "<kafka bootstrap servers>",
 *     "topic":       "<kafka topic name>",
 *     "region":      "<aws region>"          // optional; falls back to AWS_REGION or us-east-2
 *   }
 *
 * What the handler does:
 *   1. Calls SchemaByDefinitionFetcher.getORRegisterSchemaVersionId(...)
 *      to register the schema with real AWS Glue (or fetch the UUID if
 *      it already exists). This is the call the Go consumer must later
 *      see when it resolves the UUID — that's the "real interop" signal.
 *   2. Frames the payload via SerializationDataEncoder.write(...) — same
 *      18-byte header + zlib body that the Go wire-format module emits.
 *   3. Produces one record to the requested Kafka topic via plain
 *      KafkaProducer<byte[], byte[]>; key is null.
 *
 * Response body (JSON):
 *   {
 *     "schemaVersionId": "<UUID>",        // what got written into the GSR header
 *     "bytes":           "<base64>",      // the framed bytes shipped to Kafka
 *     "offset":          <long>,          // partition offset of the produced record
 *     "partition":       <int>
 *   }
 *
 * NEVER pre-validates the payload against the schema (e.g. JSON validate,
 * proto descriptor check) — interop tests pre-encode on the Go side and
 * just need the Java framing + Glue path. Validating here would force a
 * second cross-language object-shape contract.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.SerializationDataEncoder;
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
            byte[] payload = Base64.getDecoder().decode(HttpUtil.requireString(req, "payload"));
            String compression = req.hasNonNull("compression") ? req.get("compression").asText("NONE") : "NONE";
            String bootstrap = HttpUtil.requireString(req, "bootstrap");
            String topic = HttpUtil.requireString(req, "topic");
            String region = req.hasNonNull("region") ? req.get("region").asText(null) : null;

            // 1. Register (or fetch) the schema in real Glue.
            Map<String, Object> configs = new HashMap<>();
            configs.put(AWSSchemaRegistryConstants.AWS_REGION,
                    region != null ? region : RealGlueClient.defaultRegion());
            configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compression);
            configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
            GlueSchemaRegistryConfiguration cfg = new GlueSchemaRegistryConfiguration(configs);

            AWSSchemaRegistryClient client = RealGlueClient.get(region);
            SchemaByDefinitionFetcher fetcher = new SchemaByDefinitionFetcher(client, cfg);
            Map<String, String> metadata = new HashMap<>();
            metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, topic);
            UUID schemaVersionId = fetcher.getORRegisterSchemaVersionId(schemaDef, schemaName, format, metadata);

            // 2. Frame with the real GSR header.
            SerializationDataEncoder encoder = new SerializationDataEncoder(cfg);
            byte[] framed = encoder.write(payload, schemaVersionId);

            // 3. Produce to Kafka.
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
