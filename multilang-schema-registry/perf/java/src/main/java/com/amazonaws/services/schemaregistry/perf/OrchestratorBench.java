/*
 * Phase 6.3 — JMH orchestrator-level benchmark exercising the Kafka-facing
 * customer API surface:
 *
 *   GlueSchemaRegistryKafkaSerializer.serialize(topic, record)
 *   GlueSchemaRegistryKafkaDeserializer.deserialize(topic, bytes)
 *
 * This matches the Go serializer/deserializer benches at
 * pkg/gsrserde-go/{serializer,deserializer}/*_bench_test.go which exercise
 * Serializer.Serialize and Deserializer.Deserialize — the public orchestrator
 * API that includes format-layer marshal/unmarshal + wire-format encode/decode.
 *
 * Mocking strategy:
 *   - Serialize: construct with a fixed schemaVersionId so the Kafka serializer
 *     never calls getOrRegisterSchemaVersion. The measured work per iteration is
 *     DataFormatSerializer.serialize (format-layer marshal) +
 *     SerializationDataEncoder.write (wire-format + compression).
 *   - Deserialize: inject a GlueSchemaRegistryDeserializationFacade whose Guava
 *     cache is pre-populated with the schema for our UUID. The measured work per
 *     iteration is GlueSchemaRegistryDeserializerDataParser.getSchemaVersionId +
 *     cache.get (warm hit) + DeserializerFactory.deserialize (format-layer unmarshal).
 *
 * Glue is never called. The numbers represent warm-cache orchestrator throughput.
 */
package com.amazonaws.services.schemaregistry.perf;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Orchestrator-level encode/decode benchmark. Mirrors Go's
 * BenchmarkSerializerSerialize and BenchmarkDeserializerDeserialize at
 * the Kafka-customer-facing API layer.
 *
 * Threads(1) is the default; the @Threads annotation at the method level
 * adds multi-threaded cells for concurrency comparison with Go's RunParallel.
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgs = {"-Xms2g", "-Xmx2g"})
public class OrchestratorBench {

    @Param({"100", "10240", "1048576"})
    public int payloadSize;

    @Param({"NONE", "ZLIB"})
    public String compression;

    /**
     * AVRO and JSON exercise the full format-layer serialize path.
     * PROTOBUF is omitted from this bench because protobuf serialize
     * at the Kafka layer requires proto-generated message classes that
     * the bench module doesn't have access to without significant build
     * infra. The wire-format-only protobuf path is covered by
     * EncodeDecodeBench. The Go side's PROTOBUF orchestrator bench is
     * similarly a dynamicpb.Message path.
     */
    @Param({"AVRO", "JSON"})
    public String format;

    // ---- Phase 6.2 deterministic payload generator (shared with EncodeDecodeBench) ----
    private static final long PERF_PAYLOAD_SEED = 0x9E3779B97F4A7C15L;
    private static final String PERF_PAYLOAD_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 .";

    private static byte[] generatePerfPayload(int size) {
        byte[] out = new byte[size];
        long state = PERF_PAYLOAD_SEED;
        for (int i = 0; i < size; i++) {
            state ^= state << 13;
            state ^= state >>> 7;
            state ^= state << 17;
            int idx = (int) ((state >>> 16) & 63L);
            out[i] = (byte) PERF_PAYLOAD_ALPHABET.charAt(idx);
        }
        return out;
    }

    // ---- Per-iteration state ----
    private GlueSchemaRegistryKafkaSerializer serializer;
    private GlueSchemaRegistryKafkaDeserializer deserializer;

    /** The object passed to serializer.serialize(topic, data). */
    private Object serializerInput;

    /** Pre-serialized wire bytes for the decode benchmark. */
    private byte[] wirePayload;

    private static final String TOPIC = "perf-topic";
    private static final UUID SCHEMA_VERSION_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    // Avro schema matching Go's perf.Payload with a string field
    private static final String AVRO_SCHEMA_DEF =
            "{\"type\":\"record\",\"name\":\"Payload\",\"namespace\":\"perf\","
                    + "\"fields\":[{\"name\":\"blob\",\"type\":\"string\"}]}";

    // JSON schema matching Go's perf JSON bench wrapper
    private static final String JSON_SCHEMA_DEF =
            "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
                    + "\"type\":\"object\",\"properties\":{\"blob\":{\"type\":\"string\"}},"
                    + "\"required\":[\"blob\"]}";

    @Setup
    public void setup() throws Exception {
        byte[] payload = generatePerfPayload(payloadSize);

        // Static credentials that will never be used (Glue is bypassed)
        StaticCredentialsProvider creds = StaticCredentialsProvider.create(
                AwsBasicCredentials.create("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));

        // ---- Serializer setup ----
        Map<String, Object> serConfigs = new HashMap<>();
        serConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-2");
        serConfigs.put(AWSSchemaRegistryConstants.DATA_FORMAT, format.equals("AVRO") ? DataFormat.AVRO.toString() : DataFormat.JSON.toString());
        serConfigs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "perf-bench-schema");
        serConfigs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "perf-bench-registry");
        if ("ZLIB".equals(compression)) {
            serConfigs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE,
                    AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name());
        }
        // Disable auto-registration since we have a fixed schemaVersionId
        serConfigs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "false");

        // Construct serializer with a fixed schemaVersionId — bypasses Glue
        serializer = new GlueSchemaRegistryKafkaSerializer(creds, SCHEMA_VERSION_ID, serConfigs);

        // Build the input data object matching the format
        serializerInput = buildSerializerInput(payload);

        // ---- Deserializer setup ----
        Map<String, Object> desConfigs = new HashMap<>();
        desConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-2");
        desConfigs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "perf-bench-registry");
        if ("ZLIB".equals(compression)) {
            desConfigs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE,
                    AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name());
        }
        // Avro-specific: set the record type for deserialization
        if ("AVRO".equals(format)) {
            desConfigs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, "GENERIC_RECORD");
        }

        // Create a mock AWSSchemaRegistryClient for the deserialization facade
        AWSSchemaRegistryClient mockClient = mock(AWSSchemaRegistryClient.class);
        String schemaDef = getSchemaDefinition();
        String schemaName = "perf-bench-schema";
        when(mockClient.getSchemaVersionResponse(anyString()))
                .thenReturn(GetSchemaVersionResponse.builder()
                        .schemaDefinition(schemaDef)
                        .dataFormat(DataFormat.fromValue(format))
                        .schemaArn("arn:aws:glue:us-east-2:000000000000:schema/perf-bench-registry/" + schemaName)
                        .build());

        GlueSchemaRegistryDeserializationFacade desFacade =
                GlueSchemaRegistryDeserializationFacade.builder()
                        .credentialProvider(creds)
                        .schemaRegistryClient(mockClient)
                        .configs(desConfigs)
                        .build();
        // The facade's LoadingCache will call mockClient.getSchemaVersionResponse
        // on the first deserialize — that populates the cache. Subsequent calls
        // (i.e. all measured iterations after JMH warmup) hit the warm cache.

        deserializer = new GlueSchemaRegistryKafkaDeserializer(creds, desConfigs);
        deserializer.setGlueSchemaRegistryDeserializationFacade(desFacade);

        // Pre-build the wire payload for decode by serializing once
        wirePayload = serializer.serialize(TOPIC, serializerInput);
        if (wirePayload == null) {
            throw new IllegalStateException("serializer.serialize returned null — check format/schema setup");
        }
    }

    /**
     * Encode benchmark: full orchestrator serialize (format marshal + wire encode).
     */
    @Benchmark
    public void encodeKafka(Blackhole bh) {
        bh.consume(serializer.serialize(TOPIC, serializerInput));
    }

    /**
     * Decode benchmark: full orchestrator deserialize (wire decode + format unmarshal).
     */
    @Benchmark
    public void decodeKafka(Blackhole bh) {
        bh.consume(deserializer.deserialize(TOPIC, wirePayload));
    }

    /**
     * Multi-threaded encode — mirrors Go's BenchmarkSerializerSerialize_Parallel.
     * JMH @Threads controls concurrency.
     */
    @Benchmark
    @Threads(4)
    public void encodeKafka_4threads(Blackhole bh) {
        bh.consume(serializer.serialize(TOPIC, serializerInput));
    }

    @Benchmark
    @Threads(16)
    public void encodeKafka_16threads(Blackhole bh) {
        bh.consume(serializer.serialize(TOPIC, serializerInput));
    }

    /**
     * Multi-threaded decode — mirrors Go's BenchmarkDeserializerDeserialize_Parallel.
     */
    @Benchmark
    @Threads(4)
    public void decodeKafka_4threads(Blackhole bh) {
        bh.consume(deserializer.deserialize(TOPIC, wirePayload));
    }

    @Benchmark
    @Threads(16)
    public void decodeKafka_16threads(Blackhole bh) {
        bh.consume(deserializer.deserialize(TOPIC, wirePayload));
    }

    // ---- Helpers ----

    private Object buildSerializerInput(byte[] payload) {
        String payloadStr = new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        switch (format) {
            case "AVRO":
                org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(AVRO_SCHEMA_DEF);
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("blob", payloadStr);
                return record;
            case "JSON":
                // JSON format serializer expects a JsonDataWithSchema or raw bytes/string.
                // The GSR JSON serializer accepts a JsonNode (Jackson) or Object that
                // it serializes via the JsonSerializer. For the Kafka surface, it expects
                // a JsonNode from the com.amazonaws.services.schemaregistry.serializers.json
                // package, or raw JSON bytes wrapped by the schema.
                // Simplest: return a com.fasterxml.jackson.databind.JsonNode
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                com.fasterxml.jackson.databind.node.ObjectNode node = mapper.createObjectNode();
                node.put("blob", payloadStr);
                return node;
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }

    private String getSchemaDefinition() {
        switch (format) {
            case "AVRO":
                return AVRO_SCHEMA_DEF;
            case "JSON":
                return JSON_SCHEMA_DEF;
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }
}
