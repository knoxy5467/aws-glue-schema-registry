/*
 * Phase 6 — JMH benchmark mirroring the Go bench matrix at
 * native-schema-registry/golang/pkg/gsrserde-go/{core,serializer,deserializer}/
 * *_bench_test.go.
 *
 * Scope:
 *   - encode/decode the GSR wire format via SerializationDataEncoder /
 *     GlueSchemaRegistryDeserializerDataParser
 *   - compression NONE vs ZLIB via GlueSchemaRegistryConfiguration
 *   - small (100 B) / medium (10 KB) / large (1 MB) payloads
 *   - format axis: WIRE_ONLY (header-only path) vs PROTOBUF_INDEX (also
 *     exercises ProtobufWireFormatEncoder.prefixMessageIndexToBytes, which
 *     is the Java cost we cross-compare against Go's protobuf encoder path)
 *
 * Glue is never called: the benchmarks pre-generate a UUID locally and hand
 * it to the wire-format encoder. There is intentionally NO warm/cold cache
 * axis on the Java side — the Java SerializationDataEncoder does not consult
 * any cache; the Go side's cache axis is meaningful because the Go encoder's
 * fast path includes a Caffeine-equivalent lookup, but on Java the equivalent
 * cache lives one layer up in GlueSchemaRegistrySerializationFacade (the
 * higher-level facade we are intentionally NOT benchmarking, since the Go
 * core-level benches don't either).
 *
 * Cross-language comparison: the Go bench computes throughput on the SOURCE
 * (uncompressed) bytes via b.SetBytes(len(payload)). JMH @Benchmark calls
 * return one encode/decode per invocation, so the comparable Java metric is
 * (1_000_000 / avgt_us / 1_000_000 * payloadSize) bytes/s — the README at
 * perf/README.md does this conversion.
 */
package com.amazonaws.services.schemaregistry.perf;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.serializers.SerializationDataEncoder;
import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder;
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufWireFormatEncoder;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Wire-format encode/decode benchmark.
 *
 * JMH defaults applied at the class level (warmup 3 × 1s, measurement 5 × 1s,
 * fork 2, heap -Xms2g -Xmx2g) match the Phase 6 prompt. Smoke runs override
 * these from the command line (e.g. `-i 1 -wi 1 -f 1 -r 1s`) — the prompt
 * notes that full 5-fork runs are the user's call.
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgs = {"-Xms2g", "-Xmx2g"})
public class EncodeDecodeBench {

    /** Mirror the Go benchPayloadSizes matrix. */
    @Param({"100", "10240", "1048576"})
    public int payloadSize;

    /** NONE vs ZLIB, matching the Go side. Java's GSR enum names map directly. */
    @Param({"NONE", "ZLIB"})
    public String compression;

    /**
     * WIRE_ONLY measures SerializationDataEncoder.write — the GSR 18-byte
     * header plus compression on the raw payload. PROTOBUF_INDEX wraps the
     * payload in ProtobufWireFormatEncoder.prefixMessageIndexToBytes FIRST
     * (the message-index varint + payload concatenation Java's
     * ProtobufWireFormatEncoder does), then runs the wire-format encode.
     * The latter mirrors the Go core BenchmarkEncodeWireFormat PROTOBUF
     * cells; the former mirrors the format-agnostic cells.
     */
    @Param({"WIRE_ONLY", "PROTOBUF_INDEX"})
    public String format;

    /** Source payload — random bytes so zlib doesn't get unrealistic ratios. */
    private byte[] payload;

    /**
     * Wire-format bytes the decode benchmark consumes. Rebuilt on each
     * @Setup. For PROTOBUF_INDEX this is the GSR header + compressed (or not)
     * (varint-prefix || payload); for WIRE_ONLY it is GSR header + compressed
     * (or not) raw payload.
     */
    private byte[] wirePayload;

    /** Reusable encoder pre-built with the chosen compression. */
    private SerializationDataEncoder encoder;
    private GlueSchemaRegistryDeserializerDataParser parser;

    /** Protobuf wire-format encoder + descriptor — only used when format=PROTOBUF_INDEX. */
    private ProtobufWireFormatEncoder protobufEncoder;
    private Descriptors.FileDescriptor protobufFileDescriptor;
    private Descriptors.Descriptor protobufMessageDescriptor;

    /**
     * The schema-version UUID used across every iteration of a benchmark.
     * Mirrors the Go warm path where the schema-version-id is already
     * cached; the Go cold path constructs a fresh encoder per iteration
     * (different work entirely). Java has no equivalent encoder-level
     * cache to drain, so there is no Java-side cold axis — the cache the
     * Go bench reset lives one facade layer up.
     */
    private UUID schemaVersionId;

    private final Random rng = new SecureRandom();

    @Setup
    public void setup() throws Exception {
        payload = new byte[payloadSize];
        rng.nextBytes(payload);

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-2");
        if ("ZLIB".equals(compression)) {
            cfg.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE,
                    AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name());
        }
        GlueSchemaRegistryConfiguration gsrConfig = new GlueSchemaRegistryConfiguration(cfg);

        encoder = new SerializationDataEncoder(gsrConfig);
        parser = GlueSchemaRegistryDeserializerDataParser.getInstance();
        schemaVersionId = UUID.randomUUID();

        if ("PROTOBUF_INDEX".equals(format)) {
            protobufFileDescriptor = buildPerfProtobufDescriptor();
            protobufMessageDescriptor = protobufFileDescriptor.findMessageTypeByName("Payload");
            protobufEncoder = new ProtobufWireFormatEncoder(new MessageIndexFinder());
        }

        // Pre-build the decode input so the decode benchmark measures only
        // parser.getPlainData (and decompression for ZLIB).
        wirePayload = encoder.write(materializeEncoderInput(), schemaVersionId);
    }

    /**
     * Encode benchmark. Mirrors Go BenchmarkEncodeWireFormat at the wire-format
     * level. For PROTOBUF_INDEX, also runs the message-index varint prefix on
     * each invocation — that's the part the Go protobuf cell measures inside
     * GsrEncoder.Encode at encoder.go:111-115.
     */
    @Benchmark
    public void encode(Blackhole bh) {
        bh.consume(encoder.write(materializeEncoderInput(), schemaVersionId));
    }

    /**
     * Decode benchmark. Mirrors Go BenchmarkDecodeWireFormat. For
     * PROTOBUF_INDEX, the test does NOT strip the varint prefix from the
     * decoder side because Java's stripping lives in
     * ProtobufWireFormatDecoder.getAndRemoveMessageIndex, which is one layer
     * above GlueSchemaRegistryDeserializerDataParser — same separation as
     * the Go side, where stripMessageIndex is called outside DecodeWireFormat.
     * Keeping decode WIRE_ONLY-equivalent for both format params keeps the
     * Java decode column directly comparable to the Go core wire-format
     * decode column.
     */
    @Benchmark
    public void decode(Blackhole bh) {
        // ByteBuffer.wrap is allocation-cheap; the work measured is parser.getPlainData.
        ByteBuffer buf = ByteBuffer.wrap(wirePayload);
        bh.consume(parser.getPlainData(buf));
    }

    /**
     * For WIRE_ONLY: pass the raw payload through. For PROTOBUF_INDEX: prepend
     * the varint message-index via ProtobufWireFormatEncoder before the GSR
     * header path. Allocated per call rather than cached so the protobuf
     * cell measures the prefix cost on every invocation (matches Go's
     * BenchmarkEncodeWireFormat PROTOBUF cells, which call
     * prefixMessageIndexToBytes inside Encode).
     */
    private byte[] materializeEncoderInput() {
        if ("PROTOBUF_INDEX".equals(format)) {
            // Wrap the random payload in a perf.Payload message — same
            // shape as the Go bench's dynamicpb.NewMessage call.
            DynamicMessage msg = DynamicMessage.newBuilder(protobufMessageDescriptor)
                    .setField(protobufMessageDescriptor.findFieldByName("blob"),
                              ByteString.copyFrom(payload))
                    .build();
            return protobufEncoder.encode(msg, protobufFileDescriptor);
        }
        return payload;
    }

    /**
     * Builds the same single-message FileDescriptor the Go bench uses
     * (perf.Payload { bytes blob = 1 }). Matches the BFS+lex-sort message
     * index 0 used by both languages.
     */
    private static Descriptors.FileDescriptor buildPerfProtobufDescriptor() throws Descriptors.DescriptorValidationException {
        FileDescriptorProto fdProto = FileDescriptorProto.newBuilder()
                .setName("perf.proto")
                .setPackage("perf")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Payload")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("blob")
                                .setNumber(1)
                                .setType(Type.TYPE_BYTES)
                                .build())
                        .build())
                .build();
        return Descriptors.FileDescriptor.buildFrom(fdProto, new Descriptors.FileDescriptor[0]);
    }
}
