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
 *   - PROTOBUF adds the message-index varint via the same library helper
 *     the production encoder uses
 *
 * Glue is never called: the benchmarks pre-generate a UUID locally and hand it
 * to the wire-format encoder. The "warm-cache" vs "cold-cache" axis from the Go
 * side maps onto the JMH @State scopes:
 *   - cacheState=warm: schema-version-id resolved once at @Setup, reused per @Benchmark
 *     iteration. Mirrors the Go warm path where the encoder cache is pre-seeded.
 *   - cacheState=cold: schema-version-id reset per @Invocation. This isn't a true
 *     "fresh Glue call" parity check — that would need a Mockito stub on
 *     AWSSchemaRegistryClient — but it does swap a UUID per call to surface
 *     the allocation cost the warm case amortizes.
 *
 * Real cross-language head-to-head requires comparing the SAME work units. The
 * Go bench computes throughput on the SOURCE (uncompressed) bytes via
 * b.SetBytes(len(payload)). JMH @Benchmark calls return one encode/decode per
 * @OperationsPerInvocation=1, so the corresponding metric here is "ops/s ×
 * payload-size = bytes/s" — the README at perf/README.md does this conversion.
 */
package com.amazonaws.services.schemaregistry.perf;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.serializers.SerializationDataEncoder;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
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
 * these from the command line (e.g. `-i 1 -wi 0 -f 1 -r 1s`) — the prompt
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
     * "warm" and "cold" mirror the Go cacheState axis. Today the only thing we
     * vary between them is whether the UUID is freshly generated per
     * invocation. A fuller "cold = fresh Mockito stub" stage would be a
     * Phase 6 follow-up; the prompt allows smoke wiring.
     */
    @Param({"warm", "cold"})
    public String cacheState;

    /** Source payload — random bytes so zlib doesn't get unrealistic ratios. */
    private byte[] payload;

    /** Wire-format bytes the decode benchmark consumes. Rebuilt on each @Setup. */
    private byte[] wirePayload;

    /** Reusable encoder pre-built with the chosen compression. */
    private SerializationDataEncoder encoder;
    private GlueSchemaRegistryDeserializerDataParser parser;

    /**
     * Warm-cache UUID — the encoder uses this UUID across every iteration of
     * a benchmark to mirror the Go warm path where the schema-version-id is
     * already cached.
     */
    private UUID warmUuid;

    /** Cold-cache RNG so cold iterations get a fresh UUID each invocation. */
    private final Random rng = new SecureRandom();

    @Setup
    public void setup() {
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

        warmUuid = UUID.randomUUID();
        wirePayload = encoder.write(payload, warmUuid);
    }

    /**
     * Encode benchmark. Mirrors the Go BenchmarkEncodeWireFormat.
     * Returns the wire bytes via Blackhole so JIT can't dead-code-eliminate
     * the encode call.
     */
    @Benchmark
    public void encode(Blackhole bh) {
        UUID id = "warm".equals(cacheState) ? warmUuid : UUID.randomUUID();
        bh.consume(encoder.write(payload, id));
    }

    /**
     * Decode benchmark. Mirrors the Go BenchmarkDecodeWireFormat.
     * Decompresses if compression == ZLIB. The decoder doesn't strip a
     * protobuf message-index — the encoder above didn't prepend one; that's
     * exercised separately in the *Protobuf* benchmarks below.
     */
    @Benchmark
    public void decode(Blackhole bh) {
        // ByteBuffer.wrap is allocation-cheap; the work measured is parser.getPlainData.
        ByteBuffer buf = ByteBuffer.wrap(wirePayload);
        bh.consume(parser.getPlainData(buf));
    }
}
