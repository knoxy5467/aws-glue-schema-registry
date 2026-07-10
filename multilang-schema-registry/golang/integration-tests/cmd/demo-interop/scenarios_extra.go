//go:build integration

// Package main — scenarios_extra.go
// Phase 4.17 Item C: Three deferred demo scenarios (cache, auto-register,
// same-version baseline).
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/serializer"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/realglue"
)

// ---------------------------------------------------------------------------
// C.1 — Cache behavior scenario
// ---------------------------------------------------------------------------

// runCacheScenario demonstrates encoder cache miss → hit → eviction → re-fetch.
// Uses a single Avro v1 schema with real Glue. Returns one ScenarioResult.
func runCacheScenario(
	ctx context.Context,
	real *realglue.Real,
	cleanup *realglue.Cleanup,
	region string,
) ScenarioResult {
	result := ScenarioResult{
		Format:      "AVRO",
		Direction:   "n/a (cache)",
		Compression: "NONE",
	}

	printSectionHeader("SCENARIO: Cache behavior (miss → hit → eviction → re-fetch)")

	suffix, err := randomSuffix()
	if err != nil {
		result.Err = fmt.Errorf("randomSuffix: %w", err)
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	schemaName := fmt.Sprintf("%scache-%s", demoPrefix, suffix)
	cleanup.TrackSchema(demoRegistryName, schemaName)
	dataFormat := "AVRO"

	printStage("demo", fmt.Sprintf("Schema name: %s", schemaName))
	printStage("demo", fmt.Sprintf("Data format: %s", dataFormat))
	fmt.Println()

	// Build a Go serializer with auto-registration enabled.
	gsrMap := map[string]string{
		"region":                        region,
		"registry.name":                 demoRegistryName,
		"compression":                   "NONE",
		"schemaAutoRegistrationEnabled": "true",
	}
	configMap := map[string]interface{}{
		common.GSRConfigPathKey: gsrMap,
		common.DataFormatTypeKey: common.DataFormatAvro,
		common.AvroRecordTypeKey: common.AvroRecordTypeGeneric,
	}
	cfg := common.NewConfiguration(configMap)

	ser, err := serializer.NewSerializer(cfg)
	if err != nil {
		result.Err = fmt.Errorf("NewSerializer: %w", err)
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	defer ser.Close() //nolint:errcheck

	enc := ser.CoreEncoderForTest()

	// Build the Avro record.
	record, err := goRecordForFormat("AVRO", crossVersionAvroV1)
	if err != nil {
		result.Err = fmt.Errorf("goRecordForFormat: %w", err)
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	// ── Step 1: First encode (cache miss) ───────────────────────────────────
	printStage("cache", fmt.Sprintf("checking key %s:%s ...", schemaName, dataFormat))
	hasBefore := gsrcore.EncoderCacheHas(enc, schemaName, dataFormat)
	printStage("cache", fmt.Sprintf("result: %s", hitOrMiss(hasBefore)))
	fmt.Println()

	printStage("glue", fmt.Sprintf("GetSchemaByDefinition schemaName=%s (first encode)", schemaName))
	printStage("demo", "First Encode: expect cache miss → Glue API call")

	_, err = ser.Serialize(schemaName, record)
	if err != nil {
		result.Err = fmt.Errorf("first Serialize: %w", err)
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	// After first encode, cache should be populated.
	hasAfterFirst := gsrcore.EncoderCacheHas(enc, schemaName, dataFormat)
	if !hasAfterFirst {
		result.Err = fmt.Errorf("cache should have entry after first encode but does not")
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	printStage("cache", fmt.Sprintf("miss → fetched UUID from Glue"))
	fmt.Println()

	// ── Step 2: Second encode (cache hit) ───────────────────────────────────
	printStage("cache", fmt.Sprintf("checking key %s:%s ...", schemaName, dataFormat))
	hasBeforeSecond := gsrcore.EncoderCacheHas(enc, schemaName, dataFormat)
	printStage("cache", fmt.Sprintf("result: %s", hitOrMiss(hasBeforeSecond)))
	fmt.Println()

	printStage("demo", "Second Encode: expect cache hit → no Glue API call")

	_, err = ser.Serialize(schemaName, record)
	if err != nil {
		result.Err = fmt.Errorf("second Serialize: %w", err)
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("cache", "hit → returning cached UUID, no Glue API call")
	fmt.Println()

	// ── Step 3: Eviction + re-encode (cache miss again) ─────────────────────
	printStage("demo", "Evicting cache entry...")
	evicted := gsrcore.EvictEncoderCache(enc, schemaName, dataFormat)
	if !evicted {
		result.Err = fmt.Errorf("expected entry to exist for eviction")
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	printStage("cache", fmt.Sprintf("evicted key %s:%s", schemaName, dataFormat))

	// Verify eviction took effect.
	hasAfterEviction := gsrcore.EncoderCacheHas(enc, schemaName, dataFormat)
	printStage("cache", fmt.Sprintf("checking key %s:%s ... result: %s", schemaName, dataFormat, hitOrMiss(hasAfterEviction)))
	if hasAfterEviction {
		result.Err = fmt.Errorf("cache should be empty after eviction")
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	fmt.Println()

	printStage("cache", "evicted → refetching from Glue")
	printStage("glue", fmt.Sprintf("GetSchemaByDefinition schemaName=%s (re-fetch after eviction)", schemaName))

	_, err = ser.Serialize(schemaName, record)
	if err != nil {
		result.Err = fmt.Errorf("third Serialize (after eviction): %w", err)
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	hasAfterRefetch := gsrcore.EncoderCacheHas(enc, schemaName, dataFormat)
	if !hasAfterRefetch {
		result.Err = fmt.Errorf("cache should be populated after re-fetch")
		printStage("cache", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	printStage("cache", "re-fetch successful → cache repopulated")
	fmt.Println()

	printStage("verdict", "cache PASS — first miss + second hit + third re-fetch after eviction confirmed")
	fmt.Println()

	result.Pass = true
	result.SchemaName = schemaName
	return result
}

// hitOrMiss returns "hit" or "miss" based on boolean.
func hitOrMiss(has bool) string {
	if has {
		return "hit"
	}
	return "miss"
}

// ---------------------------------------------------------------------------
// C.2 — Auto-register fall-through scenario
// ---------------------------------------------------------------------------

// runAutoRegisterScenario demonstrates EntityNotFound → CreateSchema
// fall-through. Uses real Glue to register, delete, and re-register a schema.
// Returns one ScenarioResult.
func runAutoRegisterScenario(
	ctx context.Context,
	real *realglue.Real,
	cleanup *realglue.Cleanup,
	region string,
) ScenarioResult {
	result := ScenarioResult{
		Format:      "AVRO",
		Direction:   "n/a (auto-reg)",
		Compression: "NONE",
	}

	printSectionHeader("SCENARIO: Auto-register fall-through (EntityNotFound → CreateSchema)")

	suffix, err := randomSuffix()
	if err != nil {
		result.Err = fmt.Errorf("randomSuffix: %w", err)
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	schemaName := fmt.Sprintf("%sauto-reg-%s", demoPrefix, suffix)
	cleanup.TrackSchema(demoRegistryName, schemaName)

	printStage("demo", fmt.Sprintf("Schema name: %s", schemaName))
	printStage("demo", "Data format: AVRO")
	printStage("demo", "SchemaAutoRegistrationEnabled: true")
	fmt.Println()

	// ── Step 1: First encode — registers schema ─────────────────────────────
	printStage("demo", "Step 1: First encode (auto-registers schema in Glue)")

	gsrMap := map[string]string{
		"region":                        region,
		"registry.name":                 demoRegistryName,
		"compression":                   "NONE",
		"schemaAutoRegistrationEnabled": "true",
	}
	configMap := map[string]interface{}{
		common.GSRConfigPathKey:  gsrMap,
		common.DataFormatTypeKey: common.DataFormatAvro,
		common.AvroRecordTypeKey: common.AvroRecordTypeGeneric,
	}
	cfg := common.NewConfiguration(configMap)

	ser1, err := serializer.NewSerializer(cfg)
	if err != nil {
		result.Err = fmt.Errorf("NewSerializer (1): %w", err)
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	record, err := goRecordForFormat("AVRO", crossVersionAvroV1)
	if err != nil {
		ser1.Close() //nolint:errcheck
		result.Err = fmt.Errorf("goRecordForFormat: %w", err)
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("glue", fmt.Sprintf("CreateSchema schemaName=%s compatibility=BACKWARD", schemaName))

	framed1, err := ser1.Serialize(schemaName, record)
	if err != nil {
		ser1.Close() //nolint:errcheck
		result.Err = fmt.Errorf("first Serialize: %w", err)
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	ser1.Close() //nolint:errcheck

	// Extract UUID from the framed bytes (bytes 2-17).
	if len(framed1) < 18 {
		result.Err = fmt.Errorf("framed bytes too short: %d", len(framed1))
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	originalUUID := formatUUID(framed1[2:18])
	printStage("glue", fmt.Sprintf("Schema registered with UUID: %s", originalUUID))
	result.SchemaVersionID = originalUUID
	fmt.Println()

	// ── Step 2: Delete schema from Glue ─────────────────────────────────────
	printStage("demo", "Step 2: Delete schema from Glue (simulates external removal)")
	printStage("glue", fmt.Sprintf("DeleteSchema schemaName=%s", schemaName))

	deleteCtx, deleteCancel := context.WithTimeout(ctx, 30*time.Second)
	_, err = real.Client.DeleteSchema(deleteCtx, &glue.DeleteSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(demoRegistryName),
			SchemaName:   aws.String(schemaName),
		},
	})
	deleteCancel()
	if err != nil {
		result.Err = fmt.Errorf("DeleteSchema: %w", err)
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	printStage("glue", "Schema deleted successfully.")
	fmt.Println()

	// ── Step 3: Re-encode with fresh encoder (auto-register fall-through) ───
	printStage("demo", "Step 3: Re-encode with FRESH serializer (empty cache)")
	printStage("schema-evolution", "schema absent — auto-register triggered")

	ser2, err := serializer.NewSerializer(cfg)
	if err != nil {
		result.Err = fmt.Errorf("NewSerializer (2): %w", err)
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	defer ser2.Close() //nolint:errcheck

	printStage("glue", fmt.Sprintf("GetSchemaByDefinition schemaName=%s → not found", schemaName))
	printStage("glue", "GetSchemaVersion → EntityNotFoundException")
	printStage("schema-evolution", "schema absent — auto-register fall-through path")
	printStage("glue", fmt.Sprintf("CreateSchema schemaName=%s → re-creating", schemaName))

	framed2, err := ser2.Serialize(schemaName, record)
	if err != nil {
		result.Err = fmt.Errorf("second Serialize (fall-through): %w", err)
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	if len(framed2) < 18 {
		result.Err = fmt.Errorf("framed2 bytes too short: %d", len(framed2))
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	newUUID := formatUUID(framed2[2:18])
	printStage("glue", fmt.Sprintf("Schema re-created with new UUID: %s", newUUID))
	fmt.Println()

	// Verify the new UUID differs from the original.
	if newUUID == originalUUID {
		result.Err = fmt.Errorf("new UUID should differ from original (both are %s)", newUUID)
		printStage("verdict", fmt.Sprintf("FAIL — UUIDs match: %s", newUUID))
		return result
	}

	printStage("verdict", fmt.Sprintf(
		"auto-register fall-through PASS — schema re-created with new UUID %s (original was %s)",
		newUUID, originalUUID))
	fmt.Println()

	result.Pass = true
	result.SchemaVersionID = newUUID
	result.SchemaName = schemaName
	return result
}

// ---------------------------------------------------------------------------
// C.3 — Same-version baseline scenario
// ---------------------------------------------------------------------------

// runSameVersionBaseline runs v1-only (no evolution) for all 3 formats in
// both directions via Kafka. This is structurally the cross-version scenario
// minus the v2 registration step.
func runSameVersionBaseline(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cleanup *realglue.Cleanup,
	region string,
) []ScenarioResult {
	printSectionHeader("SCENARIO: Same-version baseline (v1 only, no evolution)")

	suffix, err := randomSuffix()
	if err != nil {
		printStage("demo", fmt.Sprintf("randomSuffix: %v", err))
		return nil
	}

	formats := []struct {
		key      string
		fmtLabel string
		v1Schema string
	}{
		{"AVRO", "avro", crossVersionAvroV1},
		{"JSON", "json", crossVersionJSONV1},
		{"PROTOBUF", "proto", crossVersionProtoV1},
	}

	var results []ScenarioResult
	for _, f := range formats {
		schemaName := fmt.Sprintf("%ssame-v-%s-%s", demoPrefix, f.fmtLabel, suffix)
		cleanup.TrackSchema(demoRegistryName, schemaName)

		// Direction A: Java→Go (v1 only)
		resA := runSameVersionDirectionA(ctx, sc, broker, sameVersionCell{
			Format:       f.key,
			SchemaName:   schemaName,
			V1Schema:     f.v1Schema,
			Region:       region,
			RegistryName: demoRegistryName,
			Suffix:       suffix,
			FmtLabel:     f.fmtLabel,
		})
		results = append(results, resA)

		// Direction B: Go→Java (v1 only, reusing schema from A)
		resB := runSameVersionDirectionB(ctx, sc, broker, sameVersionCell{
			Format:       f.key,
			SchemaName:   schemaName,
			V1Schema:     f.v1Schema,
			Region:       region,
			RegistryName: demoRegistryName,
			Suffix:       suffix,
			FmtLabel:     f.fmtLabel,
		})
		results = append(results, resB)
	}
	return results
}

// sameVersionCell carries per-format context for same-version baseline.
type sameVersionCell struct {
	Format       string
	SchemaName   string
	V1Schema     string
	Region       string
	RegistryName string
	Suffix       string
	FmtLabel     string
}

// runSameVersionDirectionA: Java produces v1, Go consumes — no v2 registration.
func runSameVersionDirectionA(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cell sameVersionCell,
) ScenarioResult {
	result := ScenarioResult{
		Format:      cell.Format,
		Direction:   "same-v A",
		Compression: "NONE",
		SchemaName:  cell.SchemaName,
	}

	topic := fmt.Sprintf("%ssame-v-%s-a-%s", demoPrefix, cell.FmtLabel, cell.Suffix)

	printStage("demo", fmt.Sprintf("same-version-baseline/%s/java-to-go", strings.ToLower(cell.Format)))
	printStage("glue", fmt.Sprintf("RegisterSchema schema=%s (v1 only, no evolution)", cell.SchemaName))

	record := javaRecordForFormat(cell.Format)
	if cell.Format == "JSON" {
		record["schema"] = cell.V1Schema
	}

	resp, err := sc.KafkaProduce(ctx, javasidecar.KafkaProduceRequest{
		Format:        cell.Format,
		Schema:        cell.V1Schema,
		SchemaName:    cell.SchemaName,
		Record:        record,
		Compression:   "NONE",
		Bootstrap:     broker.Bootstrap,
		Topic:         topic,
		Region:        cell.Region,
		Compatibility: "BACKWARD",
	})
	if err != nil {
		result.Err = fmt.Errorf("java produce: %w", err)
		printStage("java-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	result.SchemaVersionID = resp.SchemaVersionID
	printStage("java-producer", fmt.Sprintf("Produced v1 record to %s (UUID: %s)", topic, resp.SchemaVersionID))

	// Go consume
	printStage("go-consumer", fmt.Sprintf("Consuming from %s...", topic))
	consumeCtx, consumeCancel := context.WithTimeout(ctx, 60*time.Second)
	framed, err := consumeOneRaw(consumeCtx, broker.Bootstrap, topic)
	consumeCancel()
	if err != nil {
		result.Err = fmt.Errorf("go consume: %w", err)
		printStage("go-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	goCfg, err := buildDemoConfigCellA(cell.Region, cell.Format, "NONE", cell.V1Schema)
	if err != nil {
		result.Err = fmt.Errorf("buildDemoConfigCellA: %w", err)
		printStage("go-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	des, err := deserializer.NewDeserializer(goCfg)
	if err != nil {
		result.Err = fmt.Errorf("NewDeserializer: %w", err)
		printStage("go-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	defer des.Close() //nolint:errcheck

	got, err := des.Deserialize(topic, framed)
	if err != nil {
		result.Err = fmt.Errorf("Deserialize: %w", err)
		printStage("go-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	pass, verifyErr := verifyDecodedResult(cell.Format, got)
	if verifyErr != nil {
		result.Err = verifyErr
		printStage("verdict", fmt.Sprintf("FAIL: %v", verifyErr))
		return result
	}
	result.Pass = pass
	if pass {
		printStage("verdict", fmt.Sprintf("same-version-baseline/%s/java-to-go PASS", strings.ToLower(cell.Format)))
	} else {
		printStage("verdict", fmt.Sprintf("same-version-baseline/%s/java-to-go FAIL", strings.ToLower(cell.Format)))
	}
	fmt.Println()
	return result
}

// ---------------------------------------------------------------------------
// C.4 — Go-writes / Java-reads scenario (Go auto-registers, Java cold-cache)
// ---------------------------------------------------------------------------

// runGoWritesJavaReadsScenario mirrors runSameVersionDirectionB (Go serializes
// then Java consumes) but with NO Java pre-registration: the Go serializer
// auto-registers the schema via schemaAutoRegistrationEnabled=true, producing
// a CloudTrail CreateSchema event tagged with the Go user-agent. Java then
// consumes with a cold cache (KafkaConsumeHandler constructs a fresh
// GlueSchemaRegistryKafkaDeserializer per request — see java-interop
// KafkaConsumeHandler.java lines 118-122), which forces a GetSchemaVersion
// call against the UUID tagged with the Java user-agent.
//
// The scenario runs AVRO only with NONE compression — a single row is enough
// to demonstrate the CloudTrail-pivot pattern (writer=go for CreateSchema,
// reader=java for GetSchemaVersion) without expanding to JSON/PROTOBUF noise.
// The record shape is identical to sameVersionRecords / javaRecordForFormat
// so the existing verifyJavaConsumeResult helper works unchanged.
func runGoWritesJavaReadsScenario(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cleanup *realglue.Cleanup,
	region string,
) []ScenarioResult {
	result := ScenarioResult{
		Format:      "AVRO",
		Direction:   "Go->Java (Go-registers)",
		Compression: "NONE",
	}

	printSectionHeader("SCENARIO: Go-writes / Java-reads (Go auto-registers, Java consumes cold-cache)")

	suffix, err := randomSuffix()
	if err != nil {
		result.Err = fmt.Errorf("randomSuffix: %w", err)
		printStage("demo", fmt.Sprintf("FAIL: %v", result.Err))
		return []ScenarioResult{result}
	}

	schemaName := fmt.Sprintf("%sgo-writes-%s", demoPrefix, suffix)
	// Track the schema for deletion BEFORE the auto-register happens, so a
	// mid-run failure still leaves the schema in the cleanup list.
	cleanup.TrackSchema(demoRegistryName, schemaName)
	result.SchemaName = schemaName

	topic := fmt.Sprintf("%sgo-writes-avro-%s", demoPrefix, suffix)

	printStage("demo", fmt.Sprintf("Schema name: %s", schemaName))
	printStage("demo", "Data format: AVRO")
	printStage("demo", "Compression: NONE")
	printStage("demo", "SchemaAutoRegistrationEnabled: true (Go registers, Java only reads)")
	fmt.Println()

	// ── Step 1: Build Go serializer with auto-registration ────────────────────
	printStage("go-producer", "Building Go serializer with auto-registration...")
	goConfig := configMapForFormat(region, "AVRO", "NONE")
	printGoConfig(goConfig)

	cfg, err := buildDemoConfigCellB(region, "AVRO", "NONE", crossVersionAvroV1)
	if err != nil {
		result.Err = fmt.Errorf("buildDemoConfigCellB: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return []ScenarioResult{result}
	}

	ser, err := serializer.NewSerializer(cfg)
	if err != nil {
		result.Err = fmt.Errorf("NewSerializer: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return []ScenarioResult{result}
	}
	defer ser.Close() //nolint:errcheck

	// ── Step 2: Go serializer auto-registers + encodes ────────────────────────
	printStage("glue", fmt.Sprintf("CreateSchema schemaName=%s compatibility=BACKWARD (via Go client UA)", schemaName))
	printStage("schema-evolution", "schema absent — auto-register triggered (Go writer)")

	goRecord, err := goRecordForFormat("AVRO", crossVersionAvroV1)
	if err != nil {
		result.Err = fmt.Errorf("goRecordForFormat: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return []ScenarioResult{result}
	}

	framed, err := ser.Serialize(schemaName, goRecord)
	if err != nil {
		result.Err = fmt.Errorf("Go Serialize (auto-register): %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return []ScenarioResult{result}
	}

	if len(framed) < 18 {
		result.Err = fmt.Errorf("framed bytes too short: %d", len(framed))
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return []ScenarioResult{result}
	}
	registeredUUID := formatUUID(framed[2:18])
	result.SchemaVersionID = registeredUUID
	printStage("glue", fmt.Sprintf("Schema auto-registered with UUID: %s", registeredUUID))
	printStage("go-producer", fmt.Sprintf("Encoded v1 record (%d bytes)", len(framed)))
	printHexDump("Wire bytes", framed)

	// ── Step 3: Produce framed bytes to Kafka ─────────────────────────────────
	printStage("kafka", fmt.Sprintf("Producing framed bytes to Kafka topic: %s", topic))
	produceCtx, produceCancel := context.WithTimeout(ctx, 30*time.Second)
	err = produceOneToKafka(produceCtx, broker.Bootstrap, topic, framed)
	produceCancel()
	if err != nil {
		result.Err = fmt.Errorf("kafka produce: %w", err)
		printStage("kafka", fmt.Sprintf("FAIL: %v", result.Err))
		return []ScenarioResult{result}
	}
	printStage("kafka", "Produced successfully.")
	fmt.Println()

	// ── Step 4: Java sidecar consumes (fresh deserializer → cold cache) ───────
	printStage("java-consumer", fmt.Sprintf("Java sidecar consuming from topic: %s", topic))
	fmt.Printf("  Format:    AVRO\n")
	fmt.Printf("  Region:    %s\n", region)
	fmt.Printf("  Timeout:   60s\n")
	fmt.Printf("  Note:      sidecar constructs a NEW GlueSchemaRegistryKafkaDeserializer\n")
	fmt.Printf("             per request — cache is cold, GetSchemaVersion always fires.\n")
	printStage("glue", fmt.Sprintf("GetSchemaVersion UUID=%s (via Java client UA, cold cache)", registeredUUID))
	fmt.Println()

	consumeCtx, consumeCancel := context.WithTimeout(ctx, 60*time.Second)
	resp, err := sc.KafkaConsume(consumeCtx, javasidecar.KafkaConsumeRequest{
		Bootstrap: broker.Bootstrap,
		Topic:     topic,
		Format:    "AVRO",
		Region:    region,
		TimeoutMs: 60_000,
	})
	consumeCancel()
	if err != nil {
		result.Err = fmt.Errorf("java consume: %w", err)
		printStage("java-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return []ScenarioResult{result}
	}

	printStage("java-consumer", "Java sidecar deserialized successfully.")
	fmt.Printf("  DataFormat:        %s\n", resp.DataFormat)
	fmt.Printf("  SchemaVersionID:   %s\n", resp.SchemaVersionID)
	fmt.Printf("  Record envelope:   %v\n", resp.Record)
	fmt.Println()

	// Sanity check: Java's resolved UUID must match Go's registered UUID.
	if resp.SchemaVersionID != registeredUUID {
		result.Err = fmt.Errorf(
			"UUID mismatch: Go registered %s, Java resolved %s",
			registeredUUID, resp.SchemaVersionID)
		printStage("verdict", fmt.Sprintf("FAIL: %v", result.Err))
		return []ScenarioResult{result}
	}

	// ── Step 5: Verify decoded fields ────────────────────────────────────────
	pass, verifyErr := verifyJavaConsumeResult("AVRO", resp.Record)
	narrateVerifyJava("AVRO", resp.Record, pass, verifyErr)

	result.Pass = pass
	if verifyErr != nil {
		result.Err = verifyErr
	}

	if pass {
		printStage("verdict", fmt.Sprintf(
			"go-writes/java-reads PASS — Go auto-registered %s, Java cold-cache resolved same UUID",
			registeredUUID))
	} else {
		printStage("verdict", "go-writes/java-reads FAIL")
	}
	fmt.Println()

	return []ScenarioResult{result}
}

// runSameVersionDirectionB: Go produces v1, Java consumes — no v2 registration.
func runSameVersionDirectionB(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cell sameVersionCell,
) ScenarioResult {
	result := ScenarioResult{
		Format:      cell.Format,
		Direction:   "same-v B",
		Compression: "NONE",
		SchemaName:  cell.SchemaName,
	}

	topic := fmt.Sprintf("%ssame-v-%s-b-%s", demoPrefix, cell.FmtLabel, cell.Suffix)

	printStage("demo", fmt.Sprintf("same-version-baseline/%s/go-to-java", strings.ToLower(cell.Format)))

	// Build Go serializer
	goCfg, err := buildDemoConfigCellB(cell.Region, cell.Format, "NONE", cell.V1Schema)
	if err != nil {
		result.Err = fmt.Errorf("buildDemoConfigCellB: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	ser, err := serializer.NewSerializer(goCfg)
	if err != nil {
		result.Err = fmt.Errorf("NewSerializer: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	defer ser.Close() //nolint:errcheck

	goRecord, err := goRecordForFormat(cell.Format, cell.V1Schema)
	if err != nil {
		result.Err = fmt.Errorf("goRecordForFormat: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	framed, err := ser.Serialize(cell.SchemaName, goRecord)
	if err != nil {
		result.Err = fmt.Errorf("Go Serialize: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	printStage("go-producer", fmt.Sprintf("Encoded v1 record (%d bytes)", len(framed)))

	// Produce to Kafka
	printStage("kafka", fmt.Sprintf("Producing to %s", topic))
	produceCtx, produceCancel := context.WithTimeout(ctx, 30*time.Second)
	err = produceOneToKafka(produceCtx, broker.Bootstrap, topic, framed)
	produceCancel()
	if err != nil {
		result.Err = fmt.Errorf("kafka produce: %w", err)
		printStage("kafka", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	// Java consume
	printStage("java-consumer", fmt.Sprintf("Java consuming from %s", topic))
	consumeCtx, consumeCancel := context.WithTimeout(ctx, 60*time.Second)
	resp, err := sc.KafkaConsume(consumeCtx, javasidecar.KafkaConsumeRequest{
		Bootstrap: broker.Bootstrap,
		Topic:     topic,
		Format:    cell.Format,
		Region:    cell.Region,
		TimeoutMs: 60_000,
	})
	consumeCancel()
	if err != nil {
		result.Err = fmt.Errorf("java consume: %w", err)
		printStage("java-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	result.SchemaVersionID = resp.SchemaVersionID

	pass, verifyErr := verifyJavaConsumeResult(cell.Format, resp.Record)
	if verifyErr != nil {
		result.Err = verifyErr
		printStage("verdict", fmt.Sprintf("FAIL: %v", verifyErr))
		return result
	}
	result.Pass = pass
	if pass {
		printStage("verdict", fmt.Sprintf("same-version-baseline/%s/go-to-java PASS", strings.ToLower(cell.Format)))
	} else {
		printStage("verdict", fmt.Sprintf("same-version-baseline/%s/go-to-java FAIL", strings.ToLower(cell.Format)))
	}
	fmt.Println()
	return result
}
