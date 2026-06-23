//go:build integration

// Phase 4.6 Java <-> Go interop: Go encodes, Java decodes.
//
// Each subtest exercises the §5.2 matrix axes (format × compression).
// Auto-register / compatibility / Kafka-client axes are out of scope
// here: this file proves wire-format parity, not orchestration parity.
// The Kafka-in-the-loop variant from §5.5 is intentionally deferred —
// the sidecar doesn't expose a /kafka-produce endpoint and adding one
// roughly doubles the sidecar surface area for one extra test cell.
//
// Gated by //go:build integration AND requireInterop, which skips when
// `java` isn't on PATH and we're not in container mode.

package integration_tests

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/javasidecar"
)

// requireInterop skips the test when local mode is selected (default) and
// the JDK binary the launcher would use is not invokable. Container mode
// is allowed regardless because the JVM runs inside a container, not on
// the host.
//
// The launcher reads Options.JavaBinary (default "java") rather than
// always going through `java` on PATH, so the probe uses the same name
// resolution to avoid wrongly skipping when a non-default binary is
// configured.
func requireInterop(t *testing.T) {
	t.Helper()
	mode, err := javasidecar.ModeFromEnv()
	if err != nil {
		t.Skipf("javasidecar: %v", err)
	}
	if mode != javasidecar.ModeLocal {
		return
	}
	bin := os.Getenv("GSR_INTEROP_JAVA")
	if bin == "" {
		bin = "java"
	}
	if _, err := exec.LookPath(bin); err != nil {
		t.Skipf("javasidecar: local mode requires %q on PATH (set GSR_INTEROP_MODE=container or GSR_INTEROP_JAVA=<path> to override)", bin)
	}
}

// startInteropSidecar wires the launcher to t.Cleanup. Reused across the
// interop_*_test.go files.
func startInteropSidecar(ctx context.Context, t *testing.T) *javasidecar.Sidecar {
	t.Helper()
	requireInterop(t)
	return javasidecar.StartForTest(ctx, t, javasidecar.Options{
		StartTimeout: 30 * time.Second,
	})
}

// interopCase covers one (format × compression) cell. The §5.2 matrix's
// record-type axis (generic vs specific Avro etc.) doesn't apply at this
// layer because the sidecar's contract is the wire-format envelope, not
// the format-specific record body — the test pre-serializes its own
// payload bytes (a stand-in for whatever the format produces) and asserts
// they round-trip byte-for-byte.
type interopCase struct {
	name        string
	format      string
	schema      string
	schemaName  string
	uuid        string
	compression string
}

// matrix flattens the §5.2 (format × compression) axes that this layer
// can exercise. Auto-register / pre-registered split is irrelevant here:
// the sidecar never calls Glue, so there's no "register" to flip.
var matrix = []interopCase{
	{name: "AVRO/NONE", format: "AVRO", schema: avroSchema, schemaName: "avro-interop", uuid: "11111111-1111-1111-1111-111111111111", compression: "NONE"},
	{name: "AVRO/ZLIB", format: "AVRO", schema: avroSchema, schemaName: "avro-interop", uuid: "22222222-2222-2222-2222-222222222222", compression: "ZLIB"},
	{name: "JSON/NONE", format: "JSON", schema: jsonSchema, schemaName: "json-interop", uuid: "33333333-3333-3333-3333-333333333333", compression: "NONE"},
	{name: "JSON/ZLIB", format: "JSON", schema: jsonSchema, schemaName: "json-interop", uuid: "44444444-4444-4444-4444-444444444444", compression: "ZLIB"},
	{name: "PROTOBUF/NONE", format: "PROTOBUF", schema: protoSchema, schemaName: "proto-interop", uuid: "55555555-5555-5555-5555-555555555555", compression: "NONE"},
	{name: "PROTOBUF/ZLIB", format: "PROTOBUF", schema: protoSchema, schemaName: "proto-interop", uuid: "66666666-6666-6666-6666-666666666666", compression: "ZLIB"},
}

const (
	avroSchema  = `{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}`
	jsonSchema  = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}`
	protoSchema = `syntax = "proto3"; message M { string v = 1; }`
)

// TestInterop_GoEncode_JavaDecode is plan §5.3 item 12, direction 1.
//
// For each matrix cell:
//  1. We pre-seed the sidecar's in-memory schema store with the chosen
//     UUID by hitting /encode once with a throwaway payload. This is
//     necessary because /decode returns null schema metadata for UUIDs
//     it has never seen, and the test asserts on schemaName /
//     schemaDefinition / dataFormat below.
//  2. The Go core (gsrserde.EncodeWireFormat + ZLIB handler) produces a
//     framed byte string.
//  3. We POST that to the Java sidecar's /decode.
//  4. We assert that Java extracts (a) our exact payload bytes back,
//     (b) our exact UUID, and (c) the schema metadata we seeded —
//     proving the GSR wire-format envelope parses identically on both
//     sides and that the in-memory store round-trips.
func TestInterop_GoEncode_JavaDecode(t *testing.T) {
	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	sc := startInteropSidecar(startCtx, t)

	for _, tc := range matrix {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Each parallel subtest owns its own context. Sharing the
			// parent's context here would canceling once the outer test
			// function returns (subtests pause behind t.Parallel and run
			// later, so the parent's defer cancel() races them).
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			payload := []byte(strings.Repeat("interop-payload-", 4) + tc.name)
			compressionByte := compressionByteFor(tc.compression)

			payloadToFrame := payload
			if compressionByte == gsrcore.CompressionByteZlib {
				zh := gsrcore.ZlibCompressionHandler{}
				compressed, err := zh.Compress(payload)
				require.NoError(t, err)
				payloadToFrame = compressed
			}

			framed, err := gsrcore.EncodeWireFormat(tc.uuid, compressionByte, payloadToFrame)
			require.NoError(t, err)

			// Pre-seed the sidecar's in-memory store via /encode so the
			// /decode response below carries schema metadata. Without
			// this seed the test could still assert payload + UUID, but
			// would have to settle for nil schemaName/schemaDefinition.
			_, err = sc.Encode(ctx, javasidecar.EncodeRequest{
				Format:          tc.format,
				Schema:          tc.schema,
				SchemaName:      tc.schemaName,
				SchemaVersionID: tc.uuid,
				Payload:         []byte("seed"),
				Compression:     "NONE",
			})
			require.NoError(t, err)

			decoded, err := sc.Decode(ctx, framed)
			require.NoError(t, err)

			require.Equal(t, payload, decoded.Payload,
				"Java-decoded payload must equal the original (pre-compression) Go payload")
			require.Equal(t, tc.uuid, decoded.SchemaVersionID)
			require.Equal(t, tc.schemaName, decoded.SchemaName)
			require.Equal(t, tc.schema, decoded.SchemaDefinition)
			require.Equal(t, tc.format, decoded.DataFormat)
		})
	}
}

func compressionByteFor(compression string) byte {
	if strings.EqualFold(compression, "ZLIB") {
		return gsrcore.CompressionByteZlib
	}
	return gsrcore.CompressionByteNone
}

