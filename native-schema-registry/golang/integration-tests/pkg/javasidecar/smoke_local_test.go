//go:build integration

// Real-JVM smoke check that spawns the actual sidecar JAR. This is the
// minimum signal that the launcher correctly drives the Java sidecar; the
// matrix-driven Go-encode / Java-decode and reverse tests live in
// integration-tests/tests/interop_*_test.go and exercise the harness
// across the §5.2 axes.
//
// Skipped (not failed) when GSR_INTEROP_SMOKE!=1 so a plain
// `go test -tags integration` doesn't always require a built JAR.

package javasidecar

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestRealJVM_HealthAndRoundTrip(t *testing.T) {
	if os.Getenv("GSR_INTEROP_SMOKE") != "1" {
		t.Skip("set GSR_INTEROP_SMOKE=1 to opt in to the real-JVM smoke check")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mode := ModeLocal
	sc := StartForTest(ctx, t, Options{
		Mode:         &mode,
		StartTimeout: 30 * time.Second,
	})

	uuid := "12345678-1234-5678-1234-567812345678"
	payload := []byte("Hello from the smoke test")

	framed, err := sc.Encode(ctx, EncodeRequest{
		Format:          "AVRO",
		Schema:          `{"type":"string"}`,
		SchemaName:      "smoke-schema",
		SchemaVersionID: uuid,
		Payload:         payload,
		Compression:     "NONE",
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// 18-byte header expected before the raw payload.
	if len(framed) != 18+len(payload) {
		t.Fatalf("framed length = %d, want %d", len(framed), 18+len(payload))
	}
	if framed[0] != 0x03 {
		t.Errorf("header[0] = 0x%02x, want 0x03 (version)", framed[0])
	}
	if framed[1] != 0x00 {
		t.Errorf("header[1] = 0x%02x, want 0x00 (compression NONE)", framed[1])
	}

	decoded, err := sc.Decode(ctx, framed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if string(decoded.Payload) != string(payload) {
		t.Errorf("payload mismatch: got %q want %q", decoded.Payload, payload)
	}
	if decoded.SchemaVersionID != uuid {
		t.Errorf("schemaVersionId = %q want %q", decoded.SchemaVersionID, uuid)
	}
	if decoded.SchemaName != "smoke-schema" {
		t.Errorf("schemaName = %q want smoke-schema", decoded.SchemaName)
	}
}
