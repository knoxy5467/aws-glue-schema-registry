//go:build integration

package integration_tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
)

// Wire-format-only scenarios from plan §5.3 items 7-12.
//
// These do NOT require Kafka or AWS — they exercise EncodeWireFormat /
// DecodeWireFormat directly. Living under the //go:build integration
// tag because they're authored as part of the §5.3 matrix, but they
// don't require AWS_INTEGRATION=1 to run; they're the "compile gate
// works without billing AWS" proof every time `go build -tags
// integration ./...` succeeds.

// §5.3 item 7 — encode then decode produces byte-identical payload
// for each format. Format-aware tests live elsewhere; this one locks
// down the wire-format envelope contract, which is format-agnostic.
func TestWireFormat_EncodeDecodeRoundTrip(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"
	cases := []struct {
		name            string
		compressionByte byte
		payload         []byte
	}{
		{"none/short", gsrcore.CompressionByteNone, []byte("hello world")},
		{"zlib/short", gsrcore.CompressionByteZlib, []byte("hello world")},
		{"none/binary", gsrcore.CompressionByteNone, []byte{0x00, 0xff, 0x01, 0xfe}},
		{"zlib/empty", gsrcore.CompressionByteZlib, []byte{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			encoded, err := gsrcore.EncodeWireFormat(uuid, tc.compressionByte, tc.payload)
			require.NoError(t, err)

			gotUUID, gotByte, gotPayload, err := gsrcore.DecodeWireFormat(encoded)
			require.NoError(t, err)
			require.Equal(t, uuid, gotUUID)
			require.Equal(t, tc.compressionByte, gotByte)
			require.Equal(t, tc.payload, gotPayload)
		})
	}
}

// §5.3 item 8 — encode produces exactly the documented 18-byte prefix
// and the correct compression byte. Hardcoded byte assertions instead
// of round-trip because the prefix layout IS the contract.
func TestWireFormat_HeaderLayout(t *testing.T) {
	t.Parallel()
	const uuid = "00010203-0405-0607-0809-0a0b0c0d0e0f"
	encoded, err := gsrcore.EncodeWireFormat(uuid, gsrcore.CompressionByteZlib, []byte{0xAA, 0xBB})
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(encoded), gsrcore.WireFormatHeaderSize, "header must be present")
	require.Equal(t, byte(0x03), encoded[0], "version byte must be 0x03")
	require.Equal(t, byte(0x05), encoded[1], "compression byte must be 0x05 for ZLIB")
	// UUID is bytes 2..17, big-endian, MSB first.
	wantUUID := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}
	require.Equal(t, wantUUID, encoded[2:gsrcore.WireFormatHeaderSize])
	require.Equal(t, []byte{0xAA, 0xBB}, encoded[gsrcore.WireFormatHeaderSize:])
}

// §5.3 item 9 — ZLIB-compressed payload is smaller than NONE for
// sufficiently large messages. "Sufficiently large" means a payload
// where header overhead doesn't swallow the gain; we use 1KB of a
// repeating byte so zlib has obvious compression headroom.
func TestWireFormat_ZlibSmallerThanNone(t *testing.T) {
	t.Parallel()
	const uuid = "deadbeef-dead-beef-dead-beefdeadbeef"
	bigPayload := make([]byte, 1024)
	for i := range bigPayload {
		bigPayload[i] = 'A'
	}

	compressed, err := gsrcore.ZlibCompressionHandler{}.Compress(bigPayload)
	require.NoError(t, err)

	noneEncoded, err := gsrcore.EncodeWireFormat(uuid, gsrcore.CompressionByteNone, bigPayload)
	require.NoError(t, err)
	zlibEncoded, err := gsrcore.EncodeWireFormat(uuid, gsrcore.CompressionByteZlib, compressed)
	require.NoError(t, err)

	require.Less(t, len(zlibEncoded), len(noneEncoded),
		"zlib-compressed wire payload (%d bytes) should be smaller than uncompressed (%d bytes)",
		len(zlibEncoded), len(noneEncoded))
}

// §5.3 item 10 — decode rejects a payload whose first byte is not
// 0x03. The current core surface returns ErrIncompatibleData wrapped
// via fmt.Errorf; the integration assertion is that callers see a
// non-nil error rather than the byte being mistakenly accepted.
func TestWireFormat_DecodeRejectsBadVersion(t *testing.T) {
	t.Parallel()
	bad := make([]byte, gsrcore.WireFormatHeaderSize+1)
	bad[0] = 0x02 // wrong version
	_, _, _, err := gsrcore.DecodeWireFormat(bad)
	require.Error(t, err)
}

// §5.3 item 11 (negative half) — decode rejects unknown compression
// byte. The "delegates to secondary deserializer" part of item 11 is
// scenario-level behavior the Phase-1 core/ surface doesn't expose
// yet (secondary deserializer is in the §2.2 "missing entirely"
// bucket); this test locks down the negative branch in the meantime
// so a future secondary-deserializer implementation has a clear
// boundary.
func TestWireFormat_DecodeRejectsBadCompressionByte(t *testing.T) {
	t.Parallel()
	bad := make([]byte, gsrcore.WireFormatHeaderSize)
	bad[0] = 0x03
	bad[1] = 0x99 // not in {0x00, 0x05}
	_, _, _, err := gsrcore.DecodeWireFormat(bad)
	require.Error(t, err)
}

// §5.3 item 12 — Java golden-byte parity. The authoritative fixtures
// live in pkg/gsrserde-go/core/testdata/golden/ and are exercised by
// the core/ unit tests. Mirror at the integration level so a
// contributor running ONLY the integration suite catches a fixture
// deletion / rename. An earlier draft of this test was a t.Log-only
// sentinel; the review correctly flagged that a no-assertion test is
// worse than a missing test because it falsely contributes to the
// green count. Now we stat the fixture files so deleting them turns
// THIS test red.
func TestWireFormat_JavaGoldenByteParityPresence(t *testing.T) {
	t.Parallel()
	// Path is relative to the test's cwd (Go sets cwd to the package
	// dir, i.e. integration-tests/tests). Two levels up reaches the
	// outer-module root, then into pkg/gsrserde-go/core/testdata.
	root := filepath.Join("..", "..", "pkg", "gsrserde-go", "core", "testdata", "golden")
	required := []string{
		"wire-only__none__fixed-uuid__hello.bin",
		"wire-only__zlib__fixed-uuid__hello.bin",
		"README.md",
	}
	for _, name := range required {
		path := filepath.Join(root, name)
		info, err := os.Stat(path)
		require.NoError(t, err, "§5.3 item 12 fixture %q missing — Java golden-byte parity coverage is broken", path)
		require.False(t, info.IsDir(), "§5.3 item 12 fixture %q is a directory, expected a file", path)
		require.Greater(t, info.Size(), int64(0), "§5.3 item 12 fixture %q is empty", path)
	}
}
