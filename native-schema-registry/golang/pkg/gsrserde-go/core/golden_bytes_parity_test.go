package gsrserde

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// goldenFixedUUID is the canonical UUID used across all golden-byte
// fixtures so diffs across fixtures are never attributable to UUID
// changes (see testdata/golden/README.md).
const goldenFixedUUID = "01020304-0506-0708-090a-0b0c0d0e0f10"

// TestGolden_WireOnly_NoCompression locks down the bare wire-format
// header bytes against testdata/golden/wire-only__none__fixed-uuid__hello.bin.
// This is the smallest possible fixture — no format-layer encoder
// involved, just (version=0x03, compression=0x00, UUID, "Hello") — and
// it runs under `go test` without the integration tag, so the wire
// format contract is locked down at unit-test time.
//
// Plan parity: §5.5 ("Java-byte equivalence"). Hand-crafted today;
// becomes authoritative once the Java fixture generator (§5.5 step 1)
// lands and produces byte-identical output.
func TestGolden_WireOnly_NoCompression(t *testing.T) {
	got, err := EncodeWireFormat(goldenFixedUUID, CompressionByteNone, []byte("Hello"))
	require.NoError(t, err)

	AssertGoldenBytes(t, "wire-only__none__fixed-uuid__hello", got)
}

// TestGolden_WireOnly_Zlib does the same for the zlib path. The zlib
// fixture's payload bytes are computed by the Go zlib writer at
// level 6 (Z_DEFAULT_COMPRESSION) so the bytes in the .bin file
// reflect what the Go side actually produces. When the Java fixture
// generator lands, mismatches here will surface zlib-level drift
// (the §9 risk row tracks this).
func TestGolden_WireOnly_Zlib(t *testing.T) {
	compressed, err := ZlibCompressionHandler{}.Compress([]byte("Hello"))
	require.NoError(t, err)

	got, err := EncodeWireFormat(goldenFixedUUID, CompressionByteZlib, compressed)
	require.NoError(t, err)

	AssertGoldenBytes(t, "wire-only__zlib__fixed-uuid__hello", got)
}
