package gsrserde

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// updateGolden, when set, makes AssertGoldenBytes overwrite the fixture
// instead of asserting against it. Standard Mitchell Hashimoto pattern;
// invoked as `go test -update-golden ./...` once the Java fixture
// generator (§5.5) lands and produces authoritative bytes.
var updateGolden = flag.Bool("update-golden", false,
	"overwrite golden fixtures under testdata/golden/ with the actual bytes (use only after running the Java fixture generator)")

// AssertGoldenBytes loads testdata/golden/<scenario>.bin and compares it
// byte-for-byte against actual. On mismatch it fails the test with a
// hex-dump-friendly diff message.
//
// This helper exists so:
//
//   - Every golden test gets the same -update-golden semantics for free.
//   - The fixture path is computed from the scenario name (no path
//     literals scattered across test files).
//   - Tests run as Tier-1 (no AWS, no Docker) — only the filesystem.
//
// scenario must be the basename only (no directory, no .bin). The
// helper prepends `testdata/golden/` and appends `.bin` so callers
// can't accidentally reference fixtures outside the directory.
func AssertGoldenBytes(t *testing.T, scenario string, actual []byte) {
	t.Helper()

	path := filepath.Join("testdata", "golden", scenario+".bin")

	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("AssertGoldenBytes: mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, actual, 0o644); err != nil {
			t.Fatalf("AssertGoldenBytes: write %s: %v", path, err)
		}
		t.Logf("AssertGoldenBytes: updated %s (%d bytes)", path, len(actual))
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("AssertGoldenBytes: read %s: %v (run with -update-golden if this is a new scenario backed by the Java generator)", path, err)
	}

	if !bytes.Equal(actual, want) {
		t.Fatalf("AssertGoldenBytes: %s mismatch.\nwant (%d bytes):\n  % x\n got (%d bytes):\n  % x",
			scenario, len(want), want, len(actual), actual)
	}
}
