//go:build integration

package integration_tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/fakeglue"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/realglue"
	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
)

// glueMode describes which backend glueHandle is wired to.
//
// "fake" is the default and the value when GSR_GLUE is unset; "real"
// is opt-in via GSR_GLUE=real. Any other value hard-fails — silent
// fall-back to fakeglue when the user typed "rael" or "REAL" would
// hide a billing test that didn't actually exercise real Glue.
type glueMode string

const (
	glueModeFake glueMode = "fake"
	glueModeReal glueMode = "real"
)

// glueHandle is the per-test handle returned by newGlueHandle. It
// holds the gsrcore.GlueClient that scenarios pass into the encoder
// / decoder constructors, plus mode-specific affordances:
//
//   - Fake is non-nil iff Mode == glueModeFake. Tests reach for it
//     to set ForceXxxError or read CallCounts.
//   - Real and Cleanup are non-nil iff Mode == glueModeReal. Tests
//     register schema / registry names via Cleanup.TrackSchema /
//     TrackRegistry so teardown deletes them at t.Cleanup time.
type glueHandle struct {
	Mode    glueMode
	Client  gsrcore.GlueClient
	Fake    *fakeglue.Fake
	Real    *realglue.Real
	Cleanup *realglue.Cleanup
}

// newGlueHandle reads GSR_GLUE and builds a handle. It is the SINGLE
// seam every §5.3 scenario goes through to acquire a GlueClient. The
// helper hard-fails (t.Fatalf) rather than silently falls back when:
//
//  1. GSR_GLUE=real but AWS_INTEGRATION != 1.
//  2. GSR_GLUE=real but realglue.New errors (config / creds).
//  3. GSR_GLUE is set to anything other than "", "fake", or "real".
func newGlueHandle(t testing.TB) *glueHandle {
	t.Helper()
	mode := resolveGlueMode(t)
	switch mode {
	case glueModeFake:
		f := fakeglue.New()
		return &glueHandle{Mode: glueModeFake, Client: f, Fake: f}
	case glueModeReal:
		if os.Getenv("AWS_INTEGRATION") != "1" {
			t.Fatalf("newGlueHandle: GSR_GLUE=real requires AWS_INTEGRATION=1 (refusing to silently bill or silently fake)")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		r, err := realglue.New(ctx)
		if err != nil {
			t.Fatalf("newGlueHandle: realglue.New: %v", err)
		}
		c := r.NewCleanup()
		// Wire Run into t.Cleanup so teardown fires on t.Fail too.
		//
		// Phase 4.8 nit #15: escalate cleanup failures from t.Logf to
		// t.Errorf. The earlier Logf treated leaked schemas as
		// informational, which (combined with the original
		// pre-fix-#1 broken leak-check command) meant leaks could
		// accumulate across runs with NO red signal. Now the leak
		// shows up as a failing test immediately, and the runbook
		// gives the user a deterministic recovery path.
		// EntityNotFoundException is already suppressed inside
		// Cleanup.Run, so this only escalates real cleanup failures.
		t.Cleanup(func() {
			tdCtx, tdCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer tdCancel()
			if err := c.Run(tdCtx); err != nil {
				t.Errorf("realglue.Cleanup.Run: %v — manual cleanup required, see integration-tests/REAL-AWS-RUNBOOK.md", err)
			}
		})
		return &glueHandle{Mode: glueModeReal, Client: r, Real: r, Cleanup: c}
	}
	t.Fatalf("newGlueHandle: unreachable mode %q", mode)
	return nil
}

// resolveGlueMode parses GSR_GLUE. Surfaces unrecognized values via
// t.Fatalf so a typo doesn't degrade silently.
func resolveGlueMode(t testing.TB) glueMode {
	t.Helper()
	v := os.Getenv("GSR_GLUE")
	switch v {
	case "", "fake":
		return glueModeFake
	case "real":
		return glueModeReal
	}
	t.Fatalf("newGlueHandle: GSR_GLUE=%q is not recognized (want \"\", \"fake\", or \"real\")", v)
	return ""
}

// scenarioGate is the RequiresRealGlue / RequiresFakeGlue annotation.
// Tests call this at the top of the body. Either flag can be true
// (skip-when-not-matching); never both true.
func scenarioGate(t testing.TB, requiresReal, requiresFake bool) {
	t.Helper()
	if requiresReal && requiresFake {
		t.Fatalf("scenarioGate: a test cannot require both real AND fake Glue")
	}
	mode := resolveGlueMode(t)
	if requiresReal && mode != glueModeReal {
		t.Skipf("scenario requires real Glue; GSR_GLUE=%q", mode)
	}
	if requiresFake && mode != glueModeFake {
		t.Skipf("scenario requires fakeglue affordances (Force* / Count); GSR_GLUE=%q", mode)
	}
}

// randomGlueName returns "gsr-go-it-<base>-<UNIX_TS>-<16 hex chars>"
// (8 random bytes → 64 bits of entropy after the per-second timestamp).
// Used by every §5.3 scenario that goes through newGlueHandle so
// retries and parallel runs against the same real-Glue account
// never collide.
//
// Phase 4.8 nit #10: earlier draft documented "8 hex bytes" but the
// implementation only read 4 bytes ([4]byte → 8 hex chars / 32 bits).
// 32 bits is enough for any realistic per-second collision budget,
// but the documented promise didn't match. Bumped the read to 8
// bytes so a future canary-style continuous loop inherits the
// stronger guarantee the comment implied.
//
// The "gsr-go-it-" prefix is significant: it is matched by the
// post-run leak check in integration-tests/REAL-AWS-RUNBOOK.md
// ("Post-run cleanup verification" section). If you change the
// prefix here, update that command in lock-step.
func randomGlueName(t testing.TB, base string) string {
	t.Helper()
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return fmt.Sprintf("gsr-go-it-%s-%d-%x", base, time.Now().Unix(), buf[:])
}
