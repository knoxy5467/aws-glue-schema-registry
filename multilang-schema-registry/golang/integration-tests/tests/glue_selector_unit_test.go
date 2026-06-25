//go:build integration

package integration_tests

import (
	"strings"
	"testing"
)

// TestSelector_DefaultIsFake_NoGSRGlueEnv proves the default behavior:
// the integration suite must not bill AWS unless the user explicitly
// opts in.
func TestSelector_DefaultIsFake_NoGSRGlueEnv(t *testing.T) {
	t.Setenv("GSR_GLUE", "")
	h := newGlueHandle(t)
	if h.Mode != glueModeFake {
		t.Fatalf("default mode = %q, want %q", h.Mode, glueModeFake)
	}
	if h.Fake == nil {
		t.Fatalf("Fake must be non-nil when mode is fake")
	}
	if h.Real != nil || h.Cleanup != nil {
		t.Fatalf("Real / Cleanup must be nil when mode is fake")
	}
}

// TestSelector_GSRGlueFake_ExplicitlyFake exercises the explicit
// GSR_GLUE=fake path so a future "default to real" regression
// surfaces immediately.
func TestSelector_GSRGlueFake_ExplicitlyFake(t *testing.T) {
	t.Setenv("GSR_GLUE", "fake")
	h := newGlueHandle(t)
	if h.Mode != glueModeFake {
		t.Fatalf("GSR_GLUE=fake should resolve to mode fake, got %q", h.Mode)
	}
}

// TestSelector_GSRGlueReal_WithoutAWSIntegration_Fatals is the
// critical safety property: GSR_GLUE=real WITHOUT AWS_INTEGRATION=1
// must hard-fail rather than silently fall back to fake (which would
// be a misleading green) and must hard-fail rather than actually
// reach for AWS (which could bill against the dev host's creds).
//
// testing.T.Fatal cannot be intercepted by the same testing.T, so we
// drive the assertion via a fresh subtest and inspect its outcome
// after the call returns.
func TestSelector_GSRGlueReal_WithoutAWSIntegration_Fatals(t *testing.T) {
	t.Setenv("GSR_GLUE", "real")
	t.Setenv("AWS_INTEGRATION", "")

	captured := newCapturingT(t)
	func() {
		defer func() {
			_ = recover() // newGlueHandle exits via runtime.Goexit; recover so the parent doesn't see it
		}()
		newGlueHandle(captured)
	}()
	if !captured.failed {
		t.Fatalf("newGlueHandle: GSR_GLUE=real + AWS_INTEGRATION!=1 must Fail; did not")
	}
	if !strings.Contains(captured.log, "AWS_INTEGRATION=1") {
		t.Fatalf("expected fatal message to mention AWS_INTEGRATION=1, got: %q", captured.log)
	}
}

// TestSelector_GSRGlueUnknown_Fatals — typo guard. A misspelled value
// like "REAL" or "rael" must not silently degrade.
func TestSelector_GSRGlueUnknown_Fatals(t *testing.T) {
	t.Setenv("GSR_GLUE", "rael")

	captured := newCapturingT(t)
	func() {
		defer recoverCapturingFatal(t)
		newGlueHandle(captured)
	}()
	if !captured.failed {
		t.Fatalf("newGlueHandle: GSR_GLUE=rael must Fail; did not")
	}
	if !strings.Contains(captured.log, "rael") {
		t.Fatalf("expected fatal message to mention the unrecognized value, got: %q", captured.log)
	}
}

// TestScenarioGate_BothFlagsFatals — sanity check on the gate itself.
// Setting both requiresReal and requiresFake to true is a programmer
// error and must surface immediately.
func TestScenarioGate_BothFlagsFatals(t *testing.T) {
	t.Setenv("GSR_GLUE", "fake")
	captured := newCapturingT(t)
	func() {
		defer recoverCapturingFatal(t)
		scenarioGate(captured, true, true)
	}()
	if !captured.failed {
		t.Fatalf("scenarioGate: both flags must Fail; did not")
	}
}

// TestScenarioGate_RequiresRealSkipsOnFake pins the documented
// behavior: a test that requires real Glue is skipped (not failed)
// when running against the fake.
func TestScenarioGate_RequiresRealSkipsOnFake(t *testing.T) {
	t.Setenv("GSR_GLUE", "fake")
	captured := newCapturingT(t)
	func() {
		defer recoverCapturingFatal(t)
		scenarioGate(captured, true, false)
	}()
	if !captured.skipped {
		t.Fatalf("scenarioGate(requiresReal=true) on fake should Skip; did not")
	}
}

// TestScenarioGate_RequiresFakeSkipsOnReal — the symmetric case so a
// test that uses Force* affordances doesn't fail noisily against
// real Glue.
func TestScenarioGate_RequiresFakeSkipsOnReal(t *testing.T) {
	t.Setenv("GSR_GLUE", "real")
	captured := newCapturingT(t)
	func() {
		defer recoverCapturingFatal(t)
		scenarioGate(captured, false, true)
	}()
	if !captured.skipped {
		t.Fatalf("scenarioGate(requiresFake=true) on real should Skip; did not")
	}
}

// TestRandomGlueName_HasRunbookPrefix pins the "gsr-go-it-" prefix
// REAL-AWS-RUNBOOK.md uses for post-run leak detection. If the prefix
// drifts, the cleanup-verification command in the runbook would miss
// leaked resources.
func TestRandomGlueName_HasRunbookPrefix(t *testing.T) {
	got := randomGlueName(t, "scenario-name")
	if !strings.HasPrefix(got, "gsr-go-it-") {
		t.Fatalf("randomGlueName must start with 'gsr-go-it-' (runbook contract), got: %q", got)
	}
}

// TestRandomGlueName_IsUnique exercises the collision-resistance
// property — two consecutive calls must return distinct strings.
// 32 bits of suffix + UNIX_TS means real collisions take 2^16 calls
// per second to start risking birthday collisions; two calls cannot.
func TestRandomGlueName_IsUnique(t *testing.T) {
	a := randomGlueName(t, "same-base")
	b := randomGlueName(t, "same-base")
	if a == b {
		t.Fatalf("randomGlueName must return distinct strings on consecutive calls, got %q twice", a)
	}
}
