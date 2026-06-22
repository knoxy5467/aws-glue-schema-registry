//go:build integration

package integration_tests

import (
	"fmt"
	"testing"
)

// capturingT is a minimal testing.TB stand-in used by the selector
// unit tests. The real *testing.T cannot be passed to a function
// whose contract is "Fatalf on bad input" because Fatalf calls
// runtime.Goexit on the test goroutine — there is no in-test way to
// observe that the function fataled.
//
// capturingT.Fatalf instead panics with a sentinel; tests defer a
// recover() to catch it and assert on the recorded state. The panic
// is the *only* way to short-circuit the function under test the same
// way Fatalf would, without bringing in goroutine-per-call plumbing.
type capturingT struct {
	testing.TB
	failed  bool
	skipped bool
	log     string
}

type capturingFatalSentinel struct{ msg string }

func newCapturingT(parent testing.TB) *capturingT {
	return &capturingT{TB: parent}
}

func (c *capturingT) Helper() {}

func (c *capturingT) Errorf(format string, args ...any) {
	c.failed = true
	c.log += fmt.Sprintf(format, args...)
}

func (c *capturingT) Fatalf(format string, args ...any) {
	c.failed = true
	msg := fmt.Sprintf(format, args...)
	c.log += msg
	panic(capturingFatalSentinel{msg: msg})
}

func (c *capturingT) Fatal(args ...any) {
	c.failed = true
	msg := fmt.Sprint(args...)
	c.log += msg
	panic(capturingFatalSentinel{msg: msg})
}

func (c *capturingT) Skip(args ...any) {
	c.skipped = true
	c.log += fmt.Sprint(args...)
	panic(capturingFatalSentinel{msg: "skip"})
}

func (c *capturingT) Skipf(format string, args ...any) {
	c.skipped = true
	c.log += fmt.Sprintf(format, args...)
	panic(capturingFatalSentinel{msg: "skip"})
}

func (c *capturingT) Logf(format string, args ...any) {
	c.log += fmt.Sprintf(format, args...)
}

func (c *capturingT) Cleanup(_ func()) {} // no-op; tests assert directly on state
