//go:build integration

package integration_tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
)

// Phase 4 — §5.3 scenario harness.
//
// The legacy testify/suite-based files in this directory predate the
// §5.3 expansion and are kept as-is. New §5.3 scenarios use the plain
// `testing.T.Run` pattern below so they can opt into t.Parallel()
// without tripping over the testify/suite + Parallel races flagged in
// the plan's §9 risk table.
//
// Every scenario function in the new files:
//
//   1. Calls requireAWSIntegration(t) so a plain `go test -tags integration`
//      run skips them.
//   2. Uses scenarioTopicName(t) for a deterministic-but-collision-free
//      topic so parallel runs against the same Kafka broker never
//      cross-talk.
//   3. Builds its serializer/deserializer with a per-scenario suffix on
//      the schema name so concurrent first-encode races aren't
//      cross-scenario.

// requireAWSIntegration skips t unless AWS_INTEGRATION=1. The
// //go:build integration tag is the first gate; this is the second.
// Tests that exercise the wire format directly (no Glue, no Kafka)
// SHOULD NOT call this — the whole point of those tests is that they
// run as Tier-1 once the integration tag is on.
func requireAWSIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("AWS_INTEGRATION") != "1" {
		t.Skip("AWS_INTEGRATION!=1; skipping Glue-billing scenario")
	}
}

// scenarioTopicName returns a topic name unique to this test plus an
// 8-hex-char random suffix. Same shape as the legacy
// generateTestTopicName but tied to a *testing.T instead of a suite
// receiver.
//
// Kafka topic names must match `[a-zA-Z0-9._-]+`. t.Name() can carry
// '/', ' ', and — for table-driven subtests with `key=value` names
// — '=', plus arbitrary punctuation. Map every non-conforming
// character to '-' so the matrix's t.Run names ('fmt=avro/comp=NONE')
// produce broker-acceptable topics rather than silently failing every
// subtest with InvalidTopicException.
func scenarioTopicName(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("phase4-%s-%x", sanitizeKafkaTopicSegment(t.Name()), buf)
}

// sanitizeKafkaTopicSegment maps any character outside Kafka's topic
// regex [a-zA-Z0-9._-] to '-'. Exported as a helper so other places
// in the suite that build topic names can stay consistent.
func sanitizeKafkaTopicSegment(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
			out = append(out, c)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

// scenarioRegistrySuffix randomizes the schema-name suffix used by a
// scenario. Glue allows up to 255 chars; we keep our use well under
// that. Returns "" when the test should reuse the existing schema
// name (e.g. compatibility tests that publish v1 then v2).
func scenarioRegistrySuffix() string {
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("-%x", buf)
}

// scenarioCtx returns a context with a generous-but-bounded timeout
// per scenario. 90s is enough for Kafka publish + AWS round-trip
// even on slow CI; longer-running scenarios should declare their own
// timeout.
func scenarioCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), defaultScenarioCtxTimeout)
}
