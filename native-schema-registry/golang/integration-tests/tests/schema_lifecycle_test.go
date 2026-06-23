//go:build integration

package integration_tests

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
)

// Schema-lifecycle scenarios from plan §5.3 items 13-17.
//
// These drive the core encoder/decoder against the in-memory
// fakeglue.Fake — no AWS billing, no Kafka — so they verify the
// cache/auto-register/version-id flows that the production
// orchestrator depends on. Living under //go:build integration
// because they belong to the §5.3 row set; they DO NOT require
// AWS_INTEGRATION=1 (the whole point is to lock down the cache
// behavior without billing AWS).

const (
	avroSchemaLifecycle = `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`
)

// §5.3 item 13 — auto-register a new schema; subsequent encode reuses
// the cached version-id. Asserted by counting CreateSchema /
// GetSchemaByDefinition invocations on the fake.
//
// Phase 4.7: gated requiresFake=true. The assertion shape (exact
// CallCounts on the GlueClient) only makes sense against fakeglue;
// real Glue has no equivalent introspection. A companion
// requiresReal=true test would assert the behavior via a separate
// route (e.g. wire-format prefix bytes equal across calls) — that
// companion is fmt:lifecycle_real_test.go in the same package.
func TestLifecycle_AutoRegister_ThenCacheReuse(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "BACKWARD",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "lifecycle-13")
	schema := &gsrcore.Schema{
		SchemaDefinition: avroSchemaLifecycle,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	}
	out1, err := enc.Encode([]byte("payload-1"), schemaName, schema)
	require.NoError(t, err)
	out2, err := enc.Encode([]byte("payload-2"), schemaName, schema)
	require.NoError(t, err)

	require.Equal(t, out1[:gsrcore.WireFormatHeaderSize], out2[:gsrcore.WireFormatHeaderSize],
		"both encodes should reuse the same wire-format prefix (cached version-id)")

	require.Equal(t, 1, h.Fake.Count("CreateSchema"),
		"second Encode must hit the cache, not call CreateSchema again")
	require.LessOrEqual(t, h.Fake.Count("GetSchemaByDefinition"), 1,
		"second Encode must hit the cache, not re-call GetSchemaByDefinition")
}

// §5.3 item 14 — auto-register disabled + unknown schema returns an
// error WITHOUT calling CreateSchema. This is the safety guarantee
// callers need when they explicitly do not want their producer to
// mutate the registry.
//
// Phase 4.7: the error-surface assertion (ErrSchemaAutoRegistrationDisabled
// is wrapped) holds on either backend, but the "CreateSchema was not
// called" assertion is fake-specific. Gate requiresFake=true so the
// real-mode run skips this and the dedicated requiresReal companion
// in fmt:lifecycle_real_test.go re-asserts the error surface against
// a never-existed schema name in real Glue.
func TestLifecycle_AutoRegisterDisabled_UnknownSchemaErrors(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		Compatibility:                 "BACKWARD",
		SchemaAutoRegistrationEnabled: false,
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "lifecycle-14")
	schema := &gsrcore.Schema{
		SchemaDefinition: avroSchemaLifecycle,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	}
	_, err = enc.Encode([]byte("payload"), schemaName, schema)

	// Phase 4.5 bug 1 fix: the encoder honors
	// SchemaAutoRegistrationEnabled now. §5.3 item 14's contract is
	// "auto-register disabled + unknown schema → returns the
	// documented error type without calling CreateSchema."
	require.Error(t, err, "auto-register=false + unknown schema must error")
	require.True(t, errors.Is(err, gsrcore.ErrSchemaAutoRegistrationDisabled),
		"surfaced error must wrap ErrSchemaAutoRegistrationDisabled (got %T: %v)", err, err)
	require.Equal(t, 0, h.Fake.Count("CreateSchema"),
		"CreateSchema must NOT be called when auto-register is disabled")
}

// §5.3 item 15 — pre-registered schema id is used when set explicitly.
// The current orchestrator API doesn't expose an explicit
// "use this version id" path; the encoder looks up by definition
// every time. This test pins the behavior under the test-seam encoder
// so a future "pre-registered schema id" feature has a clear
// regression backstop.
//
// Phase 4.7: requiresFake=true. The "primed cache short-circuits the
// Glue lookup" assertion can only be asserted by counting calls on
// the fake — the real Glue path has no equivalent introspection.
func TestLifecycle_PreRegisteredSchemaID(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	schemaName := randomGlueName(t, "lifecycle-15")
	// Seed the encoder cache as if the schema were created out-of-band.
	priorSchema := &gsrcore.Schema{
		SchemaDefinition: avroSchemaLifecycle,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
		SchemaVersionID:  "00000000-0000-0000-0000-000000000123",
	}
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)
	gsrcore.PrimeEncoderCache(enc, priorSchema.SchemaName, priorSchema.DataFormat, priorSchema)

	out, err := enc.Encode([]byte("payload"), schemaName, priorSchema)
	require.NoError(t, err)
	// Bytes 2..17 of the wire-format prefix are the UUID; assert the
	// cached UUID flowed through verbatim.
	require.Equal(t,
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x23},
		out[2:gsrcore.WireFormatHeaderSize],
		"primed cache must short-circuit Glue lookup and use the seeded version UUID")

	require.Equal(t, 0, h.Fake.Count("GetSchemaByDefinition"), "primed cache must skip Glue lookup")
	require.Equal(t, 0, h.Fake.Count("CreateSchema"), "primed cache must skip CreateSchema")
}

// §5.3 item 16 — cache TTL eviction. After TTL, a fresh
// GetSchemaByDefinition call is made. Hardcoded short TTL keeps the
// wall-clock cost down — 50ms is comfortably above patrickmn/go-cache's
// 1ms-tick resolution.
//
// Phase 4.7: requiresFake=true. Verifying TTL eviction requires
// counting GetSchemaByDefinition calls; that's fake-specific.
func TestLifecycle_CacheTTLEviction(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		SchemaAutoRegistrationEnabled: true,
		CacheTTLMillis:                50,
	})
	require.NoError(t, err)

	schemaName := randomGlueName(t, "lifecycle-16")
	schema := &gsrcore.Schema{
		SchemaDefinition: avroSchemaLifecycle,
		SchemaName:       schemaName,
		DataFormat:       "AVRO",
	}
	_, err = enc.Encode([]byte("payload-1"), schemaName, schema)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond) // > TTL + cleanup tick

	_, err = enc.Encode([]byte("payload-2"), schemaName, schema)
	require.NoError(t, err)

	// After TTL eviction, the second encode must re-resolve the
	// schema. GetSchemaByDefinition succeeds (the fake still has the
	// schema from the first encode), so CreateSchema is NOT called
	// twice.
	require.GreaterOrEqual(t, h.Fake.Count("GetSchemaByDefinition"), 2,
		"GetSchemaByDefinition should be called again after TTL eviction")
	require.Equal(t, 1, h.Fake.Count("CreateSchema"),
		"CreateSchema should still be called only once — the schema exists in Glue after the first encode")
}

// §5.3 item 17 — cache size eviction. Distinct schemas beyond the
// cache size evict the oldest. patrickmn/go-cache is TTL-only with no
// hard size cap, so the §5.3 item 17 contract cannot be asserted
// against the current core/cache implementation.
//
// An earlier draft of this test ran a 5-schema loop and ended with
// a t.Logf; the review correctly flagged that as a false-green —
// a test that asserts nothing falsely raises the §5.3 coverage
// number. Make the gap visible by t.Skip-ing with the reason. When
// the cache gains a size cap (tracked in PHASE-4-AWS-NOTES.md /
// future Phase 1 work), flip this to an actual eviction assertion.
func TestLifecycle_CacheSizeEviction(t *testing.T) {
	t.Skip("§5.3 item 17 not implemented: patrickmn/go-cache is TTL-only, no size cap. " +
		"When the cache gains a size cap, replace this skip with an actual eviction assertion.")
}
