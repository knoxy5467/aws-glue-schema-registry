package realglue

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/stretchr/testify/require"
)

// recorderCleanupClient is a hand-rolled stub of the cleanupClient
// interface. We use it instead of testify/mock because the contract
// under test is the call ORDER, which a recorder slice captures more
// directly than mock.ExpectedCall sequencing.
type recorderCleanupClient struct {
	calls          []string
	errOnSchema    map[string]error
	errOnRegistry  map[string]error
}

func newRecorder() *recorderCleanupClient {
	return &recorderCleanupClient{
		errOnSchema:   map[string]error{},
		errOnRegistry: map[string]error{},
	}
}

func (r *recorderCleanupClient) DeleteSchema(ctx context.Context, in *glue.DeleteSchemaInput, _ ...func(*glue.Options)) (*glue.DeleteSchemaOutput, error) {
	arn := ""
	if in.SchemaId != nil && in.SchemaId.SchemaName != nil {
		arn = *in.SchemaId.SchemaName
	}
	r.calls = append(r.calls, "DeleteSchema:"+arn)
	if err, ok := r.errOnSchema[arn]; ok {
		return nil, err
	}
	return &glue.DeleteSchemaOutput{}, nil
}

func (r *recorderCleanupClient) DeleteRegistry(ctx context.Context, in *glue.DeleteRegistryInput, _ ...func(*glue.Options)) (*glue.DeleteRegistryOutput, error) {
	name := ""
	if in.RegistryId != nil && in.RegistryId.RegistryName != nil {
		name = *in.RegistryId.RegistryName
	}
	r.calls = append(r.calls, "DeleteRegistry:"+name)
	if err, ok := r.errOnRegistry[name]; ok {
		return nil, err
	}
	return &glue.DeleteRegistryOutput{}, nil
}

// TestCleanup_RunDeletesInReverseOrder is the core contract:
// schemas before registries, both in reverse insertion order.
func TestCleanup_RunDeletesInReverseOrder(t *testing.T) {
	rec := newRecorder()
	c := newCleanupForTest(rec)
	c.TrackRegistry("reg-A")
	c.TrackRegistry("reg-B")
	c.TrackSchema("reg-A", "schema-A1")
	c.TrackSchema("reg-A", "schema-A2")
	c.TrackSchema("reg-B", "schema-B1")

	require.NoError(t, c.Run(context.Background()))

	wantOrder := []string{
		"DeleteSchema:schema-B1",
		"DeleteSchema:schema-A2",
		"DeleteSchema:schema-A1",
		"DeleteRegistry:reg-B",
		"DeleteRegistry:reg-A",
	}
	require.Equal(t, wantOrder, rec.calls)
}

// TestCleanup_RunContinuesOnError pins the "errors.Join — keep going"
// behavior. A single mid-run failure must NOT skip the rest, or a
// flaky DeleteSchema would leak the trailing registries.
func TestCleanup_RunContinuesOnError(t *testing.T) {
	rec := newRecorder()
	rec.errOnSchema["schema-mid"] = errors.New("boom")
	c := newCleanupForTest(rec)
	c.TrackRegistry("reg-1")
	c.TrackSchema("reg-1", "schema-first")
	c.TrackSchema("reg-1", "schema-mid")
	c.TrackSchema("reg-1", "schema-last")

	err := c.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")

	// All three schemas should have been attempted in reverse order;
	// the registry should still have been deleted afterwards.
	wantOrder := []string{
		"DeleteSchema:schema-last",
		"DeleteSchema:schema-mid",
		"DeleteSchema:schema-first",
		"DeleteRegistry:reg-1",
	}
	require.Equal(t, wantOrder, rec.calls)
}

// TestCleanup_NoTrackedResourcesIsNoop is the trivial-but-load-bearing
// path. A test that errors before creating anything must not blow up
// in t.Cleanup.
func TestCleanup_NoTrackedResourcesIsNoop(t *testing.T) {
	rec := newRecorder()
	c := newCleanupForTest(rec)
	require.NoError(t, c.Run(context.Background()))
	require.Empty(t, rec.calls)
}

// TestCleanup_DedupsSameName guards against the bug where a test
// double-registers the same registry (e.g. once at create-time, once
// at error-handler time) and Run issues two DeleteRegistry calls —
// the second 404's and the test reports cleanup failure.
func TestCleanup_DedupsSameName(t *testing.T) {
	rec := newRecorder()
	c := newCleanupForTest(rec)
	c.TrackRegistry("reg-dup")
	c.TrackRegistry("reg-dup")
	c.TrackSchema("reg-dup", "schema-dup")
	c.TrackSchema("reg-dup", "schema-dup")

	require.NoError(t, c.Run(context.Background()))
	wantOrder := []string{
		"DeleteSchema:schema-dup",
		"DeleteRegistry:reg-dup",
	}
	require.Equal(t, wantOrder, rec.calls)
}
