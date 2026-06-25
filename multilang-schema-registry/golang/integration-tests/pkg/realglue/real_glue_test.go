package realglue

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
)

// TestRealSatisfiesGlueClient is the compile-time guarantee that
// *Real stays in lock-step with gsrcore.GlueClient. If the interface
// grows a method, this test won't compile until *Real grows it too
// (or the embedded *glue.Client gains the method, which it
// automatically does because glue.NewFromConfig is the canonical
// constructor for the SDK type).
func TestRealSatisfiesGlueClient(t *testing.T) {
	var _ gsrcore.GlueClient = (*Real)(nil)
}

// staticCredentialsProvider returns a fixed set of credentials —
// avoids reaching the host's credential chain in unit tests.
type staticCredentialsProvider struct{}

func (staticCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     "AKIAUNITTEST",
		SecretAccessKey: "unit/test/secret",
		Source:          "realglue-unit-test",
	}, nil
}

// TestNew_WithInjectedConfigDoesNotTouchCredentialChain pins the
// WithAWSConfig short-circuit. If New ever starts calling
// config.LoadDefaultConfig despite an injected config, dev-host
// runs of `go test ./pkg/realglue/...` would start failing whenever
// the host has no creds. This test would catch that.
func TestNew_WithInjectedConfigDoesNotTouchCredentialChain(t *testing.T) {
	cfg := aws.Config{
		Region:      "eu-west-1",
		Credentials: staticCredentialsProvider{},
	}
	r, err := New(context.Background(), WithAWSConfig(cfg))
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, "eu-west-1", r.Region, "Region should flow through from the injected aws.Config")
	require.NotNil(t, r.Client, "*Real.Client must be non-nil for the embedded SDK methods to dispatch")
}

// TestNew_RegionPrecedence verifies the documented chain:
//
//	1. WithRegion option
//	2. AWS_REGION env (covered by TestNew_RegionFromAWSRegionEnv below)
//	3. DefaultRegion
//
// WithRegion wins even if the env var is set.
func TestNew_RegionPrecedence(t *testing.T) {
	t.Setenv("AWS_REGION", "us-west-1")
	cfg := aws.Config{Credentials: staticCredentialsProvider{}}
	r, err := New(context.Background(), WithRegion("ap-southeast-2"), WithAWSConfig(cfg))
	require.NoError(t, err)
	require.Equal(t, "ap-southeast-2", r.Region)
}

func TestNew_RegionFromAWSRegionEnv(t *testing.T) {
	t.Setenv("AWS_REGION", "us-west-1")
	cfg := aws.Config{Credentials: staticCredentialsProvider{}}
	r, err := New(context.Background(), WithAWSConfig(cfg))
	require.NoError(t, err)
	require.Equal(t, "us-west-1", r.Region)
}

func TestNew_RegionFallsBackToDefault(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	cfg := aws.Config{Credentials: staticCredentialsProvider{}}
	r, err := New(context.Background(), WithAWSConfig(cfg))
	require.NoError(t, err)
	require.Equal(t, DefaultRegion, r.Region)
}

// TestNewCleanup_BoundsToRealClient asserts the Real-to-Cleanup wiring
// without actually issuing AWS calls.
func TestNewCleanup_BoundsToRealClient(t *testing.T) {
	cfg := aws.Config{Region: "us-east-2", Credentials: staticCredentialsProvider{}}
	r, err := New(context.Background(), WithAWSConfig(cfg))
	require.NoError(t, err)
	c := r.NewCleanup()
	require.NotNil(t, c)
	// Tracking a name doesn't reach AWS; this is the invariant: track
	// is local-state-only, Run is the I/O boundary.
	c.TrackRegistry("noop-test-registry")
	require.Equal(t, []string{"noop-test-registry"}, c.registries)
}
