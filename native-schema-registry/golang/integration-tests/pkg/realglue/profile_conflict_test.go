package realglue

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

// TestNew_WithProfileAndAWSConfig_Errors locks down the
// code-review finding #3 / #13 (Phase 4.7) guard: WithProfile +
// WithAWSConfig is a programmer error and must surface immediately
// rather than silently dropping the profile.
func TestNew_WithProfileAndAWSConfig_Errors(t *testing.T) {
	cfg := aws.Config{Region: "us-east-2", Credentials: staticCredentialsProvider{}}
	_, err := New(context.Background(),
		WithProfile("some-beta-profile"),
		WithAWSConfig(cfg))
	require.Error(t, err, "WithProfile + WithAWSConfig must surface a configuration error")
	require.True(t, strings.Contains(err.Error(), "WithProfile") &&
		strings.Contains(err.Error(), "WithAWSConfig"),
		"error must name both options so callers know which to drop (got: %v)", err)
}
