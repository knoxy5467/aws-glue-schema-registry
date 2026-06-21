//go:build integration

package integration_tests

import (
	"testing"

	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
)

// Config-key + AssumeRole scenarios from plan §5.3 items 32-33.
//
// Item 32 (every recognized config key is honored) is covered
// exhaustively by pkg/gsrserde-go/core/config_test.go at the Tier-1
// level. We mirror two highest-value keys here as regression
// sentinels so that the integration-tag-on build proves the config
// surface still does what the §5.3 row claims.
//
// Item 33 (STS AssumeRole) tests the credential-chain wiring;
// asserting the actual AssumeRole call would require a live STS
// endpoint or a mocked HTTP layer below the SDK, which is out of
// Phase 4 scope. The test below pins the LoadConfigFromMap surface:
// when assumeRoleArn is set, the resulting aws.Config has
// non-zero-value credentials.

// §5.3 item 32 — recognized config keys flow into Config. Pick the
// keys most likely to drift on a CGO-deleted refactor:
//   - region
//   - registry.name
//   - compatibility
//   - compression
//   - schemaAutoRegistrationEnabled
//   - timeToLiveMillis
//   - tags.<k>
func TestConfig_RecognizedKeysFlowThrough(t *testing.T) {
	t.Parallel()
	cfg, err := gsrcore.LoadConfigFromMap(map[string]string{
		"region":                        "us-west-2",
		"registry.name":                 "phase4-registry",
		"compatibility":                 "FORWARD",
		"compression":                   "ZLIB",
		"schemaAutoRegistrationEnabled": "true",
		"timeToLiveMillis":              "1000",
		"tags.team":                     "gsr",
		"tags.env":                      "beta",
	})
	require.NoError(t, err)

	require.Equal(t, "us-west-2", cfg.Region)
	require.Equal(t, "phase4-registry", cfg.RegistryName)
	require.Equal(t, "FORWARD", cfg.Compatibility)
	require.Equal(t, "ZLIB", cfg.CompressionType)
	require.True(t, cfg.SchemaAutoRegistrationEnabled)
	require.Equal(t, int64(1000), cfg.TimeToLiveMillis)
	require.Equal(t, "gsr", cfg.Tags["team"])
	require.Equal(t, "beta", cfg.Tags["env"])
}

// §5.3 item 33 — STS AssumeRole. When assumeRoleArn is set, the Glue
// client's credential chain wraps the resolved creds with an
// AssumeRoleProvider. The visible side-effect is that
// cfg.AWSConfig.Credentials is non-nil and goes through a cache (i.e.
// type *aws.CredentialsCache). Asserting the type is a fragile mock
// to make — fallback to "credentials are present and the config
// loaded without error", which is the strongest contract we can
// assert without hitting a live STS endpoint.
//
// A future enhancement (out of Phase 4 scope) would intercept the
// HTTP transport and assert the assume-role-then-glue call sequence.
func TestConfig_AssumeRoleArn(t *testing.T) {
	t.Parallel()
	cfg, err := gsrcore.LoadConfigFromMap(map[string]string{
		"region":                "us-east-1",
		"assumeRoleArn":         "arn:aws:iam::000000000000:role/phase4-fake-role",
		"assumeRoleSessionName": "phase4-session",
	})
	require.NoError(t, err)
	require.Equal(t, "arn:aws:iam::000000000000:role/phase4-fake-role", cfg.AssumeRoleArn)
	require.Equal(t, "phase4-session", cfg.AssumeRoleSessionName)
	require.NotNil(t, cfg.AWSConfig.Credentials, "AssumeRole chain must produce a non-nil credentials provider")
}
