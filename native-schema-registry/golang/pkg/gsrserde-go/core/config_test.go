package gsrserde

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadConfigFromMapDefaults locks down the defaults Java applies when no
// config keys are present. Anchored to
// common/src/main/java/com/amazonaws/services/schemaregistry/common/configs/GlueSchemaRegistryConfiguration.java
// and AWSSchemaRegistryConstants.java.
func TestLoadConfigFromMapDefaults(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{})
	require.NoError(t, err)

	assert.Equal(t, DefaultRegistryName, cfg.RegistryName)
	assert.Equal(t, DefaultCompatibility, cfg.Compatibility)
	assert.Equal(t, DefaultCompressionType, cfg.CompressionType)
	assert.Equal(t, DefaultCacheTTLMillis, cfg.TimeToLiveMillis)
	assert.Equal(t, DefaultCacheSize, cfg.CacheSize)
	assert.False(t, cfg.SchemaAutoRegistrationEnabled)
	// Description is synthesized from resolved region + registryName per §3.2.
	// With no region and registryName defaulted to "default-registry", the
	// resulting string is "DEFAULT-DESCRIPTION--default-registry" (two dashes
	// — the region segment is empty). Pinned by AC-2 / INV-5.
	assert.Equal(t, "DEFAULT-DESCRIPTION--default-registry", cfg.Description)
	assert.Empty(t, cfg.Endpoint)
	assert.Empty(t, cfg.ProxyURL)
	assert.Empty(t, cfg.UserAgentApp)
	assert.Empty(t, cfg.AssumeRoleArn)
	assert.Empty(t, cfg.SecondaryDeserializer)
	assert.Empty(t, cfg.AvroRecordType)
	assert.Empty(t, cfg.ProtobufMessageType)
	assert.Empty(t, cfg.Tags)
	assert.Empty(t, cfg.Metadata)
}

// Each Java config key gets its own per-key Tier-1 test asserting both the
// parsed Config field AND any side effect on the resulting aws.Config (where
// applicable). Anchored to plan §7 Phase 1 item 5: "one Tier-1 test per key
// verifying the side effect (e.g. userAgentApp reaching aws.Config, region
// honored, etc.)."

func TestConfig_Region(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyRegion: "us-east-2"})
	require.NoError(t, err)
	assert.Equal(t, "us-east-2", cfg.Region)
	// Side effect: aws.Config.Region honors the input.
	assert.Equal(t, "us-east-2", cfg.AWSConfig.Region)
}

func TestConfig_Endpoint(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyEndpoint: "https://glue.local"})
	require.NoError(t, err)
	assert.Equal(t, "https://glue.local", cfg.Endpoint)
}

func TestConfig_ProxyURL_PopulatesHTTPClient(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyProxyURL: "http://proxy.local:8888"})
	require.NoError(t, err)
	assert.Equal(t, "http://proxy.local:8888", cfg.ProxyURL)
	// Side effect: aws.Config.HTTPClient is non-nil (the default chain
	// installs an HTTP client lazily, so a non-nil here means the proxy
	// option ran).
	assert.NotNil(t, cfg.AWSConfig.HTTPClient)
}

func TestConfig_ProxyURL_InvalidErrors(t *testing.T) {
	_, err := LoadConfigFromMap(map[string]string{ConfigKeyProxyURL: "://not a url"})
	require.Error(t, err)
}

func TestConfig_RegistryName(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyRegistryName: "my-registry"})
	require.NoError(t, err)
	assert.Equal(t, "my-registry", cfg.RegistryName)
}

func TestConfig_Compatibility(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyCompatibility: "FULL_ALL"})
	require.NoError(t, err)
	assert.Equal(t, "FULL_ALL", cfg.Compatibility)
}

func TestConfig_Description(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyDescription: "go canary"})
	require.NoError(t, err)
	assert.Equal(t, "go canary", cfg.Description)
}

func TestConfig_SchemaAutoRegistration(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want bool
	}{
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"false", false},
		{"FALSE", false},
		{"", false},     // omitted ⇒ default false
		{"yes", false},  // Java Boolean.parseBoolean is strict — only "true" is true
		{"1", false},    // same
	} {
		t.Run(tt.in, func(t *testing.T) {
			m := map[string]string{}
			if tt.in != "" {
				m[ConfigKeySchemaAutoRegistration] = tt.in
			}
			cfg, err := LoadConfigFromMap(m)
			require.NoError(t, err)
			assert.Equal(t, tt.want, cfg.SchemaAutoRegistrationEnabled, "input=%q", tt.in)
		})
	}
}

func TestConfig_CompressionType_JavaKeyWins(t *testing.T) {
	// Both keys set; Java's "compression" key wins (the historical Go
	// "compressionType" stays in for back-compat but the canonical name is
	// what Java emits).
	cfg, err := LoadConfigFromMap(map[string]string{
		"compressionType":        "NONE",
		ConfigKeyCompressionType: "ZLIB",
	})
	require.NoError(t, err)
	assert.Equal(t, "ZLIB", cfg.CompressionType)
}

func TestConfig_CompressionType_LegacyGoKey(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{"compressionType": "ZLIB"})
	require.NoError(t, err)
	assert.Equal(t, "ZLIB", cfg.CompressionType)
}

func TestConfig_CacheTTLMillis(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyCacheTTLMillis: "1234"})
	require.NoError(t, err)
	assert.Equal(t, int64(1234), cfg.TimeToLiveMillis)
}

// TestConfig_CacheTTLMillis_NonNumericErrors locks down the §3.1 semantic
// flip: non-numeric input no longer falls back to default; it surfaces as
// ErrInvalidCacheTTL. Renamed from
// TestConfig_CacheTTLMillis_NonNumericFallsBackToDefault per INV-11.
func TestConfig_CacheTTLMillis_NonNumericErrors(t *testing.T) {
	_, err := LoadConfigFromMap(map[string]string{ConfigKeyCacheTTLMillis: "not-a-number"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidCacheTTL), "want ErrInvalidCacheTTL, got %v", err)

	// Empty input still picks the default.
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyCacheTTLMillis: ""})
	require.NoError(t, err)
	assert.Equal(t, DefaultCacheTTLMillis, cfg.TimeToLiveMillis)
}

func TestConfig_CacheSize(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyCacheSize: "42"})
	require.NoError(t, err)
	assert.Equal(t, 42, cfg.CacheSize)
}

func TestConfig_AvroRecordType(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyAvroRecordType: "GENERIC_RECORD"})
	require.NoError(t, err)
	assert.Equal(t, "GENERIC_RECORD", cfg.AvroRecordType)
}

func TestConfig_ProtobufMessageType(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyProtobufMessageType: "DYNAMIC_MESSAGE"})
	require.NoError(t, err)
	assert.Equal(t, "DYNAMIC_MESSAGE", cfg.ProtobufMessageType)
}

func TestConfig_SecondaryDeserializer(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeySecondaryDeserializer: "com.example.Foo"})
	require.NoError(t, err)
	assert.Equal(t, "com.example.Foo", cfg.SecondaryDeserializer)
}

func TestConfig_UserAgentApp(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyUserAgentApp: "my-service"})
	require.NoError(t, err)
	assert.Equal(t, "my-service", cfg.UserAgentApp)
	// Side effect: APIOptions is non-empty (the user-agent middleware was
	// appended). We don't trigger a real Glue call here so we can't read the
	// final User-Agent header; the presence of an APIOption is the visible
	// surface in unit-test land.
	assert.NotEmpty(t, cfg.AWSConfig.APIOptions)
}

func TestConfig_AssumeRoleArn_InstallsCredentialsChain(t *testing.T) {
	// We can't reach AWS here, so we assert that the credentials chain
	// installed a non-nil provider; constructing one is the side effect.
	cfg, err := LoadConfigFromMap(map[string]string{
		ConfigKeyAssumeRoleArn:         "arn:aws:iam::123456789012:role/go-test",
		ConfigKeyAssumeRoleSessionName: "session-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123456789012:role/go-test", cfg.AssumeRoleArn)
	assert.Equal(t, "session-1", cfg.AssumeRoleSessionName)
	assert.NotNil(t, cfg.AWSConfig.Credentials)
}

func TestConfig_AssumeRoleArn_DefaultSessionName(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{
		ConfigKeyAssumeRoleArn: "arn:aws:iam::123456789012:role/go-test",
	})
	require.NoError(t, err)
	// AssumeRoleSessionName field reflects raw input ("" when absent), but
	// the credentials provider falls back to DefaultAssumeRoleSession.
	assert.Empty(t, cfg.AssumeRoleSessionName)
}

func TestConfig_SchemaNameGenerationClass(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeySchemaNameGenerationClass: "com.example.MyStrategy"})
	require.NoError(t, err)
	assert.Equal(t, "com.example.MyStrategy", cfg.SchemaNameGenerationClass)
}

func TestConfig_Tags(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{
		"tags.env":  "beta",
		"tags.team": "schema-registry",
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"env":  "beta",
		"team": "schema-registry",
	}, cfg.Tags)
}

func TestConfig_Metadata(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{
		"metadata.commit":  "abc123",
		"metadata.version": "1.0.0",
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"commit":  "abc123",
		"version": "1.0.0",
	}, cfg.Metadata)
}

// TestConfig_CompressionType_InvalidErrors locks down §3.1 — invalid
// compression values fail-fast with ErrInvalidCompressionType. Plan §7 pins
// the "GZip" case specifically.
func TestConfig_CompressionType_InvalidErrors(t *testing.T) {
	for _, in := range []string{"GZip", "snappy"} {
		t.Run(in, func(t *testing.T) {
			_, err := LoadConfigFromMap(map[string]string{ConfigKeyCompressionType: in})
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidCompressionType), "want ErrInvalidCompressionType, got %v", err)
		})
	}
}

// TestConfig_Compatibility_InvalidErrors locks down §3.1 — the Java
// Compatibility enum is CASE-EXACT, so lowercase variants and typos MUST be
// rejected. See GlueSchemaRegistryConfiguration.java:173-178.
func TestConfig_Compatibility_InvalidErrors(t *testing.T) {
	for _, in := range []string{"FORWARDS", "backward"} {
		t.Run(in, func(t *testing.T) {
			_, err := LoadConfigFromMap(map[string]string{ConfigKeyCompatibility: in})
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidCompatibility), "want ErrInvalidCompatibility, got %v", err)
		})
	}
}

// TestConfig_CacheSize_NonNumericErrors locks down §3.1 — non-numeric
// cacheSize input surfaces as ErrInvalidCacheSize; empty input picks default.
func TestConfig_CacheSize_NonNumericErrors(t *testing.T) {
	_, err := LoadConfigFromMap(map[string]string{ConfigKeyCacheSize: "NaN"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidCacheSize), "want ErrInvalidCacheSize, got %v", err)

	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyCacheSize: ""})
	require.NoError(t, err)
	assert.Equal(t, DefaultCacheSize, cfg.CacheSize)
}

// TestConfig_DescriptionDefault_SynthesizedFromRegionAndRegistry locks down
// AC-2 / INV-5 / S-8: when the description key is absent or empty,
// LoadConfigFromMap synthesizes "DEFAULT-DESCRIPTION-<region>-<registryName>"
// using the RESOLVED region and registry name (post-default fallback). Mirrors
// Java GlueSchemaRegistryConfiguration.java:343-352. The "DEFAULT-DESCRIPTION"
// prefix is exact — no casing change. Empty region collapses to two dashes.
func TestConfig_DescriptionDefault_SynthesizedFromRegionAndRegistry(t *testing.T) {
	t.Run("region and registry both set", func(t *testing.T) {
		cfg, err := LoadConfigFromMap(map[string]string{
			ConfigKeyRegion:       "us-east-2",
			ConfigKeyRegistryName: "my-registry",
		})
		require.NoError(t, err)
		assert.Equal(t, "DEFAULT-DESCRIPTION-us-east-2-my-registry", cfg.Description)
	})

	t.Run("region set, registry defaulted", func(t *testing.T) {
		cfg, err := LoadConfigFromMap(map[string]string{
			ConfigKeyRegion: "us-east-2",
		})
		require.NoError(t, err)
		// Registry name resolves to DefaultRegistryName; description sees the
		// post-default value, not the raw absent key.
		assert.Equal(t, DefaultRegistryName, cfg.RegistryName)
		assert.Equal(t, "DEFAULT-DESCRIPTION-us-east-2-default-registry", cfg.Description)
	})

	t.Run("region and registry absent", func(t *testing.T) {
		cfg, err := LoadConfigFromMap(map[string]string{})
		require.NoError(t, err)
		// Empty region segment collapses to two dashes — Java parity.
		assert.Equal(t, "DEFAULT-DESCRIPTION--default-registry", cfg.Description)
	})

	t.Run("explicit description passes through unchanged", func(t *testing.T) {
		cfg, err := LoadConfigFromMap(map[string]string{
			ConfigKeyDescription: "user supplied",
			ConfigKeyRegion:      "us-east-2",
		})
		require.NoError(t, err)
		assert.Equal(t, "user supplied", cfg.Description)
	})
}

// TestConfig_Validators_EmptyInputsStillSucceed locks AC-1-positive: when all
// four validated keys are absent / empty, LoadConfigFromMap succeeds and the
// defaults are honored.
func TestConfig_Validators_EmptyInputsStillSucceed(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{
		ConfigKeyCompressionType: "",
		ConfigKeyCompatibility:   "",
		ConfigKeyCacheTTLMillis:  "",
		ConfigKeyCacheSize:       "",
	})
	require.NoError(t, err)
	assert.Equal(t, DefaultCompressionType, cfg.CompressionType)
	assert.Equal(t, DefaultCompatibility, cfg.Compatibility)
	assert.Equal(t, DefaultCacheTTLMillis, cfg.TimeToLiveMillis)
	assert.Equal(t, DefaultCacheSize, cfg.CacheSize)
}

// Sanity check that the constants exposed by core/config.go are the
// string values Java emits, not Go-idiomatic snake_case. Java callers
// targeting the same config keys must get the same behavior.
func TestConfigKeys_AreJavaIdentical(t *testing.T) {
	pairs := map[string]string{
		"region":                        ConfigKeyRegion,
		"endpoint":                      ConfigKeyEndpoint,
		"proxyUrl":                      ConfigKeyProxyURL,
		"registry.name":                 ConfigKeyRegistryName,
		"compatibility":                 ConfigKeyCompatibility,
		"description":                   ConfigKeyDescription,
		"schemaAutoRegistrationEnabled": ConfigKeySchemaAutoRegistration,
		"compression":                   ConfigKeyCompressionType,
		"timeToLiveMillis":              ConfigKeyCacheTTLMillis,
		"cacheSize":                     ConfigKeyCacheSize,
		"avroRecordType":                ConfigKeyAvroRecordType,
		"protobufMessageType":           ConfigKeyProtobufMessageType,
		"secondaryDeserializer":         ConfigKeySecondaryDeserializer,
		"userAgentApp":                  ConfigKeyUserAgentApp,
		"assumeRoleArn":                 ConfigKeyAssumeRoleArn,
		"assumeRoleSessionName":         ConfigKeyAssumeRoleSessionName,
		"schemaNameGenerationClass":     ConfigKeySchemaNameGenerationClass,
	}
	for want, got := range pairs {
		assert.Equal(t, want, got, "config-key constant must match the Java constant string")
	}
}

// TestConfig_UserAgentApp_DefaultAppliedWhenAbsent locks down AC-3 / INV-3:
// when `userAgentApp` is absent, the raw Config.UserAgentApp stays empty,
// Config.EffectiveUserAgentApp resolves to "default", and the User-Agent
// middleware is still installed on AWSConfig.APIOptions (Java always-on
// parity per GlueSchemaRegistryConfiguration.java:66).
func TestConfig_UserAgentApp_DefaultAppliedWhenAbsent(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{})
	require.NoError(t, err)

	// INV-3-raw: raw field reflects raw input ("" when absent).
	assert.Empty(t, cfg.UserAgentApp)
	// INV-3-effective: resolved field carries "default" when raw is empty.
	assert.Equal(t, DefaultUserAgentApp, cfg.EffectiveUserAgentApp)
	// AC-3: middleware ALWAYS installed (Java parity).
	assert.NotEmpty(t, cfg.AWSConfig.APIOptions)
}

// TestConfig_UserAgentApp_HeaderEmitsResolvedValue exercises the installed
// API options against a synthetic smithy middleware stack. The captured
// User-Agent header MUST contain "glue-schema-registry-go/<resolved>" for
// both the default (key absent) and explicit (key set) cases. Pins AC-3 /
// S-3 / S-4.
func TestConfig_UserAgentApp_HeaderEmitsResolvedValue(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cfgMap   map[string]string
		expected string
	}{
		{
			name:     "default applied when key absent",
			cfgMap:   map[string]string{},
			expected: "glue-schema-registry-go/default",
		},
		{
			name:     "explicit value flows through",
			cfgMap:   map[string]string{ConfigKeyUserAgentApp: "my-service"},
			expected: "glue-schema-registry-go/my-service",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfigFromMap(tc.cfgMap)
			require.NoError(t, err)
			require.NotEmpty(t, cfg.AWSConfig.APIOptions, "user-agent middleware must be installed")

			// Build a synthetic smithy stack, apply each captured APIOption,
			// then run the stack against a no-op handler. The
			// awsmiddleware.RequestUserAgent middleware writes the
			// User-Agent header in its Build step; we inspect it via an
			// inspector middleware appended after Build.
			stack := smithymiddleware.NewStack("ua-test", smithyhttp.NewStackRequest)
			for _, opt := range cfg.AWSConfig.APIOptions {
				require.NoError(t, opt(stack))
			}
			// Force-install the SDK's RequestUserAgent middleware on the
			// build step (the AddUserAgentKey option only mutates an
			// existing middleware; if none was previously added the option
			// adds one, which is what we need here).
			require.NoError(t, awsmiddleware.AddRequestUserAgentMiddleware(stack))

			var captured http.Header
			require.NoError(t, stack.Build.Add(
				smithymiddleware.BuildMiddlewareFunc("ua-inspector",
					func(ctx context.Context, in smithymiddleware.BuildInput, next smithymiddleware.BuildHandler) (smithymiddleware.BuildOutput, smithymiddleware.Metadata, error) {
						req := in.Request.(*smithyhttp.Request)
						captured = req.Header.Clone()
						return next.HandleBuild(ctx, in)
					}),
				smithymiddleware.After,
			))

			handler := smithymiddleware.DecorateHandler(
				smithymiddleware.HandlerFunc(func(ctx context.Context, input interface{}) (interface{}, smithymiddleware.Metadata, error) {
					return nil, smithymiddleware.Metadata{}, nil
				}),
				stack,
			)
			_, _, err = handler.Handle(context.Background(), nil)
			require.NoError(t, err)

			ua := captured.Get("User-Agent")
			require.NotEmpty(t, ua, "User-Agent header must be populated by the middleware")
			assert.True(t, strings.Contains(ua, tc.expected),
				"User-Agent %q must contain %q", ua, tc.expected)
		})
	}
}
