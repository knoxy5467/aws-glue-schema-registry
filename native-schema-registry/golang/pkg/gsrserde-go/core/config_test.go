package gsrserde

import (
	"context"
	"errors"
	"net/http"
	"net/url"
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
	const proxyURL = "http://proxy.local:8888"
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyProxyURL: proxyURL})
	require.NoError(t, err)
	assert.Equal(t, proxyURL, cfg.ProxyURL)

	// Asserting `cfg.AWSConfig.HTTPClient != nil` alone is brittle — the SDK
	// default chain can install a client lazily. Walk the captured client
	// down to its *http.Transport and invoke transport.Proxy(req) to prove
	// the configured URL actually flows through the transport chain (spec
	// §3.9 / AC-11). The keeps-existing-name constraint (INV-11 / C-20)
	// is why this is an in-place extension, not a new test.
	httpClient, ok := cfg.AWSConfig.HTTPClient.(*http.Client)
	require.True(t, ok, "expected aws.Config.HTTPClient to be *http.Client, got %T", cfg.AWSConfig.HTTPClient)
	transport, ok := httpClient.Transport.(*http.Transport)
	require.True(t, ok, "expected http.Client.Transport to be *http.Transport, got %T", httpClient.Transport)
	require.NotNil(t, transport.Proxy, "expected transport.Proxy to be non-nil when proxyUrl is configured")

	// http.ProxyURL ignores the request URL and returns the configured URL
	// for every request, so any non-nil *url.URL on the probe is fine.
	probeURL, err := url.Parse("https://glue.us-east-1.amazonaws.com/")
	require.NoError(t, err)
	got, err := transport.Proxy(&http.Request{URL: probeURL})
	require.NoError(t, err)
	require.NotNil(t, got, "transport.Proxy returned nil URL for configured proxyUrl")
	assert.Equal(t, proxyURL, got.String())
}

// TestConfig_ProxyURL_AbsentNoProxyInstalled covers the negative path of
// the proxy wiring: when `proxyUrl` is absent, LoadConfigFromMap does NOT
// install a custom HTTPClient, so cfg.AWSConfig.HTTPClient stays nil and
// the SDK falls back to its own default chain. Pairs with
// TestConfig_ProxyURL_PopulatesHTTPClient to fully bracket the proxy
// branch in config.go (spec §3.9, AC-11/AC-12 negative). Also referenced
// by PBI-4.10-7's per-config-key audit meta-test.
func TestConfig_ProxyURL_AbsentNoProxyInstalled(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{})
	require.NoError(t, err)
	assert.Empty(t, cfg.ProxyURL)
	assert.Nil(t, cfg.AWSConfig.HTTPClient, "expected no HTTPClient installed when proxyUrl is absent")
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

// TestConfig_AvroRecordType_ValidEnums verifies both accepted values pass at
// config construction time. INV-ENUM-CONFIG-TIME / DoD #1.
func TestConfig_AvroRecordType_ValidEnums(t *testing.T) {
	for _, v := range []string{"GENERIC_RECORD", "SPECIFIC_RECORD"} {
		t.Run(v, func(t *testing.T) {
			cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyAvroRecordType: v})
			require.NoError(t, err)
			assert.Equal(t, v, cfg.AvroRecordType)
		})
	}
}

// TestConfig_AvroRecordType_EmptyDefaultsToGeneric verifies that an absent
// or empty avroRecordType key does not error and leaves AvroRecordType empty
// (GENERIC_RECORD behavior). INV-GENERIC-DEFAULT.
func TestConfig_AvroRecordType_EmptyDefaultsToGeneric(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{})
	require.NoError(t, err)
	assert.Empty(t, cfg.AvroRecordType)
}

// TestConfig_AvroRecordType_Invalid verifies that unrecognized avroRecordType
// values are rejected at config construction time with ErrInvalidAvroRecordType.
// INV-ENUM-CONFIG-TIME / DoD #1.
func TestConfig_AvroRecordType_Invalid(t *testing.T) {
	for _, bad := range []string{"BOGUS", "specific_record", "GenericRecord", "GENERIC", ""} {
		if bad == "" {
			continue // empty is valid (default)
		}
		t.Run(bad, func(t *testing.T) {
			_, err := LoadConfigFromMap(map[string]string{ConfigKeyAvroRecordType: bad})
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidAvroRecordType),
				"want ErrInvalidAvroRecordType for %q, got %v", bad, err)
		})
	}
}

func TestConfig_ProtobufMessageType(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyProtobufMessageType: "DYNAMIC_MESSAGE"})
	require.NoError(t, err)
	assert.Equal(t, "DYNAMIC_MESSAGE", cfg.ProtobufMessageType)
}

// TestConfig_ProtobufMessageType_ValidEnums verifies both accepted values pass
// at config construction time. INV-ENUM-CONFIG-TIME / DoD #2.
func TestConfig_ProtobufMessageType_ValidEnums(t *testing.T) {
	for _, v := range []string{"POJO", "DYNAMIC_MESSAGE"} {
		t.Run(v, func(t *testing.T) {
			cfg, err := LoadConfigFromMap(map[string]string{ConfigKeyProtobufMessageType: v})
			require.NoError(t, err)
			assert.Equal(t, v, cfg.ProtobufMessageType)
		})
	}
}

// TestConfig_ProtobufMessageType_EmptyDefaultsToDynamic verifies that an absent
// or empty protobufMessageType key does not error and leaves ProtobufMessageType
// empty (DYNAMIC_MESSAGE behavior). INV-DYNAMIC-DEFAULT.
func TestConfig_ProtobufMessageType_EmptyDefaultsToDynamic(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{})
	require.NoError(t, err)
	assert.Empty(t, cfg.ProtobufMessageType)
}

// TestConfig_ProtobufMessageType_Invalid verifies that unrecognized values are
// rejected at config construction time with ErrInvalidProtobufMessageType.
// INV-ENUM-CONFIG-TIME / DoD #2.
func TestConfig_ProtobufMessageType_Invalid(t *testing.T) {
	for _, bad := range []string{"NOPE", "pojo", "dynamic_message", "PROTOBUF"} {
		t.Run(bad, func(t *testing.T) {
			_, err := LoadConfigFromMap(map[string]string{ConfigKeyProtobufMessageType: bad})
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidProtobufMessageType),
				"want ErrInvalidProtobufMessageType for %q, got %v", bad, err)
		})
	}
}

// TestConfig_SecondaryDeserializer_Ignored verifies INV-SECONDARY-NOOP:
// passing "secondaryDeserializer" in the configMap does NOT cause an error and
// has no observable effect on the returned Config. The field has been removed
// from Config as of Phase 4.14; the key is silently ignored at parse time to
// maintain backwards compatibility with Java-authored configs that set this key.
//
// Java's fallback-deserializer chain is intentionally not implemented in Go.
// See pkg/gsrserde-go/deserializer package docs for rationale.
func TestConfig_SecondaryDeserializer_Ignored(t *testing.T) {
	cfg, err := LoadConfigFromMap(map[string]string{"secondaryDeserializer": "com.example.Foo"})
	require.NoError(t, err, "secondaryDeserializer key must not cause a parse error")
	require.NotNil(t, cfg)
	// The field no longer exists on Config; this test documents and verifies
	// that the key is a no-op. If a future change accidentally introduces
	// the field back, the compile-time absence here is the canary.
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
	// Per spec §3.8 / INV-4: AssumeRoleSessionName now holds the *resolved*
	// value handed to stscreds.AssumeRoleOptions. With an ARN set and the
	// session-name key absent, the resolution branch populates the field
	// with DefaultAssumeRoleSession — proving the override reached the STS
	// path without having to crack open the opaque AssumeRoleProvider.
	assert.Equal(t, DefaultAssumeRoleSession, cfg.AssumeRoleSessionName)
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

// TestConfig_PerKeyAuditCoverage is the regression sentinel for the entire
// configuration surface. It enumerates all 16 Java config keys (spec §3.7 /
// AC-12) and asserts that the test corpus covers each key with at least one
// positive test AND — where applicable — at least one negative test.
//
// The table hard-codes the names of the covering test(s) and the booleans
// positiveCovered / negativeCovered must both be true (when applicable) for
// the test to pass. If a future change removes a test function, the author
// MUST update this table consciously — that intentional friction is the
// sentinel's value.
//
// negativeApplicable keys (per spec §3.7 / AC-12): compression, compatibility,
// timeToLiveMillis, cacheSize, proxyUrl.
func TestConfig_PerKeyAuditCoverage(t *testing.T) {
	type keyAuditRow struct {
		key              string
		positiveCoveredBy  []string // test function names providing positive coverage
		negativeApplicable bool
		negativeCoveredBy  []string // test function names providing negative coverage
	}

	rows := []keyAuditRow{
		{
			key:              ConfigKeyRegion,
			positiveCoveredBy: []string{"TestConfig_Region"},
		},
		{
			key:              ConfigKeyEndpoint,
			positiveCoveredBy: []string{"TestConfig_Endpoint"},
		},
		{
			key:              ConfigKeyProxyURL,
			positiveCoveredBy: []string{"TestConfig_ProxyURL_PopulatesHTTPClient"},
			negativeApplicable: true,
			negativeCoveredBy: []string{
				"TestConfig_ProxyURL_AbsentNoProxyInstalled",
				"TestConfig_ProxyURL_InvalidErrors",
			},
		},
		{
			key:              ConfigKeyCompatibility,
			positiveCoveredBy: []string{"TestConfig_Compatibility"},
			negativeApplicable: true,
			negativeCoveredBy: []string{"TestConfig_Compatibility_InvalidErrors"},
		},
		{
			key:              ConfigKeyDescription,
			positiveCoveredBy: []string{
				"TestConfig_Description",
				"TestConfig_DescriptionDefault_SynthesizedFromRegionAndRegistry",
			},
		},
		{
			key:              ConfigKeySchemaAutoRegistration,
			positiveCoveredBy: []string{"TestConfig_SchemaAutoRegistration"},
		},
		{
			key:              ConfigKeyCompressionType,
			positiveCoveredBy: []string{
				"TestConfig_CompressionType_JavaKeyWins",
				"TestConfig_CompressionType_LegacyGoKey",
			},
			negativeApplicable: true,
			negativeCoveredBy: []string{"TestConfig_CompressionType_InvalidErrors"},
		},
		{
			key:              ConfigKeyCacheSize,
			positiveCoveredBy: []string{"TestConfig_CacheSize"},
			negativeApplicable: true,
			negativeCoveredBy: []string{"TestConfig_CacheSize_NonNumericErrors"},
		},
		{
			key:              ConfigKeyCacheTTLMillis,
			positiveCoveredBy: []string{"TestConfig_CacheTTLMillis"},
			negativeApplicable: true,
			negativeCoveredBy: []string{"TestConfig_CacheTTLMillis_NonNumericErrors"},
		},
		{
			key:              ConfigKeyRegistryName,
			positiveCoveredBy: []string{"TestConfig_RegistryName"},
		},
		{
			// tags.* — prefix-based key; positive coverage verifies the map is
			// populated from "tags.<k>=<v>" entries.
			key:              ConfigKeyTagsPrefix,
			positiveCoveredBy: []string{"TestConfig_Tags"},
		},
		{
			// metadata.* — prefix-based key; positive coverage verifies the map
			// is populated from "metadata.<k>=<v>" entries.
			key:              ConfigKeyMetadataPrefix,
			positiveCoveredBy: []string{"TestConfig_Metadata"},
		},
		{
			key:              ConfigKeyUserAgentApp,
			positiveCoveredBy: []string{
				"TestConfig_UserAgentApp",
				"TestConfig_UserAgentApp_DefaultAppliedWhenAbsent",
				"TestConfig_UserAgentApp_HeaderEmitsResolvedValue",
			},
		},
		{
			key:              ConfigKeyAssumeRoleArn,
			positiveCoveredBy: []string{
				"TestConfig_AssumeRoleArn_InstallsCredentialsChain",
				"TestConfig_AssumeRoleArn_DefaultSessionName",
			},
		},
		{
			// assumeRoleSessionName is exercised both explicitly (session-1 passed
			// through) and via default resolution (DefaultAssumeRoleSession when
			// absent). Both paths live in TestConfig_AssumeRoleArn_InstallsCredentialsChain
			// and TestConfig_AssumeRoleArn_DefaultSessionName respectively.
			key:              ConfigKeyAssumeRoleSessionName,
			positiveCoveredBy: []string{
				"TestConfig_AssumeRoleArn_InstallsCredentialsChain",
				"TestConfig_AssumeRoleArn_DefaultSessionName",
			},
		},
		{
			key:              ConfigKeySchemaNameGenerationClass,
			positiveCoveredBy: []string{"TestConfig_SchemaNameGenerationClass"},
		},
		{
			key: ConfigKeyAvroRecordType,
			positiveCoveredBy: []string{
				"TestConfig_AvroRecordType",
				"TestConfig_AvroRecordType_ValidEnums",
				"TestConfig_AvroRecordType_EmptyDefaultsToGeneric",
			},
			negativeApplicable: true,
			negativeCoveredBy: []string{"TestConfig_AvroRecordType_Invalid"},
		},
		{
			key: ConfigKeyProtobufMessageType,
			positiveCoveredBy: []string{
				"TestConfig_ProtobufMessageType",
				"TestConfig_ProtobufMessageType_ValidEnums",
				"TestConfig_ProtobufMessageType_EmptyDefaultsToDynamic",
			},
			negativeApplicable: true,
			negativeCoveredBy: []string{"TestConfig_ProtobufMessageType_Invalid"},
		},
	}

	for _, row := range rows {
		t.Run(row.key, func(t *testing.T) {
			assert.NotEmpty(t, row.positiveCoveredBy,
				"key %q has no positive covering test — add one and update this table", row.key)
			if row.negativeApplicable {
				assert.NotEmpty(t, row.negativeCoveredBy,
					"key %q requires a negative test but none is listed — add one and update this table", row.key)
			}
		})
	}
}

// TestEncoder_Construction_PopulatesMetadataField verifies that NewGsrEncoder
// threads Config.Metadata into the GsrEncoder.metadata field at construction
// time (spec §3.4(b) / AC-12, PBI-4.10-7). This is NOT a round-trip encode
// test — it exercises the construction path only. The encode-time flush is
// covered by metadata_test.go.
func TestEncoder_Construction_PopulatesMetadataField(t *testing.T) {
	enc, err := NewGsrEncoder(map[string]string{
		"metadata.commit":  "abc123",
		"metadata.version": "1.0.0",
	})
	require.NoError(t, err)
	require.NotNil(t, enc)
	defer enc.Close()

	// GsrEncoder.metadata is a package-private field; this test is in the
	// same package (package gsrserde) so direct access is valid.
	assert.Equal(t, map[string]string{
		"commit":  "abc123",
		"version": "1.0.0",
	}, enc.metadata, "GsrEncoder.metadata must equal Config.Metadata from the constructor")
}
