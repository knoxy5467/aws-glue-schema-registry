package gsrserde

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	smithymiddleware "github.com/aws/smithy-go/middleware"
)

// Config-key constants — string-identical to Java
// common/src/main/java/com/amazonaws/services/schemaregistry/utils/AWSSchemaRegistryConstants.java
// so Go callers can populate the configMap with the same names Java callers do.
const (
	ConfigKeyRegion                      = "region"
	ConfigKeyEndpoint                    = "endpoint"
	ConfigKeyProxyURL                    = "proxyUrl"
	ConfigKeyRegistryName                = "registry.name"
	ConfigKeyCompatibility               = "compatibility"
	ConfigKeyDescription                 = "description"
	ConfigKeySchemaAutoRegistration      = "schemaAutoRegistrationEnabled"
	ConfigKeyCompressionType             = "compression"
	ConfigKeyCacheTTLMillis              = "timeToLiveMillis"
	ConfigKeyCacheSize                   = "cacheSize"
	ConfigKeyAvroRecordType              = "avroRecordType"
	ConfigKeyProtobufMessageType         = "protobufMessageType"
	ConfigKeyTagsPrefix                  = "tags."
	ConfigKeyMetadataPrefix              = "metadata."
	ConfigKeySecondaryDeserializer       = "secondaryDeserializer"
	ConfigKeyUserAgentApp                = "userAgentApp"
	ConfigKeyAssumeRoleArn               = "assumeRoleArn"
	ConfigKeyAssumeRoleSessionName       = "assumeRoleSessionName"
	ConfigKeySchemaNameGenerationClass   = "schemaNameGenerationClass"

	// Defaults — Java
	// common/src/main/java/com/amazonaws/services/schemaregistry/utils/AWSSchemaRegistryConstants.java
	DefaultRegistryName       = "default-registry"
	DefaultCompatibility      = "BACKWARD"
	DefaultCompressionType    = "NONE"
	DefaultCacheTTLMillis     = int64(24 * 60 * 60 * 1000) // 24 hours
	DefaultCacheSize          = 200
	DefaultAssumeRoleSession  = "aws-glue-schema-registry-go"
	// DefaultUserAgentApp mirrors Java
	// common/src/main/java/com/amazonaws/services/schemaregistry/common/configs/GlueSchemaRegistryConfiguration.java:66
	// (`userAgentApp = "default"`). Used by the always-on User-Agent middleware
	// when the `userAgentApp` configMap key is absent.
	DefaultUserAgentApp       = "default"
)

// Config holds the parsed configuration for the GSR Go client.
//
// Java reference: GlueSchemaRegistryConfiguration.java.
//
// Fields with no Go-side use yet (AvroRecordType, ProtobufMessageType,
// JacksonSerializationFeatures, JacksonDeserializationFeatures) are parsed
// and held here so callers can forward them when the format-layer adapters
// land in Phase 3 — keeping the config surface stable beats re-introducing
// the keys later.
type Config struct {
	AWSConfig                     aws.Config
	Region                        string
	Endpoint                      string
	ProxyURL                      string
	RegistryName                  string
	Compatibility                 string
	Description                   string
	SchemaAutoRegistrationEnabled bool
	CompressionType               string
	TimeToLiveMillis              int64
	CacheSize                     int

	AvroRecordType        string
	ProtobufMessageType   string
	SecondaryDeserializer string
	UserAgentApp          string
	// EffectiveUserAgentApp carries the resolved value used by the User-Agent
	// middleware: equals UserAgentApp when raw is non-empty, otherwise
	// DefaultUserAgentApp. Raw UserAgentApp is preserved (INV-3 / C-15) so
	// callers introspecting raw input can still distinguish default from
	// explicit. Mirrors Java's distinction between the public getter (empty
	// when not set) and the wire stamp (always non-empty).
	EffectiveUserAgentApp string
	AssumeRoleArn         string
	AssumeRoleSessionName string
	SchemaNameGenerationClass string

	Tags     map[string]string
	Metadata map[string]string
}

// LoadConfigFromMap parses a Java-style configMap into a Config + the
// underlying aws.Config. Defaults match Java's defaults exactly.
//
// Two key normalizations:
//   - "compression" is the Java key (AWSSchemaRegistryConstants.COMPRESSION_TYPE).
//     The historical Go scaffolding used "compressionType"; both are accepted
//     for backwards compatibility, with "compression" winning if both are set.
//   - tags.<key> = <value> and metadata.<key> = <value> patterns expand into
//     the Tags / Metadata maps; Java accepts a Map<String, String> directly
//     but Go's flat configMap[string]string requires this convention.
func LoadConfigFromMap(configMap map[string]string) (*Config, error) {
	region := configMap[ConfigKeyRegion]

	// Build aws.Config with region + endpoint + proxy + user-agent +
	// assume-role wiring, all in one place so the test-per-key cases in
	// config_test.go can lock the side effects down.
	loadOpts := []func(*config.LoadOptions) error{}
	if region != "" {
		loadOpts = append(loadOpts, config.WithRegion(region))
	}
	// User-Agent middleware is ALWAYS-ON, mirroring Java which always installs
	// the GSR user-agent under `userAgentApp` with default `"default"`. Raw
	// configMap value (empty when absent) is preserved on Config.UserAgentApp
	// per INV-3 / C-15; the resolved value used by the middleware lives on
	// Config.EffectiveUserAgentApp.
	//
	// AddUserAgentKeyValue is used instead of AddUserAgentKey because the
	// SDK's key-only helper sanitizes the `/` separator (RFC 7230 token
	// rules), producing "glue-schema-registry-go-default". The key/value
	// helper preserves the `/` (`key + "/" + value`) so the emitted header
	// is "glue-schema-registry-go/<effective>" — wire-format parity with
	// Java which stamps "aws-glue-schema-registry-java/<app>".
	effectiveUserAgent := configMap[ConfigKeyUserAgentApp]
	if effectiveUserAgent == "" {
		effectiveUserAgent = DefaultUserAgentApp
	}
	loadOpts = append(loadOpts, config.WithAPIOptions([]func(*smithymiddleware.Stack) error{
		awsmiddleware.AddUserAgentKeyValue("glue-schema-registry-go", effectiveUserAgent),
	}))
	if proxy := configMap[ConfigKeyProxyURL]; proxy != "" {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, err
		}
		transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
		loadOpts = append(loadOpts, config.WithHTTPClient(&http.Client{Transport: transport}))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, err
	}

	// AssumeRole wrap, mirroring Java GlueSchemaRegistryConfiguration's
	// optional STS credential chain.
	if arn := configMap[ConfigKeyAssumeRoleArn]; arn != "" {
		session := configMap[ConfigKeyAssumeRoleSessionName]
		if session == "" {
			session = DefaultAssumeRoleSession
		}
		stsClient := sts.NewFromConfig(cfg)
		cfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, arn, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = session
		}))
	}

	// Endpoint override applies regardless of region, mirroring Java's
	// .endPoint behavior on the SerDe-level config (not on aws.Config).
	endpoint := configMap[ConfigKeyEndpoint]

	registryName := DefaultRegistryName
	if v := configMap[ConfigKeyRegistryName]; v != "" {
		registryName = v
	}

	compatibility := DefaultCompatibility
	if v := configMap[ConfigKeyCompatibility]; v != "" {
		if err := validateCompatibility(v); err != nil {
			return nil, err
		}
		compatibility = v
	}

	compressionType := DefaultCompressionType
	// Accept both the Java key "compression" and the historical Go key
	// "compressionType". Java wins if both are set.
	compressionExplicit := false
	if v := configMap["compressionType"]; v != "" {
		compressionType = v
		compressionExplicit = true
	}
	if v := configMap[ConfigKeyCompressionType]; v != "" {
		compressionType = v
		compressionExplicit = true
	}
	// Validate only when an explicit non-empty value was supplied; absent
	// keys fall through to DefaultCompressionType.
	if compressionExplicit {
		if err := validateCompressionType(compressionType); err != nil {
			return nil, err
		}
	}

	autoRegister := false
	if v := configMap[ConfigKeySchemaAutoRegistration]; v != "" {
		autoRegister = parseBool(v)
	}

	ttl := DefaultCacheTTLMillis
	if v := configMap[ConfigKeyCacheTTLMillis]; v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: %q", ErrInvalidCacheTTL, v)
		}
		ttl = parsed
	}

	cacheSize := DefaultCacheSize
	if v := configMap[ConfigKeyCacheSize]; v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("%w: %q", ErrInvalidCacheSize, v)
		}
		cacheSize = parsed
	}

	// Synthesize the default description AFTER region + registryName have been
	// resolved so the registry-name segment reflects the post-default fallback
	// (e.g. "default-registry"), not the raw configMap value. Mirrors Java
	// GlueSchemaRegistryConfiguration.java:343-352. The prefix string
	// "DEFAULT-DESCRIPTION" is exact (no casing change). When region is empty
	// (no key set) the resulting string is "DEFAULT-DESCRIPTION--<registryName>"
	// (two dashes) — matches Java's use of whatever string region resolved to.
	description := configMap[ConfigKeyDescription]
	if description == "" {
		description = fmt.Sprintf("DEFAULT-DESCRIPTION-%s-%s", region, registryName)
	}

	return &Config{
		AWSConfig:                     cfg,
		Region:                        region,
		Endpoint:                      endpoint,
		ProxyURL:                      configMap[ConfigKeyProxyURL],
		RegistryName:                  registryName,
		Compatibility:                 compatibility,
		Description:                   description,
		SchemaAutoRegistrationEnabled: autoRegister,
		CompressionType:               compressionType,
		TimeToLiveMillis:              ttl,
		CacheSize:                     cacheSize,

		AvroRecordType:            configMap[ConfigKeyAvroRecordType],
		ProtobufMessageType:       configMap[ConfigKeyProtobufMessageType],
		SecondaryDeserializer:     configMap[ConfigKeySecondaryDeserializer],
		UserAgentApp:              configMap[ConfigKeyUserAgentApp],
		EffectiveUserAgentApp:     effectiveUserAgent,
		AssumeRoleArn:             configMap[ConfigKeyAssumeRoleArn],
		AssumeRoleSessionName:     configMap[ConfigKeyAssumeRoleSessionName],
		SchemaNameGenerationClass: configMap[ConfigKeySchemaNameGenerationClass],

		Tags:     collectPrefixedMap(configMap, ConfigKeyTagsPrefix),
		Metadata: collectPrefixedMap(configMap, ConfigKeyMetadataPrefix),
	}, nil
}

// parseBool accepts the same lenient truthy/falsy strings Java's
// Boolean.parseBoolean does: only "true" (case-insensitive) is true.
func parseBool(s string) bool {
	return strings.EqualFold(s, "true")
}

// collectPrefixedMap extracts keys with the given prefix into a sub-map
// keyed by the stripped suffix. Used for tags.<key> and metadata.<key>.
func collectPrefixedMap(configMap map[string]string, prefix string) map[string]string {
	out := make(map[string]string)
	for k, v := range configMap {
		if strings.HasPrefix(k, prefix) {
			out[strings.TrimPrefix(k, prefix)] = v
		}
	}
	return out
}

// validCompressionTypes is the set Java's
// AWSSchemaRegistryConstants.COMPRESSION enum accepts.
var validCompressionTypes = map[string]struct{}{
	"NONE": {},
	"ZLIB": {},
}

// validateCompressionType rejects values outside the Java enum. The check is
// case-insensitive on the upper-case-normalized value to match Java's
// COMPRESSION.valueOf-after-toUpperCase shape, but the stored field preserves
// the caller's casing. Empty input is the caller's responsibility — this
// helper is only invoked on explicit non-empty values.
func validateCompressionType(value string) error {
	if _, ok := validCompressionTypes[strings.ToUpper(value)]; !ok {
		return fmt.Errorf("%w: %q (want one of NONE, ZLIB)", ErrInvalidCompressionType, value)
	}
	return nil
}

// validCompatibilities is the case-exact set Java's
// software.amazon.awssdk.services.glue.model.Compatibility enum emits via
// knownValues(); see GlueSchemaRegistryConfiguration.java:173-178.
var validCompatibilities = map[string]struct{}{
	"NONE":         {},
	"DISABLED":     {},
	"BACKWARD":     {},
	"BACKWARD_ALL": {},
	"FORWARD":      {},
	"FORWARD_ALL": {},
	"FULL":         {},
	"FULL_ALL":     {},
}

// validateCompatibility rejects values not in the Java enum set. The match is
// CASE-EXACT — lowercase variants like "backward" and typos like "FORWARDS"
// MUST be rejected (Java's Compatibility.valueOf is case-sensitive on the
// known-values list).
func validateCompatibility(value string) error {
	if _, ok := validCompatibilities[value]; !ok {
		return fmt.Errorf("%w: %q", ErrInvalidCompatibility, value)
	}
	return nil
}
