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
	ConfigKeyUserAgentApp                = "userAgentApp"
	ConfigKeyAssumeRoleArn               = "assumeRoleArn"
	ConfigKeyAssumeRoleSessionName       = "assumeRoleSessionName"
	ConfigKeySchemaNameGenerationClass   = "schemaNameGenerationClass"

	// TransportMetadataKey is the canonical schema-version metadata key under
	// which the per-call `transportName` (Kafka topic, queue name, etc.) is
	// recorded for downstream Glue analytics. String-identical to Java
	// `AWSSchemaRegistryConstants.java:157` (`TRANSPORT_METADATA_KEY =
	// "x-amz-meta-transport"`). The encoder's metadata-flush helper always
	// injects this entry — even when `transportName == ""` — to match the
	// unconditional `metadata.put(...)` at Java
	// `GlueSchemaRegistrySerializationFacade.java:90-95`. Pinned by spec §3.4
	// (AC-9, AC-9b) and INV-7 (always-on transport metadata).
	TransportMetadataKey = "x-amz-meta-transport"

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
// Fields AvroRecordType and ProtobufMessageType are validated at parse time
// (LoadConfigFromMap) and held here for consumption by the format-layer
// deserializers (PBI-02, PBI-03). Invalid enum values are rejected with
// ErrInvalidAvroRecordType / ErrInvalidProtobufMessageType respectively,
// mirroring Java's config-constructor throw.
//
// Note: Config.SecondaryDeserializer was removed as of Phase 4.14. Passing
// the "secondaryDeserializer" key in a configMap is a no-op (silently
// ignored). See pkg/gsrserde-go/deserializer package docs for rationale.
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
	UserAgentApp          string
	// EffectiveUserAgentApp carries the resolved value used by the User-Agent
	// middleware: equals UserAgentApp when raw is non-empty, otherwise
	// DefaultUserAgentApp. Raw UserAgentApp is preserved (INV-3 / C-15) so
	// callers introspecting raw input can still distinguish default from
	// explicit. Mirrors Java's distinction between the public getter (empty
	// when not set) and the wire stamp (always non-empty).
	EffectiveUserAgentApp string
	AssumeRoleArn         string
	// AssumeRoleSessionName holds the *resolved* session name handed to
	// stscreds.AssumeRoleOptions when AssumeRoleArn is set: the raw
	// `assumeRoleSessionName` value when supplied, otherwise
	// DefaultAssumeRoleSession. When AssumeRoleArn is empty the AssumeRole
	// path is dormant and this field preserves raw input semantics (empty
	// when the key is absent). Acts as a Tier-1 test seam (INV-4 / C-16)
	// because stscreds.AssumeRoleProvider's options field is unexported and
	// the resolved value cannot otherwise be observed in unit tests.
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

	// Resolve the AssumeRole session name once so the SAME value reaches both
	// stscreds.AssumeRoleOptions and Config.AssumeRoleSessionName — the field
	// serves as a Tier-1 test seam proving the override reached the STS path
	// (spec §3.8 / INV-4 / C-16). Resolution only kicks in when an ARN is
	// configured; without an ARN the AssumeRole path is dormant and the field
	// preserves raw input semantics.
	resolvedSession := configMap[ConfigKeyAssumeRoleSessionName]
	if resolvedSession == "" && configMap[ConfigKeyAssumeRoleArn] != "" {
		resolvedSession = DefaultAssumeRoleSession
	}

	// AssumeRole wrap, mirroring Java GlueSchemaRegistryConfiguration's
	// optional STS credential chain.
	if arn := configMap[ConfigKeyAssumeRoleArn]; arn != "" {
		stsClient := sts.NewFromConfig(cfg)
		cfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, arn, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = resolvedSession
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

	// Validate avroRecordType enum at config-construction time, mirroring
	// Java's config-constructor throw. Empty string means "use default
	// (GENERIC_RECORD)" — valid and passes through unchanged.
	avroRecordType := configMap[ConfigKeyAvroRecordType]
	if avroRecordType != "" {
		if err := validateAvroRecordType(avroRecordType); err != nil {
			return nil, err
		}
	}

	// Validate protobufMessageType enum at config-construction time. Empty
	// string means "use default (auto-dispatch / DYNAMIC_MESSAGE behavior)".
	protobufMessageType := configMap[ConfigKeyProtobufMessageType]
	if protobufMessageType != "" {
		if err := validateProtobufMessageType(protobufMessageType); err != nil {
			return nil, err
		}
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

		AvroRecordType:            avroRecordType,
		ProtobufMessageType:       protobufMessageType,
		UserAgentApp:              configMap[ConfigKeyUserAgentApp],
		EffectiveUserAgentApp:     effectiveUserAgent,
		AssumeRoleArn:             configMap[ConfigKeyAssumeRoleArn],
		AssumeRoleSessionName:     resolvedSession,
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

// validAvroRecordTypes is the case-exact set Java's AvroRecordType enum emits
// via AvroRecordType.valueOf. Case-exact per Java: "GENERIC_RECORD" and
// "SPECIFIC_RECORD" are valid; lowercase or alternate forms are not.
var validAvroRecordTypes = map[string]struct{}{
	"GENERIC_RECORD":  {},
	"SPECIFIC_RECORD": {},
}

// validateAvroRecordType rejects values outside the Java AvroRecordType enum.
// The match is CASE-EXACT — lowercase variants and typos MUST be rejected.
// Empty input is the caller's responsibility — this helper is only invoked on
// explicit non-empty values.
func validateAvroRecordType(value string) error {
	if _, ok := validAvroRecordTypes[value]; !ok {
		return fmt.Errorf("%w: %q (want one of GENERIC_RECORD, SPECIFIC_RECORD)", ErrInvalidAvroRecordType, value)
	}
	return nil
}

// validProtobufMessageTypes is the case-exact set Java's ProtobufMessageType
// enum emits. Case-exact per Java: "POJO" and "DYNAMIC_MESSAGE" are valid.
var validProtobufMessageTypes = map[string]struct{}{
	"POJO":            {},
	"DYNAMIC_MESSAGE": {},
}

// validateProtobufMessageType rejects values outside the Java
// ProtobufMessageType enum. CASE-EXACT — lowercase variants and typos MUST
// be rejected. Empty input is the caller's responsibility — only invoked on
// explicit non-empty values.
func validateProtobufMessageType(value string) error {
	if _, ok := validProtobufMessageTypes[value]; !ok {
		return fmt.Errorf("%w: %q (want one of POJO, DYNAMIC_MESSAGE)", ErrInvalidProtobufMessageType, value)
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
