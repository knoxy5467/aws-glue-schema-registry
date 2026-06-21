package gsrserde

import (
	"context"
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
	if userAgent := configMap[ConfigKeyUserAgentApp]; userAgent != "" {
		ua := userAgent
		loadOpts = append(loadOpts, config.WithAPIOptions([]func(*smithymiddleware.Stack) error{
			awsmiddleware.AddUserAgentKey("glue-schema-registry-go/" + ua),
		}))
	}
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
		compatibility = v
	}

	compressionType := DefaultCompressionType
	// Accept both the Java key "compression" and the historical Go key
	// "compressionType". Java wins if both are set.
	if v := configMap["compressionType"]; v != "" {
		compressionType = v
	}
	if v := configMap[ConfigKeyCompressionType]; v != "" {
		compressionType = v
	}

	autoRegister := false
	if v := configMap[ConfigKeySchemaAutoRegistration]; v != "" {
		autoRegister = parseBool(v)
	}

	ttl := DefaultCacheTTLMillis
	if v := configMap[ConfigKeyCacheTTLMillis]; v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			ttl = parsed
		}
	}

	cacheSize := DefaultCacheSize
	if v := configMap[ConfigKeyCacheSize]; v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			cacheSize = parsed
		}
	}

	return &Config{
		AWSConfig:                     cfg,
		Region:                        region,
		Endpoint:                      endpoint,
		ProxyURL:                      configMap[ConfigKeyProxyURL],
		RegistryName:                  registryName,
		Compatibility:                 compatibility,
		Description:                   configMap[ConfigKeyDescription],
		SchemaAutoRegistrationEnabled: autoRegister,
		CompressionType:               compressionType,
		TimeToLiveMillis:              ttl,
		CacheSize:                     cacheSize,

		AvroRecordType:            configMap[ConfigKeyAvroRecordType],
		ProtobufMessageType:       configMap[ConfigKeyProtobufMessageType],
		SecondaryDeserializer:     configMap[ConfigKeySecondaryDeserializer],
		UserAgentApp:              configMap[ConfigKeyUserAgentApp],
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
