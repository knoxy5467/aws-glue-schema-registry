package gsrserde

import (
	"context"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// Config holds the configuration for GSR
type Config struct {
	AWSConfig                     aws.Config
	RegistryName                  string
	Compatibility                 string
	Description                   string
	SchemaAutoRegistrationEnabled bool
	CompressionType               string
	TimeToLiveMillis              int64
	CacheSize                     int
	Endpoint                      string
	Tags                          map[string]string
}

// LoadConfigFromMap loads configuration from a map
func LoadConfigFromMap(configMap map[string]string) (*Config, error) {
	var cfg aws.Config
	var err error
	if region, exists := configMap["region"]; exists {
		cfg, err = config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	} else {
		cfg, err = config.LoadDefaultConfig(context.Background())
	}
	if err != nil {
		return nil, err
	}

	registryName := "default-registry"
	if name, exists := configMap["registry.name"]; exists {
		registryName = name
	}

	compatibility := "BACKWARD"
	if comp, exists := configMap["compatibility"]; exists {
		compatibility = comp
	}

	description := ""
	if desc, exists := configMap["description"]; exists {
		description = desc
	}

	schemaAutoRegistrationEnabled := false
	if autoReg, exists := configMap["schemaAutoRegistrationEnabled"]; exists {
		schemaAutoRegistrationEnabled = autoReg == "true"
	}

	compressionType := "NONE"
	if comp, exists := configMap["compressionType"]; exists {
		compressionType = comp
	}

	timeToLiveMillis := int64(24 * 60 * 60 * 1000) // 24 hours default
	if ttl, exists := configMap["timeToLiveMillis"]; exists {
		if parsed, err := strconv.ParseInt(ttl, 10, 64); err == nil {
			timeToLiveMillis = parsed
		}
	}

	cacheSize := 200
	if cs, exists := configMap["cacheSize"]; exists {
		if parsed, err := strconv.Atoi(cs); err == nil {
			cacheSize = parsed
		}
	}

	endpoint := ""
	if ep, exists := configMap["endpoint"]; exists {
		endpoint = ep
	}

	tags := make(map[string]string)
	for key, value := range configMap {
		if strings.HasPrefix(key, "tags.") {
			tagKey := strings.TrimPrefix(key, "tags.")
			tags[tagKey] = value
		}
	}

	return &Config{
		AWSConfig:                     cfg,
		RegistryName:                  registryName,
		Compatibility:                 compatibility,
		Description:                   description,
		SchemaAutoRegistrationEnabled: schemaAutoRegistrationEnabled,
		CompressionType:               compressionType,
		TimeToLiveMillis:              timeToLiveMillis,
		CacheSize:                     cacheSize,
		Endpoint:                      endpoint,
		Tags:                          tags,
	}, nil
}
