package gsrserde

import (
	"testing"
)

func TestLoadConfigFromMapDefaults(t *testing.T) {
	config, err := LoadConfigFromMap(map[string]string{})
	if err != nil {
		t.Fatalf("LoadConfigFromMap failed: %v", err)
	}

	if config.RegistryName != "default-registry" {
		t.Errorf("Expected default registry name, got %s", config.RegistryName)
	}
	if config.Compatibility != "BACKWARD" {
		t.Errorf("Expected BACKWARD compatibility, got %s", config.Compatibility)
	}
	if config.CompressionType != "NONE" {
		t.Errorf("Expected NONE compression, got %s", config.CompressionType)
	}
}
