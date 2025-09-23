package gsrserde

import (
	"testing"
	"time"
)

func TestCacheTTL(t *testing.T) {
	// Create cache with 100ms TTL
	cache, err := NewCache(100)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	schema := &Schema{SchemaName: "test-schema"}
	
	// Set value
	cache.Set("test-key", schema)
	
	// Should exist immediately
	if value, exists := cache.Get("test-key"); !exists {
		t.Error("Value should exist immediately after set")
	} else if value.(*Schema).SchemaName != "test-schema" {
		t.Error("Retrieved value doesn't match")
	}
	
	// Wait for TTL expiration
	time.Sleep(150 * time.Millisecond)
	
	// Should be expired
	if _, exists := cache.Get("test-key"); exists {
		t.Error("Value should be expired after TTL")
	}
}

func TestCacheInterface(t *testing.T) {
	cache, err := NewCache(60000)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Test non-existent key
	if _, exists := cache.Get("nonexistent"); exists {
		t.Error("Non-existent key should not exist")
	}
	
	// Test basic set/get
	schema := &Schema{SchemaName: "test"}
	cache.Set("key", schema)
	
	if value, exists := cache.Get("key"); !exists {
		t.Error("Value should exist immediately after set")
	} else if value.(*Schema).SchemaName != "test" {
		t.Error("Retrieved value doesn't match")
	}
}

func TestCacheSize(t *testing.T) {
	// Note: go-cache doesn't have built-in size limits
	// This test verifies basic functionality
	cache, err := NewCache(60000)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Add items
	cache.Set("key1", &Schema{SchemaName: "schema1"})
	cache.Set("key2", &Schema{SchemaName: "schema2"})
	cache.Set("key3", &Schema{SchemaName: "schema3"})
	
	// All should exist (go-cache doesn't enforce size limits)
	if _, exists := cache.Get("key1"); !exists {
		t.Error("key1 should exist")
	}
	if _, exists := cache.Get("key2"); !exists {
		t.Error("key2 should exist")
	}
	if _, exists := cache.Get("key3"); !exists {
		t.Error("key3 should exist")
	}
}
