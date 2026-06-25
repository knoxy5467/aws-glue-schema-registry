package gsrserde

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultSchemaNameStrategy_SchemaName_ReturnsTransportNameVerbatim(t *testing.T) {
	s := DefaultSchemaNameStrategy{}
	assert.Equal(t, "my-topic", s.SchemaName("my-topic"))
	assert.Equal(t, "stream/x/y", s.SchemaName("stream/x/y"))
	assert.Equal(t, "", s.SchemaName(""))
}

func TestDefaultSchemaNameStrategy_SchemaNameForData_DelegatesToSchemaName(t *testing.T) {
	s := DefaultSchemaNameStrategy{}
	// Java parity: AWSSchemaNamingStrategy.getSchemaName(transportName, data)
	// default delegates to getSchemaName(transportName) — the data argument
	// is ignored.
	assert.Equal(t, "topic-1", s.SchemaNameForData("topic-1", []byte("payload")))
	assert.Equal(t, "topic-1", s.SchemaNameForData("topic-1", nil))
}

func TestDefaultSchemaNameStrategy_SchemaNameForKey(t *testing.T) {
	s := DefaultSchemaNameStrategy{}
	// Java parity:
	//   isKey=true  → getSchemaName(transportName)
	//   isKey=false → getSchemaName(transportName, data)
	// In the default impl both branches collapse to transportName.
	assert.Equal(t, "topic-1", s.SchemaNameForKey("topic-1", []byte("payload"), true))
	assert.Equal(t, "topic-1", s.SchemaNameForKey("topic-1", []byte("payload"), false))
}

// customNamingStrategy is the Java analog of CustomNamingStrategy from
// common/src/test/java/com/amazonaws/services/schemaregistry/utils/external/CustomNamingStrategy.java
// (Java tests inject it via reflection on schemaNameGenerationClass). The
// Go path is dependency-injection — implementers satisfy the interface
// directly. This test asserts that a non-default impl plugs in cleanly.
type customNamingStrategy struct{ Suffix string }

func (c customNamingStrategy) SchemaName(transportName string) string {
	return transportName + c.Suffix
}
func (c customNamingStrategy) SchemaNameForData(transportName string, data []byte) string {
	return c.SchemaName(transportName)
}
func (c customNamingStrategy) SchemaNameForKey(transportName string, data []byte, isKey bool) string {
	if isKey {
		return c.SchemaName(transportName) + "-key"
	}
	return c.SchemaNameForData(transportName, data)
}

func TestCustomSchemaNameStrategy_SatisfiesInterface(t *testing.T) {
	var s SchemaNameStrategy = customNamingStrategy{Suffix: "-value"}
	assert.Equal(t, "orders-value", s.SchemaName("orders"))
	assert.Equal(t, "orders-value", s.SchemaNameForData("orders", []byte("payload")))
	assert.Equal(t, "orders-value-key", s.SchemaNameForKey("orders", []byte("payload"), true))
	assert.Equal(t, "orders-value", s.SchemaNameForKey("orders", []byte("payload"), false))
}
