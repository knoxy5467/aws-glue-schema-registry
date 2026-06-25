package gsrserde

// SchemaNameStrategy maps a transportName (a Kafka topic, Kinesis stream
// name, etc.) plus optional payload context to the Glue schema name the
// encoder will register or look up against.
//
// Java parity:
//
//	common/src/main/java/com/amazonaws/services/schemaregistry/common/AWSSchemaNamingStrategy.java
//
// Java exposes three method variants: getSchemaName(transportName),
// getSchemaName(transportName, data), and
// getSchemaName(transportName, data, isKey). Java's default impls collapse
// the last two onto the first; the Go interface follows the same shape and
// promises the same default semantics — implementers only need to provide
// the (transportName) overload, and embed DefaultSchemaNameStrategy to get
// the other two for free.
//
// The original Loop plan said Java's class-name-by-config-key idiom
// (schemaNameGenerationClass = "com.example.MyStrategy") would NOT carry
// over to Go because Go has no dynamic class loading. Go callers inject a
// SchemaNameStrategy at construction time instead; the
// SchemaNameGenerationClass config field is kept on Config for diagnostic
// purposes only (so callers ported from Java config can see what the Java
// side would have picked).
type SchemaNameStrategy interface {
	// SchemaName is the primary contract — returns the schema name for the
	// given transportName.
	SchemaName(transportName string) string

	// SchemaNameForData is invoked when the encoder has the payload in hand.
	// Java's default delegates to SchemaName(transportName); embedders
	// inheriting from DefaultSchemaNameStrategy get the same behavior.
	SchemaNameForData(transportName string, data []byte) string

	// SchemaNameForKey distinguishes Kafka key vs. value: when isKey is
	// true Java's default returns SchemaName(transportName) (ignores the
	// payload), otherwise SchemaNameForData(transportName, data).
	SchemaNameForKey(transportName string, data []byte, isKey bool) string
}

// DefaultSchemaNameStrategy mirrors Java's AWSSchemaNamingStrategyDefaultImpl
// and is the only impl shipped in core/. It returns the transportName
// verbatim — callers that need anything else implement SchemaNameStrategy
// themselves.
type DefaultSchemaNameStrategy struct{}

func (DefaultSchemaNameStrategy) SchemaName(transportName string) string {
	return transportName
}

func (s DefaultSchemaNameStrategy) SchemaNameForData(transportName string, data []byte) string {
	return s.SchemaName(transportName)
}

func (s DefaultSchemaNameStrategy) SchemaNameForKey(transportName string, data []byte, isKey bool) string {
	if isKey {
		return s.SchemaName(transportName)
	}
	return s.SchemaNameForData(transportName, data)
}

// Compile-time check that DefaultSchemaNameStrategy satisfies the interface.
var _ SchemaNameStrategy = DefaultSchemaNameStrategy{}
