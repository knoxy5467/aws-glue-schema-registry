package gsrserde

// Schema represents a schema in the registry
type Schema struct {
	Definition     string
	DataFormat     string
	SchemaName     string
	AdditionalInfo string
}

// NewSchema creates a new schema
func NewSchema(definition, dataFormat, schemaName string) *Schema {
	return &Schema{
		Definition:     definition,
		DataFormat:     dataFormat,
		SchemaName:     schemaName,
		AdditionalInfo: "",
	}
}
