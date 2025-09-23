package gsrserde

import (
	core "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
)

// Serializer is a wrapper around the pure Go schema registry serializer
type Serializer struct {
	serializer *core.GsrEncoder
	closed     bool
}

// NewSerializerFromMap creates a new serializer instance from a config map
func NewSerializer(configMap map[string]string) (*Serializer, error) {
	coreSerializer, err := core.NewGsrEncoder(configMap)
	if err != nil {
		return nil, err
	}
	
	return &Serializer{
		serializer: coreSerializer,
		closed:     false,
	}, nil
}

// Encode serializes data with the given schema
func (s *Serializer) Encode(data []byte, transportName string, schema *Schema) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}
	
	// Convert to core schema format
	coreSchema := &core.Schema{
		SchemaName:       schema.SchemaName,
		SchemaDefinition: schema.Definition,
		DataFormat:       schema.DataFormat,
	}
	
	return s.serializer.Encode(data, transportName, coreSchema)
}

// Close releases all resources
func (s *Serializer) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	return s.serializer.Close()
}
