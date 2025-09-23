package gsrserde

import (
	core "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
)

// Deserializer is a wrapper around the pure Go schema registry deserializer
type Deserializer struct {
	deserializer *core.GsrDecoder
	closed       bool
}

// NewDeserializerFromMap creates a new deserializer instance from a config map
func NewDeserializer(configMap map[string]string) (*Deserializer, error) {
	coreDeserializer, err := core.NewGsrDecoder(configMap)
	if err != nil {
		return nil, err
	}
	
	return &Deserializer{
		deserializer: coreDeserializer,
		closed:       false,
	}, nil
}

// Decode deserializes the encoded data
func (d *Deserializer) Decode(data []byte) ([]byte, error) {
	if d.closed {
		return nil, ErrClosed
	}
	
	return d.deserializer.Decode(data)
}

// CanDecode checks if the data can be decoded
func (d *Deserializer) CanDecode(data []byte) (bool, error) {
	if d.closed {
		return false, ErrClosed
	}
	
	return d.deserializer.CanDecode(data)
}

// DecodeSchema extracts the schema from encoded data
func (d *Deserializer) DecodeSchema(data []byte) (*Schema, error) {
	if d.closed {
		return nil, ErrClosed
	}
	
	coreSchema, err := d.deserializer.DecodeSchema(data)
	if err != nil {
		return nil, err
	}
	
	// Convert from core schema format
	return &Schema{
		SchemaName: coreSchema.SchemaName,
		Definition: coreSchema.SchemaDefinition,
		DataFormat: coreSchema.DataFormat,
	}, nil
}

// Close releases all resources
func (d *Deserializer) Close() error {
	if d == nil || d.closed {
		return nil
	}
	d.closed = true
	return d.deserializer.Close()
}
