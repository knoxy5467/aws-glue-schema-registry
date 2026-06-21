package serializer

import (
	"errors"
	"fmt"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// ErrClosed is returned when Serialize/Close is called on a Serializer
// whose Close() has already run. The legacy CGO wrapper exposed an
// identically-named error from a sibling package; deleting that wrapper
// pulled the sentinel into this package.
var ErrClosed = errors.New("serializer is closed")

// ErrNilData is returned by ValidateData when data is nil.
var ErrNilData = errors.New("data cannot be nil")

// Serializer orchestrates the format-layer SerDe and the GSR wire-format
// encode. It owns a *gsrcore.GsrEncoder for the header/compression/UUID
// resolution; the format-layer DataFormatSerializer handles the payload
// encoding.
type Serializer struct {
	coreEncoder      *gsrcore.GsrEncoder
	formatSerializer DataFormatSerializer
	formatFactory    SerializerFactory
	config           *common.Configuration
	schemaNaming     gsrcore.SchemaNameStrategy
	closed           bool
}

// NewSerializer is the production constructor. It builds a real
// *gsrcore.GsrEncoder from the config's GsrConfig map (which in turn loads
// an aws.Config and instantiates the Glue SDK client) and uses
// DefaultSchemaNameStrategy.
func NewSerializer(config *common.Configuration) (*Serializer, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}
	enc, err := gsrcore.NewGsrEncoder(config.GsrConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create core encoder: %w", err)
	}
	return NewSerializerWithEncoderAndStrategy(config, enc, gsrcore.DefaultSchemaNameStrategy{})
}

// NewSerializerWithEncoder is the test-seam constructor: callers supply a
// pre-built *gsrcore.GsrEncoder (typically backed by a fake GlueClient via
// gsrcore.NewGsrEncoderForTest) and get the default SchemaNameStrategy.
func NewSerializerWithEncoder(config *common.Configuration, enc *gsrcore.GsrEncoder) (*Serializer, error) {
	return NewSerializerWithEncoderAndStrategy(config, enc, gsrcore.DefaultSchemaNameStrategy{})
}

// NewSerializerWithEncoderAndStrategy is the most general constructor. The
// production NewSerializer and the test seam NewSerializerWithEncoder both
// route through this.
func NewSerializerWithEncoderAndStrategy(config *common.Configuration, enc *gsrcore.GsrEncoder, strategy gsrcore.SchemaNameStrategy) (*Serializer, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}
	if enc == nil {
		return nil, fmt.Errorf("encoder cannot be nil")
	}
	if strategy == nil {
		strategy = gsrcore.DefaultSchemaNameStrategy{}
	}
	factory := GetSerializerFactory()
	formatSer, err := factory.GetSerializer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create format serializer: %w", err)
	}
	return &Serializer{
		coreEncoder:      enc,
		formatSerializer: formatSer,
		formatFactory:    factory,
		config:           config,
		schemaNaming:     strategy,
	}, nil
}

// Serialize encodes a payload using the configured format serializer and
// prepends the GSR wire-format header (delegated to core).
func (s *Serializer) Serialize(topic string, data interface{}) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}
	if data == nil {
		return nil, nil
	}

	schema, err := s.schemaFromData(data, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema from data: %w", err)
	}

	if err := s.formatSerializer.SetAdditionalSchemaInfo(data, schema); err != nil {
		return nil, fmt.Errorf("failed to set additional schema info: %w", err)
	}

	if err := s.ValidateData(data); err != nil {
		return nil, fmt.Errorf("data validation failed: %w", err)
	}

	serializedData, err := s.formatSerializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize data: %w", err)
	}

	encodedData, err := s.coreEncoder.Encode(serializedData, topic, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to encode GSR data: %w", err)
	}
	return encodedData, nil
}

// ValidateData validates that the provided data can be serialized.
func (s *Serializer) ValidateData(data interface{}) error {
	if s.closed {
		return ErrClosed
	}
	if data == nil {
		return ErrNilData
	}
	return s.formatSerializer.ValidateObject(data)
}

// schemaFromData mirrors the legacy getSchemaFromData but routes the
// schema name through the injected SchemaNameStrategy rather than the
// hard-coded "topic + \"-value\"" of the CGO path.
func (s *Serializer) schemaFromData(data interface{}, topic string) (*gsrcore.Schema, error) {
	if data == nil {
		return nil, ErrNilData
	}

	schema := &gsrcore.Schema{}

	switch data.(type) {
	case *avro.AvroRecord:
		schema.DataFormat = "AVRO"
	default:
		if _, ok := data.(interface{ ProtoMessage() }); ok {
			schema.DataFormat = "PROTOBUF"
		} else {
			schema.DataFormat = "JSON"
		}
	}

	definition, err := s.formatSerializer.GetSchemaDefinition(data)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema definition: %w", err)
	}
	schema.SchemaDefinition = definition

	if err := s.formatSerializer.SetAdditionalSchemaInfo(data, schema); err != nil {
		return nil, fmt.Errorf("failed to set additional schema info: %w", err)
	}

	schema.SchemaName = s.schemaNaming.SchemaName(topic)
	return schema, nil
}

// GetConfiguration returns the current configuration.
func (s *Serializer) GetConfiguration() *common.Configuration {
	return s.config
}

// Close releases all resources owned by the serializer.
func (s *Serializer) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	if s.coreEncoder != nil {
		if err := s.coreEncoder.Close(); err != nil {
			return fmt.Errorf("failed to close core encoder: %w", err)
		}
	}
	return nil
}

// IsClosed reports whether Close() has been called.
func (s *Serializer) IsClosed() bool { return s.closed }
