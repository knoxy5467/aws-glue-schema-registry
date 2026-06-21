package deserializer

import (
	"errors"
	"fmt"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

// ErrClosed is returned when Deserialize is called on a Deserializer whose
// Close() has already run.
var ErrClosed = errors.New("deserializer is closed")

// ErrNilData is the sentinel for nil data input on the read-side surfaces
// (GetSchema). Deserialize itself returns nil on nil input (Java parity).
var ErrNilData = errors.New("data cannot be nil")

// Deserializer orchestrates the GSR wire-format decode (via core) and the
// format-layer payload decode.
type Deserializer struct {
	coreDecoder        *gsrcore.GsrDecoder
	formatDeserializer DataFormatDeserializer
	formatFactory      DeserializerFactory
	config             *common.Configuration
	closed             bool
}

// NewDeserializer is the production constructor.
func NewDeserializer(config *common.Configuration) (*Deserializer, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}
	dec, err := gsrcore.NewGsrDecoder(config.GsrConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create core decoder: %w", err)
	}
	return NewDeserializerWithDecoder(config, dec)
}

// NewDeserializerWithDecoder is the test-seam constructor: callers supply a
// pre-built *gsrcore.GsrDecoder (typically backed by a fake GlueClient via
// gsrcore.NewGsrDecoderForTest).
func NewDeserializerWithDecoder(config *common.Configuration, dec *gsrcore.GsrDecoder) (*Deserializer, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}
	if dec == nil {
		return nil, fmt.Errorf("decoder cannot be nil")
	}
	factory := GetDeserializerFactory()
	formatDes, err := factory.GetDeserializer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create format deserializer: %w", err)
	}
	return &Deserializer{
		coreDecoder:        dec,
		formatDeserializer: formatDes,
		formatFactory:      factory,
		config:             config,
	}, nil
}

// Deserialize decodes the GSR wire-format payload, looks up the schema via
// core's cache, and delegates payload decoding to the format-layer impl.
// The topic argument is retained for parity with the legacy/Java surface
// but is not used by the core decoder (the wire-format prefix carries the
// schema-version-id directly).
func (d *Deserializer) Deserialize(topic string, data []byte) (interface{}, error) {
	if d.closed {
		return nil, ErrClosed
	}
	if data == nil {
		return nil, nil
	}

	canDecode, err := d.coreDecoder.CanDecode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to check if data can be decoded: %w", err)
	}
	if !canDecode {
		return nil, fmt.Errorf("byte data cannot be decoded: data does not contain GSR header")
	}

	decodedBytes, err := d.coreDecoder.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode GSR data: %w", err)
	}

	schema, err := d.coreDecoder.DecodeSchema(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode schema: %w", err)
	}

	result, err := d.formatDeserializer.Deserialize(decodedBytes, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize %s data: %w", d.config.DataFormat, err)
	}
	return result, nil
}

// CanDeserialize reports whether data is GSR-framed.
func (d *Deserializer) CanDeserialize(data []byte) (bool, error) {
	if d.closed {
		return false, ErrClosed
	}
	if data == nil {
		return false, nil
	}
	return d.coreDecoder.CanDecode(data)
}

// GetSchema returns the schema referenced by the wire-format prefix without
// decoding the payload.
func (d *Deserializer) GetSchema(data []byte) (*gsrcore.Schema, error) {
	if d.closed {
		return nil, ErrClosed
	}
	if data == nil {
		return nil, ErrNilData
	}
	return d.coreDecoder.DecodeSchema(data)
}

// GetConfiguration returns the current configuration.
func (d *Deserializer) GetConfiguration() *common.Configuration {
	return d.config
}

// Close releases all resources owned by the deserializer.
func (d *Deserializer) Close() error {
	if d == nil || d.closed {
		return nil
	}
	d.closed = true
	if d.coreDecoder != nil {
		if err := d.coreDecoder.Close(); err != nil {
			return fmt.Errorf("failed to close core decoder: %w", err)
		}
	}
	return nil
}

// IsClosed reports whether Close() has been called.
func (d *Deserializer) IsClosed() bool { return d.closed }
