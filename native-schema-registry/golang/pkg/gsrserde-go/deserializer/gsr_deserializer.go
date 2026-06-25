// Package deserializer provides the orchestrating Deserializer that combines
// core wire-format decoding with format-layer payload decoding.
//
// # Transport-agnostic
//
// Although the Deserialize method accepts a `topic` parameter (named after
// Kafka convention), this package has no Kafka dependency. The `topic`
// argument is retained for API symmetry with the serializer but is not
// consumed by the core decoder — the wire-format prefix carries the
// schema-version-id directly. Callers may pass a Kinesis stream name, an SQS
// queue name, an HTTP path, or any other application-defined identifier (or
// the empty string). The production library at pkg/gsrserde-go/ imports no
// Kafka libraries; reference Kafka adapters live in
// integration-tests/pkg/clients/.
//
// # Secondary Deserializer (Not Implemented)
//
// Java's GlueSchemaRegistryKafkaDeserializer supports a
// "secondaryDeserializer" config key that routes non-GSR-framed payloads to a
// fallback deserializer (e.g. a Confluent deserializer). The Go client does
// NOT implement this fallback chain. Instead, callers that encounter
// non-GSR-framed data receive a typed ErrIncompatibleData error (reachable via
// errors.Is(err, gsrcore.ErrIncompatibleData)). Callers may inspect this error
// and route to their own pre-existing decoder as they see fit.
//
// The GSR wire-format header begins with the version byte 0x03. Any payload
// whose first byte is not 0x03, or whose 18-byte header is otherwise malformed,
// will cause Deserialize to return an error wrapping gsrcore.ErrIncompatibleData.
//
// Example — routing non-GSR payloads to an existing decoder:
//
//	result, err := d.Deserialize(topic, data)
//	if err != nil {
//	    if errors.Is(err, gsrcore.ErrIncompatibleData) {
//	        // payload is not GSR-framed; route to your own decoder
//	        return myLegacyDecoder.Decode(data)
//	    }
//	    return nil, err
//	}
//
// The Config.SecondaryDeserializer field has been removed as of Phase 4.14.
// Passing the "secondaryDeserializer" key in a configMap is a no-op (the key
// is silently ignored).
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
		return nil, fmt.Errorf("%w: data does not contain GSR header (leading byte is not 0x03 or header is malformed)", gsrcore.ErrIncompatibleData)
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
