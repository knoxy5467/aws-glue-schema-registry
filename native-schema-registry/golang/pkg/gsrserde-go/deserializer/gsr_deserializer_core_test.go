// Tier-1 tests for gsr_deserializer.Deserializer against the core-backed
// implementation. No AWS calls; the core.GsrDecoder is built with a fake
// Glue client + a pre-populated cache so Decode hits the cached path and
// no SDK call is made.

package deserializer

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
)

type fakeGlueClient struct{ mock.Mock }

func (f *fakeGlueClient) GetSchemaByDefinition(ctx context.Context, in *glue.GetSchemaByDefinitionInput, _ ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
	return nil, nil
}
func (f *fakeGlueClient) GetSchemaVersion(ctx context.Context, in *glue.GetSchemaVersionInput, _ ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	args := f.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetSchemaVersionOutput), args.Error(1)
}
func (f *fakeGlueClient) CreateSchema(ctx context.Context, in *glue.CreateSchemaInput, _ ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
	return nil, nil
}
func (f *fakeGlueClient) RegisterSchemaVersion(ctx context.Context, in *glue.RegisterSchemaVersionInput, _ ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
	return nil, nil
}
func (f *fakeGlueClient) PutSchemaVersionMetadata(ctx context.Context, in *glue.PutSchemaVersionMetadataInput, _ ...func(*glue.Options)) (*glue.PutSchemaVersionMetadataOutput, error) {
	return nil, nil
}
func (f *fakeGlueClient) QuerySchemaVersionMetadata(ctx context.Context, in *glue.QuerySchemaVersionMetadataInput, _ ...func(*glue.Options)) (*glue.QuerySchemaVersionMetadataOutput, error) {
	return nil, nil
}
func (f *fakeGlueClient) GetTags(ctx context.Context, in *glue.GetTagsInput, _ ...func(*glue.Options)) (*glue.GetTagsOutput, error) {
	return nil, nil
}

const knownJSONVersionID = "33333333-3333-3333-3333-333333333333"

// jsonSchemaDefinition is used to populate the decoder cache so the test
// payload's UUID resolves to a JSON schema without an AWS call.
const jsonSchemaDefinition = `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`

// framePayload builds a minimal 18-byte-prefixed GSR payload for the given
// schemaVersionID + payload bytes. Mirrors the Java wire format
// (version 0x03, compression byte 0x00, 16-byte UUID, then payload).
func framePayload(t *testing.T, schemaVersionID string, payload []byte) []byte {
	t.Helper()
	id, err := uuid.Parse(schemaVersionID)
	require.NoError(t, err)
	out := make([]byte, 0, gsrcore.WireFormatHeaderSize+len(payload))
	out = append(out, gsrcore.WireFormatVersionByte)
	out = append(out, gsrcore.CompressionByteNone)
	// Java writes MSB then LSB — uuid.MarshalBinary is MSB-first already.
	bin, err := id.MarshalBinary()
	require.NoError(t, err)
	out = append(out, bin...)
	out = append(out, payload...)
	// Sanity: the prefix must be exactly 18 bytes.
	require.Equal(t, gsrcore.WireFormatHeaderSize, 18)
	require.Equal(t, gsrcore.WireFormatHeaderSize, len(out)-len(payload))
	// Quiet the unused-import warning when payload is short.
	_ = binary.BigEndian
	return out
}

// newCoreDecoder builds a core.GsrDecoder wired with the fake Glue client
// and a pre-populated cache so Decode never hits AWS.
func newCoreDecoder(t *testing.T, fake *fakeGlueClient, cached map[string]*gsrcore.Schema) *gsrcore.GsrDecoder {
	t.Helper()
	dec, err := gsrcore.NewGsrDecoderForTest(fake, gsrcore.GsrDecoderOptions{
		RegistryName: "test-registry",
	})
	require.NoError(t, err)
	for id, schema := range cached {
		gsrcore.PrimeSchemaCache(dec, id, schema)
	}
	return dec
}

func jsonConfig() *common.Configuration {
	return common.NewConfiguration(map[string]any{
		common.DataFormatTypeKey: common.DataFormatJSON,
	})
}

func TestDeserialize_JsonFormat_UsesCoreCachedPath(t *testing.T) {
	fake := &fakeGlueClient{}
	dec := newCoreDecoder(t, fake, map[string]*gsrcore.Schema{
		knownJSONVersionID: {
			SchemaName:       "my-json-topic",
			SchemaDefinition: jsonSchemaDefinition,
			DataFormat:       "JSON",
			SchemaVersionID:  knownJSONVersionID,
		},
	})

	d, err := NewDeserializerWithDecoder(jsonConfig(), dec)
	require.NoError(t, err)

	payload := []byte(`{"name":"bob"}`)
	framed := framePayload(t, knownJSONVersionID, payload)

	got, err := d.Deserialize("my-json-topic", framed)
	require.NoError(t, err)
	// JSON deserializer returns the payload as a string.
	require.Equal(t, string(payload), got)

	// Cached path was used — GetSchemaVersion never called.
	fake.AssertNotCalled(t, "GetSchemaVersion", mock.Anything, mock.Anything)
}

func TestDeserialize_NilData_ReturnsNil(t *testing.T) {
	fake := &fakeGlueClient{}
	dec := newCoreDecoder(t, fake, nil)
	d, err := NewDeserializerWithDecoder(jsonConfig(), dec)
	require.NoError(t, err)

	out, err := d.Deserialize("any-topic", nil)
	require.NoError(t, err)
	require.Nil(t, out)
}

func TestDeserialize_NonGSRBytes_ReturnsError(t *testing.T) {
	fake := &fakeGlueClient{}
	dec := newCoreDecoder(t, fake, nil)
	d, err := NewDeserializerWithDecoder(jsonConfig(), dec)
	require.NoError(t, err)

	_, err = d.Deserialize("any-topic", []byte("not GSR"))
	require.Error(t, err)
}

func TestDeserialize_ClosedDeserializer_ReturnsError(t *testing.T) {
	fake := &fakeGlueClient{}
	dec := newCoreDecoder(t, fake, nil)
	d, err := NewDeserializerWithDecoder(jsonConfig(), dec)
	require.NoError(t, err)

	require.NoError(t, d.Close())

	_, err = d.Deserialize("any-topic", []byte{0x03, 0x00, 0x01})
	require.Error(t, err)
}
