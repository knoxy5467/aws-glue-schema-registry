package gsrserde

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// singleMessageSchema is a proto3 schema with one top-level message. The
// BFS+lex-sort algorithm yields a single entry, so SingleMessage's wire index
// is 0 and prefixMessageIndexToBytes prepends a single 0x00 byte.
const singleMessageSchema = `syntax = "proto3"; message SingleMessage { string name = 1; }`

func TestConvertBase64SchemaToStringSchema(t *testing.T) {
	fdProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test_message.proto"),
		Package: proto.String("test"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("TestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:   proto.String("name"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	data, err := proto.Marshal(fdProto)
	require.NoError(t, err)
	base64Schema := base64.StdEncoding.EncodeToString(data)

	protoText, err := ConvertBase64SchemaToStringSchema(base64Schema)
	require.NoError(t, err)

	assert.Contains(t, protoText, `syntax = "proto3"`)
	assert.Contains(t, protoText, "package test")
	assert.Contains(t, protoText, "message TestMessage")
	assert.Contains(t, protoText, "string id = 1")
	assert.Contains(t, protoText, "string name = 2")
	_ = strings.Contains // keep import in case future cases re-need it
}

func TestPrefixMessageIndexToBytes_SingleMessageEmitsLeadingZeroVarint(t *testing.T) {
	data := []byte("test data")

	result, err := prefixMessageIndexToBytes(data, singleMessageSchema, "SingleMessage")
	require.NoError(t, err)

	// varint(0) is a single 0x00 byte; payload follows.
	assert.Equal(t, append([]byte{0x00}, data...), result)
}

func TestPrefixMessageIndexToBytes_MessageNotFoundReturnsTypedError(t *testing.T) {
	_, err := prefixMessageIndexToBytes([]byte("test"), singleMessageSchema, "NotInSchema")

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMessageTypeNotFound),
		"caller relies on errors.Is(err, ErrMessageTypeNotFound) to refuse to ship a corrupted payload")
}

// TestVarintBoundaries asserts the hand-encoded varint forms at the boundaries
// Java's CodedOutputStream.writeUInt32NoTag cares about: 127 is the last
// single-byte varint, 128 is the first two-byte, 16383 is the last two-byte,
// 16384 is the first three-byte. prefixMessageIndexToBytes derives the index
// from the schema, so this test asserts via stripMessageIndex against
// hand-built varint prefixes — that's the round-trip half of the property.
func TestStripMessageIndex_VarintBoundaries(t *testing.T) {
	cases := []struct {
		name    string
		prefix  []byte
		wantIdx uint32
	}{
		{"index 0 — single byte", []byte{0x00}, 0},
		{"index 127 — last single-byte", []byte{0x7f}, 127},
		{"index 128 — first two-byte", []byte{0x80, 0x01}, 128},
		{"index 16383 — last two-byte", []byte{0xff, 0x7f}, 16383},
		{"index 16384 — first three-byte", []byte{0x80, 0x80, 0x01}, 16384},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte{0xAB, 0xCD}
			wire := append(append([]byte{}, tc.prefix...), payload...)

			gotIdx, rest, err := stripMessageIndex(wire)
			require.NoError(t, err)
			assert.Equal(t, tc.wantIdx, gotIdx)
			assert.Equal(t, payload, rest)
		})
	}
}

func TestStripMessageIndex_SingleMessageStripsLeadingZero(t *testing.T) {
	payload := []byte("test data")
	wire, err := prefixMessageIndexToBytes(payload, singleMessageSchema, "SingleMessage")
	require.NoError(t, err)

	idx, got, err := stripMessageIndex(wire)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), idx)
	assert.Equal(t, payload, got)
}

func TestStripMessageIndex_RoundTripProperty(t *testing.T) {
	for _, p := range [][]byte{
		{},
		{0x00},
		{0xFF, 0xFE},
		[]byte("Hello"),
		make([]byte, 256),
	} {
		wire, err := prefixMessageIndexToBytes(p, singleMessageSchema, "SingleMessage")
		require.NoError(t, err)

		_, got, err := stripMessageIndex(wire)
		require.NoError(t, err)
		assert.Equalf(t, p, got, "round trip with payload len=%d", len(p))
	}
}

func TestStripMessageIndex_EmptyInputErrors(t *testing.T) {
	_, _, err := stripMessageIndex(nil)
	assert.Error(t, err)
}

func TestStripMessageIndex_MalformedVarintErrors(t *testing.T) {
	// All five bytes have the continuation bit set; no sixth byte to
	// terminate. stripMessageIndex must surface this rather than silently
	// keep reading off the end of the slice.
	bad := []byte{0x80, 0x80, 0x80, 0x80, 0x80}
	_, _, err := stripMessageIndex(bad)
	assert.Error(t, err)
}
