package gsrserde

import (
	"encoding/base64"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestConvertBase64SchemaToStringSchema(t *testing.T) {
	// Create a simple FileDescriptorProto
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

	// Marshal and encode as base64
	data, err := proto.Marshal(fdProto)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}
	base64Schema := base64.StdEncoding.EncodeToString(data)

	// Test conversion
	protoText, err := ConvertBase64SchemaToStringSchema(base64Schema)
	if err != nil {
		t.Fatalf("Conversion failed: %v", err)
	}

	// Verify output
	if !strings.Contains(protoText, "syntax = \"proto3\"") {
		t.Error("Missing syntax")
	}
	if !strings.Contains(protoText, "package test") {
		t.Error("Missing package")
	}
	if !strings.Contains(protoText, "message TestMessage") {
		t.Error("Missing message")
	}
	if !strings.Contains(protoText, "string id = 1") {
		t.Error("Missing id field")
	}
	if !strings.Contains(protoText, "string name = 2") {
		t.Error("Missing name field")
	}
}

// TODO(phase 1): The two tests below were written against an early stub of
// prefixMessageIndexToBytes / stripMessageIndex that returned the input
// unchanged. They are now red because the implementation correctly emits and
// consumes the unsigned-varint message-index prefix per Java parity (see
// protobuf_utils.go doc comments).
//
// Replace these with spec-anchored assertions during the Phase 1 core rewrite:
//   - prefixMessageIndexToBytes("test data", schema, msgType="TestMessage")
//     where the schema has a single top-level message TestMessage → expect
//     [0x00, 't', 'e', 's', 't', ' ', 'd', 'a', 't', 'a'] (varint(0) + payload).
//   - stripMessageIndex([0x00, 't', 'e', 's', 't', ...]) → expect ['t', 'e', ...].
//   - Multi-byte varint boundaries: index 127 → [0x7f, ...]; index 128 →
//     [0x80, 0x01, ...]; index 16383 → [0xff, 0x7f, ...]; index 16384 →
//     [0x80, 0x80, 0x01, ...].
//   - Round-trip property: stripMessageIndex(prefixMessageIndexToBytes(p, …)) == p.
func TestPrefixMessageIndexToBytes(t *testing.T) {
	data := []byte("test data")
	result := prefixMessageIndexToBytes(data, "schema", "message")

	if string(result) != "test data" {
		t.Error("Expected data unchanged")
	}
}

func TestStripMessageIndex(t *testing.T) {
	data := []byte("test data")
	result := stripMessageIndex(data)

	if string(result) != "test data" {
		t.Error("Expected data unchanged")
	}
}
