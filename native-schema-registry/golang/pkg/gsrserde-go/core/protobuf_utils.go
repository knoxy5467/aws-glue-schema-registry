package gsrserde

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ConvertBase64SchemaToStringSchema converts base64 FileDescriptorProto to .proto text
func ConvertBase64SchemaToStringSchema(base64Schema string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(base64Schema)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64 schema: %w", err)
	}
	
	var fdProto descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(data, &fdProto); err != nil {
		return "", fmt.Errorf("failed to unmarshal FileDescriptorProto: %w", err)
	}
	
	// Convert to FileDescriptor using jhump protoreflect
	fileDesc, err := desc.CreateFileDescriptorFromSet(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{&fdProto},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create file descriptor: %w", err)
	}
	
	// Use protoprint to convert to .proto text
	printer := protoprint.Printer{OmitComments: protoprint.CommentsAll}
	
	var writer strings.Builder
	err = printer.PrintProtoFile(fileDesc, &writer)
	if err != nil {
		return "", fmt.Errorf("failed to print proto file: %w", err)
	}
	
	return writer.String(), nil
}

// prefixMessageIndexToBytes prepends the protobuf message-index varint to the
// payload. The message index identifies which message type within the schema
// the payload was serialized as.
//
// Java parity reference:
//
//	serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/protobuf/ProtobufWireFormatEncoder.java:49-62
//	  - writes messageIndex via CodedOutputStream.writeUInt32NoTag(int) (NOT zig-zag,
//	    despite that file's javadoc on line 35 — the implementation is plain unsigned varint).
//	  - then writeRawBytes(payload) appends the payload unchanged.
//
// Wire layout: <varint-encoded uint32 message index> || <payload bytes>.
// For a schema with a single top-level message, messageIndex is 0, so the
// prefix is the single byte 0x00 followed by the payload.
func prefixMessageIndexToBytes(data []byte, schemaDefinition, messageType string) []byte {
	messageIndex := getMessageIndexFromProtoDefinition(schemaDefinition, messageType)

	buf := make([]byte, 0, len(data)+5) // 5 bytes max for varint32

	// Plain unsigned varint, equivalent to CodedOutputStream.writeUInt32NoTag.
	for messageIndex >= 0x80 {
		buf = append(buf, byte(messageIndex)|0x80)
		messageIndex >>= 7
	}
	buf = append(buf, byte(messageIndex))

	buf = append(buf, data...)

	return buf
}

// stripMessageIndex consumes the unsigned varint message-index prefix from data
// and returns the remaining payload bytes.
//
// Java parity reference:
//
//	serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/deserializers/protobuf/ProtobufWireFormatDecoder.java:33-37
//	  getAndRemoveMessageIndex(byte[]) calls CodedInputStream.readUInt32() and
//	  returns (index, remainingStream). The remaining stream is everything after
//	  the consumed varint bytes.
//
// This implementation throws away the decoded index because callers that need
// the index value are expected to use a separate lookup against the schema's
// MessageIndexFinder equivalent. If a future caller needs the index, this
// function should be split into one that returns (index, remainder).
func stripMessageIndex(data []byte) []byte {
	if len(data) < 1 {
		return data
	}

	var index uint32
	var shift uint
	pos := 0

	for pos < len(data) {
		b := data[pos]
		pos++

		index |= uint32(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 32 {
			break // Prevent overflow on a malformed > 5-byte varint.
		}
	}
	_ = index

	if pos < len(data) {
		return data[pos:]
	}
	return []byte{}
}

// getMessageIndexFromProtoDefinition computes the index of messageType within
// the schema's message-type list, using the same algorithm as Java's
// MessageIndexFinder.
//
// Java parity reference:
//
//	serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/protobuf/MessageIndexFinder.java:93-117
//	  - Build the message-type set by BFS over fileDesc.getMessageTypes() then
//	    descriptor.getNestedTypes() (level-order traversal).
//	  - Sort the resulting list lexicographically by Descriptor::getFullName().
//	  - The target's index in the sorted list is the wire message-index.
//
// Example from MessageIndexFinder.java:74-88 — schema:
//
//	message B { message C {} message A { message D {} } }
//
// produces sorted indices: B=0, B.A=1, B.A.D=2, B.C=3.
//
// PARITY DIVERGENCE — TODO(phase 1):
// Java throws AWSSchemaRegistryException when descriptorToFind is not in the
// schema (MessageIndexFinder.java:35-39). This Go implementation silently
// returns 0, which would cause the encoder to emit the prefix for the *first*
// sorted message type and produce a payload that decodes as the wrong type.
// Phase 1 must change the signature to (uint32, error) and propagate the
// not-found case.
func getMessageIndexFromProtoDefinition(schemaDefinition, messageType string) uint32 {
	fileDesc, err := parseSchemaDefinitionToDescriptor(schemaDefinition)
	if err != nil {
		return 0
	}

	messageTypes := getAllMessageTypesFromDescriptor(fileDesc)

	sort.Strings(messageTypes)

	for i, msgType := range messageTypes {
		if msgType == messageType {
			return uint32(i)
		}
	}

	// PARITY DIVERGENCE: Java throws here. See doc comment above.
	return 0
}

func parseSchemaDefinitionToDescriptor(schemaDefinition string) (*desc.FileDescriptor, error) {
	// Try to parse as base64 FileDescriptorProto first (GSR format)
	data, err := base64.StdEncoding.DecodeString(schemaDefinition)
	if err == nil {
		var fdProto descriptorpb.FileDescriptorProto
		if err := proto.Unmarshal(data, &fdProto); err == nil {
			// Convert to FileDescriptor using jhump protoreflect
			fileDesc, err := desc.CreateFileDescriptorFromSet(&descriptorpb.FileDescriptorSet{
				File: []*descriptorpb.FileDescriptorProto{&fdProto},
			})
			if err == nil {
				return fileDesc, nil
			}
		}
	}
	
	// If base64 parsing fails, try parsing as text using protoparse
	parser := protoparse.Parser{
		ImportPaths:      []string{},
		InferImportPaths: true,
	}
	
	accessor := protoparse.FileContentsFromMap(map[string]string{
		"schema.proto": schemaDefinition,
	})
	parser.Accessor = accessor
	
	fileDescs, err := parser.ParseFiles("schema.proto")
	if err != nil || len(fileDescs) == 0 {
		return nil, err
	}
	
	return fileDescs[0], nil
}

// getAllMessageTypesFromDescriptor returns the fully-qualified names of every
// message type reachable from fileDesc, in BFS (level-order) order — matching
// the Java MessageIndexFinder.java:93-106 traversal. The caller is responsible
// for sorting the result; sorting is intentionally separated so this function
// can be unit-tested for traversal order independent of the lexicographic sort.
func getAllMessageTypesFromDescriptor(fileDesc *desc.FileDescriptor) []string {
	var messageTypes []string

	queue := make([]*desc.MessageDescriptor, 0)

	for _, msgDesc := range fileDesc.GetMessageTypes() {
		queue = append(queue, msgDesc)
	}

	for len(queue) > 0 {
		msgDesc := queue[0]
		queue = queue[1:]

		messageTypes = append(messageTypes, msgDesc.GetFullyQualifiedName())

		for _, nestedDesc := range msgDesc.GetNestedMessageTypes() {
			queue = append(queue, nestedDesc)
		}
	}

	return messageTypes
}
