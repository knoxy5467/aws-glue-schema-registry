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

func prefixMessageIndexToBytes(data []byte, schemaDefinition, messageType string) []byte {
	// Get message index from schema definition
	messageIndex := getMessageIndexFromProtoDefinition(schemaDefinition, messageType)
	
	// Create buffer for varint encoding + data
	buf := make([]byte, 0, len(data)+5) // 5 bytes max for varint32
	
	// Encode message index as varint (matches Java's writeUInt32NoTag)
	for messageIndex >= 0x80 {
		buf = append(buf, byte(messageIndex)|0x80)
		messageIndex >>= 7
	}
	buf = append(buf, byte(messageIndex))
	
	// Append original data
	buf = append(buf, data...)
	
	return buf
}

func stripMessageIndex(data []byte) []byte {
	if len(data) < 1 {
		return data
	}
	
	// Decode varint to find where actual data starts
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
			break // Prevent overflow
		}
	}
	
	// Return data after the varint
	if pos < len(data) {
		return data[pos:]
	}
	return []byte{}
}

func getMessageIndexFromProtoDefinition(schemaDefinition, messageType string) uint32 {
	// Parse schema definition (base64 FileDescriptorProto) into descriptor
	fileDesc, err := parseSchemaDefinitionToDescriptor(schemaDefinition)
	if err != nil {
		return 0
	}
	
	// Get all message types from descriptor (matches Java implementation)
	messageTypes := getAllMessageTypesFromDescriptor(fileDesc)
	
	// Sort lexicographically (matches Java implementation)
	sort.Strings(messageTypes)
	
	// Find index of the target message type
	for i, msgType := range messageTypes {
		if msgType == messageType {
			return uint32(i)
		}
	}
	
	// Default to 0 if not found
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

func getAllMessageTypesFromDescriptor(fileDesc *desc.FileDescriptor) []string {
	var messageTypes []string
	
	// Level-order traversal like Java implementation
	queue := make([]*desc.MessageDescriptor, 0)
	
	// Add top-level message types
	for _, msgDesc := range fileDesc.GetMessageTypes() {
		queue = append(queue, msgDesc)
	}
	
	// Process queue (breadth-first traversal)
	for len(queue) > 0 {
		msgDesc := queue[0]
		queue = queue[1:]
		
		// Add this message type
		messageTypes = append(messageTypes, msgDesc.GetFullyQualifiedName())
		
		// Add nested message types to queue
		for _, nestedDesc := range msgDesc.GetNestedMessageTypes() {
			queue = append(queue, nestedDesc)
		}
	}
	
	return messageTypes
}
