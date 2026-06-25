package gsrserde

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// protoDescriptorCache memoizes parseSchemaDefinitionToDescriptor results
// keyed by the raw schema-definition string (Phase 8B).
//
// Why this matters: every PROTOBUF encode passes through
// prefixMessageIndexToBytes → getMessageIndexFromProtoDefinition →
// parseSchemaDefinitionToDescriptor. Parsing/linking a protobuf
// FileDescriptorProto (or .proto text via protoparse) is expensive enough
// to dominate small-payload throughput; the pre-cache perf README
// numbers showed protobuf 100 B encode at 1.4 MB/s vs Avro 100 B at
// 28+ MB/s, and called this out as a deferred optimization.
//
// Cache key trade-off: same as the Avro cache — raw text. The protobuf
// schema-definition string flowing through this path is either base64
// FileDescriptorProto (GSR canonical form, byte-stable) or .proto text
// (registered by the customer). Both forms are typically registered once
// and re-used, so raw-text keying captures the win.
//
// Import scope: parseSchemaDefinitionToDescriptor uses
// protoparse.FileContentsFromMap with a single-file map and no external
// ImportPaths. The descriptor it produces is a pure function of the
// schema text — no external dependencies, so caching by schema text alone
// is correct.
//
// Concurrent safety: sync.Map is safe for concurrent Load/Store/LoadOrStore.
// Returning a shared *desc.FileDescriptor across goroutines is safe;
// jhump's FileDescriptor is treated as read-only after CreateFileDescriptor
// returns, and our callers only call read-only accessors
// (GetMessageTypes, GetFullyQualifiedName).
//
// Eviction: unbounded sync.Map. A single application uses a small set of
// distinct schemas — usually one per registered Glue schema — so the cache
// size is bounded by the deployment, not by traffic. A bounded LRU would
// be a future enhancement if a workload rotates through thousands of
// distinct schemas.
var protoDescriptorCache sync.Map // map[string]*desc.FileDescriptor

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

// prefixMessageIndexToBytes prepends the protobuf message-index unsigned varint
// to the payload. The message index identifies which message type within the
// schema the payload was serialized as.
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
//
// Returns a wrapped ErrMessageTypeNotFound when messageType is not in the
// schema's BFS+lex-sorted descriptor list. Callers MUST surface this error
// rather than emit the prefix for some other message — that would corrupt the
// wire format.
func prefixMessageIndexToBytes(data []byte, schemaDefinition, messageType string) ([]byte, error) {
	messageIndex, err := getMessageIndexFromProtoDefinition(schemaDefinition, messageType)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0, len(data)+5) // 5 bytes max for varint32

	// Plain unsigned varint, equivalent to CodedOutputStream.writeUInt32NoTag.
	for messageIndex >= 0x80 {
		buf = append(buf, byte(messageIndex)|0x80)
		messageIndex >>= 7
	}
	buf = append(buf, byte(messageIndex))

	buf = append(buf, data...)

	return buf, nil
}

// stripMessageIndex consumes the unsigned varint message-index prefix from data
// and returns the (index, remainingPayload).
//
// Java parity reference:
//
//	serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/deserializers/protobuf/ProtobufWireFormatDecoder.java:33-37
//	  getAndRemoveMessageIndex(byte[]) calls CodedInputStream.readUInt32() and
//	  returns (index, remainingStream). The remaining stream is everything after
//	  the consumed varint bytes.
//
// Returns NewDeserializationError when the varint is malformed (more than 5
// bytes or runs off the end of data without a continuation-bit terminator).
func stripMessageIndex(data []byte) (uint32, []byte, error) {
	if len(data) < 1 {
		return 0, nil, NewDeserializationError("protobuf payload too short to contain message-index varint")
	}

	var index uint32
	var shift uint
	pos := 0
	terminated := false

	for pos < len(data) {
		b := data[pos]
		pos++

		index |= uint32(b&0x7F) << shift
		if b&0x80 == 0 {
			terminated = true
			break
		}
		shift += 7
		if shift >= 32 {
			return 0, nil, NewDeserializationError("malformed protobuf message-index varint: exceeds 5 bytes")
		}
	}

	if !terminated {
		return 0, nil, NewDeserializationError("malformed protobuf message-index varint: missing continuation terminator")
	}

	return index, data[pos:], nil
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
// Returns ErrMessageTypeNotFound (wrapped with the messageType for context)
// when messageType is not in the sorted list — mirrors Java's
// AWSSchemaRegistryException at MessageIndexFinder.java:35-39.
func getMessageIndexFromProtoDefinition(schemaDefinition, messageType string) (uint32, error) {
	fileDesc, err := parseSchemaDefinitionToDescriptor(schemaDefinition)
	if err != nil {
		return 0, fmt.Errorf("parse schema definition: %w", err)
	}

	messageTypes := getAllMessageTypesFromDescriptor(fileDesc)

	sort.Strings(messageTypes)

	for i, msgType := range messageTypes {
		if msgType == messageType {
			return uint32(i), nil
		}
	}

	return 0, fmt.Errorf("%w: %q (sorted candidates: %v)", ErrMessageTypeNotFound, messageType, messageTypes)
}

// parseSchemaDefinitionToDescriptor parses a protobuf schema definition into
// a jhump *desc.FileDescriptor, accepting either base64 FileDescriptorProto
// (GSR canonical form) or .proto text.
//
// Results are memoized in protoDescriptorCache keyed by the raw schema text
// (Phase 8B). The returned *desc.FileDescriptor is shared across calls and
// MUST be treated as read-only by callers. Errors are NOT cached — a
// transient parse failure is uncommon, and caching errors risks a
// poison-pill scenario if jhump/protoreflect behavior changes across
// versions.
func parseSchemaDefinitionToDescriptor(schemaDefinition string) (*desc.FileDescriptor, error) {
	if cached, ok := protoDescriptorCache.Load(schemaDefinition); ok {
		return cached.(*desc.FileDescriptor), nil
	}

	fileDesc, err := parseSchemaDefinitionToDescriptorUncached(schemaDefinition)
	if err != nil {
		return nil, err
	}

	// LoadOrStore handles the race where two goroutines parse the same
	// schema concurrently — one's value wins, both return the winner.
	actual, _ := protoDescriptorCache.LoadOrStore(schemaDefinition, fileDesc)
	return actual.(*desc.FileDescriptor), nil
}

// parseSchemaDefinitionToDescriptorUncached performs the actual parse work
// without consulting the cache. Split out so the cache wrapper can be
// disabled cleanly in tests (and so future maintainers see the cache layer
// as orthogonal to the parse logic).
func parseSchemaDefinitionToDescriptorUncached(schemaDefinition string) (*desc.FileDescriptor, error) {
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

// protoDescriptorCacheClearForTest clears the proto descriptor cache.
// Exposed only to in-package tests; lower-cased to keep it out of the
// public API.
func protoDescriptorCacheClearForTest() {
	protoDescriptorCache.Range(func(key, _ interface{}) bool {
		protoDescriptorCache.Delete(key)
		return true
	})
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
