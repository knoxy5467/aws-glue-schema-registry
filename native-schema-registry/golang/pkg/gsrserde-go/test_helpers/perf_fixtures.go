// Phase 6 — shared benchmark fixtures. Avoids duplicating identical
// FileDescriptorProto/schema-string literals across
// {core,serializer,deserializer}/*_bench_test.go.
//
// Lives in test_helpers (not in a _test.go file) so all three benchmark
// packages can import it; each bench package's tests still own their
// per-bench wiring (mocks, fakes, payload encoders).

package test_helpers

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// PerfAvroSchema is the single-field record schema both Avro benches use.
const PerfAvroSchema = `{"type":"record","name":"PerfRecord","namespace":"perf","fields":[{"name":"blob","type":"string"}]}`

// PerfJSONSchema is the single-field object schema both JSON benches use.
const PerfJSONSchema = `{"type":"object","properties":{"blob":{"type":"string"}},"required":["blob"]}`

// PerfProtoTextSchema is the parser-friendly source form of the proto schema
// the core decoder bench needs to feed into prefixMessageIndexToBytes.
const PerfProtoTextSchema = `syntax = "proto3"; package perf; message Payload { bytes blob = 1; }`

// PerfPayload returns a deterministic-but-non-trivial byte buffer of the
// requested size, restricted to printable ASCII. Restricting to ASCII keeps
// `json.Marshal(map[string]string{...: string(p)})` lossless — a previous
// version of the bench used crypto/rand bytes here, which made
// `encoding/json` substitute U+FFFD for invalid UTF-8 sequences and
// distorted JSON throughput numbers (Phase 6.1 review findings 3 + 4).
//
// "Non-trivial" matters for compression benches: a single repeated byte
// compresses to ~0 bytes in zlib; the rotating-printable-ASCII pattern
// here compresses to roughly the same ratio as realistic JSON text.
func PerfPayload(size int) []byte {
	if size <= 0 {
		return nil
	}
	out := make([]byte, size)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 "
	for i := range out {
		out[i] = alphabet[i%len(alphabet)]
	}
	return out
}

// PerfPayloadDescriptor lazy-builds a FileDescriptor for
//
//	syntax = "proto3"; package perf; message Payload { bytes blob = 1; }
//
// dynamicpb wraps the returned descriptor so callers can produce a
// proto.Message without owning a generated .pb.go file. Built once and
// memoized per process so the bench setup doesn't repay the cost.
var perfPayloadFD protoreflect.FileDescriptor

func init() {
	fdProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("perf.proto"),
		Package: proto.String("perf"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Payload"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:     proto.String("blob"),
						Number:   proto.Int32(1),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
						JsonName: proto.String("blob"),
					},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdProto, nil)
	if err != nil {
		panic(fmt.Errorf("test_helpers: build perf proto FileDescriptor: %w", err))
	}
	perfPayloadFD = fd
}

// PerfPayloadDescriptor returns the cached perf.proto FileDescriptor.
// Callers wrap msg := dynamicpb.NewMessage(fd.Messages().ByName("Payload"))
// and set the `blob` field to ship a Phase 6 protobuf benchmark payload.
func PerfPayloadDescriptor() protoreflect.FileDescriptor {
	return perfPayloadFD
}

// PerfPayloadMessageName is "perf.Payload" — the BFS+lex-sorted full name
// the encoder's prefixMessageIndexToBytes uses to resolve message-index 0.
const PerfPayloadMessageName = "perf.Payload"

// Compile-time assert the perf message name actually matches the
// descriptor. Catches drift between PerfPayloadMessageName and the proto
// definition above at package init.
func init() {
	got := string(perfPayloadFD.Messages().Get(0).FullName())
	if got != PerfPayloadMessageName {
		panic(fmt.Errorf("test_helpers: PerfPayloadMessageName=%q but descriptor produces %q",
			PerfPayloadMessageName, got))
	}
	// Quick sanity check the proto-text schema stays in sync with the
	// descriptor's package + message name. If the text drifts the core
	// decoder bench's prefixMessageIndexToBytes will fail to resolve.
	if !strings.Contains(PerfProtoTextSchema, "package perf;") ||
		!strings.Contains(PerfProtoTextSchema, "message Payload") {
		panic("test_helpers: PerfProtoTextSchema out of sync with PerfPayloadDescriptor")
	}
}
