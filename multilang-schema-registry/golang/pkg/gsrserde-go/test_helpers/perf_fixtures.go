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

// PerfPayload returns a deterministic, printable-ASCII byte buffer of the
// requested size. The bytes are produced by an xorshift64 PRNG seeded with
// a fixed constant, then mapped into a 64-character ASCII alphabet via
// `alphabet[(rng>>16) & 63]`.
//
// Why xorshift over a simple alphabet[i%len(alphabet)]:
//   - The modulo pattern is a 63-byte cycle, which LZ77 reduces to ~1%
//     output size. ZLIB benchmarks then measure compression CPU against
//     a near-empty deflate stream — far better than realistic JSON/Avro
//     text (~10-30% ratio). Phase 6.1 /code-review finding 1 flagged this.
//   - xorshift64 has period 2^64-1; the output has no detectable
//     periodicity at any practical buffer size, so zlib hits realistic
//     dictionary turnover.
//   - Deterministic from a fixed seed → same bytes every run → benchstat
//     deltas reflect code changes, not input drift.
//   - All output bytes land in a printable-ASCII window so
//     `json.Marshal(string(p))` stays lossless (the original UTF-8 fix
//     that motivated swapping away from crypto/rand).
//
// The Java side of the bench uses the same xorshift64 + same seed + same
// alphabet so Go and Java consume byte-identical payloads for a given
// size — without that, the ZLIB cross-language column is incomparable.
// See multilang-schema-registry/perf/java/.../EncodeDecodeBench.java for
// the matching Java implementation.
//
// PerfPayloadSeed is the xorshift64 starting state. Pinned because
// benchstat regression detection requires byte-stable inputs.
const PerfPayloadSeed uint64 = 0x9E3779B97F4A7C15 // golden-ratio constant — arbitrary, just non-zero

// PerfPayloadAlphabet is the 64-character set the PRNG output is mapped
// into. The Java side ships an identical string literal; do not edit one
// without editing the other.
const PerfPayloadAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ."

func PerfPayload(size int) []byte {
	if size <= 0 {
		return nil
	}
	out := make([]byte, size)
	state := PerfPayloadSeed
	for i := range out {
		// xorshift64 (Marsaglia 2003) — well-distributed bits per step.
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		// Take the middle bits to avoid low-bit cyclic structure that
		// some xorshift variants leak; mask to the alphabet size (64).
		out[i] = PerfPayloadAlphabet[(state>>16)&63]
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

// PerfPayloadMessageName is "perf.Payload" — the BFS+lex-sorted full name
// the encoder's prefixMessageIndexToBytes uses to resolve message-index 0.
const PerfPayloadMessageName = "perf.Payload"

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

	// Sanity-check the constants line up with the descriptor. Catches
	// drift between PerfPayloadMessageName / PerfProtoTextSchema and the
	// proto definition above at package init — much friendlier than a
	// mysterious "message type not found" failure deep inside the bench.
	got := string(perfPayloadFD.Messages().Get(0).FullName())
	if got != PerfPayloadMessageName {
		panic(fmt.Errorf("test_helpers: PerfPayloadMessageName=%q but descriptor produces %q",
			PerfPayloadMessageName, got))
	}
	if !strings.Contains(PerfProtoTextSchema, "package perf;") ||
		!strings.Contains(PerfProtoTextSchema, "message Payload") {
		panic("test_helpers: PerfProtoTextSchema out of sync with PerfPayloadDescriptor")
	}
}

// PerfPayloadDescriptor returns the cached perf.proto FileDescriptor.
// Callers wrap msg := dynamicpb.NewMessage(fd.Messages().ByName("Payload"))
// and set the `blob` field to ship a Phase 6 protobuf benchmark payload.
func PerfPayloadDescriptor() protoreflect.FileDescriptor {
	return perfPayloadFD
}
