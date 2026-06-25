//go:build integration

// Phase 4.16 Layer C — shared fixture helpers factored from Layer B's
// fixture_avro_evolution_test.go. These helpers are used by the Avro
// evolution test (Layer B), the Avro interop test (Layer C), and the
// Proto interop test (Layer C).

package integration_tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bufbuild/protocompile"
	hambaavro "github.com/hamba/avro/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ---------------------------------------------------------------------------
// buildSampleRecord walks a hamba/avro parsed Schema and returns a
// map[string]interface{} populated with minimal sample values suitable for
// hambaavro.Marshal. The goal is "encode-decode round-trips", not exhaustive
// Avro feature coverage.
//
// Type mapping:
//   - string  -> "x"
//   - int     -> int(42)
//   - long    -> int64(42)
//   - float   -> float32(1.0)
//   - double  -> float64(1.0)
//   - boolean -> true
//   - bytes   -> []byte("b")
//   - null    -> nil
//   - record  -> recursive sub-record
//   - enum    -> first symbol
//   - array   -> empty slice
//   - map     -> empty map
//   - union   -> first non-null type (or nil if nullable-only)
//   - fixed   -> zero-filled []byte of correct size
//
// If an exotic type is encountered that this helper cannot handle, it returns
// nil for that field — the encoder either accepts it (union with null) or
// errors, and the test logs it.
// ---------------------------------------------------------------------------

func buildSampleRecord(schema hambaavro.Schema) map[string]interface{} {
	rs, ok := schema.(*hambaavro.RecordSchema)
	if !ok {
		return nil
	}
	record := make(map[string]interface{}, len(rs.Fields()))
	for _, field := range rs.Fields() {
		record[field.Name()] = buildSampleValue(field.Type())
	}
	return record
}

func buildSampleValue(schema hambaavro.Schema) interface{} {
	switch schema.Type() {
	case hambaavro.String:
		return "x"
	case hambaavro.Int:
		return int(42)
	case hambaavro.Long:
		return int64(42)
	case hambaavro.Float:
		return float32(1.0)
	case hambaavro.Double:
		return float64(1.0)
	case hambaavro.Boolean:
		return true
	case hambaavro.Bytes:
		return []byte("b")
	case hambaavro.Null:
		return nil
	case hambaavro.Record:
		return buildSampleRecord(schema)
	case hambaavro.Enum:
		es := schema.(*hambaavro.EnumSchema)
		symbols := es.Symbols()
		if len(symbols) > 0 {
			return symbols[0]
		}
		return ""
	case hambaavro.Array:
		// Return an empty slice — satisfies the schema without needing to
		// generate item values.
		return []interface{}{}
	case hambaavro.Map:
		// Return an empty map.
		return map[string]interface{}{}
	case hambaavro.Union:
		us := schema.(*hambaavro.UnionSchema)
		types := us.Types()
		// Prefer the first non-null type in the union.
		for _, t := range types {
			if t.Type() != hambaavro.Null {
				val := buildSampleValue(t)
				// hamba/avro union encoding for map[string]interface{} requires
				// the value to be wrapped in a map with the type name as key
				// ONLY for named types (record, enum, fixed). For primitives,
				// the raw value works directly.
				return val
			}
		}
		// All-null union (unusual but valid).
		return nil
	case hambaavro.Fixed:
		fs := schema.(*hambaavro.FixedSchema)
		return make([]byte, fs.Size())
	default:
		// Exotic/unknown type — return nil. If the field is in a union with
		// null this works; otherwise the caller logs the encode failure.
		return nil
	}
}

// ---------------------------------------------------------------------------
// fixtureProtoBasePath resolves the shared/test/protos directory from the test
// file location (integration-tests/tests/).
// ---------------------------------------------------------------------------

func fixtureProtoBasePath(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join(".", "..", "..", "..", "shared", "test", "protos"))
	require.NoError(t, err, "resolving shared/test/protos path")
	_, err = os.Stat(p)
	require.NoError(t, err, "shared/test/protos directory must exist: %s", p)
	return p
}

// ---------------------------------------------------------------------------
// compileProtoFixture compiles a single .proto file from the shared fixtures
// directory using bufbuild/protocompile with the protos directory as import
// root (so google/type/ imports resolve) and google well-known types.
// Returns the compiled file descriptor.
// ---------------------------------------------------------------------------

func compileProtoFixture(t *testing.T, protosDir, filename string) protoreflect.FileDescriptor {
	t.Helper()

	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: []string{protosDir},
		}),
	}

	files, err := compiler.Compile(context.Background(), filename)
	require.NoError(t, err, "protocompile failed for %s", filename)
	require.True(t, len(files) > 0, "expected at least one compiled file for %s", filename)

	return files[0]
}

// ---------------------------------------------------------------------------
// buildDynamicProtoFromFile compiles a .proto file and returns a
// *dynamicpb.Message populated with sample values for the first top-level
// message type declared in the file. Uses reflection over the descriptor's
// fields to build sample values.
// ---------------------------------------------------------------------------

func buildDynamicProtoFromFile(t *testing.T, protosDir, filename string) (*dynamicpb.Message, protoreflect.MessageDescriptor) {
	t.Helper()

	fd := compileProtoFixture(t, protosDir, filename)

	msgs := fd.Messages()
	require.True(t, msgs.Len() > 0, "no message types in %s", filename)

	// Use the first top-level message type.
	md := msgs.Get(0)
	msg := dynamicpb.NewMessage(md)

	// Populate sample values for all non-message, non-map, non-repeated fields.
	populateDynamicProtoFields(t, msg, md)

	return msg, md
}

// populateDynamicProtoFields fills scalar fields in a dynamicpb.Message with
// sample values. For nested messages, bytes, and repeated fields it uses
// minimal defaults. OneOf fields get their first alternative populated.
func populateDynamicProtoFields(t *testing.T, msg *dynamicpb.Message, md protoreflect.MessageDescriptor) {
	t.Helper()

	// Track which oneofs we've already set a value for.
	oneofSet := make(map[protoreflect.Name]bool)

	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)

		// Skip map fields — they're complex and not needed for basic interop.
		if fd.IsMap() {
			continue
		}

		// For oneof fields, only set the first alternative.
		if oo := fd.ContainingOneof(); oo != nil {
			if oneofSet[oo.Name()] {
				continue
			}
			oneofSet[oo.Name()] = true
		}

		// Skip repeated fields (lists) — empty is valid.
		if fd.IsList() {
			continue
		}

		// Set scalar values based on field kind.
		val := sampleProtoValue(t, fd)
		if val.IsValid() {
			msg.Set(fd, val)
		}
	}
}

// sampleProtoValue returns a sample protoreflect.Value for the given field
// descriptor. Returns an invalid Value for message-typed fields (which would
// require recursion into sub-messages).
func sampleProtoValue(t *testing.T, fd protoreflect.FieldDescriptor) protoreflect.Value {
	t.Helper()

	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(42)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(42)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(42)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(42)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1.5)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1.5)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("sample")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte("data"))
	case protoreflect.EnumKind:
		// Use the first enum value (index 0).
		enumValues := fd.Enum().Values()
		if enumValues.Len() > 0 {
			return protoreflect.ValueOfEnum(enumValues.Get(0).Number())
		}
		return protoreflect.ValueOfEnum(0)
	case protoreflect.MessageKind, protoreflect.GroupKind:
		// Skip nested messages — empty/nil is valid for optional messages.
		return protoreflect.Value{}
	default:
		return protoreflect.Value{}
	}
}

// ---------------------------------------------------------------------------
// randomFixtureInteropName generates a unique schema name for interop tests.
// ---------------------------------------------------------------------------

func randomFixtureInteropName(t *testing.T, prefix, mode string) string {
	t.Helper()
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return fmt.Sprintf("gsr-go-it-%s-%s-%x", prefix, mode, buf[:])
}
