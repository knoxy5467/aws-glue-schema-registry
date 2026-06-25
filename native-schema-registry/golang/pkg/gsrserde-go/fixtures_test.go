package gsrserde_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bufbuild/protocompile"
	hambaavro "github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ---------------------------------------------------------------------------
// Test 1: .proto parse sweep
// ---------------------------------------------------------------------------

// protoSkipList contains files we intentionally skip. Each entry documents why.
var protoSkipList = map[string]string{
	// php_generic_services was removed from google.protobuf.FileOptions in
	// protobuf v27+ (2024). The fixture uses this deprecated option which
	// protocompile (using the modern descriptor) rightfully rejects.
	// This is a fixture artifact, not a parser bug.
	"TestOrderingSyntax3Options.proto": "uses removed php_generic_services option (protobuf v27+ dropped it)",
}

func TestSharedFixtures_ProtoParseSweep(t *testing.T) {
	// The protos directory contains all 51 fixtures + the google/type/ imports.
	protosDir, err := filepath.Abs(filepath.Join(".", "..", "..", "..", "shared", "test", "protos"))
	require.NoError(t, err, "resolving protos dir")
	_, err = os.Stat(protosDir)
	require.NoError(t, err, "protos directory must exist: %s", protosDir)

	// Collect all .proto files (including those in google/type/ subdirectory).
	var allProtos []string
	err = filepath.Walk(protosDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".proto") {
			allProtos = append(allProtos, path)
		}
		return nil
	})
	require.NoError(t, err, "walking protos directory")

	// Only test top-level protos (not google/type/ which are dependencies).
	var topLevelProtos []string
	for _, p := range allProtos {
		rel, _ := filepath.Rel(protosDir, p)
		if !strings.HasPrefix(rel, "google/") && !strings.HasPrefix(rel, "google\\") {
			topLevelProtos = append(topLevelProtos, p)
		}
	}
	require.NotEmpty(t, topLevelProtos, "expected top-level .proto fixtures")

	var passed, skipped, failed int

	for _, protoPath := range topLevelProtos {
		basename := filepath.Base(protoPath)
		t.Run("proto/"+basename, func(t *testing.T) {
			if reason, ok := protoSkipList[basename]; ok {
				skipped++
				t.Skipf("skipped: %s", reason)
				return
			}

			// Use protocompile with the protos dir as import path so
			// google/type/ imports resolve. WithStandardImports handles
			// google/protobuf/ well-known-types.
			compiler := protocompile.Compiler{
				Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
					ImportPaths: []string{protosDir},
				}),
			}

			// Compile using the relative path from the import root.
			rel, relErr := filepath.Rel(protosDir, protoPath)
			require.NoError(t, relErr)

			files, compileErr := compiler.Compile(context.Background(), rel)
			if compileErr != nil {
				failed++
				t.Fatalf("protocompile failed for %s: %v", basename, compileErr)
				return
			}

			require.True(t, len(files) > 0, "expected at least one compiled file")

			// The first file in the result is our target.
			fd := files[0]
			require.NotNil(t, fd, "compiled file descriptor must not be nil")

			// Round-trip: FileDescriptor -> FileDescriptorProto -> marshal ->
			// unmarshal -> NewFile -> assert structural equality.
			roundTripProtoDescriptor(t, fd, basename)
			passed++
		})
	}

	t.Logf("proto parser sweep: %d/%d parsed cleanly, %d skipped",
		passed, len(topLevelProtos), skipped)

	if failed > 0 {
		t.Errorf("proto parser sweep: %d non-skipped files failed to parse", failed)
	}
}

// roundTripProtoDescriptor marshals a FileDescriptor to binary, unmarshals it,
// re-parses via protodesc.NewFile, and asserts structural equality on message
// names and field counts.
func roundTripProtoDescriptor(t *testing.T, fd protoreflect.FileDescriptor, basename string) {
	t.Helper()

	// Step 1: Convert to FileDescriptorProto.
	fdProto := protodesc.ToFileDescriptorProto(fd)
	require.NotNil(t, fdProto, "ToFileDescriptorProto must not return nil")

	// Step 2: Marshal to binary.
	b, err := proto.Marshal(fdProto)
	require.NoError(t, err, "marshal FileDescriptorProto for %s", basename)

	// Step 3: Unmarshal back.
	var fdProto2 descriptorpb.FileDescriptorProto
	err = proto.Unmarshal(b, &fdProto2)
	require.NoError(t, err, "unmarshal FileDescriptorProto for %s", basename)

	// Step 4: Rebuild via protodesc.NewFile. We need a resolver that knows
	// about the file's dependencies. Register all deps into a protoregistry.Files.
	depRegistry := buildDependencyRegistry(t, fd)
	reParsed, err := protodesc.NewFile(&fdProto2, depRegistry)
	require.NoError(t, err, "protodesc.NewFile for %s", basename)

	// Step 5: Assert structural equality — message-name list matches and
	// field-count-per-message matches. We deliberately avoid full descriptor
	// diff because protodesc round-trips can normalize certain option encodings
	// (e.g., uninterpreted options), making byte-level equality brittle.
	assertMessageStructure(t, fd, reParsed, basename)
}

// buildDependencyRegistry creates a protoregistry.Files containing all
// transitive imports of fd, so protodesc.NewFile can resolve them.
func buildDependencyRegistry(t *testing.T, fd protoreflect.FileDescriptor) *protoregistry.Files {
	t.Helper()
	reg := new(protoregistry.Files)
	seen := map[string]bool{}
	registerDeps(t, fd, reg, seen)
	return reg
}

func registerDeps(t *testing.T, fd protoreflect.FileDescriptor, reg *protoregistry.Files, seen map[string]bool) {
	t.Helper()
	imports := fd.Imports()
	for i := 0; i < imports.Len(); i++ {
		imp := imports.Get(i).FileDescriptor
		if imp == nil {
			continue
		}
		path := string(imp.Path())
		if seen[path] {
			continue
		}
		seen[path] = true
		// Register transitive deps first (depth-first).
		registerDeps(t, imp, reg, seen)
		// Ignore AlreadyExists errors — WKTs may already be in the global registry.
		_ = reg.RegisterFile(imp)
	}
}

// assertMessageStructure checks that two file descriptors have the same
// message type count, message names, and per-message field counts.
func assertMessageStructure(t *testing.T, original, reparsed protoreflect.FileDescriptor, basename string) {
	t.Helper()

	origMsgs := original.Messages()
	repMsgs := reparsed.Messages()
	require.Equal(t, origMsgs.Len(), repMsgs.Len(),
		"%s: message count mismatch", basename)

	for i := 0; i < origMsgs.Len(); i++ {
		origMsg := origMsgs.Get(i)
		repMsg := repMsgs.Get(i)
		assert.Equal(t, string(origMsg.Name()), string(repMsg.Name()),
			"%s: message[%d] name mismatch", basename, i)
		assert.Equal(t, origMsg.Fields().Len(), repMsg.Fields().Len(),
			"%s: message[%d] %s field count mismatch", basename, i, origMsg.Name())
	}
}

// ---------------------------------------------------------------------------
// Test 2: .avsc parse sweep
// ---------------------------------------------------------------------------

func TestSharedFixtures_AvroParseSweep(t *testing.T) {
	avroDir, err := filepath.Abs(filepath.Join(".", "..", "..", "..", "shared", "test", "avro"))
	require.NoError(t, err, "resolving avro dir")
	_, err = os.Stat(avroDir)
	require.NoError(t, err, "avro directory must exist: %s", avroDir)

	var total, passed, expectedFailures int

	err = filepath.Walk(avroDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".avsc") {
			return nil
		}

		rel, relErr := filepath.Rel(avroDir, path)
		if relErr != nil {
			return relErr
		}

		// Use forward slashes for consistent subtest naming.
		relName := filepath.ToSlash(rel)

		t.Run("avro/"+relName, func(t *testing.T) {
			total++
			data, readErr := os.ReadFile(path)
			require.NoError(t, readErr, "reading %s", relName)

			schemaStr := string(data)
			schema, parseErr := hambaavro.Parse(schemaStr)

			// Files under negative/malformed/ MUST fail to parse.
			isMalformed := strings.Contains(relName, "negative/malformed/")

			if isMalformed {
				require.Error(t, parseErr,
					"%s: expected parse error for malformed schema", relName)
				expectedFailures++
				return
			}

			// All other files MUST parse cleanly.
			require.NoError(t, parseErr,
				"%s: schema must parse cleanly", relName)

			// Round-trip: schema.String() -> re-parse -> compare fingerprints.
			// We use SHA-256 fingerprints (built into hamba/avro Schema interface)
			// to confirm structural equality after canonical-form serialization.
			canonical := schema.String()
			schema2, parseErr2 := hambaavro.Parse(canonical)
			require.NoError(t, parseErr2,
				"%s: re-parse of canonical form failed", relName)

			// Compare SHA-256 fingerprints for structural equality.
			fp1 := schema.Fingerprint()
			fp2 := schema2.Fingerprint()
			assert.Equal(t, fp1, fp2,
				"%s: fingerprint mismatch after round-trip", relName)
			passed++
		})

		return nil
	})
	require.NoError(t, err, "walking avro directory")

	t.Logf("avro parser sweep: %d/%d parsed cleanly, %d in negative/malformed/ failed as expected",
		passed, total, expectedFailures)
}
