package gsrserde

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefixMessageIndexToBytes_ErrorCases(t *testing.T) {
	tests := []struct {
		name             string
		data             []byte
		schemaDefinition string
		schemaName       string
		wantErrIs        error
	}{
		{
			name:             "Invalid proto definition surfaces parse error",
			data:             []byte("test"),
			schemaDefinition: "invalid proto syntax",
			schemaName:       "TestMessage",
		},
		{
			name:             "Empty schema definition surfaces parse error",
			data:             []byte("test"),
			schemaDefinition: "",
			schemaName:       "TestMessage",
		},
		{
			name:             "Message not found returns wrapped ErrMessageTypeNotFound",
			data:             []byte("test"),
			schemaDefinition: `syntax = "proto3"; message Other { string name = 1; }`,
			schemaName:       "NonExistent",
			wantErrIs:        ErrMessageTypeNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := prefixMessageIndexToBytes(tt.data, tt.schemaDefinition, tt.schemaName)
			require.Error(t, err)
			if tt.wantErrIs != nil {
				assert.True(t, errors.Is(err, tt.wantErrIs),
					"caller relies on errors.Is(err, %v) to discriminate not-found from parse errors", tt.wantErrIs)
			}
		})
	}
}

// TestStripMessageIndex_EdgeCases asserts the spec-anchored behavior of
// stripMessageIndex now that the function correctly consumes the leading
// unsigned varint per Java ProtobufWireFormatDecoder.java:33-37.
func TestStripMessageIndex_EdgeCases(t *testing.T) {
	t.Run("empty data errors", func(t *testing.T) {
		_, _, err := stripMessageIndex(nil)
		assert.Error(t, err)
	})

	t.Run("single varint(0) byte returns empty payload", func(t *testing.T) {
		idx, rest, err := stripMessageIndex([]byte{0x00})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), idx)
		assert.Empty(t, rest)
	})

	t.Run("four bytes — one varint(0) then three payload bytes", func(t *testing.T) {
		idx, rest, err := stripMessageIndex([]byte{0x00, 0x00, 0x00, 0x00})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), idx)
		assert.Equal(t, []byte{0x00, 0x00, 0x00}, rest)
	})

	t.Run("varint(0) + Hello — strips one byte", func(t *testing.T) {
		idx, rest, err := stripMessageIndex([]byte{0x00, 0x48, 0x65, 0x6c, 0x6c, 0x6f})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), idx)
		assert.Equal(t, []byte("Hello"), rest)
	})

	t.Run("varint(128) two-byte prefix — strips two bytes", func(t *testing.T) {
		// 0x80, 0x01 is varint(128); payload is "hi".
		idx, rest, err := stripMessageIndex([]byte{0x80, 0x01, 'h', 'i'})
		require.NoError(t, err)
		assert.Equal(t, uint32(128), idx)
		assert.Equal(t, []byte("hi"), rest)
	})
}

// TestGetMessageIndexFromProtoDefinition_ComplexCases — the indirect tests via
// prefixMessageIndexToBytes are kept, but now (a) assert against the prefix
// bytes the function returns (varint(idx) + data), and (b) check the
// not-found path returns wrapped ErrMessageTypeNotFound rather than silently
// producing prefix(0).
func TestGetMessageIndexFromProtoDefinition_ComplexCases(t *testing.T) {
	tests := []struct {
		name             string
		schemaDefinition string
		messageName      string
		wantPrefix       []byte // expected varint prefix bytes
		wantErrIs        error
	}{
		{
			// Lex-sort on FQ name with empty package: [FirstMessage, SecondMessage, ThirdMessage].
			// SecondMessage is at index 1.
			name: "Multiple messages — SecondMessage lex-sorts to index 1",
			schemaDefinition: `syntax = "proto3";
				message FirstMessage { string name = 1; }
				message SecondMessage { int32 id = 1; }
				message ThirdMessage { bool active = 1; }`,
			messageName: "SecondMessage",
			wantPrefix:  []byte{0x01},
		},
		{
			name: "Nested messages — outer message lex-sorts to index 0",
			schemaDefinition: `syntax = "proto3";
				message Outer {
					message Inner { string value = 1; }
					Inner inner = 1;
				}`,
			messageName: "Outer",
			wantPrefix:  []byte{0x00},
		},
		{
			name: "Java worked example MessageIndexFinder.java:74-88 — B.A is index 1",
			// message B { message C {} message A { message D {} } }
			// BFS visits B, B.C, B.A, B.A.D → sort lex → [B, B.A, B.A.D, B.C]
			// indices 0,1,2,3 — B.A at 1.
			schemaDefinition: `syntax = "proto3";
				message B {
					message C {}
					message A { message D {} }
				}`,
			messageName: "B.A",
			wantPrefix:  []byte{0x01},
		},
		{
			name: "Java worked example — B.C is index 3",
			schemaDefinition: `syntax = "proto3";
				message B {
					message C {}
					message A { message D {} }
				}`,
			messageName: "B.C",
			wantPrefix:  []byte{0x03},
		},
		{
			name:             "Invalid syntax — parse error, not ErrMessageTypeNotFound",
			schemaDefinition: "not a valid proto definition",
			messageName:      "TestMessage",
			wantErrIs:        nil, // parse error; we just need *some* error
		},
		{
			name:             "Empty definition — parse error",
			schemaDefinition: "",
			messageName:      "TestMessage",
			wantErrIs:        nil,
		},
		{
			name:             "Message not in schema — typed not-found",
			schemaDefinition: `syntax = "proto3"; message Only { string name = 1; }`,
			messageName:      "NotPresent",
			wantErrIs:        ErrMessageTypeNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte("test-data")
			result, err := prefixMessageIndexToBytes(data, tt.schemaDefinition, tt.messageName)

			if tt.wantPrefix != nil {
				require.NoError(t, err)
				assert.Equal(t, append(append([]byte{}, tt.wantPrefix...), data...), result)
				return
			}

			require.Error(t, err)
			if tt.wantErrIs != nil {
				assert.True(t, errors.Is(err, tt.wantErrIs))
			}
		})
	}
}
