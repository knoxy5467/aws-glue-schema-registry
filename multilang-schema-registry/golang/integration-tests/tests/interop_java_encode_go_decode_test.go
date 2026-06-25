//go:build integration

// Phase 4.6 Java <-> Go interop: Java encodes, Go decodes.
//
// Reverse of interop_go_encode_java_decode_test.go. Proves the same wire
// contract from the opposite direction.

package integration_tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/javasidecar"
)

// TestInterop_JavaEncode_GoDecode is plan §5.3 item 12, direction 2.
//
// For each matrix cell:
//  1. The Java sidecar's /encode produces a framed byte string from a
//     payload we choose.
//  2. We invoke gsrserde.DecodeWireFormat on those bytes.
//  3. We assert the decoded UUID + compression byte + decompressed
//     payload all match what we sent in.
func TestInterop_JavaEncode_GoDecode(t *testing.T) {
	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	sc := startInteropSidecar(startCtx, t)

	for _, tc := range matrix {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// See interop_go_encode_java_decode_test.go for the reason
			// each subtest owns its own context here.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			payload := []byte(strings.Repeat("reverse-interop-", 3) + tc.name)
			wantCompressionByte := compressionByteFor(tc.compression)

			framed, err := sc.Encode(ctx, javasidecar.EncodeRequest{
				Format:          tc.format,
				Schema:          tc.schema,
				SchemaName:      tc.schemaName,
				SchemaVersionID: tc.uuid,
				Payload:         payload,
				Compression:     tc.compression,
			})
			require.NoError(t, err)

			gotUUID, gotByte, gotPayload, err := gsrcore.DecodeWireFormat(framed)
			require.NoError(t, err)
			require.Equal(t, tc.uuid, gotUUID)
			require.Equal(t, wantCompressionByte, gotByte)

			// gotPayload is the raw body after the header; if compression
			// was on, it's still compressed. Decompress in Go and assert
			// the result equals the original.
			final := gotPayload
			if gotByte == gsrcore.CompressionByteZlib {
				zh := gsrcore.ZlibCompressionHandler{}
				decompressed, err := zh.Decompress(gotPayload)
				require.NoError(t, err)
				final = decompressed
			}
			require.Equal(t, payload, final)
		})
	}
}
