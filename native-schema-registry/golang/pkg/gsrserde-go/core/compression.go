package gsrserde

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"strings"
)

// CompressionType is the configuration-level name for a compression scheme.
// Anchored to Java GlueSchemaRegistryConfiguration.compressionType (string),
// which accepts "NONE" and "ZLIB".
type CompressionType string

const (
	CompressionTypeNone CompressionType = "NONE"
	CompressionTypeZlib CompressionType = "ZLIB"
)

// CompressionHandler abstracts the compress/decompress pair. The wire-format
// module reads a byte off the prefix; the factory below maps that byte to a
// handler. Anchored to Java
// common/src/main/java/com/amazonaws/services/schemaregistry/common/GlueSchemaRegistryCompressionHandler.java.
type CompressionHandler interface {
	// CompressionByte is the wire-format byte this handler emits/recognises.
	CompressionByte() byte

	// Compress returns a compressed copy of data. Implementations MUST pin
	// the compression level so identical inputs produce identical bytes —
	// see plan §5.5 (golden-byte fixtures rely on this).
	Compress(data []byte) ([]byte, error)

	// Decompress returns the decompressed payload.
	Decompress(data []byte) ([]byte, error)
}

// ZlibCompressionHandler is the only non-trivial implementation. Pins
// compression level to zlib.DefaultCompression (level 6), which matches what
// Java's `new Deflater()` does by default
// (Deflater.DEFAULT_COMPRESSION == -1 ⇒ Z_DEFAULT_COMPRESSION ⇒ level 6).
type ZlibCompressionHandler struct{}

func (ZlibCompressionHandler) CompressionByte() byte { return CompressionByteZlib }

func (ZlibCompressionHandler) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	// zlib.DefaultCompression is -1, which the writer interprets as
	// Z_DEFAULT_COMPRESSION (level 6). Same default as Java's Deflater.
	w, err := zlib.NewWriterLevel(&buf, zlib.DefaultCompression)
	if err != nil {
		return nil, fmt.Errorf("zlib NewWriterLevel: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("zlib write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("zlib close: %w", err)
	}
	return buf.Bytes(), nil
}

func (ZlibCompressionHandler) Decompress(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("zlib NewReader: %w", err)
	}
	defer r.Close()

	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("zlib read: %w", err)
	}
	return out, nil
}

// CompressionFactory resolves either a CompressionType (config-time) or a
// compression byte (wire-time) to a CompressionHandler.
//
// For CompressionTypeNone / CompressionByteNone, returns (nil, nil). The
// encoder treats nil as "skip the compress step" and writes
// CompressionByteNone into the header; the decoder treats nil as "payload is
// raw".
type CompressionFactory struct{}

func (CompressionFactory) HandlerForType(t CompressionType) (CompressionHandler, error) {
	switch CompressionType(strings.ToUpper(string(t))) {
	case "", CompressionTypeNone:
		return nil, nil
	case CompressionTypeZlib:
		return ZlibCompressionHandler{}, nil
	default:
		return nil, fmt.Errorf("unsupported compression type %q (want one of NONE, ZLIB)", t)
	}
}

func (CompressionFactory) HandlerForByte(b byte) (CompressionHandler, error) {
	switch b {
	case CompressionByteNone:
		return nil, nil
	case CompressionByteZlib:
		return ZlibCompressionHandler{}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported compression byte %#x", ErrIncompatibleData, b)
	}
}
