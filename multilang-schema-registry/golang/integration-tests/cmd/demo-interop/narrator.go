//go:build integration

// Package main — narrator.go
// Narration formatting helpers for the demo-interop binary.
package main

import (
	"fmt"
	"strings"
	"time"
	"unicode"
)

const separatorWidth = 80

// printSeparator prints a full-width line of '=' characters.
func printSeparator() {
	fmt.Println(strings.Repeat("=", separatorWidth))
}

// printBanner prints the top-level demo header per spec §4.1.
func printBanner(version, accountID, region string) {
	printSeparator()
	fmt.Printf("  GSR Go Client — Java↔Go Cross-Language Cross-Version Interop Demo\n")
	fmt.Printf("  Date:     %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Printf("  Version:  %s\n", version)
	fmt.Printf("  Account:  %s\n", accountID)
	fmt.Printf("  Region:   %s\n", region)
	fmt.Printf("  Registry: default-registry\n")
	printSeparator()
	fmt.Println()
}

// printSectionHeader prints a separator and title for each scenario section.
func printSectionHeader(title string) {
	fmt.Println(strings.Repeat("─", separatorWidth))
	fmt.Printf("  %s\n", title)
	fmt.Println(strings.Repeat("─", separatorWidth))
	fmt.Println()
}

// printHexDump decomposes GSR wire bytes and prints each segment with hex and
// a human-readable label. The GSR v3 wire format is:
//
//	byte 0     — header byte (0x03)
//	byte 1     — compression byte
//	bytes 2-17 — schema-version UUID (16 bytes, big-endian)
//	bytes 18+  — serialized payload body
//
// label is printed as the top-level heading for the dump block.
// Each segment line is prefixed with [wire-format] for grep-ability.
func printHexDump(label string, payload []byte) {
	fmt.Printf("  %s (%d bytes total):\n", label, len(payload))
	if len(payload) == 0 {
		fmt.Println("    (empty)")
		return
	}

	// Header byte
	printStage("wire-format", fmt.Sprintf("Header byte: 0x%02X (GSR v3 wire format)  %s", payload[0], formatHexLine(payload[0:1])))

	if len(payload) < 2 {
		return
	}
	// Compression byte
	compressionLabel := compressionName(payload[1])
	printStage("wire-format", fmt.Sprintf("Compression byte: 0x%02X (%s)  %s", payload[1], compressionLabel, formatHexLine(payload[1:2])))

	if len(payload) < 18 {
		printStage("wire-format", "(truncated — expected ≥18 bytes for UUID)")
		return
	}

	// 16-byte schema-version UUID
	uuidBytes := payload[2:18]
	printStage("wire-format", fmt.Sprintf("Schema version UUID: %s  %s", formatUUID(uuidBytes), formatHexLine(uuidBytes)))

	// Payload body
	body := payload[18:]
	if len(body) > 0 {
		printStage("wire-format", fmt.Sprintf("Payload body: %d bytes  %s", len(body), formatHexLine(body)))
	} else {
		printStage("wire-format", "Payload body: (empty)")
	}
	fmt.Println()
}

// printGoConfig pretty-prints the Go client config map (gsrConfigPath values).
func printGoConfig(cfg map[string]string) {
	fmt.Println("  Go config:")
	keys := []string{
		"region",
		"registry.name",
		"compression",
		"schemaAutoRegistrationEnabled",
	}
	for _, k := range keys {
		if v, ok := cfg[k]; ok {
			fmt.Printf("    %-34s %s\n", k+":", v)
		}
	}
	// Print any remaining keys not in the ordered list.
	for k, v := range cfg {
		found := false
		for _, ordered := range keys {
			if k == ordered {
				found = true
				break
			}
		}
		if !found {
			fmt.Printf("    %-34s %s\n", k+":", v)
		}
	}
	fmt.Println()
}

// printEqualityCheck prints both values and a PASS/FAIL indicator.
func printEqualityCheck(expected, actual interface{}, label string) {
	match := fmt.Sprintf("%v", expected) == fmt.Sprintf("%v", actual)
	indicator := "✓"
	if !match {
		indicator = "✗"
	}
	fmt.Printf("  %s: %v == %v  %s\n", label, expected, actual, indicator)
}

// printStage prints a tagged stage line: [<TAG>] <message>.
func printStage(tag, message string) {
	fmt.Printf("[%s] %s\n", tag, message)
}

// ── internal helpers ──────────────────────────────────────────────────────────

// compressionName maps a GSR compression byte to its human-readable name.
func compressionName(b byte) string {
	switch b {
	case 0x00:
		return "NONE"
	case 0x05:
		return "ZLIB"
	default:
		return fmt.Sprintf("unknown(0x%02X)", b)
	}
}

// formatUUID formats a 16-byte slice as a standard UUID string
// (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
func formatUUID(b []byte) string {
	if len(b) != 16 {
		return fmt.Sprintf("(invalid length %d)", len(b))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// hexString returns the hex-encoded representation of b with space separators.
func hexString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	parts := make([]string, len(b))
	for i, v := range b {
		parts[i] = fmt.Sprintf("%02X", v)
	}
	return strings.Join(parts, " ")
}

// formatHexLine returns a compact hex+ASCII representation of b, capped at 32
// bytes for readability.
func formatHexLine(b []byte) string {
	display := b
	truncated := ""
	if len(b) > 32 {
		display = b[:32]
		truncated = fmt.Sprintf(" ...+%d bytes", len(b)-32)
	}
	hex := hexString(display)
	ascii := toASCII(display)
	return fmt.Sprintf("    %-95s  |%s|%s", hex, ascii, truncated)
}

// toASCII returns an ASCII printable representation of b, replacing
// non-printable bytes with '.'.
func toASCII(b []byte) string {
	out := make([]byte, len(b))
	for i, v := range b {
		if v > 0x1F && v < 0x7F && unicode.IsPrint(rune(v)) {
			out[i] = v
		} else {
			out[i] = '.'
		}
	}
	return string(out)
}
