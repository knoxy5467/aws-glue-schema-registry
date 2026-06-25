//go:build integration

// Package main is the entry point for the Phase 4.15 demo binary.
//
// Usage:
//
//	cd integration-tests && go run -tags integration ./cmd/demo-interop/
//
// Prerequisites:
//   - AWS credentials for account 850995546034 (AWS_PROFILE or env vars)
//   - JDK 11+ on PATH (or GSR_INTEROP_JAVA pointing to a JDK binary)
//   - Docker running (testcontainers-go Kafka broker)
//   - Java sidecar JAR built: make java-sidecar-build
//   - default-registry exists in Glue in us-east-2
//
// The binary runs all 12 scenario cells (3 formats × 2 directions × 2
// compressions {NONE, ZLIB}), narrates each step to stdout, cleans up all
// demo-4.15-* Glue schemas on exit (success or failure), and exits 0 only
// when all 12 cells pass.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/realglue"
)

const (
	demoVersion      = "4.15"
	demoAccountID    = "850995546034"
	demoRegistryName = "default-registry"
	demoPrefix       = "demo-4.15-"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── AWS / Glue client ─────────────────────────────────────────────────────
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = realglue.DefaultRegion
	}
	registryName := os.Getenv("DEMO_REGISTRY")
	if registryName == "" {
		registryName = demoRegistryName
	}

	real, err := realglue.New(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] realglue.New: %v\n", err)
		os.Exit(1)
	}
	region = real.Region

	// ── Cleanup — deferred + signal handler ──────────────────────────────────
	cleanup := real.NewCleanup()
	cleanup.TrackSchemaPrefix(registryName, demoPrefix)

	// Run cleanup on exit; captures the current context via the cancel above.
	cleanupAndExit := func(code int) {
		fmt.Println()
		printStage("demo", "Deleting demo schemas from Glue...")
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanCancel()
		if err := cleanup.Run(cleanCtx); err != nil {
			fmt.Fprintf(os.Stderr, "[demo] Warning: cleanup errors: %v\n", err)
		} else {
			fmt.Println("[demo] All demo-4.15-* schemas deleted.")
		}
		printStage("demo", fmt.Sprintf("Demo completed. Exit code: %d", code))
		printSeparator()
		os.Exit(code)
	}

	// Signal handler for SIGINT / SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Fprintf(os.Stderr, "\n[SIGNAL] Received %v — running cleanup before exit.\n", sig)
		cancel()
		cleanupAndExit(1)
	}()

	// ── Startup banner ────────────────────────────────────────────────────────
	printBanner(demoVersion, demoAccountID, region)

	// ── Kafka startup ─────────────────────────────────────────────────────────
	printStage("demo", "Kafka broker starting (testcontainers-go)...")
	startCtx, startCancel := context.WithTimeout(ctx, 2*time.Minute)
	broker, stopBroker, err := kafkaharness.StartShared(startCtx)
	startCancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] kafkaharness.StartShared: %v\n", err)
		cleanupAndExit(1)
	}
	defer func() {
		if stopErr := stopBroker(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "[KAFKA] Stop returned error (non-fatal): %v\n", stopErr)
		}
	}()
	printStage("demo", fmt.Sprintf("Kafka broker ready at %s", broker.Bootstrap))

	// ── Java sidecar startup ──────────────────────────────────────────────────
	printStage("demo", "Java sidecar starting (local mode)...")
	sidecarCtx, sidecarCancel := context.WithTimeout(ctx, 60*time.Second)
	sc, err := javasidecar.New(sidecarCtx, javasidecar.Options{
		StartTimeout: 30 * time.Second,
	})
	sidecarCancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] javasidecar.New: %v\n", err)
		cleanupAndExit(1)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if stopErr := sc.Stop(stopCtx); stopErr != nil {
			fmt.Fprintf(os.Stderr, "[SIDECAR] Stop returned error (non-fatal): %v\n", stopErr)
		}
	}()
	printStage("demo", fmt.Sprintf("Java sidecar ready at %s", sc.BaseURL()))
	fmt.Println()

	// ── Run scenarios ─────────────────────────────────────────────────────────
	results := runAllScenarios(ctx, sc, broker, cleanup, region)

	// ── Summary ───────────────────────────────────────────────────────────────
	exitCode := printSummary(results)
	cleanupAndExit(exitCode)
}

// printSummary renders the Markdown-style results table per PBI-04 and returns
// 0 if all scenarios pass, 1 otherwise.
func printSummary(results []ScenarioResult) int {
	fmt.Println()
	printStage("demo", "=== Summary ===")
	fmt.Println()

	if len(results) == 0 {
		fmt.Println("  (no scenarios ran)")
		fmt.Println()
		return 0
	}

	// Markdown-style table: | Direction | Format | Compression | Schema Version | Result |
	fmt.Println("| Direction | Format | Compression | Schema Version | Result |")
	fmt.Println("|-----------|--------|-------------|----------------|--------|")

	passing := 0
	total := len(results)
	for _, r := range results {
		direction := "Java->Go"
		if r.Direction == "B" {
			direction = "Go->Java"
		}
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
		} else {
			passing++
		}
		svID := r.SchemaVersionID
		if svID == "" {
			svID = "(none)"
		}
		// Truncate UUID for table readability (first 8 chars).
		if len(svID) > 8 {
			svID = svID[:8] + "..."
		}
		fmt.Printf("| %-9s | %-6s | %-11s | %-14s | %-6s |\n",
			direction, r.Format, r.Compression, svID, status)
	}

	fmt.Println()
	fmt.Printf("Total: %d PASS / %d FAIL / %d TOTAL\n", passing, total-passing, total)
	fmt.Println()

	if passing == total && total > 0 {
		return 0
	}
	return 1
}
