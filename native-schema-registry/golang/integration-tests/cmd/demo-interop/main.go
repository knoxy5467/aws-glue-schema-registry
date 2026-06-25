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
// The binary runs all 6 scenario pairs (3 formats × 2 directions), narrates
// each step to stdout, cleans up all demo-4.15-* Glue schemas on exit
// (success or failure), and exits 0 only when all 6 pairs pass.
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
		printStage("CLEANUP", "Deleting demo schemas from Glue...")
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanCancel()
		if err := cleanup.Run(cleanCtx); err != nil {
			fmt.Fprintf(os.Stderr, "[CLEANUP] Warning: cleanup errors: %v\n", err)
		} else {
			fmt.Println("[CLEANUP] All demo-4.15-* schemas deleted.")
		}
		printStage("DONE", fmt.Sprintf("Demo completed. Exit code: %d", code))
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
	printStage("STARTUP", "Kafka broker starting (testcontainers-go)...")
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
	printStage("STARTUP", fmt.Sprintf("Kafka broker ready at %s", broker.Bootstrap))

	// ── Java sidecar startup ──────────────────────────────────────────────────
	printStage("STARTUP", "Java sidecar starting (local mode)...")
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
	printStage("STARTUP", fmt.Sprintf("Java sidecar ready at %s", sc.BaseURL()))
	fmt.Println()

	// ── Run scenarios ─────────────────────────────────────────────────────────
	results := runAllScenarios(ctx, sc, broker, cleanup)

	// ── Summary ───────────────────────────────────────────────────────────────
	exitCode := printSummary(results)
	cleanupAndExit(exitCode)
}

// printSummary renders the results table and returns 0 if all pass, 1 otherwise.
func printSummary(results []ScenarioResult) int {
	fmt.Println()
	printSeparator()
	fmt.Println("  DEMO RESULTS")
	printSeparator()
	fmt.Println()

	if len(results) == 0 {
		fmt.Println("  (no scenarios ran — runAllScenarios is a placeholder in PBI-01)")
		fmt.Println()
		return 0
	}

	// Build per-format rows (Direction A and B).
	type row struct {
		format  string
		dirA    string
		dirB    string
	}
	rowMap := map[string]*row{}
	order := []string{}
	for _, r := range results {
		if _, ok := rowMap[r.Format]; !ok {
			rowMap[r.Format] = &row{format: r.Format, dirA: "—", dirB: "—"}
			order = append(order, r.Format)
		}
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
		}
		switch r.Direction {
		case "A":
			rowMap[r.Format].dirA = status
		case "B":
			rowMap[r.Format].dirB = status
		}
	}

	fmt.Printf("  %-14s  %-23s  %-23s\n", "Format", "Direction A (Java→Go)", "Direction B (Go→Java)")
	fmt.Printf("  %-14s  %-23s  %-23s\n",
		"──────────────",
		"─────────────────────",
		"─────────────────────")
	passing := 0
	total := 0
	for _, f := range order {
		r := rowMap[f]
		fmt.Printf("  %-14s  %-23s  %-23s\n", r.format, r.dirA, r.dirB)
		if r.dirA == "PASS" {
			passing++
		}
		if r.dirB == "PASS" {
			passing++
		}
		total += 2
	}
	fmt.Println()
	fmt.Printf("  Total: %d/%d PASS\n", passing, total)
	fmt.Println()

	if passing == total && total > 0 {
		return 0
	}
	return 1
}
