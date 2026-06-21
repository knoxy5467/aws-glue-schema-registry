//go:build integration
// +build integration

package integration_tests

import "time"

const (
	defaultKafkaBroker = "localhost:9092"
	defaultAWSRegion   = "us-east-1"
	testRegistryName   = "default-registry"

	// defaultScenarioCtxTimeout caps a single §5.3 scenario's wall
	// clock. Long enough for Kafka publish + AWS round-trip on slow
	// CI; short enough that a stuck scenario fails fast rather than
	// holding the 15-min make test-integ budget hostage.
	defaultScenarioCtxTimeout = 90 * time.Second
)
