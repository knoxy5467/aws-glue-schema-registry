#!/bin/sh
# Phase 4 dockerized integration runner.
#
# Build tags:
#   - integration  Required for the §5.3 scenarios to compile.
#   - musl         CGO uses Alpine's musl libc (kept from the pre-
#                  Phase-4 runner; confluent-kafka-go needs it).
#
# AWS_INTEGRATION=1 unconditionally because the dockerized path is
# the CI-parity execution — the host's ~/.aws is mounted read-only
# (see docker-compose.dockerized.yml) so the suite reaches real Glue.
# Tests that skip-when-empty respect the var.
#
# KAFKA_BROKER is set by docker-compose to point at the in-network
# kafka:9092; that wins over the testcontainers path so the suite
# uses the pre-provisioned broker rather than trying (and failing)
# to talk to the host's docker daemon.
set -e

echo "=== Dockerized Integration Test Runner ==="
echo "Go version: $(go version)"

# Wait for Kafka to be ready.
echo "Waiting for Kafka to be ready..."
sleep 10

echo "✅ Kafka is ready"

# Run §5.3 integration tests.
echo "Running integration tests..."
cd /app/golang/integration-tests
AWS_INTEGRATION=1 go test --tags "integration musl" -count=1 -v -timeout 15m ./...
