# GSR Go client — Phase 4 Makefile.
#
# Targets in priority order:
#   make test               unit + component tests (<30s, no Docker / AWS)
#   make test-integ         §5.3 integration scenarios against testcontainers + Glue
#   make test-integ-dockerized   the same suite run inside a CI parity container
#
# Phase 4 changes (vs. the pre-Phase-4 Makefile):
#   - test-integ now drives the suite via the //go:build integration tag
#     + AWS_INTEGRATION=1 env var. Docker startup is owned by
#     testcontainers-go inside the test process; docker-compose stays
#     as a fallback for environments that can't reach /var/run/docker.sock
#     from the test process.
#   - test-integ-dockerized mounts ~/.aws read-only into the container
#     so the suite can reach Glue using the host's credentials without
#     baking secrets into the image.
#   - 15-minute timeout cap on test-integ so a stuck Kafka container
#     fails fast.

BUILD_DIR := build
COVERAGE_DIR := $(BUILD_DIR)/coverage
PKG_DIR := $(BUILD_DIR)/pkg

PROTO_SRC_DIR := ../shared/test/protos
PROTO_OUT_DIR := integration-tests/testpb
GO_MODULE_PATH := github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang
GO_MODULE_PREFIX := github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb
PACKAGE_PREFIX_TO_TRIM := com.amazonaws.services.schemaregistry.tests.protobuf

# 15-minute cap on the integration run — enough for testcontainers
# Kafka startup + the full §5.3 matrix + Glue round-trip; short enough
# that a stuck container surfaces fast.
INTEG_TIMEOUT := 15m

GO ?= go

.PHONY: all
all: build

.PHONY: setup-dirs
setup-dirs:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(COVERAGE_DIR)
	@mkdir -p $(PKG_DIR)

.PHONY: build
build: setup-dirs
	@echo "Building Go module..."
	$(GO) build -o $(PKG_DIR)/ ./...
	$(GO) mod download
	$(GO) mod tidy
	@echo "Build complete. Artifacts in $(BUILD_DIR)/"

.PHONY: cover
cover:
	@echo "Usage: make cover-<target>"
	@echo "Available coverage targets:"
	@echo "  make cover-test        # Unit + component tests with coverage"
	@echo "  make cover-test-integ  # Integration tests with coverage"
	@echo "  make cover-test-all    # All tests with coverage"

cover-%: setup-dirs
	@echo "Running $* with coverage..."
	$(MAKE) $* COVERAGE=true

# Unit + Component tests: fast inner loop, never touch Docker or AWS.
# Stays under 30s on a warm cache per the §8 TDD discipline rule.
.PHONY: test
test: setup-dirs
ifdef COVERAGE
	@echo "Running unit and component tests with coverage..."
	$(GO) test -count=1 -coverprofile=$(COVERAGE_DIR)/unit-coverage.out $(shell $(GO) list ./... | grep -v integration-tests)
	$(GO) tool cover -html=$(COVERAGE_DIR)/unit-coverage.out -o $(COVERAGE_DIR)/unit-coverage.html
	@echo "Unit and component test coverage report: $(COVERAGE_DIR)/unit-coverage.html"
else
	@echo "Running unit and component tests..."
	$(GO) test -count=1 -short $(shell $(GO) list ./... | grep -v integration-tests) 2>&1 | tee $(BUILD_DIR)/unit-test-output.txt
endif

# Integration tests — §5.3 scenarios.
#
# Sets AWS_INTEGRATION=1 unconditionally; tests that bill AWS check
# the env var and skip-when-empty so this target is the production
# path that actually exercises Glue. Docker startup is owned by
# testcontainers-go from inside the test process (no `docker compose
# up` required); the legacy docker-compose.yml stays as a fallback
# for CI environments that pre-provision Kafka.
.PHONY: test-integ
test-integ: setup-dirs generate-protos
ifdef COVERAGE
	@echo "Running integration tests with coverage..."
	cd integration-tests && AWS_INTEGRATION=1 $(GO) test -tags integration -count=1 -timeout $(INTEG_TIMEOUT) \
	  -coverpkg=../pkg/gsrserde-go/... -coverprofile=../$(COVERAGE_DIR)/integration-tests-coverage.out ./...
	cd integration-tests && $(GO) tool cover -html=../$(COVERAGE_DIR)/integration-tests-coverage.out -o ../$(COVERAGE_DIR)/integration-tests-coverage.html
	@echo "Integration test coverage report: $(COVERAGE_DIR)/integration-tests-coverage.html"
else
	@echo "Running integration tests (AWS_INTEGRATION=1, timeout $(INTEG_TIMEOUT))..."
	cd integration-tests && AWS_INTEGRATION=1 $(GO) test -tags integration -count=1 -timeout $(INTEG_TIMEOUT) ./... 2>&1 | tee ../$(BUILD_DIR)/integration-test-output.txt
	@echo "Integration test report: $(BUILD_DIR)/integration-test-output.txt"
endif

# test-integ-real runs the §5.3 integration suite against REAL AWS
# Glue (GSR_GLUE=real). Phase 4.7 wired the realglue package + the
# newGlueHandle selector; this target is the user-facing entry point.
#
# Safety layers:
#   1. Hard fail if neither AWS_PROFILE nor AWS_ACCESS_KEY_ID is set
#      (no creds → realglue.New errors at construction, but failing
#      earlier here keeps the user from waiting on Kafka startup
#      before learning their creds aren't set).
#   2. 5s sleep + banner so an accidental invocation is interruptible.
#   3. 30-minute timeout cap so a stuck real-Glue call doesn't run
#      forever.
#
# Region defaults to AWS_REGION, falling back to us-east-2 inside
# realglue.New (plan §6.3 anchor — matches the existing Java/C#
# canary account).
REAL_INTEG_TIMEOUT := 30m
REAL_REGION := $(if $(AWS_REGION),$(AWS_REGION),us-east-2)

.PHONY: test-integ-real
test-integ-real: setup-dirs generate-protos
	@if [ -z "$$AWS_PROFILE" ] && [ -z "$$AWS_ACCESS_KEY_ID" ]; then \
		echo "ERROR: test-integ-real requires AWS credentials"; \
		echo "  set AWS_PROFILE=<your-beta-profile> OR"; \
		echo "  set AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY"; \
		echo "  see integration-tests/REAL-AWS-RUNBOOK.md for the full prereqs list"; \
		exit 1; \
	fi
	@echo ""
	@echo "============================================================"
	@echo "About to bill real AWS Glue in $(REAL_REGION)."
	@echo "Press Ctrl-C within 5s to abort."
	@echo "============================================================"
	@echo ""
	@sleep 5
	cd integration-tests && GSR_GLUE=real AWS_INTEGRATION=1 $(GO) test -tags integration -count=1 -timeout $(REAL_INTEG_TIMEOUT) ./... 2>&1 | tee ../$(BUILD_DIR)/integration-test-real-output.txt
	@echo ""
	@echo "Run complete. Verify no leaked SCHEMAS in default-registry:"
	@echo "  aws glue list-schemas --registry-id RegistryName=default-registry --region $(REAL_REGION) \\"
	@echo "    --query 'Schemas[?starts_with(SchemaName, \`gsr-go-it-\`)].SchemaName' --output text"
	@echo "  (should print nothing; see integration-tests/REAL-AWS-RUNBOOK.md for manual cleanup)"

# test-integ-confluent adds the `confluent` build tag so the
# confluent-kafka-go adapter (CGO + librdkafka) and its leg of the
# §5.3 matrix are exercised. Separated from test-integ because
# librdkafka isn't on every dev host; plan §7 lists sarama + confluent
# as the two officially supported clients, so confluent coverage runs
# in the merge gate, not the default `make test-integ` developer flow.
.PHONY: test-integ-confluent
test-integ-confluent: setup-dirs generate-protos
	@echo "Running integration tests with confluent tag (AWS_INTEGRATION=1, timeout $(INTEG_TIMEOUT))..."
	cd integration-tests && AWS_INTEGRATION=1 $(GO) test -tags 'integration confluent' -count=1 -timeout $(INTEG_TIMEOUT) ./... 2>&1 | tee ../$(BUILD_DIR)/integration-test-confluent-output.txt
	@echo "Confluent integration test report: $(BUILD_DIR)/integration-test-confluent-output.txt"

.PHONY: test-all
test-all: test test-integ

.PHONY: clean
clean:
	@echo "Cleaning build directory..."
	rm -rf $(BUILD_DIR)
	@echo "Clean complete."

# Generate protobuf files. Requires `protoc` + `protoc-gen-go` on PATH.
# If protoc isn't installed, `make test-integ` still works because the
# generated testpb/ is committed; this target is only needed when
# .proto files change.
.PHONY: generate-protos
generate-protos: setup-dirs
	@echo "Generating protobuf files..."
	@mkdir -p $(PROTO_OUT_DIR)
	@if ! command -v protoc >/dev/null 2>&1; then \
		echo "  -> protoc not on PATH; skipping (committed testpb/ is in use)"; \
		exit 0; \
	fi
	@for proto_file in $(PROTO_SRC_DIR)/*.proto; do \
		if [ -f "$$proto_file" ]; then \
			basename=$$(basename "$$proto_file"); \
			package_name=$$(awk '/^\s*package\s+[^;]+;/ { print $$2 }' "$$proto_file" | tr -d ';'); \
			has_go_package=$$(grep -q "option go_package" "$$proto_file" && echo "true" || echo "false"); \
			echo "Processing $$basename: package=$$package_name, has_go_package=$$has_go_package"; \
			if [ "$$has_go_package" = "false" ]; then \
				trimmed_package=$$(echo "$$package_name" | sed "s/^$(PACKAGE_PREFIX_TO_TRIM)\.//"); \
				package_path=$$(echo "$$trimmed_package" | sed 's/\./\//g'); \
				output_dir="$(PROTO_OUT_DIR)/$$package_path"; \
				import_path="$(GO_MODULE_PREFIX)/$$package_path"; \
				mkdir -p "$$output_dir"; \
				protoc --proto_path=$(PROTO_SRC_DIR) \
					--go_out="$$output_dir" \
					--go_opt=paths=source_relative \
					--go_opt=M$$basename="$$import_path" \
					"$$proto_file"; \
			else \
				protoc --proto_path=$(PROTO_SRC_DIR) \
					--go_out=$(PROTO_OUT_DIR) \
					--go_opt=paths=source_relative \
					"$$proto_file"; \
			fi; \
		fi; \
	done
	@echo "Generating integration test protobuf files..."
	@protoc --proto_path=integration-tests --go_out=integration-tests/testpb --go_opt=paths=source_relative integration-tests/test_message.proto
	@echo "Protobuf generation complete"

.PHONY: generate-protos-custom
generate-protos-custom:
	$(MAKE) generate-protos \
		PROTO_SRC_DIR=$(or $(CUSTOM_PROTO_SRC_DIR),$(PROTO_SRC_DIR)) \
		PROTO_OUT_DIR=$(or $(CUSTOM_PROTO_OUT_DIR),$(PROTO_OUT_DIR)) \
		PACKAGE_PREFIX_TO_TRIM=$(or $(CUSTOM_PACKAGE_PREFIX),$(PACKAGE_PREFIX_TO_TRIM))

.PHONY: clean-protos
clean-protos:
	@echo "Cleaning generated protobuf files..."
	@rm -rf $(PROTO_OUT_DIR)
	@echo "Protobuf files cleaned"

# Dockerized integration tests: run the same suite inside an Alpine
# Go container, with the host's ~/.aws mounted read-only so the
# in-container suite can reach Glue. This is the CI-parity path that
# proves a fresh-image developer can run test-integ end-to-end.
#
# Why read-only mount: AWS credentials should never be writable from
# inside a build container — accidental writes (e.g. an `aws configure`
# inside the container) would silently rotate the host's creds.
.PHONY: test-integ-dockerized
test-integ-dockerized: setup-dirs
ifdef COVERAGE
	@echo "Running dockerized integration tests with coverage..."
	cd integration-tests && COVERAGE=true AWS_INTEGRATION=1 docker compose -f docker-compose.dockerized.yml up --build --abort-on-container-exit
	cd integration-tests && docker compose -f docker-compose.dockerized.yml down
	@echo "Alpine integration test coverage report: $(COVERAGE_DIR)/dockerized-integration-coverage.html"
else
	@echo "Running Alpine integration tests..."
	cd integration-tests && AWS_INTEGRATION=1 docker compose -f docker-compose.dockerized.yml up --build --abort-on-container-exit | tee ../$(BUILD_DIR)/dockerized-integ-docker-logs.txt
	cd integration-tests && docker compose -f docker-compose.dockerized.yml logs golang-tests > ../$(BUILD_DIR)/dockerized-test-logs.txt 2>&1 || true
	cd integration-tests && docker compose -f docker-compose.dockerized.yml down
	@echo "Alpine integration test report: $(BUILD_DIR)/dockerized-integration-test-output.txt"
endif

.PHONY: cover-test-integ-dockerized
cover-test-integ-dockerized: setup-dirs
	@echo "Running Alpine integration tests with coverage..."
	$(MAKE) test-integ-dockerized COVERAGE=true

.PHONY: build-dockerized-test-image
build-dockerized-test-image: setup-dirs generate-protos
	@echo "Building Alpine test image..."
	docker build -f integration-tests/Dockerfile.dockerized-tests -t golang-gsr-tests:dockerized ..

.PHONY: clean-dockerized
clean-dockerized:
	@echo "Cleaning Alpine test containers and images..."
	cd integration-tests && docker compose -f docker-compose.dockerized.yml down --remove-orphans || true
	docker rmi golang-gsr-tests:dockerized || true

# Phase 4.6 — Java <-> Go interop sidecar (plan §5.5).
#
# java-sidecar-build packages the fat JAR via Maven and tags the Docker image
# locally so testcontainers-go can pick it up in container mode.
#
# JAVA_MVN resolution order:
#   1. value from the user's environment / make invocation (override wins)
#   2. `mvn` on PATH (covers system-wide installs)
#   3. ~/.local/opt/apache-maven-3.9.9/bin/mvn (matches README install hint)
# If none of those exist the target errors with a clear message.
JAVA_INTEROP_DIR     := integration-tests/java-interop
JAVA_INTEROP_JAR     := $(JAVA_INTEROP_DIR)/target/java-interop-sidecar.jar
JAVA_SIDECAR_IMAGE   := gsr-go-it-java-sidecar:latest
JAVA_MVN             ?= $(or $(shell command -v mvn 2>/dev/null),$(HOME)/.local/opt/apache-maven-3.9.9/bin/mvn)

.PHONY: java-sidecar-build
java-sidecar-build:
	@if [ ! -x "$(JAVA_MVN)" ] && ! command -v "$(JAVA_MVN)" >/dev/null 2>&1; then \
		echo "ERROR: Maven not found at $(JAVA_MVN)"; \
		echo "  Set JAVA_MVN=<path-to-mvn> or follow integration-tests/java-interop/README.md to install Maven 3.9+."; \
		exit 1; \
	fi
	@echo "Building Java interop sidecar JAR via $(JAVA_MVN)..."
	$(JAVA_MVN) -B -DskipTests -f $(JAVA_INTEROP_DIR)/pom.xml package
	@echo "Building Docker image $(JAVA_SIDECAR_IMAGE)..."
	docker build -t $(JAVA_SIDECAR_IMAGE) $(JAVA_INTEROP_DIR)
	@echo "Java sidecar JAR: $(JAVA_INTEROP_JAR)"
	@echo "Java sidecar image: $(JAVA_SIDECAR_IMAGE)"

# test-integ-interop runs only the Phase 4.6 interop_*_test.go cells in
# local mode. Requires a JDK on PATH; falls back to a skip if absent.
.PHONY: test-integ-interop
test-integ-interop:
	@echo "Running interop tests (GSR_INTEROP_MODE=local)..."
	cd integration-tests && GSR_INTEROP_MODE=local $(GO) test -tags integration -count=1 -timeout=10m -run=Interop ./... 2>&1 | tee ../$(BUILD_DIR)/integration-test-interop-local.txt

# test-integ-interop-container is the same suite in container mode.
# Requires the Docker image built by java-sidecar-build.
.PHONY: test-integ-interop-container
test-integ-interop-container:
	@echo "Running interop tests (GSR_INTEROP_MODE=container)..."
	cd integration-tests && GSR_INTEROP_MODE=container $(GO) test -tags integration -count=1 -timeout=15m -run=Interop ./... 2>&1 | tee ../$(BUILD_DIR)/integration-test-interop-container.txt

# ---- Phase 6 benchmarks ----------------------------------------------------
#
# bench-go         Smoke-runs the Go benchmark matrix and writes the output
#                  under perf/baselines/go/ at the repo root.
# bench-go-full    Same, but with -count=5 -benchtime=2s for benchstat-quality
#                  numbers. Takes ~10 min.
# bench-java       Builds target/benchmarks.jar (Phase 6 JMH module) and runs
#                  the smoke matrix into perf/baselines/java/.
# bench-java-full  Same, with the JMH @Warmup / @Measurement / @Fork defaults
#                  (3+5×1s, fork 2). Takes ~10 min.
# bench-compare    Pairwise benchstat over the two most recent Go core
#                  baselines, plus a banner reminder to refresh the Java side.
#
# All targets shell out to the scripts in $(PERF_SCRIPTS) so the regeneration
# logic lives in one place (the README under perf/ documents the same
# invocations).
PERF_ROOT     := ../../perf
PERF_SCRIPTS  := $(PERF_ROOT)/scripts
BENCHSTAT     ?= $(HOME)/go/bin/benchstat

.PHONY: bench-go
bench-go:
	$(PERF_SCRIPTS)/run-go-bench.sh smoke

.PHONY: bench-go-full
bench-go-full:
	$(PERF_SCRIPTS)/run-go-bench.sh --full

.PHONY: bench-java
bench-java:
	$(PERF_SCRIPTS)/run-java-bench.sh smoke

.PHONY: bench-java-full
bench-java-full:
	$(PERF_SCRIPTS)/run-java-bench.sh --full

# bench-compare: feed the two most recent core baselines under
# perf/baselines/go/ to benchstat. Two timestamped files are required —
# the very first bench-go run only has one, so the target prints a
# friendly hint and exits 0 in that case.
.PHONY: bench-compare
bench-compare:
	@if [ ! -x "$(BENCHSTAT)" ]; then \
		echo "ERROR: benchstat not found at $(BENCHSTAT)"; \
		echo "  Install with: $(GO) install golang.org/x/perf/cmd/benchstat@latest"; \
		exit 1; \
	fi
	@latest_two=$$(ls -1t $(PERF_ROOT)/baselines/go/core-2*.txt 2>/dev/null | head -2); \
	count=$$(echo "$$latest_two" | wc -l); \
	if [ "$$count" -lt 2 ]; then \
		echo "Only $$count timestamped Go baseline file(s) — run 'make bench-go' at least twice before benchstat has a delta to report."; \
		exit 0; \
	fi; \
	echo "==> benchstat $$latest_two"; \
	$(BENCHSTAT) $$latest_two
	@echo ""
	@echo "Java JMH does not have a benchstat-equivalent ranker yet. To refresh"
	@echo "the Java side of the perf/README.md comparison table:"
	@echo "  make bench-java         # smoke (single fork, ~1 min)"
	@echo "  make bench-java-full    # default JMH config (~10 min, defensible)"

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build                       Compile/validate module and create package artifacts"
	@echo "  test                        Run unit + component tests (excludes integration tests, <30s, no Docker/AWS)"
	@echo "  test-integ                  Run §5.3 integration tests (AWS_INTEGRATION=1, fakeglue, testcontainers-go Kafka, $(INTEG_TIMEOUT) timeout)"
	@echo "  test-integ-real             Run §5.3 integration tests against REAL AWS Glue (GSR_GLUE=real, bills AWS, $(REAL_INTEG_TIMEOUT) timeout)"
	@echo "  test-integ-dockerized       Run integration tests inside an Alpine container (CI parity, mounts ~/.aws ro)"
	@echo "  java-sidecar-build          Build Phase 4.6 Java interop sidecar JAR + Docker image"
	@echo "  test-integ-interop          Run interop_*_test.go in GSR_INTEROP_MODE=local"
	@echo "  test-integ-interop-container Run interop_*_test.go in GSR_INTEROP_MODE=container"
	@echo "  bench-go                    Phase 6 Go benchmark smoke (writes perf/baselines/go/)"
	@echo "  bench-go-full               Phase 6 Go benchmark count=5 benchtime=2s (~10 min)"
	@echo "  bench-java                  Phase 6 JMH benchmark smoke (writes perf/baselines/java/)"
	@echo "  bench-java-full             Phase 6 JMH benchmark default config (~10 min)"
	@echo "  bench-compare               Pairwise benchstat over the two most recent Go baselines"
	@echo "  test-all                    Run all tests (unit, component, and integration)"
	@echo "  cover-test                  Run unit + component tests with coverage"
	@echo "  cover-test-integ            Run integration tests with coverage"
	@echo "  cover-test-integ-dockerized Run dockerized integration tests with coverage"
	@echo "  cover-test-all              Run all tests with coverage"
	@echo "  generate-protos             Generate .pb.go files from .proto files (no-op when protoc missing)"
	@echo "  generate-protos-custom      Generate protos with custom variables"
	@echo "  build-dockerized-test-image Build Alpine test image only"
	@echo "  clean-protos                Remove generated .pb.go files"
	@echo "  clean-dockerized            Clean Alpine containers and images"
	@echo "  clean                       Remove build directory"
	@echo "  help                        Show this help message"
