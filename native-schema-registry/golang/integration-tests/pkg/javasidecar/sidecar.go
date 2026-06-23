// Package javasidecar launches the Java <-> Go interop sidecar
// (a JVM process running the real aws-glue-schema-registry Java library)
// and exposes a small Go client for its /encode + /decode HTTP surface.
//
// Phase 4.6 deliverable 3 — see plan §5.5.
//
// Two modes, selected by GSR_INTEROP_MODE:
//
//	"local"     (default) — spawns `java -jar <path>` directly. Requires a
//	                        JDK on PATH and the sidecar JAR built via
//	                        `make java-sidecar-build`. Fastest inner loop.
//
//	"container" — pulls/uses the local `gsr-go-it-java-sidecar:latest`
//	              image via testcontainers-go. Hermetic; CI-friendly.
//
// In both modes New() returns a *Sidecar whose Encode/Decode methods talk
// to the running JVM. The caller is expected to register Stop() on
// t.Cleanup; the helper StartForTest does this automatically.
package javasidecar

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Mode selects how the sidecar is brought up.
type Mode int

const (
	ModeLocal Mode = iota
	ModeContainer
)

// String renders the mode as it appears in GSR_INTEROP_MODE.
func (m Mode) String() string {
	switch m {
	case ModeLocal:
		return "local"
	case ModeContainer:
		return "container"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

// ModeFromEnv resolves the active mode from GSR_INTEROP_MODE. Unset / empty
// / "local" all map to ModeLocal; "container" maps to ModeContainer.
// Anything else is an error so a typo doesn't silently fall back to local.
func ModeFromEnv() (Mode, error) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GSR_INTEROP_MODE")))
	switch v {
	case "", "local":
		return ModeLocal, nil
	case "container":
		return ModeContainer, nil
	default:
		return ModeLocal, fmt.Errorf("javasidecar: unrecognized GSR_INTEROP_MODE=%q", v)
	}
}

// Options configures sidecar startup.
type Options struct {
	// Mode overrides the GSR_INTEROP_MODE env. Zero value = read from env.
	Mode *Mode

	// JarPath is the path to java-interop-sidecar.jar (local mode only).
	// Default: <repo-root>/native-schema-registry/golang/integration-tests/java-interop/target/java-interop-sidecar.jar.
	JarPath string

	// JavaBinary overrides the java command (local mode only). Default: "java".
	JavaBinary string

	// Image is the Docker image tag (container mode only). Default:
	// "gsr-go-it-java-sidecar:latest".
	Image string

	// ContainerPort is the port the JVM listens on inside the container
	// (container mode only). Default: 8080. The host-side mapped port is
	// discovered via testcontainers-go's GetMappedPort.
	ContainerPort int

	// StartTimeout caps how long New() waits for /health to come back 200.
	// Default: 30 seconds.
	StartTimeout time.Duration

	// HTTPClient overrides the http.Client used to talk to the sidecar.
	// Default: a fresh client with a 30s per-request timeout.
	HTTPClient *http.Client

	// commander lets tests stub out exec.Command for the Tier-1 unit test.
	// nil in production paths.
	commander commander
}

// Sidecar is a running sidecar process or container plus a client for it.
type Sidecar struct {
	baseURL string
	client  *http.Client
	mode    Mode
	stop    func(ctx context.Context) error
	once    sync.Once
}

// commander is the exec.Command seam — production wires it to realCommander.
// The Tier-1 test substitutes a fakeCommander that records arguments and
// returns a fixed stdout/stderr without spawning a JVM.
type commander interface {
	Command(ctx context.Context, name string, args ...string) commandRun
}

type commandRun interface {
	StdoutPipe() (io.ReadCloser, error)
	StderrPipe() (io.ReadCloser, error)
	Start() error
	Process() *os.Process
	Wait() error
	// PGroup reports whether Start put the child in its own process
	// group. When false, Stop signals the child directly (Kill(pid))
	// rather than Kill(-pid), which would otherwise hit the launcher's
	// own process group.
	PGroup() bool
}

// realCommander is the production exec.Command wrapper.
type realCommander struct{}

func (realCommander) Command(ctx context.Context, name string, args ...string) commandRun {
	cmd := exec.CommandContext(ctx, name, args...)
	// Put the child in its own process group so SIGTERM to the launcher
	// can be propagated to the JVM cleanly via Kill(-pid, ...).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return &realCommandRun{cmd: cmd, pgid: true}
}

type realCommandRun struct {
	cmd  *exec.Cmd
	pgid bool // mirrors SysProcAttr.Setpgid so Stop knows whether kill(-pid) is safe
}

func (r *realCommandRun) StdoutPipe() (io.ReadCloser, error) { return r.cmd.StdoutPipe() }
func (r *realCommandRun) StderrPipe() (io.ReadCloser, error) { return r.cmd.StderrPipe() }
func (r *realCommandRun) Start() error                       { return r.cmd.Start() }
func (r *realCommandRun) Process() *os.Process               { return r.cmd.Process }
func (r *realCommandRun) Wait() error                        { return r.cmd.Wait() }
func (r *realCommandRun) PGroup() bool                       { return r.pgid }

// New starts a sidecar and returns it once /health is responding 200.
// Callers MUST defer Stop(ctx) (or use StartForTest, which wires
// t.Cleanup for them).
func New(ctx context.Context, opts Options) (*Sidecar, error) {
	mode, err := resolveMode(opts)
	if err != nil {
		return nil, err
	}
	if opts.StartTimeout == 0 {
		opts.StartTimeout = 30 * time.Second
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}

	switch mode {
	case ModeLocal:
		return startLocal(ctx, opts)
	case ModeContainer:
		return startContainer(ctx, opts)
	default:
		return nil, fmt.Errorf("javasidecar: unsupported mode %v", mode)
	}
}

// StartForTest is the common path used by integration tests: it calls New,
// pipes stderr to t.Log, and registers Stop on t.Cleanup so a panicking
// test doesn't leak a JVM.
func StartForTest(ctx context.Context, t testing.TB, opts Options) *Sidecar {
	t.Helper()
	sc, err := New(ctx, opts)
	if err != nil {
		t.Fatalf("javasidecar: start failed: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := sc.Stop(stopCtx); err != nil {
			t.Logf("javasidecar: stop returned error (test may still pass): %v", err)
		}
	})
	return sc
}

func resolveMode(opts Options) (Mode, error) {
	if opts.Mode != nil {
		return *opts.Mode, nil
	}
	return ModeFromEnv()
}

// startLocal forks a JVM with `java -jar <jar> --port=0` and parses the
// `PORT: <n>` header line out of stdout.
func startLocal(ctx context.Context, opts Options) (*Sidecar, error) {
	jar := opts.JarPath
	if jar == "" {
		var err error
		jar, err = defaultJarPath()
		if err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(jar); err != nil {
		return nil, fmt.Errorf("javasidecar: jar not found at %s: %w (build it with `make java-sidecar-build`)", jar, err)
	}

	javaBin := opts.JavaBinary
	if javaBin == "" {
		// GSR_INTEROP_JAVA lets hermetic-build hosts point the launcher
		// at a fully-qualified JDK install when plain `java` isn't on
		// PATH. Matches the probe in tests/interop_*_test.go's
		// requireInterop helper.
		javaBin = os.Getenv("GSR_INTEROP_JAVA")
	}
	if javaBin == "" {
		javaBin = "java"
	}

	cmdr := opts.commander
	if cmdr == nil {
		cmdr = realCommander{}
	}

	// Use a derived cancel context so Stop can kill the child even when
	// the caller passed a context that doesn't cancel mid-test.
	runCtx, cancel := context.WithCancel(context.Background())
	run := cmdr.Command(runCtx, javaBin, "-jar", jar, "--port=0")

	stdout, err := run.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("javasidecar: stdout pipe: %w", err)
	}
	stderr, err := run.StderrPipe()
	if err != nil {
		_ = stdout.Close()
		cancel()
		return nil, fmt.Errorf("javasidecar: stderr pipe: %w", err)
	}
	if err := run.Start(); err != nil {
		// exec's Cmd.Wait normally closes the pipes; if Start failed
		// Wait is illegal, so close them here to avoid leaking FDs on
		// the partial-fork path.
		_ = stdout.Close()
		_ = stderr.Close()
		cancel()
		return nil, fmt.Errorf("javasidecar: start java: %w", err)
	}

	// Drain stderr in the background so the JVM doesn't block on a full
	// pipe. Routed to os.Stderr; tests that want it captured can wrap
	// Options.HTTPClient and replace this with t.Log via a custom hook.
	go func() { _, _ = io.Copy(os.Stderr, stderr) }()

	// Use a teeing reader so the PORT: line is observed AND stdout keeps
	// draining after — otherwise readPortLine's error paths could leave
	// the stdout pipe with no reader, blocking the writer (e.g. the
	// fakeCommander used in unit tests).
	portR, portW := io.Pipe()
	go func() {
		_, _ = io.Copy(io.MultiWriter(portW, io.Discard), stdout)
		_ = portW.Close()
	}()

	port, err := readPortLine(ctx, portR, opts.StartTimeout)
	if err != nil {
		cancel()
		_ = run.Wait()
		_ = portR.Close()
		return nil, err
	}
	// Continue consuming any remaining bytes on portR so the copier
	// goroutine doesn't block on a full pipe.
	go func() { _, _ = io.Copy(io.Discard, portR) }()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	pgroup := run.PGroup()
	stop := func(stopCtx context.Context) error {
		proc := run.Process()
		if proc != nil {
			// Send SIGTERM so the shutdown hook runs and the HttpServer
			// drains in-flight requests. Negative PID signals the
			// process group, but ONLY when we know Start put the child
			// in its own group — otherwise Kill(-pid) would deliver to
			// our own process group and kill the test runner.
			_ = signalProcess(proc, syscall.SIGTERM, pgroup)
		}
		cancel()
		// Cap how long we wait for the JVM to exit. If it hangs, the
		// context cancellation above will deliver SIGKILL.
		done := make(chan error, 1)
		go func() { done <- run.Wait() }()
		select {
		case err := <-done:
			// exec returns a non-nil error for SIGTERM exits; that's
			// expected and not a failure.
			var exitErr *exec.ExitError
			if err == nil || errors.As(err, &exitErr) {
				return nil
			}
			return err
		case <-stopCtx.Done():
			if proc != nil {
				_ = signalProcess(proc, syscall.SIGKILL, pgroup)
			}
			return stopCtx.Err()
		}
	}

	sc := &Sidecar{
		baseURL: baseURL,
		client:  opts.HTTPClient,
		mode:    ModeLocal,
		stop:    stop,
	}
	if err := waitHealthy(ctx, sc, opts.StartTimeout); err != nil {
		_ = sc.Stop(context.Background())
		return nil, fmt.Errorf("javasidecar: never became healthy: %w", err)
	}
	return sc, nil
}

// readPortLine consumes lines from stdout until the first one matches
// `PORT: <n>` (the sidecar's contract — see SidecarMain.java).
func readPortLine(ctx context.Context, stdout io.ReadCloser, timeout time.Duration) (int, error) {
	type result struct {
		port int
		err  error
	}
	out := make(chan result, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "PORT:") {
				var port int
				if _, err := fmt.Sscanf(line, "PORT: %d", &port); err != nil {
					out <- result{0, fmt.Errorf("javasidecar: parse port line %q: %w", line, err)}
					return
				}
				out <- result{port, nil}
				return
			}
		}
		if err := scanner.Err(); err != nil {
			out <- result{0, fmt.Errorf("javasidecar: read stdout: %w", err)}
			return
		}
		out <- result{0, errors.New("javasidecar: jvm exited before printing PORT: line")}
	}()

	select {
	case r := <-out:
		return r.port, r.err
	case <-time.After(timeout):
		return 0, fmt.Errorf("javasidecar: timed out after %s waiting for PORT: line", timeout)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// startContainer brings the sidecar up via testcontainers-go. Implemented in
// sidecar_container.go (build-tagged) so the non-integration build doesn't
// drag in the testcontainers-go transitive deps.
//
// Stub here so production callers can still link without the integration tag;
// startContainer returns an explicit error rather than silently falling back
// to local.
func startContainerStub() (*Sidecar, error) {
	return nil, errors.New("javasidecar: container mode requires the integration build tag")
}

// Stop tears down the sidecar. Safe to call multiple times.
func (s *Sidecar) Stop(ctx context.Context) error {
	var err error
	s.once.Do(func() {
		err = s.stop(ctx)
	})
	return err
}

// BaseURL returns the http://host:port URL the sidecar listens on.
// Exposed for tests that want to hit /health directly.
func (s *Sidecar) BaseURL() string { return s.baseURL }

// Mode returns the mode the sidecar was launched in.
func (s *Sidecar) Mode() Mode { return s.mode }

// EncodeRequest is the wire shape of POST /encode.
type EncodeRequest struct {
	Format          string `json:"format"`
	Schema          string `json:"schema"`
	SchemaName      string `json:"schemaName"`
	SchemaVersionID string `json:"schemaVersionId"`
	Payload         []byte `json:"-"`
	Compression     string `json:"compression,omitempty"`
}

// DecodeResponse is the wire shape of POST /decode.
type DecodeResponse struct {
	Payload          []byte
	SchemaVersionID  string
	SchemaName       string
	SchemaDefinition string
	DataFormat       string
}

// KafkaProduceRequest is the input to POST /kafka-produce. Phase 4.6.5:
// the sidecar drives GlueSchemaRegistryKafkaSerializer.serialize(topic,
// record) end-to-end, so callers ship the LOGICAL typed record in a
// per-format JSON envelope (Record) instead of pre-encoded bytes.
//
// Envelope shapes:
//
//	AVRO     : {"fields": {"<name>": <value>, ...}}
//	JSON     : {"schema": "<jsonSchema>", "payload": "<jsonDoc>"}
//	PROTOBUF : {"messageTypeFullName": "<test.TestMessage>",
//	            "fieldsJson": "<json>"}
type KafkaProduceRequest struct {
	Format      string
	Schema      string
	SchemaName  string
	Record      map[string]any // per-format envelope; see above
	Compression string
	Bootstrap   string
	Topic       string
	Region      string // optional; sidecar falls back to AWS_REGION / us-east-2
}

// KafkaProduceResponse reports what the sidecar produced.
type KafkaProduceResponse struct {
	SchemaVersionID string
	Bytes           []byte // the framed bytes shipped to Kafka
	Offset          int64
	Partition       int32
}

// KafkaConsumeRequest is the input to POST /kafka-consume. The sidecar
// drives GlueSchemaRegistryKafkaDeserializer.deserialize(topic, bytes)
// and returns the per-format JSON envelope in Record.
//
// Format must match the producer's format so the sidecar knows how to
// reconstruct the typed record into the envelope.
type KafkaConsumeRequest struct {
	Bootstrap string
	Topic     string
	Format    string // AVRO | JSON | PROTOBUF — drives RecordCodec envelope shape
	GroupID   string // optional; sidecar generates a random one if empty
	Region    string // optional
	TimeoutMs int    // optional; sidecar defaults to 30000
}

// KafkaConsumeResponse reports what the sidecar consumed.
type KafkaConsumeResponse struct {
	SchemaVersionID  string
	DataFormat       string
	SchemaDefinition string
	SchemaArn        string
	// Record is the per-format JSON envelope: see KafkaProduceRequest for
	// the shape. Empty when the sidecar couldn't reconstruct it.
	Record map[string]any
}

// Encode posts the request to the sidecar's /encode endpoint and returns
// the framed GSR bytes.
func (s *Sidecar) Encode(ctx context.Context, req EncodeRequest) ([]byte, error) {
	compression, err := compressionOrDefault(req.Compression)
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"format":          req.Format,
		"schema":          req.Schema,
		"schemaName":      req.SchemaName,
		"schemaVersionId": req.SchemaVersionID,
		"payload":         base64.StdEncoding.EncodeToString(req.Payload),
		"compression":     compression,
	}
	var resp struct {
		Bytes string `json:"bytes"`
	}
	if err := s.postJSON(ctx, "/encode", body, &resp); err != nil {
		return nil, err
	}
	out, err := base64.StdEncoding.DecodeString(resp.Bytes)
	if err != nil {
		return nil, fmt.Errorf("javasidecar: decode response bytes: %w", err)
	}
	return out, nil
}

// KafkaProduce drives GlueSchemaRegistryKafkaSerializer.serialize(topic,
// record) on the Java side: the sidecar reconstructs the typed Java
// record from the per-format envelope, registers the schema with real
// Glue, frames + produces one record to Kafka.
func (s *Sidecar) KafkaProduce(ctx context.Context, req KafkaProduceRequest) (*KafkaProduceResponse, error) {
	compression, err := compressionOrDefault(req.Compression)
	if err != nil {
		return nil, err
	}
	if req.Record == nil {
		return nil, fmt.Errorf("javasidecar: KafkaProduceRequest.Record must not be nil")
	}
	body := map[string]any{
		"format":      req.Format,
		"schema":      req.Schema,
		"schemaName":  req.SchemaName,
		"record":      req.Record,
		"compression": compression,
		"bootstrap":   req.Bootstrap,
		"topic":       req.Topic,
	}
	if req.Region != "" {
		body["region"] = req.Region
	}
	var raw struct {
		SchemaVersionID string `json:"schemaVersionId"`
		Bytes           string `json:"bytes"`
		Offset          int64  `json:"offset"`
		Partition       int32  `json:"partition"`
	}
	if err := s.postJSON(ctx, "/kafka-produce", body, &raw); err != nil {
		return nil, err
	}
	framed, err := base64.StdEncoding.DecodeString(raw.Bytes)
	if err != nil {
		return nil, fmt.Errorf("javasidecar: decode produced bytes: %w", err)
	}
	return &KafkaProduceResponse{
		SchemaVersionID: raw.SchemaVersionID,
		Bytes:           framed,
		Offset:          raw.Offset,
		Partition:       raw.Partition,
	}, nil
}

// KafkaConsume drives GlueSchemaRegistryKafkaDeserializer.deserialize on
// the Java side: poll Kafka for one record, deserialize through the full
// library entry point, and return the typed record as a per-format JSON
// envelope (see KafkaConsumeResponse.Record).
func (s *Sidecar) KafkaConsume(ctx context.Context, req KafkaConsumeRequest) (*KafkaConsumeResponse, error) {
	if req.Format == "" {
		return nil, fmt.Errorf("javasidecar: KafkaConsumeRequest.Format must be set (AVRO|JSON|PROTOBUF)")
	}
	body := map[string]any{
		"bootstrap": req.Bootstrap,
		"topic":     req.Topic,
		"format":    req.Format,
	}
	if req.GroupID != "" {
		body["groupId"] = req.GroupID
	}
	if req.Region != "" {
		body["region"] = req.Region
	}
	if req.TimeoutMs > 0 {
		body["timeoutMs"] = req.TimeoutMs
	}
	var raw struct {
		SchemaVersionID  string         `json:"schemaVersionId"`
		DataFormat       string         `json:"dataFormat"`
		SchemaDefinition string         `json:"schemaDefinition"`
		SchemaArn        string         `json:"schemaArn"`
		Record           map[string]any `json:"record"`
	}
	if err := s.postJSON(ctx, "/kafka-consume", body, &raw); err != nil {
		return nil, err
	}
	return &KafkaConsumeResponse{
		SchemaVersionID:  raw.SchemaVersionID,
		DataFormat:       raw.DataFormat,
		SchemaDefinition: raw.SchemaDefinition,
		SchemaArn:        raw.SchemaArn,
		Record:           raw.Record,
	}, nil
}

// Decode posts the framed bytes to the sidecar's /decode endpoint.
func (s *Sidecar) Decode(ctx context.Context, framed []byte) (*DecodeResponse, error) {
	body := map[string]any{
		"bytes": base64.StdEncoding.EncodeToString(framed),
	}
	var raw struct {
		Payload          string `json:"payload"`
		SchemaVersionID  string `json:"schemaVersionId"`
		SchemaName       string `json:"schemaName"`
		SchemaDefinition string `json:"schemaDefinition"`
		DataFormat       string `json:"dataFormat"`
	}
	if err := s.postJSON(ctx, "/decode", body, &raw); err != nil {
		return nil, err
	}
	payload, err := base64.StdEncoding.DecodeString(raw.Payload)
	if err != nil {
		return nil, fmt.Errorf("javasidecar: decode response payload: %w", err)
	}
	return &DecodeResponse{
		Payload:          payload,
		SchemaVersionID:  raw.SchemaVersionID,
		SchemaName:       raw.SchemaName,
		SchemaDefinition: raw.SchemaDefinition,
		DataFormat:       raw.DataFormat,
	}, nil
}

func (s *Sidecar) postJSON(ctx context.Context, path string, body any, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("javasidecar: marshal %s body: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("javasidecar: POST %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("javasidecar: POST %s -> %d: %s", path, resp.StatusCode, strings.TrimSpace(string(buf)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// waitHealthy polls /health until 200 or timeout.
func waitHealthy(ctx context.Context, sc *Sidecar, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("timed out (last error: %w)", lastErr)
			}
			return errors.New("timed out")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, sc.baseURL+"/health", nil)
		if err != nil {
			return err
		}
		resp, err := sc.client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
			lastErr = fmt.Errorf("status=%d", resp.StatusCode)
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// signalProcess sends sig to proc, addressing the process group when pgroup
// is true and the OS supports it, and addressing the process directly
// otherwise. Centralized here so the Stop closure stays readable.
func signalProcess(proc *os.Process, sig syscall.Signal, pgroup bool) error {
	if proc == nil {
		return nil
	}
	if pgroup {
		return syscall.Kill(-proc.Pid, sig)
	}
	return syscall.Kill(proc.Pid, sig)
}

// compressionOrDefault validates the caller's compression choice and
// normalizes it. Returning a Go-side error here keeps callers from
// shipping typos like "GZIP" to the sidecar and getting an opaque 500 in
// response — the sidecar only speaks NONE and ZLIB.
func compressionOrDefault(c string) (string, error) {
	if c == "" {
		return "NONE", nil
	}
	up := strings.ToUpper(c)
	switch up {
	case "NONE", "ZLIB":
		return up, nil
	default:
		return "", fmt.Errorf("javasidecar: unsupported compression %q (want NONE or ZLIB)", c)
	}
}

// defaultJarPath walks up from the current working directory looking for
// integration-tests/java-interop/target/java-interop-sidecar.jar. The
// search bounds at the Go module root (go.mod under integration-tests/).
func defaultJarPath() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	want := filepath.Join("integration-tests", "java-interop", "target", "java-interop-sidecar.jar")
	dir := wd
	for i := 0; i < 8; i++ {
		candidate := filepath.Join(dir, want)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("javasidecar: could not locate %s starting from %s", want, wd)
}
