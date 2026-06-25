// Tier-1 launcher tests: NO real JVM is spawned. Local-mode startup is
// exercised via a fake commander that returns a canned PORT: line and a
// mini HTTP server that satisfies /health and /encode + /decode.
//
// Container mode is NOT tested here — it needs Docker, which is integration
// tier.

package javasidecar

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCommander replays scripted stdout/stderr and skips spawning a real
// process. It satisfies commander.
type fakeCommander struct {
	mu        sync.Mutex
	calledBin string
	calledArg []string
	stdout    string
	stderr    string
	started   bool
	waitCh    chan struct{}
}

func (f *fakeCommander) Command(_ context.Context, name string, args ...string) commandRun {
	f.mu.Lock()
	f.calledBin = name
	f.calledArg = append([]string(nil), args...)
	f.mu.Unlock()
	return &fakeCommandRun{owner: f}
}

type fakeCommandRun struct {
	owner       *fakeCommander
	stdoutPipe  io.ReadCloser
	stderrPipe  io.ReadCloser
	stdoutWrite *io.PipeWriter
	stderrWrite *io.PipeWriter
}

func (r *fakeCommandRun) StdoutPipe() (io.ReadCloser, error) {
	pr, pw := io.Pipe()
	r.stdoutPipe, r.stdoutWrite = pr, pw
	return pr, nil
}

func (r *fakeCommandRun) StderrPipe() (io.ReadCloser, error) {
	pr, pw := io.Pipe()
	r.stderrPipe, r.stderrWrite = pr, pw
	return pr, nil
}

func (r *fakeCommandRun) Start() error {
	r.owner.mu.Lock()
	r.owner.started = true
	r.owner.waitCh = make(chan struct{})
	stdout := r.owner.stdout
	stderr := r.owner.stderr
	r.owner.mu.Unlock()
	go func() {
		_, _ = io.WriteString(r.stdoutWrite, stdout)
		_, _ = io.WriteString(r.stderrWrite, stderr)
		// Stay open until Wait() is signalled — mirrors a real long-running JVM.
		<-r.owner.waitCh
		_ = r.stdoutWrite.Close()
		_ = r.stderrWrite.Close()
	}()
	return nil
}

func (r *fakeCommandRun) Process() *os.Process { return nil }
func (r *fakeCommandRun) PGroup() bool          { return false }

func (r *fakeCommandRun) Wait() error {
	r.owner.mu.Lock()
	wc := r.owner.waitCh
	r.owner.mu.Unlock()
	if wc != nil {
		select {
		case <-wc:
		default:
			close(wc)
		}
	}
	return nil
}

// minimalSidecarHTTP stands in for the JVM's HttpServer. It speaks /health
// and a trivial /encode + /decode that just round-trips the payload through
// base64, so the launcher's POST helpers can be exercised end-to-end.
func minimalSidecarHTTP(t *testing.T) (port int, shutdown func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/encode", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		// Echo: prepend an 18-byte stub header so the launcher's caller can
		// pretend it got a framed response.
		raw, _ := base64.StdEncoding.DecodeString(body.Payload)
		framed := append(make([]byte, 18), raw...)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"bytes": base64.StdEncoding.EncodeToString(framed),
		})
	})
	mux.HandleFunc("/decode", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Bytes string `json:"bytes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		raw, _ := base64.StdEncoding.DecodeString(body.Bytes)
		// Strip the 18-byte stub header.
		var payload []byte
		if len(raw) >= 18 {
			payload = raw[18:]
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"payload":          base64.StdEncoding.EncodeToString(payload),
			"schemaVersionId":  "00000000-0000-0000-0000-000000000000",
			"schemaName":       "fake-schema",
			"schemaDefinition": "{}",
			"dataFormat":       "AVRO",
		})
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	port = ln.Addr().(*net.TCPAddr).Port
	shutdown = func() {
		_ = srv.Close()
		_ = ln.Close()
	}
	return port, shutdown
}

func TestModeFromEnv(t *testing.T) {
	cases := []struct {
		env     string
		want    Mode
		wantErr bool
	}{
		{"", ModeLocal, false},
		{"local", ModeLocal, false},
		{"LOCAL", ModeLocal, false},
		{"container", ModeContainer, false},
		{"CONTAINER", ModeContainer, false},
		{" local ", ModeLocal, false},
		{"bogus", ModeLocal, true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("env=%q", tc.env), func(t *testing.T) {
			t.Setenv("GSR_INTEROP_MODE", tc.env)
			got, err := ModeFromEnv()
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v err=%v", tc.wantErr, err)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestLocalLauncher_InvokesJavaWithExpectedArgs(t *testing.T) {
	port, shutdown := minimalSidecarHTTP(t)
	defer shutdown()

	tmpJar := writeFakeJar(t)
	fc := &fakeCommander{
		stdout: fmt.Sprintf("PORT: %d\n", port),
	}
	mode := ModeLocal

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sc, err := New(ctx, Options{
		Mode:         &mode,
		JarPath:      tmpJar,
		JavaBinary:   "fake-java",
		StartTimeout: 2 * time.Second,
		commander:    fc,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer sc.Stop(ctx)

	fc.mu.Lock()
	bin := fc.calledBin
	args := append([]string(nil), fc.calledArg...)
	started := fc.started
	fc.mu.Unlock()

	if !started {
		t.Errorf("commander Start was not invoked")
	}
	if bin != "fake-java" {
		t.Errorf("bin = %q, want fake-java", bin)
	}
	wantArgs := []string{"-jar", tmpJar, "--port=0"}
	if len(args) != len(wantArgs) {
		t.Fatalf("args = %v, want %v", args, wantArgs)
	}
	for i := range wantArgs {
		if args[i] != wantArgs[i] {
			t.Errorf("arg[%d] = %q, want %q", i, args[i], wantArgs[i])
		}
	}
	if !strings.HasPrefix(sc.BaseURL(), "http://127.0.0.1:") {
		t.Errorf("baseURL = %q, want loopback URL", sc.BaseURL())
	}
}

func TestLocalLauncher_EncodeDecodeRoundtrip(t *testing.T) {
	port, shutdown := minimalSidecarHTTP(t)
	defer shutdown()

	tmpJar := writeFakeJar(t)
	fc := &fakeCommander{stdout: fmt.Sprintf("PORT: %d\n", port)}
	mode := ModeLocal

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sc, err := New(ctx, Options{
		Mode:         &mode,
		JarPath:      tmpJar,
		StartTimeout: 2 * time.Second,
		commander:    fc,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer sc.Stop(ctx)

	framed, err := sc.Encode(ctx, EncodeRequest{
		Format:          "AVRO",
		Schema:          `{"type":"string"}`,
		SchemaName:      "s",
		SchemaVersionID: "00000000-0000-0000-0000-000000000000",
		Payload:         []byte("hi"),
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(framed) != 20 { // 18 stub header + "hi"
		t.Errorf("framed length = %d, want 20", len(framed))
	}
	resp, err := sc.Decode(ctx, framed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if string(resp.Payload) != "hi" {
		t.Errorf("payload = %q, want %q", resp.Payload, "hi")
	}
	if resp.SchemaName != "fake-schema" {
		t.Errorf("schemaName = %q", resp.SchemaName)
	}
}

func TestLocalLauncher_MissingJarReturnsError(t *testing.T) {
	mode := ModeLocal
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := New(ctx, Options{
		Mode:         &mode,
		JarPath:      "/does/not/exist/sidecar.jar",
		StartTimeout: 1 * time.Second,
		commander:    &fakeCommander{},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "jar not found") {
		t.Errorf("error = %v, want it to mention `jar not found`", err)
	}
}

func TestLocalLauncher_HealthTimeoutSurfaces(t *testing.T) {
	// Stand up an HTTP server that 500s on /health so waitHealthy times out.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	port := srv.Listener.Addr().(*net.TCPAddr).Port
	tmpJar := writeFakeJar(t)
	fc := &fakeCommander{stdout: fmt.Sprintf("PORT: %d\n", port)}
	mode := ModeLocal

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := New(ctx, Options{
		Mode:         &mode,
		JarPath:      tmpJar,
		StartTimeout: 300 * time.Millisecond,
		commander:    fc,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "healthy") {
		t.Errorf("error = %v, want it to mention `healthy`", err)
	}
}

func TestContainerStub_RefusesWithoutIntegrationTag(t *testing.T) {
	// On the non-integration build, startContainerStub is the active
	// implementation. We assert it returns an explicit error rather than
	// silently dropping back to local mode.
	_, err := startContainerStub()
	if err == nil {
		t.Fatal("expected error from container stub")
	}
	if !strings.Contains(err.Error(), "container mode") {
		t.Errorf("stub error = %v, want mention of container mode", err)
	}
}

func writeFakeJar(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/fake-sidecar.jar"
	if err := os.WriteFile(path, []byte("not a real jar"), 0o644); err != nil {
		t.Fatalf("write fake jar: %v", err)
	}
	return path
}
