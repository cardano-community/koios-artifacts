package health

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestHealthzAlwaysOK verifies that /healthz returns 200 once the server
// is up, regardless of DB state. The daemon should remain "alive" even if
// the DB is unreachable — that's what /readyz is for.
func TestHealthzAlwaysOK(t *testing.T) {
	s := NewServer(Config{
		ListenAddr:           "127.0.0.1",
		ListenPort:           0, // unused; we hit via Addr
		ReadyzDBGraceSeconds: 30,
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop(context.Background())

	resp, err := http.Get("http://" + s.Addr() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "json") {
		t.Errorf("/healthz Content-Type = %q, want JSON", got)
	}
}

// TestReadyzNoPingFn: without a SetPingFn call the server treats the DB
// as "unknown" and returns 200 with status "ready_no_db".
func TestReadyzNoPingFn(t *testing.T) {
	s := NewServer(Config{
		ListenAddr:           "127.0.0.1",
		ListenPort:           0,
		ReadyzDBGraceSeconds: 30,
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop(context.Background())

	resp, err := http.Get("http://" + s.Addr() + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/readyz status = %d, want 200", resp.StatusCode)
	}
	body := readAll(t, resp)
	if !strings.Contains(body, "ready_no_db") {
		t.Errorf("/readyz body = %q, want substring ready_no_db", body)
	}
}

// TestReadyzHealthyPing: after a successful SetPingFn ping, /readyz reports
// 200 with status "ready".
func TestReadyzHealthyPing(t *testing.T) {
	s := NewServer(Config{
		ListenAddr:           "127.0.0.1",
		ListenPort:           0,
		ReadyzDBGraceSeconds: 30,
	})
	s.SetPingFn(func(ctx context.Context) error { return nil })
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop(context.Background())

	resp, err := http.Get("http://" + s.Addr() + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/readyz status = %d, want 200", resp.StatusCode)
	}
	body := readAll(t, resp)
	if !strings.Contains(body, `"status":"ready"`) {
		t.Errorf("/readyz body = %q, want substring status:ready", body)
	}
}

// TestReadyzStalePing: SetPingFn succeeds on the initial SetPingFn call
// (so the first /readyz probe returns 200). After the grace period elapses,
// a second probe triggers an in-handler re-ping — which the test makes
// fail — so the response is 503 with body explaining the degradation.
func TestReadyzStalePing(t *testing.T) {
	var failRePing atomic.Bool
	s := NewServer(Config{
		ListenAddr:           "127.0.0.1",
		ListenPort:           0,
		ReadyzDBGraceSeconds: 1,
	})
	s.SetPingFn(func(ctx context.Context) error {
		if failRePing.Load() {
			return errDB
		}
		return nil
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop(context.Background())

	// First call: ping returns nil, lastDB is fresh, 200.
	resp, _ := http.Get("http://" + s.Addr() + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("first /readyz status = %d, want 200", resp.StatusCode)
	}

	// Now flip the failure flag and wait past the grace period.
	failRePing.Store(true)
	time.Sleep(2 * time.Second)

	resp2, _ := http.Get("http://" + s.Addr() + "/readyz")
	if resp2.StatusCode != http.StatusServiceUnavailable {
		resp2.Body.Close()
		t.Errorf("after grace: /readyz status = %d, want 503", resp2.StatusCode)
	}
	body := readAll(t, resp2)
	if !strings.Contains(body, "db_unreachable") {
		t.Errorf("/readyz degraded body = %q, want substring db_unreachable", body)
	}
}

// TestReadyzWithPingFunc that fails: SetPingFn returns error → DB
// marked unreachable → 503 immediately.
func TestReadyzFailingPingFunc(t *testing.T) {
	s := NewServer(Config{
		ListenAddr:           "127.0.0.1",
		ListenPort:           0,
		ReadyzDBGraceSeconds: 60,
	})
	s.SetPingFn(func(ctx context.Context) error { return errDB })
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop(context.Background())

	resp, _ := http.Get("http://" + s.Addr() + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("/readyz with failing ping status = %d, want 503", resp.StatusCode)
	}
}

// TestDebugPoolsBehindFlag: /debug/pools is only registered when
// ExposeDebugPools=true. When false, requests get 404 (not 200).
func TestDebugPoolsBehindFlag(t *testing.T) {
	for _, expose := range []bool{false, true} {
		s := NewServer(Config{
			ListenAddr:           "127.0.0.1",
			ListenPort:           0,
			ReadyzDBGraceSeconds: 30,
			ExposeDebugPools:     expose,
		})
		if err := s.Start(); err != nil {
			t.Fatalf("Start (expose=%v): %v", expose, err)
		}
		// Inject a snapshot via SetSnapshotDebugPools regardless of expose
		// (the handler just may not be registered).
		s.SetSnapshotDebugPools(func() any { return map[string]any{"ok": true} })

		resp, err := http.Get("http://" + s.Addr() + "/debug/pools")
		if err != nil {
			t.Fatalf("GET /debug/pools (expose=%v): %v", expose, err)
		}
		resp.Body.Close()
		if expose && resp.StatusCode != http.StatusOK {
			t.Errorf("expose=true: status = %d, want 200", resp.StatusCode)
		}
		if !expose && resp.StatusCode != http.StatusNotFound {
			t.Errorf("expose=false: status = %d, want 404 (route must not be registered)", resp.StatusCode)
		}
		s.Stop(context.Background())
	}
}

// TestDebugPoolsRuntimeToggle verifies that SetExposeDebugPools flips the
// /debug/pools visibility at runtime, without a listener rebind. This is
// the contract SIGHUP depends on for fetch.work_queue.expose_debug_endpoint.
func TestDebugPoolsRuntimeToggle(t *testing.T) {
	s := NewServer(Config{
		ListenAddr:           "127.0.0.1",
		ListenPort:           0,
		ReadyzDBGraceSeconds: 30,
		ExposeDebugPools:     true,
	})
	s.SetSnapshotDebugPools(func() any { return map[string]any{"ok": true} })
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop(context.Background())

	// Initially exposed.
	resp, _ := http.Get("http://" + s.Addr() + "/debug/pools")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initial expose=true: status = %d, want 200", resp.StatusCode)
	}

	// Flip off at runtime.
	if prev := s.SetExposeDebugPools(false); !prev {
		t.Errorf("SetExposeDebugPools(false) returned prev=%v, want true", prev)
	}
	resp2, _ := http.Get("http://" + s.Addr() + "/debug/pools")
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("after SetExposeDebugPools(false): status = %d, want 404", resp2.StatusCode)
	}

	// Flip back on.
	if prev := s.SetExposeDebugPools(true); prev {
		t.Errorf("SetExposeDebugPools(true) returned prev=%v, want false", prev)
	}
	resp3, _ := http.Get("http://" + s.Addr() + "/debug/pools")
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("after SetExposeDebugPools(true): status = %d, want 200", resp3.StatusCode)
	}
}

// errDB is a sentinel used in TestReadyzFailingPingFunc.
type dbErr string

func (e dbErr) Error() string { return string(e) }

var errDB = dbErr("simulated db error")

// readAll drains and closes resp.Body. Safe to call repeatedly.
func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var sb strings.Builder
	buf := make([]byte, 1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
	return sb.String()
}

// guard against an unused import on sync/atomic in case future tests need it.
var _ = atomic.Bool{}
