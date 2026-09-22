// Package health serves the daemon's /healthz and /readyz endpoints. The
// /debug/pools endpoint is registered unconditionally but gated by an
// atomic flag; when disabled it returns 404 so the route appears unregistered
// (per spec §health-endpoints).
//
// Spec: openspec/specs/pool-offchain-fetcher/spec.md §health-endpoints.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// PingFunc checks database reachability. Set via SetPingFn after the DB is up.
type PingFunc func(ctx context.Context) error

// Config holds the constructor-time options.
type Config struct {
	ListenAddr           string
	ListenPort           int
	ReadyzDBGraceSeconds int
	// PingFn may be nil at construction time (e.g., fetch.enabled = false).
	PingFn PingFunc
	// ExposeDebugPools is passed in by the daemon after it has loaded config.
	ExposeDebugPools bool
	// SnapshotDebugPools is set after construction; nil until SetSnapshotDebugPools runs.
	SnapshotDebugPools func() any
}

// Server is the health HTTP server.
type Server struct {
	cfg          Config
	logger       *slog.Logger
	srv          *http.Server
	addr         string
	lastDB       atomic.Int64 // unix nano of last successful DB ping
	pingFn       atomic.Pointer[PingFunc]
	listen       net.Listener
	debugExposed atomic.Bool // mirrors ExposeDebugPools; flipped at runtime by SIGHUP
}

// NewServer constructs a Server but does not start it. Call Start to bind.
func NewServer(cfg Config) *Server {
	s := &Server{
		cfg:    cfg,
		logger: slog.Default().With(slog.String("subsystem", "health")),
	}
	s.debugExposed.Store(cfg.ExposeDebugPools)
	if cfg.PingFn != nil {
		s.SetPingFn(cfg.PingFn)
	}
	return s
}

// SetLogger overrides the logger (useful during early init before slog default).
func (s *Server) SetLogger(l *slog.Logger) { s.logger = l }

// SetPingFn installs the DB ping function. Safe to call before or after Start.
func (s *Server) SetPingFn(fn PingFunc) {
	s.pingFn.Store(&fn)
	// Probe immediately so /readyz is happy ASAP after the DB is up.
	if fn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := fn(ctx); err == nil {
			s.lastDB.Store(time.Now().UnixNano())
		}
	}
}

// SetSnapshotDebugPools injects a snapshot function for /debug/pools. Safe to
// call before or after Start. The handler is registered unconditionally but
// returns 404 when ExposeDebugPools (or the runtime flag flipped by
// SetExposeDebugPools) is false.
func (s *Server) SetSnapshotDebugPools(fn func() any) {
	s.cfg.SnapshotDebugPools = fn
}

// SetExposeDebugPools toggles the /debug/pools endpoint at runtime. Per spec,
// this knob is hot-reloadable on SIGHUP. Returns the previous value so the
// caller can emit a precise log line.
func (s *Server) SetExposeDebugPools(v bool) bool {
	return s.debugExposed.Swap(v)
}

// ExposeDebugPools returns the current exposure flag.
func (s *Server) ExposeDebugPools() bool {
	return s.debugExposed.Load()
}

// Addr returns the actual bound address (useful when port == 0 for tests).
func (s *Server) Addr() string { return s.addr }

// Start binds the listener and serves in a goroutine. Returns the bound addr.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	// Always register /debug/pools; the handler itself returns 404 when the
	// runtime flag is off. This makes hot-reload of expose_debug_endpoint
	// possible without rebinding the listener.
	mux.HandleFunc("/debug/pools", s.handleDebugPools)

	addr := net.JoinHostPort(s.cfg.ListenAddr, fmt.Sprintf("%d", s.cfg.ListenPort))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	s.listen = ln
	s.addr = ln.Addr().String()
	s.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("health server crashed", slog.String("err", err.Error()))
		}
	}()
	return nil
}

// Stop gracefully shuts down within the context's deadline.
func (s *Server) Stop(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

// -----------------------------------------------------------------------------
// Handlers
// -----------------------------------------------------------------------------

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	pingFn := s.pingFn.Load()
	if pingFn == nil || *pingFn == nil {
		// No DB yet (e.g., fetch.enabled = false, or DB never came up). We
		// treat this as "ready but no work" — operators can confirm via
		// /healthz + a process supervisor.
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready_no_db"})
		return
	}
	grace := time.Duration(s.cfg.ReadyzDBGraceSeconds) * time.Second
	last := time.Unix(0, s.lastDB.Load())
	age := time.Since(last)
	if age > grace {
		// Try to ping again — maybe the DB came back.
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := (*pingFn)(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable,
				map[string]any{
					"status":                "degraded",
					"reason":                "db_unreachable",
					"last_ping_age_seconds": int(age.Seconds()),
				})
			return
		}
		s.lastDB.Store(time.Now().UnixNano())
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func (s *Server) handleDebugPools(w http.ResponseWriter, r *http.Request) {
	if !s.debugExposed.Load() {
		http.NotFound(w, r)
		return
	}
	if s.cfg.SnapshotDebugPools == nil {
		writeJSON(w, http.StatusOK, map[string]any{"in_flight": []any{}, "recent_failures": []any{}})
		return
	}
	writeJSON(w, http.StatusOK, s.cfg.SnapshotDebugPools())
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// Best-effort: nothing we can do mid-response.
		_ = err
	}
}

// reserved for future extension
