// Package fetcher (pool.go) implements per-pool HTTP fetching + validation
// + in-memory retry. One PoolFetcher represents the work done for a single pool
// in a single cycle.
package fetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/ipfilter"
	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/storage"
)

// WorkItem is the canonical input to a fetch attempt. It is the same as
// storage.WorkItem so we don't import-cycle. We mirror it here to keep the
// fetcher package self-contained for testing.
type WorkItem struct {
	PoolID         int64
	PoolUpdateID   int64
	URL            string
	OnChainHash    []byte
	Bech32         string // public.pool_hash.view; used by per-pool IP-filter exemption
	LastPolled     *time.Time
	LastPoolUpdate *int64
}

// Result is the outcome of one fetch attempt.
type Result struct {
	PoolID       int64
	PoolUpdateID int64
	URL          string

	Validated *ValidationResult // nil on validation failure
	Err       error             // non-nil on HTTP / validate failure; never both non-nil

	HTTPStatusCode int
	ContentType    string
	BytesRead      int
	Duration       time.Duration
}

// Succeeded returns true when the fetch + validation succeeded.
func (r *Result) Succeeded() bool { return r.Validated != nil && r.Err == nil }

// PoolFetcher executes one WorkItem (or many, in series) and tracks per-pool
// retry counters.
type PoolFetcher struct {
	httpClient  *http.Client
	cfg         ValidationConfig
	logger      *slog.Logger
	ua          string
	maxAttempts uint // mirrors config.Config.Fetch.Retry.MaxAttempts

	mu       sync.Mutex
	retries  map[int64]uint // pool_id -> consecutive failure count
	mismatch map[int64]bool // pool_id -> hash_mismatch already logged this process lifetime

	// Errors is an optional sink for failed fetches. When set, every failed
	// attempt is persisted to grest.pool_offchain_fetch_error so operators
	// can answer "why is this pool missing?" without scraping logs.
	Errors ErrorSink
}

// ErrorSink persists a single failed fetch. Implemented by storage.Pool.
type ErrorSink interface {
	InsertFetchError(ctx context.Context, e storage.FetchError, bodyPreviewBytes int) error
}

// SetUserAgent overrides the default "koios-pool-meta-fetcher/dev" User-Agent.
func (p *PoolFetcher) SetUserAgent(s string) {
	if s != "" {
		p.ua = s
	}
}

// NewPoolFetcher constructs a fetcher with the given HTTP client, validation
// configuration, and the per-pool retry cap (mirrors fetch.retry.max_attempts).
func NewPoolFetcher(client *http.Client, cfg ValidationConfig, maxAttempts int, logger *slog.Logger) *PoolFetcher {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return &PoolFetcher{
		httpClient:  client,
		cfg:         cfg,
		logger:      logger.With(slog.String("subsystem", "fetcher.pool")),
		ua:          "koios-pool-meta-fetcher/dev",
		retries:     map[int64]uint{},
		mismatch:    map[int64]bool{},
		maxAttempts: uint(maxAttempts),
	}
}

// RetryCount returns the current consecutive-failure count for the pool.
func (p *PoolFetcher) RetryCount(poolID int64) uint {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.retries[poolID]
}

// RetrySnapshot is one entry in the per-pool retry state surfaced via
// /debug/pools. next_delay is computed from the configured backoff curve so
// operators see the wall-clock delay before the pool is retried again.
type RetrySnapshot struct {
	PoolID    int64
	Attempts  uint
	NextDelay time.Duration
}

// RetrySnapshot returns a copy of every pool currently above 0 consecutive
// failures, paired with its next-backoff delay. The base interval comes from
// `cfg.Fetch.Poll.FailedPoolRetrySeconds` and the curve is
// `delay = base * 2^attempts`, matching the SQL the daemon writes into
// `grest.pool_offchain_metadata.next_attempt_at` on each failure.
func (p *PoolFetcher) RetrySnapshot(baseSeconds int) []RetrySnapshot {
	if baseSeconds < 1 {
		baseSeconds = 60
	}
	p.mu.Lock()
	ids := make([]int64, 0, len(p.retries))
	for id, n := range p.retries {
		if n > 0 {
			ids = append(ids, id)
		}
	}
	p.mu.Unlock()
	out := make([]RetrySnapshot, 0, len(ids))
	for _, id := range ids {
		n := p.RetryCount(id)
		out = append(out, RetrySnapshot{
			PoolID:    id,
			Attempts:  n,
			NextDelay: retryDelay(baseSeconds, int(n)),
		})
	}
	return out
}

// retryDelay returns the in-memory mirror of the DB's
// `base * pow(2, LEAST(consecutive_failures, 10))` next-attempt calculation,
// where `consecutive_failures` is read pre-increment in the same SQL UPDATE.
// Equivalent to: `base * 2^min(N-1, 10)` for the post-increment count N.
// The exponent caps at 10, giving a maximum delay of `base * 1024`
// (≈ 17 h at the default 60 s base).
func retryDelay(baseSeconds int, consecutiveFailures int) time.Duration {
	if baseSeconds < 1 {
		baseSeconds = 60
	}
	if consecutiveFailures < 1 {
		consecutiveFailures = 1
	}
	// Clamp N so that (N-1) <= 10, matching the SQL LEAST(consecutive_failures, 10)
	// reading the pre-increment value.
	if consecutiveFailures > 11 {
		consecutiveFailures = 11
	}
	return time.Duration(baseSeconds) * time.Second * time.Duration(1<<uint(consecutiveFailures-1))
}

// MaxAttempts returns the configured per-pool retry cap.
func (p *PoolFetcher) MaxAttempts() uint { return p.maxAttempts }

// ShouldSkip returns true when the pool has been retried cfg.MaxAttempts times
// without success and should be excluded this cycle.
func (p *PoolFetcher) ShouldSkip(w WorkItem) bool {
	return p.RetryCount(w.PoolID) >= p.MaxAttempts()
}

// Fetch performs one HTTP GET, applies the validation pipeline, records the
// outcome, and returns a *Result. Context is honoured for cancellation.
//
// The HTTP client MUST already be configured with the IP filter (see
// internal/ipfilter.WrapTransport).
func (p *PoolFetcher) Fetch(ctx context.Context, w WorkItem) *Result {
	start := time.Now()

	// Attach per-pool context for IP-filter exemption check.
	ctx = context.WithValue(ctx, ipfilter.PoolIDKey{}, w.bech32())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.URL, nil)
	if err != nil {
		r := &Result{Err: fmt.Errorf("invalid url: %w", err), Duration: time.Since(start)}
		p.persistError(ctx, w, r, nil, "invalid_url", err.Error())
		return p.record(w, r)
	}
	req.Header.Set("User-Agent", p.UserAgent())
	req.Header.Set("Accept", "application/json, application/ld+json, text/plain;q=0.9, */*;q=0.1")

	httpResult := &Result{
		PoolID:       w.PoolID,
		PoolUpdateID: w.PoolUpdateID,
		URL:          w.URL,
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		category, classified := classifyTransport(err)
		httpResult.Err = classified
		httpResult.Duration = time.Since(start)
		p.persistError(ctx, w, httpResult, nil, category, err.Error())
		return p.record(w, httpResult)
	}
	defer resp.Body.Close()

	httpResult.HTTPStatusCode = resp.StatusCode
	httpResult.ContentType = resp.Header.Get("Content-Type")

	// Read body up to max+1 to detect overflow.
	maxBytes := p.cfg.MaxBodyBytes
	limited := io.LimitReader(resp.Body, int64(maxBytes)+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		httpResult.Err = fmt.Errorf("read body: %w", err)
		httpResult.Duration = time.Since(start)
		p.persistError(ctx, w, httpResult, nil, "read_body", err.Error())
		return p.record(w, httpResult)
	}
	httpResult.BytesRead = len(body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		httpResult.Err = fmt.Errorf("status %d", resp.StatusCode)
		httpResult.Duration = time.Since(start)
		p.persistError(ctx, w, httpResult, body, "http_status", fmt.Sprintf("HTTP %d %s", resp.StatusCode, resp.Status))
		return p.record(w, httpResult)
	}

	validated, verr := Validate(body, httpResult.ContentType, w.OnChainHash, p.cfg)
	if verr != nil {
		httpResult.Err = verr
		httpResult.Duration = time.Since(start)
		p.persistError(ctx, w, httpResult, body, classifyValidatorErr(verr), verr.Error())
		return p.record(w, httpResult)
	}

	httpResult.Validated = validated
	httpResult.Duration = time.Since(start)

	// Informational: log hash mismatch once per cycle. We can do it here.
	if !validated.HashMatched && p.shouldLogMismatch(w) {
		p.logger.Info("hash_mismatch",
			slog.Int64("pool_id", w.PoolID),
			slog.String("url", w.URL),
			slog.String("declared_hash", fmt.Sprintf("%x", w.OnChainHash)),
			slog.String("actual_hash", fmt.Sprintf("%x", validated.MetaHash)),
		)
		// Don't persist hash_mismatch — per spec it's informational, not a
		// failure. Operators consult the live row instead.
	}

	return p.record(w, httpResult)
}

// persistError writes a row to <schema>.pool_offchain_fetch_error when the
// Errors sink is set. Failures to persist (DB unreachable mid-cycle) are
// logged at warn but do not affect the fetch result.
func (p *PoolFetcher) persistError(ctx context.Context, w WorkItem, r *Result, body []byte, class string, msg string) {
	if p.Errors == nil {
		return
	}
	e := storage.FetchError{
		PoolID:         w.PoolID,
		PMRID:          w.PoolUpdateID,
		MetaURL:        w.URL,
		HTTPStatusCode: r.HTTPStatusCode,
		ErrorClass:     class,
		ErrorMessage:   truncate(msg, 1024),
		ResponseBody:   body,
	}
	if e.ErrorMessage == "" && r.Err != nil {
		e.ErrorMessage = truncate(r.Err.Error(), 1024)
	}
	// Best-effort; don't kill the cycle over a write error. 4 KiB preview
	// is enough to diagnose 99% of "weird JSON / HTML" cases.
	if err := p.Errors.InsertFetchError(ctx, e, 4096); err != nil {
		p.logger.Warn("persist_fetch_error_failed",
			slog.Int64("pool_id", w.PoolID),
			slog.String("err", err.Error()),
		)
	}
}

// classifyTransport returns the (category, wrapped-error) pair for a
// transport error from client.Do. The category is suitable for the
// error_class column; the wrapped error has the prefix attached so logs
// are grep-friendly.
func classifyTransport(err error) (string, error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "context deadline exceeded"):
		return "timeout", fmt.Errorf("timeout: %w", err)
	case strings.Contains(msg, "no such host"):
		return "dns_failure", fmt.Errorf("dns_failure: %w", err)
	case strings.Contains(msg, "connection refused"):
		return "connection_refused", fmt.Errorf("connection_refused: %w", err)
	case strings.Contains(msg, "connection reset"):
		return "connection_reset", fmt.Errorf("connection_reset: %w", err)
	case strings.Contains(msg, "tls: "):
		return "tls_failure", fmt.Errorf("tls_failure: %w", err)
	case strings.Contains(msg, "ip_blocked"):
		return "ip_blocked", fmt.Errorf("ip_blocked: %w", err)
	default:
		return "transport_other", fmt.Errorf("transport_other: %w", err)
	}
}

func classifyValidatorErr(err error) string {
	if err == nil {
		return ""
	}
	var ve *ValidationError
	if errors.As(err, &ve) {
		switch ve.Field {
		case "size":
			return "size_violation"
		case "content_type":
			return "content_type_rejected"
		case "json":
			return "json_validation"
		default:
			return "validator_" + ve.Field
		}
	}
	return "validator_other"
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...[truncated]"
}

// record centralises retry-counter bookkeeping. On success the counter resets;
// on failure it increments up to MaxAttempts.
func (p *PoolFetcher) record(w WorkItem, r *Result) *Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r.Succeeded() {
		delete(p.retries, w.PoolID)
	} else {
		p.retries[w.PoolID]++
	}
	return r
}

// shouldLogMismatch returns true when a hash_mismatch log should be emitted
// for the pool. Deduplicated in-memory: at most once per pool until the daemon
// restarts. This avoids log spam for pools with mismatched hashes.
func (p *PoolFetcher) shouldLogMismatch(w WorkItem) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mismatch[w.PoolID] {
		return false
	}
	p.mismatch[w.PoolID] = true
	return true
}

// UserAgent returns the configured User-Agent string.
func (p *PoolFetcher) UserAgent() string {
	if p.ua == "" {
		return "koios-pool-meta-fetcher/dev"
	}
	return p.ua
}

// bech32 returns the pool's bech32 ID for ipfilter context. Surfaced as a
// method so the IP-filter DialContext can be populated uniformly regardless
// of which WorkItem implementation is in use.
func (w WorkItem) bech32() string { return w.Bech32 }

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

// ErrValidation is exported for external matchers that prefer errors.As.
var ErrValidation = errors.New("validation failed")
