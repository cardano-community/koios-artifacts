// Package fetcher_test contains integration tests that run the fetcher end
// to end: real Postgres + httptest server serving fixture metadata. Skipped
// when KOIOS_PG_TEST_DSN is unset.
//
//go:build !short

package fetcher_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/blake2b"

	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/config"
	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/fetcher"
	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/ipfilter"
	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/storage"
)

func slogDiscard() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, nil))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// storageFixture bootstraps a fresh isolated schema, applies migrations,
// and inserts the public.pool_hash / public.pool_update rows that the
// fetcher's discovery query needs.
type storageFixture struct {
	t        *testing.T
	dsn      string
	schema   string
	pg       *storage.Pool
	teardown func()

	mux    *http.ServeMux
	server *httptest.Server

	mu       sync.Mutex
	fetches  map[string]int // URL path -> call count
	idOffset int64          // unique base so concurrent test runs don't collide on PKs
}

func newFixture(t *testing.T) *storageFixture {
	t.Helper()
	adminDSN := os.Getenv("KOIOS_PG_TEST_DSN")
	if adminDSN == "" {
		t.Skip("KOIOS_PG_TEST_DSN not set; skipping fetcher integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adminPool, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer adminPool.Close()

	schema := fmt.Sprintf("pmf_test_%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS public.pool_hash (
			id BIGINT PRIMARY KEY,
			view VARCHAR(64)
		)`,
		`CREATE TABLE IF NOT EXISTS public.pool_update (
			id BIGINT PRIMARY KEY,
			hash_id BIGINT,
			registered_tx_id BIGINT,
			cert_index INT,
			meta_id BIGINT,
			active_epoch_no INT,
			vrf_key_hash BYTEA,
			margin DOUBLE PRECISION,
			fixed_cost BIGINT,
			pledge BIGINT,
			deposit BIGINT,
			reward_addr_id BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS public.epoch_param (
			epoch_no INT PRIMARY KEY,
			max_block_height BIGINT,
			max_epoch_slot NUMERIC,
			pool_deposit BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS public.pool_metadata_ref (
			id BIGINT PRIMARY KEY,
			hash BYTEA,
			url VARCHAR(256),
			registered_tx_id BIGINT
		)`,
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='grest_owner') THEN CREATE ROLE grest_owner NOLOGIN; END IF; END $$`,
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='web_anon') THEN CREATE ROLE web_anon NOLOGIN; END IF; END $$`,
	} {
		if _, err := adminPool.Exec(ctx, ddl); err != nil {
			t.Fatalf("DDL bootstrap: %v", err)
		}
	}

	dsnURL := mustParse(t, adminDSN)

	if err := storage.RunMigrations(ctx, dsnURL, schema); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	pg, err := storage.NewPool(ctx, dsnURL, storage.PoolOptions{Schema: schema})
	if err != nil {
		t.Fatalf("pg: %v", err)
	}

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)

	return &storageFixture{
		t:      t,
		dsn:    dsnURL,
		schema: schema,
		pg:     pg,
		teardown: func() {
			server.Close()
			pg.Close()
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer dropCancel()
			_, _ = adminPool.Exec(dropCtx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		},
		mux:      mux,
		server:   server,
		fetches:  map[string]int{},
		idOffset: time.Now().UnixNano() & 0xFFFFFF, // 24-bit base
	}
}

func (f *storageFixture) close() { f.teardown() }

// insertPool registers a pool in public.pool_hash + public.pool_update +
// public.pool_metadata_ref + public.epoch_param (the discovery query reads
// the maximum epoch_no) and returns the pool's id, last_pool_update_id,
// and the metadata URL the server should respond at.
//
// pool IDs are offset by a per-fixture random base so concurrent test
// runs / reruns against the same Postgres don't collide on PRIMARY KEY
// constraints on the (very minimal) public stubs.
func (f *storageFixture) insertPool(id int64, jsonBody []byte) (poolUpdateID int64, metaURL string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := pgxPool(f.t, f.dsn)
	defer pool.Close()

	offset := f.idOffset
	poolHashID := offset + id
	poolUpdateID = offset + id + 1_000_000 // keep pool_update id disjoint from pool_hash id

	if _, err := pool.Exec(ctx,
		`INSERT INTO public.pool_hash (id, view) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		poolHashID, fmt.Sprintf("pool1test%d", id)); err != nil {
		f.t.Fatalf("insert pool_hash: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.pool_update (id, hash_id, registered_tx_id, cert_index, meta_id,
		                              active_epoch_no, vrf_key_hash, margin, fixed_cost, pledge,
		                              deposit, reward_addr_id)
		 VALUES ($1, $2, 1, 0, 1, 0, decode('00','hex'), 0.0, 0, 0, 0, 1)`,
		poolUpdateID, poolHashID); err != nil {
		f.t.Fatalf("insert pool_update: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.epoch_param (epoch_no) VALUES (0) ON CONFLICT DO NOTHING`); err != nil {
		f.t.Fatalf("insert epoch_param: %v", err)
	}

	metaURL = f.server.URL + "/metadata/" + strconv.FormatInt(id, 10)
	f.t.Logf("insertPool(%d): hash_id=%d pu_id=%d URL=%s", id, poolHashID, poolUpdateID, metaURL)
	return poolUpdateID, metaURL
}

func (f *storageFixture) serveMetadata(path string, body []byte, status int) {
	f.mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.fetches[path]++
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
}

func (f *storageFixture) fetchCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetches[path]
}

// TestHappyPath: discovery -> fetch -> upsert. Sets up 3 pools in discovery,
// the test server returns valid JSON for all of them, and the fetcher
// upserts rows for each.
func TestHappyPath(t *testing.T) {
	f := newFixture(t)
	defer f.close()

	// Set up 3 pools with known JSON bodies and known hashes.
	pools := []struct {
		id   int64
		body []byte
		path string
	}{
		{1, []byte(`{"name":"Alpha","ticker":"ALP","description":"first"}`), "/metadata/1"},
		{2, []byte(`{"name":"Beta","ticker":"BET","description":"second"}`), "/metadata/2"},
		{3, []byte(`{"name":"Gamma","ticker":"GAM","description":"third"}`), "/metadata/3"},
	}
	for _, p := range pools {
		f.serveMetadata(p.path, p.body, http.StatusOK)
		puID, _ := f.insertPool(p.id, p.body)
		// Insert pool_metadata_ref row with a unique pmr id.
		ctx := context.Background()
		pool := pgxPool(t, f.dsn)
		pmrID := f.idOffset + p.id
		wantHash := blake2b.Sum256(p.body)
		if _, err := pool.Exec(ctx,
			`INSERT INTO public.pool_metadata_ref (id, hash, url, registered_tx_id)
			 VALUES ($1, $2, $3, 1)`,
			pmrID, wantHash[:], f.server.URL+p.path); err != nil {
			t.Fatalf("insert pmr: %v", err)
		}
		// The pool_update row above sets meta_id=1. Override to the pmr id
		// we just inserted so the discovery query joins correctly.
		if _, err := pool.Exec(ctx,
			`UPDATE public.pool_update SET meta_id = $1 WHERE id = $2`, pmrID, puID); err != nil {
			t.Fatalf("update pool_update meta_id: %v", err)
		}
		pool.Close()
	}

	// Configure fetcher.
	cfg := &config.Config{
		NetworkName: "preview",
		SchemaName:  f.schema,
	}
	cfg.Fetch.Enabled = true
	cfg.Fetch.Poll.PollIntervalSeconds = 60
	cfg.Fetch.Poll.MinPollIntervalSecs = 30
	cfg.Fetch.Poll.FailedPoolRetrySeconds = 30
	cfg.Fetch.WorkQueue.Concurrency = 4
	cfg.Fetch.WorkQueue.BatchSize = 100
	cfg.Fetch.WorkQueue.MaxInflight = 8
	cfg.Fetch.WorkQueue.WriteBatchSize = 10
	cfg.Fetch.HTTP.TimeoutSeconds = 5
	cfg.Fetch.HTTP.ConnectTimeoutSeconds = 2
	cfg.Fetch.HTTP.MaxRedirects = 3
	cfg.Fetch.HTTP.UserAgent = "koios-pool-meta-fetcher/test"
	cfg.Fetch.HTTP.SkipTLSVerify = true
	cfg.Fetch.HTTP.TLSMinVersion = "1.2"
	cfg.Fetch.HTTP.HTTP2 = false
	cfg.Fetch.Validation.MinBodyBytes = 16
	cfg.Fetch.Validation.MaxBodyBytes = 65536
	cfg.Fetch.Validation.AllowedContentTypes = []string{"application/json", "text/plain"}
	cfg.Fetch.Validation.AllowTextHTMLWithBrace = true
	cfg.Fetch.Validation.JSON.RequiredFields = []string{"name", "ticker"}
	cfg.Fetch.Validation.JSON.MaxDepth = 4
	cfg.Fetch.Validation.JSON.MaxStringLength = 4096
	cfg.Fetch.Validation.JSON.MaxKeysPerObject = 32
	cfg.Fetch.Validation.JSON.MaxArrayLength = 64
	cfg.Fetch.Validation.Hash.Algorithm = "blake2b-256"
	cfg.Fetch.Validation.Hash.LogMismatches = true
	cfg.Fetch.PrivateIPPolicy = "block"
	cfg.Fetch.Retry.MaxAttempts = 3
	cfg.Fetch.PoolsOverrides = []config.PoolOverride{}

	// The fetcher dials out — for the loopback httptest server we need to
	// bypass the IP-filter deny-list (RFC1918 covers 127.0.0.1).
	filter := ipfilter.New(ipfilter.PolicyAllowAll, nil, slogDiscard())
	httpClient := &http.Client{
		Timeout:   cfg.HTTPTimeout(),
		Transport: filter.WrapTransport(&http.Transport{}),
	}
	pf := fetcher.NewPoolFetcher(httpClient, fetcher.ValidationConfig{
		MinBodyBytes:           cfg.Fetch.Validation.MinBodyBytes,
		MaxBodyBytes:           cfg.Fetch.Validation.MaxBodyBytes,
		AllowedContentTypes:    cfg.Fetch.Validation.AllowedContentTypes,
		AllowTextHTMLWithBrace: cfg.Fetch.Validation.AllowTextHTMLWithBrace,
		JSON: fetcher.JSONValidationConfig{
			RequiredFields:        cfg.Fetch.Validation.JSON.RequiredFields,
			MaxDepth:              cfg.Fetch.Validation.JSON.MaxDepth,
			MaxStringLength:       cfg.Fetch.Validation.JSON.MaxStringLength,
			MaxKeysPerObject:      cfg.Fetch.Validation.JSON.MaxKeysPerObject,
			MaxArrayLength:        cfg.Fetch.Validation.JSON.MaxArrayLength,
			AllowAdditionalFields: true,
		},
		Hash: fetcher.HashValidationConfig{
			Algorithm:     string(cfg.Fetch.Validation.Hash.Algorithm),
			LogMismatches: cfg.Fetch.Validation.Hash.LogMismatches,
		},
	}, cfg.Fetch.Retry.MaxAttempts, slogDiscard())
	pf.SetUserAgent(cfg.Fetch.HTTP.UserAgent)

	pf.Errors = f.pg

	fet := fetcher.New(cfg, f.pg, pf, slogDiscard())

	// One cycle.
	runCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	done := make(chan struct{})
	go func() { fet.Run(runCtx); close(done) }()
	time.Sleep(25 * time.Second)
	cancel()
	<-done

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer queryCancel()

	// Verify: 3 rows in pool_offchain_metadata.
	var count int
	if err := f.pg.QueryRow(queryCtx,
		fmt.Sprintf("SELECT count(*) FROM %s.pool_offchain_metadata", f.schema)).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 3 {
		t.Errorf("upserted %d rows, want 3", count)
	}

	// Verify: each pool was fetched exactly once.
	for _, p := range pools {
		if got := f.fetchCount(p.path); got < 1 {
			t.Errorf("pool %d fetched %d times, want >=1", p.id, got)
		}
	}

	// Verify: all three pools are is_valid=true with correct hashes.
	for _, p := range pools {
		var (
			id    int64
			hash  []byte
			valid bool
		)
		err := f.pg.QueryRow(queryCtx,
			fmt.Sprintf("SELECT pool_id, meta_hash, is_valid FROM %s.pool_offchain_metadata WHERE meta_url = $1", f.schema),
			f.server.URL+p.path).Scan(&id, &hash, &valid)
		if err != nil {
			t.Errorf("read row for pool %d: %v", p.id, err)
			continue
		}
		if !valid {
			t.Errorf("pool %d is_valid = false", p.id)
		}
		wantHash := blake2b.Sum256(p.body)
		if string(hash) != string(wantHash[:]) {
			t.Errorf("pool %d hash mismatch: got %x want %x", p.id, hash, wantHash[:])
		}
	}
}

// TestHashMismatchPersists: the fetcher fetches a payload whose hash differs
// from the on-chain declared hash. Per spec, hash mismatch is info-only — the
// payload IS upserted (with the actual hash) and the mismatch is logged, but
// no fetch_error row is written.
func TestHashMismatchPersists(t *testing.T) {
	f := newFixture(t)
	defer f.close()

	body := []byte(`{"name":"MismatchCo","ticker":"MMT"}`)
	f.serveMetadata("/metadata/1", body, http.StatusOK)
	puID, _ := f.insertPool(1, body)
	ctx := context.Background()
	pool := pgxPool(t, f.dsn)
	defer pool.Close()
	pmrID := f.idOffset + 1
	// Insert a pmr with a DELIBERATELY WRONG hash so the validator records a mismatch.
	wrongArr := blake2b.Sum256([]byte(`{"name":"different"}`))
	wrong := wrongArr[:]
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.pool_metadata_ref (id, hash, url, registered_tx_id)
		 VALUES ($1, $2, $3, 1)`,
		pmrID, wrong, f.server.URL+"/metadata/1"); err != nil {
		t.Fatalf("pmr: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE public.pool_update SET meta_id = $1 WHERE id = $2`, pmrID, puID); err != nil {
		t.Fatalf("update pool_update meta_id: %v", err)
	}

	cfg := newTestConfig(f.schema)
	filter := ipfilter.New(ipfilter.PolicyAllowAll, nil, slogDiscard())
	httpClient := &http.Client{
		Timeout:   cfg.HTTPTimeout(),
		Transport: filter.WrapTransport(&http.Transport{}),
	}
	pf := fetcher.NewPoolFetcher(httpClient, fetcherToValidationConfig(cfg), cfg.Fetch.Retry.MaxAttempts, slogDiscard())
	pf.SetUserAgent(cfg.Fetch.HTTP.UserAgent)
	pf.Errors = f.pg
	fet := fetcher.New(cfg, f.pg, pf, slogDiscard())

	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	done := make(chan struct{})
	go func() { fet.Run(runCtx); close(done) }()
	time.Sleep(25 * time.Second)
	cancel()
	<-done

	// Use a fresh context for verification (runCtx is cancelled).
	queryCtx, queryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer queryCancel()

	// Per spec (pool-offchain-fetcher §hash-capture-informational): hash mismatch
	// is informational. The row in pool_offchain_metadata IS upserted with
	// is_valid=true, but NO row is written to pool_offchain_fetch_error.
	var metaCount int
	if err := f.pg.QueryRow(queryCtx,
		fmt.Sprintf("SELECT count(*) FROM %s.pool_offchain_metadata", f.schema)).Scan(&metaCount); err != nil {
		t.Fatalf("count pool_offchain_metadata: %v", err)
	}
	if metaCount != 1 {
		t.Errorf("expected 1 row in pool_offchain_metadata, got %d", metaCount)
	}
	var fetchErrorCount int
	if err := pool.QueryRow(queryCtx,
		fmt.Sprintf("SELECT count(*) FROM %s.pool_offchain_fetch_error", f.schema)).Scan(&fetchErrorCount); err != nil {
		t.Fatalf("count fetch_errors: %v", err)
	}
	if fetchErrorCount != 0 {
		t.Errorf("expected 0 fetch_error rows for info-only hash mismatch, got %d", fetchErrorCount)
	}
	// And the persisted hash must be the computed (actual) one, not the declared one.
	var gotHash []byte
	if err := f.pg.QueryRow(queryCtx,
		fmt.Sprintf("SELECT meta_hash FROM %s.pool_offchain_metadata", f.schema)).Scan(&gotHash); err != nil {
		t.Fatalf("read meta_hash: %v", err)
	}
	wantHash := blake2b.Sum256(body)
	if string(gotHash) != string(wantHash[:]) {
		t.Errorf("persisted hash = %x, want actual blake2b-256 %x", gotHash, wantHash[:])
	}
}

// Test404PersistsAsFetchError: a 404 should be persisted as an
// http_status fetch_error, not trigger exponential backoff (because
// no row exists for the pool yet).
func Test404PersistsAsFetchError(t *testing.T) {
	f := newFixture(t)
	defer f.close()

	body := []byte(`{"name":"404Co","ticker":"NF4"}`)
	f.mux.HandleFunc("/metadata/1", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.fetches["/metadata/1"]++
		f.mu.Unlock()
		http.Error(w, "not found", http.StatusNotFound)
	})
	puID, _ := f.insertPool(1, body)
	ctx := context.Background()
	pool := pgxPool(t, f.dsn)
	defer pool.Close()
	pmrID := f.idOffset + 1
	hashArr := blake2b.Sum256(body)
	hash := hashArr[:]
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.pool_metadata_ref (id, hash, url, registered_tx_id)
		 VALUES ($1, $2, $3, 1)`,
		pmrID, hash, f.server.URL+"/metadata/1"); err != nil {
		t.Fatalf("pmr: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE public.pool_update SET meta_id = $1 WHERE id = $2`, pmrID, puID); err != nil {
		t.Fatalf("update pool_update meta_id: %v", err)
	}

	cfg := newTestConfig(f.schema)
	filter := ipfilter.New(ipfilter.PolicyAllowAll, nil, slogDiscard())
	httpClient := &http.Client{
		Timeout:   cfg.HTTPTimeout(),
		Transport: filter.WrapTransport(&http.Transport{}),
	}
	pf := fetcher.NewPoolFetcher(httpClient, fetcherToValidationConfig(cfg), cfg.Fetch.Retry.MaxAttempts, slogDiscard())
	pf.SetUserAgent(cfg.Fetch.HTTP.UserAgent)
	pf.Errors = f.pg
	fet := fetcher.New(cfg, f.pg, pf, slogDiscard())

	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	done := make(chan struct{})
	go func() { fet.Run(runCtx); close(done) }()
	time.Sleep(25 * time.Second)
	cancel()
	<-done

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer queryCancel()

	// 1 fetch_error row with http_status_code=404.
	var n int
	err := pool.QueryRow(queryCtx,
		fmt.Sprintf("SELECT count(*) FROM %s.pool_offchain_fetch_error WHERE http_status_code = 404", f.schema)).Scan(&n)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n < 1 {
		t.Errorf("expected >=1 fetch_error rows with status 404, got %d", n)
	}
}

// helpers ---

func newTestConfig(schema string) *config.Config {
	cfg := &config.Config{
		NetworkName: "preview",
		SchemaName:  schema,
	}
	cfg.Fetch.Enabled = true
	cfg.Fetch.Poll.PollIntervalSeconds = 60
	cfg.Fetch.Poll.MinPollIntervalSecs = 30
	cfg.Fetch.Poll.FailedPoolRetrySeconds = 30
	cfg.Fetch.WorkQueue.Concurrency = 4
	cfg.Fetch.WorkQueue.BatchSize = 100
	cfg.Fetch.WorkQueue.MaxInflight = 8
	cfg.Fetch.WorkQueue.WriteBatchSize = 10
	cfg.Fetch.HTTP.TimeoutSeconds = 5
	cfg.Fetch.HTTP.ConnectTimeoutSeconds = 2
	cfg.Fetch.HTTP.MaxRedirects = 3
	cfg.Fetch.HTTP.UserAgent = "koios-pool-meta-fetcher/test"
	cfg.Fetch.HTTP.SkipTLSVerify = true
	cfg.Fetch.HTTP.TLSMinVersion = "1.2"
	cfg.Fetch.HTTP.HTTP2 = false
	cfg.Fetch.Validation.MinBodyBytes = 16
	cfg.Fetch.Validation.MaxBodyBytes = 65536
	cfg.Fetch.Validation.AllowedContentTypes = []string{"application/json", "text/plain"}
	cfg.Fetch.Validation.AllowTextHTMLWithBrace = true
	cfg.Fetch.Validation.JSON.RequiredFields = []string{"name", "ticker"}
	cfg.Fetch.Validation.JSON.MaxDepth = 4
	cfg.Fetch.Validation.JSON.MaxStringLength = 4096
	cfg.Fetch.Validation.JSON.MaxKeysPerObject = 32
	cfg.Fetch.Validation.JSON.MaxArrayLength = 64
	cfg.Fetch.Validation.Hash.Algorithm = "blake2b-256"
	cfg.Fetch.Validation.Hash.LogMismatches = true
	cfg.Fetch.PrivateIPPolicy = "block"
	cfg.Fetch.Retry.MaxAttempts = 3
	cfg.Fetch.PoolsOverrides = []config.PoolOverride{}
	return cfg
}

func fetcherToValidationConfig(cfg *config.Config) fetcher.ValidationConfig {
	return fetcher.ValidationConfig{
		MinBodyBytes:           cfg.Fetch.Validation.MinBodyBytes,
		MaxBodyBytes:           cfg.Fetch.Validation.MaxBodyBytes,
		AllowedContentTypes:    cfg.Fetch.Validation.AllowedContentTypes,
		AllowTextHTMLWithBrace: cfg.Fetch.Validation.AllowTextHTMLWithBrace,
		JSON: fetcher.JSONValidationConfig{
			RequiredFields:        cfg.Fetch.Validation.JSON.RequiredFields,
			MaxDepth:              cfg.Fetch.Validation.JSON.MaxDepth,
			MaxStringLength:       cfg.Fetch.Validation.JSON.MaxStringLength,
			MaxKeysPerObject:      cfg.Fetch.Validation.JSON.MaxKeysPerObject,
			MaxArrayLength:        cfg.Fetch.Validation.JSON.MaxArrayLength,
			AllowAdditionalFields: true,
		},
		Hash: fetcher.HashValidationConfig{
			Algorithm:     string(cfg.Fetch.Validation.Hash.Algorithm),
			LogMismatches: cfg.Fetch.Validation.Hash.LogMismatches,
		},
	}
}

func pgxPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	return pool
}

func mustParse(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	return u.String()
}
