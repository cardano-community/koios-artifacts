// Package storage_test contains integration tests that exercise the storage
// layer against a real PostgreSQL instance. Skipped when KOIOS_PG_TEST_DSN
// is unset (e.g. in -short mode or when CI doesn't have a DB).
//
//go:build !short

package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/blake2b"
)

// dsn returns the test DSN from KOIOS_PG_TEST_DSN, or skips the test.
func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("KOIOS_PG_TEST_DSN")
	if v == "" {
		t.Skip("KOIOS_PG_TEST_DSN not set; skipping storage integration test")
	}
	return v
}

// isolatedSchema creates a throwaway schema (named with a random suffix)
// so concurrent test runs don't collide. It returns the schema name and a
// teardown func that drops it. The schema is dropped before the test
// returns so cleanup is automatic.
//
// Public.pool_hash and public.pool_update are stubbed as empty tables so
// the migration's foreign-key constraints can resolve. In production these
// are populated by cardano-db-sync.
func isolatedSchema(t *testing.T, adminDSN string) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer pool.Close()

	// Stub the public FK targets if missing (idempotent).
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
		// Roles referenced by the migration's GRANTs.
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='grest_owner') THEN CREATE ROLE grest_owner NOLOGIN; END IF; END $$`,
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='web_anon') THEN CREATE ROLE web_anon NOLOGIN; END IF; END $$`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("create stub: %v (ddl=%s)", err, ddl)
		}
	}

	schema := fmt.Sprintf("pool_pm_test_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return schema, func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		_, _ = pool.Exec(dropCtx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
	}
}

// connect dials the given DSN and pings; the caller closes the pool.
func connect(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if _, err := pool.Exec(ctx, "SELECT 1"); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	return pool
}

// TestPoolSchemaConfig verifies the schema field is wired into queries
// correctly: writes to schema A do not leak into schema B.
func TestPoolSchemaConfig(t *testing.T) {
	adminDSN := dsn(t)
	schemaA, teardownA := isolatedSchema(t, adminDSN)
	defer teardownA()
	schemaB, teardownB := isolatedSchema(t, adminDSN)
	defer teardownB()

	dsnURL := mustParseURL(t, adminDSN)

	// Insert FK fixture rows into public.pool_hash / public.pool_update so the
	// cache table's FK constraints can resolve.
	insertPublicFixtures(t, adminDSN, []int64{1}, []int64{1})

	poolA, err := NewPool(context.Background(), dsnURL, PoolOptions{Schema: schemaA})
	if err != nil {
		t.Fatalf("pool A: %v", err)
	}
	defer poolA.Close()
	poolB, err := NewPool(context.Background(), dsnURL, PoolOptions{Schema: schemaB})
	if err != nil {
		t.Fatalf("pool B: %v", err)
	}
	defer poolB.Close()

	// Bootstrap the migration on schema A only.
	if err := RunMigrations(context.Background(), dsnURL, schemaA); err != nil {
		t.Fatalf("migrations on A: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := []byte(`{"name":"A","ticker":"A"}`)
	wantHash := blake2b.Sum256(body)
	metaA := PoolMetadata{
		PoolID:         1,
		MetaURL:        "https://example.com/a.json",
		MetaHash:       wantHash[:],
		MetaJSON:       body,
		LastPolled:     time.Now(),
		LastPoolUpdate: 1,
		IsValid:        true,
	}
	if err := poolA.UpsertBatch(ctx, []PoolMetadata{metaA}); err != nil {
		t.Fatalf("upsert A: %v", err)
	}

	// Schema B has no table; trying to upsert there must fail.
	if err := poolB.UpsertBatch(ctx, []PoolMetadata{metaA}); err == nil {
		t.Errorf("upsert on B succeeded unexpectedly; schema isolation broken")
	}

	var countA int
	if err := poolA.QueryRow(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s.pool_offchain_metadata", schemaA)).Scan(&countA); err != nil {
		t.Fatalf("count A: %v", err)
	}
	if countA != 1 {
		t.Errorf("schema A row count = %d, want 1", countA)
	}
	var countB int
	if err := poolB.QueryRow(ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_schema=$1",
		schemaB).Scan(&countB); err != nil {
		t.Fatalf("count B: %v", err)
	}
	if countB != 0 {
		t.Errorf("schema B table count = %d, want 0", countB)
	}
}

// TestMarkFailedOnBrandNewPool covers the spec scenario where a brand-new
// pool's very first fetch attempt fails. MarkPoolAttempted must UPSERT a
// placeholder row (URL only, NULL meta_hash/meta_json, is_valid=FALSE)
// so the discovery query picks the pool back up on the failure cadence
// instead of waiting a full poll_interval.
func TestMarkFailedOnBrandNewPool(t *testing.T) {
	adminDSN := dsn(t)
	schema, teardown := isolatedSchema(t, adminDSN)
	defer teardown()

	dsnURL := mustParseURL(t, adminDSN)
	const poolID int64 = 7
	const puID int64 = 13
	insertPublicFixtures(t, adminDSN, []int64{poolID}, []int64{puID})

	if err := RunMigrations(context.Background(), dsnURL, schema); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	pool, err := NewPool(context.Background(), dsnURL, PoolOptions{Schema: schema})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First-failure path: no prior row exists.
	if err := pool.MarkPoolAttempted(ctx, poolID, puID,
		"https://example.com/p7.json", false, 60); err != nil {
		t.Fatalf("mark failed (no prior row): %v", err)
	}

	var (
		gotURL    string
		gotValid  bool
		gotHash   []byte
		gotJSON   []byte
		gotCount  int
		gotNextAt *time.Time
		gotFailAt *time.Time
	)
	err = pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT meta_url, is_valid, meta_hash, meta_json,
		       consecutive_failures, next_attempt_at, last_failed_polled_at
		FROM %s.pool_offchain_metadata WHERE pool_id = $1`, schema), poolID).
		Scan(&gotURL, &gotValid, &gotHash, &gotJSON, &gotCount, &gotNextAt, &gotFailAt)
	if err != nil {
		t.Fatalf("read placeholder row: %v", err)
	}
	if gotURL != "https://example.com/p7.json" {
		t.Errorf("placeholder meta_url = %q, want the attempted URL", gotURL)
	}
	if gotValid {
		t.Errorf("placeholder is_valid = TRUE, want FALSE (no payload fetched yet)")
	}
	if gotHash != nil {
		t.Errorf("placeholder meta_hash = %x, want NULL", gotHash)
	}
	if gotJSON != nil {
		t.Errorf("placeholder meta_json = %s, want NULL", gotJSON)
	}
	if gotCount != 1 {
		t.Errorf("consecutive_failures = %d, want 1", gotCount)
	}
	if gotNextAt == nil {
		t.Errorf("next_attempt_at IS NULL, want a future timestamp")
	} else if !within(time.Until(*gotNextAt), 60*time.Second) {
		t.Errorf("next_attempt_at - now = %v, want ~60s", time.Until(*gotNextAt))
	}
	if gotFailAt == nil {
		t.Errorf("last_failed_polled_at IS NULL, want now()")
	}

	// Subsequent failure on the same pool must update (not insert).
	if err := pool.MarkPoolAttempted(ctx, poolID, puID,
		"https://example.com/p7.json", false, 60); err != nil {
		t.Fatalf("mark failed (existing row): %v", err)
	}
	if gotCount != 1 {
		t.Errorf("after 2nd failure: consecutive_failures = %d, want 2", gotCount)
	}

	// Successful upsert fills the payload and resets the counter.
	body := []byte(`{"name":"P7"}`)
	hash := blake2b.Sum256(body)
	if err := pool.UpsertBatch(ctx, []PoolMetadata{{
		PoolID:         poolID,
		MetaURL:        "https://example.com/p7.json",
		MetaHash:       hash[:],
		MetaJSON:       body,
		LastPolled:     time.Now(),
		LastPoolUpdate: puID,
		IsValid:        true,
	}}); err != nil {
		t.Fatalf("recovery upsert: %v", err)
	}
	var (
		hashIsNull bool
		jsonIsNull bool
	)
	err = pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT is_valid, meta_hash IS NULL, meta_json IS NULL,
		       consecutive_failures, next_attempt_at
		FROM %s.pool_offchain_metadata WHERE pool_id = $1`, schema), poolID).
		Scan(&gotValid, &hashIsNull, &jsonIsNull, &gotCount, &gotNextAt)
	if err != nil {
		t.Fatalf("read recovered row: %v", err)
	}
	if !gotValid {
		t.Errorf("after success: is_valid = FALSE, want TRUE")
	}
	if hashIsNull {
		t.Errorf("after success: meta_hash IS NULL = true, want populated")
	}
	if jsonIsNull {
		t.Errorf("after success: meta_json IS NULL = true, want populated")
	}
	if gotCount != 0 {
		t.Errorf("after success: consecutive_failures = %d, want 0", gotCount)
	}
	if gotNextAt != nil {
		t.Errorf("after success: next_attempt_at = %v, want NULL", gotNextAt)
	}
}

// TestUpsertAndMarkFailedRoundTrip exercises the full lifecycle:
//  1. Migration applies
//  2. Insert a known-good pool row via UpsertBatch
//  3. MarkPoolAttempted(false) bumps consecutive_failures
//  4. Verify next_attempt_at grew exponentially
//  5. Insert another row via UpsertBatch; success path resets
func TestUpsertAndMarkFailedRoundTrip(t *testing.T) {
	adminDSN := dsn(t)
	schema, teardown := isolatedSchema(t, adminDSN)
	defer teardown()

	dsnURL := mustParseURL(t, adminDSN)

	const poolID int64 = 42
	const puID int64 = 99
	insertPublicFixtures(t, adminDSN, []int64{poolID}, []int64{puID})

	if err := RunMigrations(context.Background(), dsnURL, schema); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	pool, err := NewPool(context.Background(), dsnURL, PoolOptions{Schema: schema})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initial success.
	body := []byte(`{"name":"Pool42","ticker":"P42"}`)
	hash1 := blake2b.Sum256(body)
	if err := pool.UpsertBatch(ctx, []PoolMetadata{{
		PoolID:         poolID,
		MetaURL:        "https://example.com/p42.json",
		MetaHash:       hash1[:],
		MetaJSON:       body,
		LastPolled:     time.Now(),
		LastPoolUpdate: puID,
		IsValid:        true,
	}}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	// Failure #1: 1x base.
	if err := pool.MarkPoolAttempted(ctx, poolID, puID, "https://example.com/p42.json", false, 30); err != nil {
		t.Fatalf("mark 1: %v", err)
	}
	gotCount, gotNextAt := mustReadRetry(t, ctx, schema, pool, poolID)
	if gotCount != 1 {
		t.Errorf("after fail #1: consecutive_failures = %d, want 1", gotCount)
	}
	if !within(gotNextAt, 30*time.Second) {
		t.Errorf("after fail #1: next_attempt_at - now = %v, want ~30s", gotNextAt)
	}

	// Failure #2: 2x base.
	if err := pool.MarkPoolAttempted(ctx, poolID, puID, "https://example.com/p42.json", false, 30); err != nil {
		t.Fatalf("mark 2: %v", err)
	}
	gotCount, gotNextAt = mustReadRetry(t, ctx, schema, pool, poolID)
	if gotCount != 2 {
		t.Errorf("after fail #2: consecutive_failures = %d, want 2", gotCount)
	}
	if !within(gotNextAt, 60*time.Second) {
		t.Errorf("after fail #2: next_attempt_at - now = %v, want ~60s", gotNextAt)
	}

	// Failure #4 should be 8x base.
	for i := 0; i < 2; i++ {
		if err := pool.MarkPoolAttempted(ctx, poolID, puID, "https://example.com/p42.json", false, 30); err != nil {
			t.Fatalf("mark %d: %v", 3+i, err)
		}
	}
	gotCount, gotNextAt = mustReadRetry(t, ctx, schema, pool, poolID)
	if gotCount != 4 {
		t.Errorf("after 4 fails: consecutive_failures = %d, want 4", gotCount)
	}
	if !within(gotNextAt, 240*time.Second) {
		t.Errorf("after 4 fails: next_attempt_at - now = %v, want ~240s", gotNextAt)
	}

	// Successful upsert resets.
	hash2 := blake2b.Sum256(body)
	if err := pool.UpsertBatch(ctx, []PoolMetadata{{
		PoolID:         poolID,
		MetaURL:        "https://example.com/p42.json",
		MetaHash:       hash2[:],
		MetaJSON:       body,
		LastPolled:     time.Now(),
		LastPoolUpdate: puID,
		IsValid:        true,
	}}); err != nil {
		t.Fatalf("recovery upsert: %v", err)
	}
	var (
		count     int
		nextAtRaw *time.Time
	)
	if err := pool.QueryRow(ctx,
		fmt.Sprintf("SELECT consecutive_failures, next_attempt_at FROM %s.pool_offchain_metadata WHERE pool_id = $1", schema),
		poolID).Scan(&count, &nextAtRaw); err != nil {
		t.Fatalf("read after recovery: %v", err)
	}
	if count != 0 {
		t.Errorf("after recovery: consecutive_failures = %d, want 0", count)
	}
	if nextAtRaw != nil {
		t.Errorf("after recovery: next_attempt_at should be NULL, got %v", nextAtRaw)
	}
}

// TestInsertFetchError exercises the failure-log path: row inserted with
// each error_class category, body preview truncated, status_code NULL when
// transport-only.
func TestInsertFetchError(t *testing.T) {
	adminDSN := dsn(t)
	schema, teardown := isolatedSchema(t, adminDSN)
	defer teardown()

	dsnURL := mustParseURL(t, adminDSN)
	if err := RunMigrations(context.Background(), dsnURL, schema); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	pool, err := NewPool(context.Background(), dsnURL, PoolOptions{Schema: schema})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Big body — truncated to 4096 by InsertFetchError.
	bigBody := []byte(strings.Repeat("x", 8192))
	err = pool.InsertFetchError(ctx, FetchError{
		PoolID:         1,
		PMRID:          100,
		MetaURL:        "https://example.com/1.json",
		HTTPStatusCode: 500,
		ErrorClass:     "http_status",
		ErrorMessage:   "HTTP 500 Internal Server Error",
		ResponseBody:   bigBody,
	}, 4096)
	if err != nil {
		t.Fatalf("insert 1: %v", err)
	}

	// Transport-only (no HTTP status code).
	if err := pool.InsertFetchError(ctx, FetchError{
		PoolID:       2,
		PMRID:        200,
		MetaURL:      "https://example.com/2.json",
		ErrorClass:   "timeout",
		ErrorMessage: "Get ...: context deadline exceeded",
	}, 4096); err != nil {
		t.Fatalf("insert 2: %v", err)
	}

	rows, err := pool.Query(ctx,
		fmt.Sprintf("SELECT pool_id, error_class, http_status_code, octet_length(response_preview) FROM %s.pool_offchain_fetch_error ORDER BY pool_id", schema))
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()
	type rec struct {
		pid      int64
		class    string
		status   pgtype.Int4
		bytesLen pgtype.Int8
	}
	var got []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.pid, &r.class, &r.status, &r.bytesLen); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	if got[0].pid != 1 || got[0].class != "http_status" || !got[0].status.Valid || got[0].status.Int32 != 500 {
		t.Errorf("row 1 mismatch: %+v", got[0])
	}
	if !got[0].bytesLen.Valid || got[0].bytesLen.Int64 != 4096 {
		t.Errorf("row 1 body preview len = %v, want 4096", got[0].bytesLen)
	}
	if got[1].pid != 2 || got[1].class != "timeout" {
		t.Errorf("row 2 mismatch: %+v", got[1])
	}
	if got[1].status.Valid {
		t.Errorf("row 2 http_status_code should be NULL for transport error, got %v", got[1].status.Int32)
	}
	if got[1].bytesLen.Valid {
		t.Errorf("row 2 body preview should be NULL, got %v", got[1].bytesLen.Int64)
	}
}

// mustReadRetry reads consecutive_failures and next_attempt_at-now().
func mustReadRetry(t *testing.T, ctx context.Context, schema string, pool *Pool, poolID int64) (int, time.Duration) {
	t.Helper()
	var count int
	var nextAtRaw *time.Time
	if err := pool.QueryRow(ctx,
		fmt.Sprintf("SELECT consecutive_failures, next_attempt_at FROM %s.pool_offchain_metadata WHERE pool_id = $1", schema),
		poolID).Scan(&count, &nextAtRaw); err != nil {
		t.Fatalf("read retry state: %v", err)
	}
	if nextAtRaw == nil {
		return count, 0
	}
	return count, time.Until(*nextAtRaw)
}

// insertPublicFixtures inserts the FK targets (public.pool_hash and
// public.pool_update) needed by the cache table's foreign-key constraints.
func insertPublicFixtures(t *testing.T, adminDSN string, poolHashIDs, poolUpdateIDs []int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := connect(t, adminDSN)
	defer pool.Close()
	for _, id := range poolHashIDs {
		if _, err := pool.Exec(ctx,
			`INSERT INTO public.pool_hash (id, view) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			id, fmt.Sprintf("pool1test%d", id)); err != nil {
			t.Fatalf("insert pool_hash %d: %v", id, err)
		}
	}
	for _, id := range poolUpdateIDs {
		if _, err := pool.Exec(ctx,
			`INSERT INTO public.pool_update (id, hash_id, registered_tx_id, cert_index, meta_id,
			                              active_epoch_no, vrf_key_hash, margin, fixed_cost, pledge,
			                              deposit, reward_addr_id)
			 VALUES ($1, 1, 1, 0, 1, 0, decode('00','hex'), 0.0, 0, 0, 0, 1)
			 ON CONFLICT DO NOTHING`, id); err != nil {
			t.Fatalf("insert pool_update %d: %v", id, err)
		}
	}
}

func within(got, want time.Duration) bool {
	d := got - want
	if d < -2*time.Second || d > 2*time.Second {
		return false
	}
	return true
}

func mustParseURL(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	return u.String()
}

// hashBytes is a small helper to render byte slice as hex (for debugging).
func hashBytes(b []byte) string { return hex.EncodeToString(b) }
