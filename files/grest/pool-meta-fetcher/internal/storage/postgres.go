// Package storage handles all PostgreSQL access for koios-pool-meta-fetcher:
// connection pooling (pgxpool), per-cycle work-queue discovery, and batched
// UPSERT into grest.pool_offchain_metadata.
package storage

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	migrateiofs "github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// File-system helpers used by migrationsSource. Defined as vars so tests can
// stub them if ever needed; default to the real os calls.
var (
	osStatFunc   = func(p string) (os.FileInfo, error) { return os.Stat(p) }
	osGetenvFunc = func(k string) string { return os.Getenv(k) }
)

// Pool wraps pgxpool.Pool with daemon-specific helpers.
type Pool struct {
	*pgxpool.Pool
	appName string
	schema  string // namespace for grest.* tables; defaults to "grest"
}

// PoolOptions configures the pgxpool.
type PoolOptions struct {
	Schema           string
	MaxConns         int32
	MinConns         int32
	MaxConnLifetime  time.Duration
	MaxConnIdleTime  time.Duration
	StatementTimeout time.Duration
	ApplicationName  string
}

// NewPool configures and opens the pgxpool. It validates connectivity via a
// single Ping before returning.
func NewPool(ctx context.Context, dbURL string, opts PoolOptions) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("parse db url: %w", err)
	}
	if opts.MaxConns > 0 {
		cfg.MaxConns = opts.MaxConns
	}
	if opts.MinConns > 0 {
		cfg.MinConns = opts.MinConns
	}
	if opts.MaxConnLifetime > 0 {
		cfg.MaxConnLifetime = opts.MaxConnLifetime
	}
	if opts.MaxConnIdleTime > 0 {
		cfg.MaxConnIdleTime = opts.MaxConnIdleTime
	}
	if opts.ApplicationName != "" {
		cfg.ConnConfig.Config.RuntimeParams["application_name"] = opts.ApplicationName
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Pool{Pool: pool, appName: opts.ApplicationName, schema: opts.Schema}, nil
}

// Ping returns (last_ping_at, age, err) for the readyz health check.
func (p *Pool) Ping(ctx context.Context) (time.Time, time.Duration, error) {
	t0 := time.Now()
	err := p.Pool.Ping(ctx)
	return t0, time.Since(t0), err
}

// -----------------------------------------------------------------------------
// Work queue (pool discovery, per spec pool-offchain-fetcher §pool-discovery)
// -----------------------------------------------------------------------------

// WorkItem is one row from the work-queue query.
type WorkItem struct {
	PoolID         int64
	PoolUpdateID   int64
	URL            string
	OnChainHash    []byte
	Bech32         string // public.pool_hash.view — used by allow_list / deny_list
	LastPolled     *time.Time
	LastPoolUpdate *int64
}

// DiscoverWorkItems returns the pools needing a fetch in this cycle.
//
// The query joins public.pool_update -> public.pool_hash -> public.pool_metadata_ref,
// picks the most-recent pool_update per pool, and LEFT-JOINs the cache table
// with two "needs work" predicates: row missing, OR last_pool_update_id differs,
// OR last_polled is older than `pollStaleAfter`.
func (p *Pool) DiscoverWorkItems(
	ctx context.Context,
	batchSize int,
	pollStaleAfter time.Duration,
	failedRetryAfter time.Duration,
	allowList, denyList []string,
) ([]WorkItem, error) {
	schemaIdent := pgx.Identifier{p.Schema()}.Sanitize()
	// Per-pool retry cadence is encoded in next_attempt_at by MarkPoolAttempted;
	// the failedRetryAfter argument is accepted for backward-compat but unused.
	var pollSecs int64 = int64(pollStaleAfter.Seconds())
	if pollSecs <= 0 {
		pollSecs = 60
	}
	_ = failedRetryAfter
	q := fmt.Sprintf(`
WITH latest_pu AS (
  SELECT DISTINCT ON (pu.hash_id)
    pu.id           AS pu_id,
    pu.hash_id      AS pool_hash_id,
    pu.meta_id      AS meta_id
  FROM public.pool_update pu
  WHERE pu.active_epoch_no <= (SELECT MAX(epoch_no) FROM public.epoch_param)
  ORDER BY pu.hash_id, pu.registered_tx_id DESC, pu.cert_index DESC
)
SELECT
  ph.id,
  lp.pu_id,
  pmr.url,
  pmr.hash,
  ph.view,
  pom.last_polled,
  pom.last_pool_update_id
FROM latest_pu lp
JOIN public.pool_hash          ph  ON ph.id  = lp.pool_hash_id
JOIN public.pool_metadata_ref  pmr ON pmr.id = lp.meta_id
LEFT JOIN %s.pool_offchain_metadata pom ON pom.pool_id = ph.id
WHERE pmr.url IS NOT NULL
  AND (
       pom.pool_id IS NULL                                                                  -- brand-new pool
    OR pom.last_pool_update_id IS DISTINCT FROM lp.pu_id                                     -- on-chain change
    -- last attempt was success AND it's old enough (long poll cycle)
    OR (pom.next_attempt_at IS NULL
        AND pom.last_succeeded_polled_at < now() - make_interval(secs => $1::bigint))
    -- last attempt was failure; the daemon set next_attempt_at to
    -- now() + base * 2^(count-1) on the failure. Check the schedule.
    OR (pom.next_attempt_at IS NOT NULL AND pom.next_attempt_at <= now())
  )
LIMIT $2;
`, schemaIdent)
	rows, err := p.Query(ctx, q, pollSecs, batchSize)
	if err != nil {
		return nil, fmt.Errorf("discover work: %w", err)
	}
	defer rows.Close()

	var items []WorkItem
	for rows.Next() {
		var wi WorkItem
		if err := rows.Scan(
			&wi.PoolID, &wi.PoolUpdateID, &wi.URL, &wi.OnChainHash, &wi.Bech32,
			&wi.LastPolled, &wi.LastPoolUpdate,
		); err != nil {
			return nil, fmt.Errorf("scan work item: %w", err)
		}
		items = append(items, wi)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work items: %w", err)
	}
	// allow_list / deny_list filtering is enforced in the fetcher
	// (applyFilterLists) so glob patterns can be honored without dragging
	// them into SQL. Both lists are still accepted here for spec compliance.
	_ = allowList
	_ = denyList
	return items, nil
}

// Schema returns the configured schema name (defaults to "grest").
func (p *Pool) Schema() string {
	if p.schema == "" {
		return "grest"
	}
	return p.schema
}

// -----------------------------------------------------------------------------
// Batched UPSERT
// -----------------------------------------------------------------------------

// PoolMetadata is the upsert payload for one row in grest.pool_offchain_metadata.
type PoolMetadata struct {
	PoolID         int64
	MetaURL        string
	MetaHash       []byte // 32 bytes (Blake2b-256 of the raw response body, computed at fetch time)
	MetaJSON       []byte // raw json.Marshal output (what RPCs return)
	LastPolled     time.Time
	LastPoolUpdate int64
	IsValid        bool
}

// UpsertBatch performs an UPSERT for a batch of PoolMetadata rows in one
// transaction. The fastest path uses a temp staging table populated via
// COPY and then INSERT ... ON CONFLICT to merge into the target — this gives
// bulk-load throughput AND correct conflict resolution (a plain COPY FROM
// would 23505 on duplicate pool_id).
func (p *Pool) UpsertBatch(ctx context.Context, batch []PoolMetadata) error {
	if len(batch) == 0 {
		return nil
	}
	tx, err := p.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin upsert tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	schemaIdent := pgx.Identifier{p.Schema()}.Sanitize()
	tableIdent := pgx.Identifier{p.Schema(), "pool_offchain_metadata"}.Sanitize()
	stageIdent := pgx.Identifier{"pmf_stage"}.Sanitize()
	_ = schemaIdent // tableIdent already encodes the schema; this kept for clarity in case the stage ever differs.

	// Stage the batch into a temp table (LIKE target) and merge with ON
	// CONFLICT. Concurrent per-cycle runs would NOT collide because the
	// same target pool_id always replaces the prior row, but we still wrap
	// the merge in a tx so partial failures roll back cleanly.
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP;`,
		stageIdent, tableIdent)); err != nil {
		return fmt.Errorf("create stage: %w", err)
	}

	rows := make([][]any, 0, len(batch))
	for _, m := range batch {
		rows = append(rows, []any{
			m.PoolID,
			m.MetaURL,
			m.MetaHash,
			m.MetaJSON,
			m.LastPolled,
			m.LastPoolUpdate,
			m.IsValid,
		})
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"pmf_stage"},
		[]string{"pool_id", "meta_url", "meta_hash", "meta_json",
			"last_polled", "last_pool_update_id", "is_valid"},
		pgx.CopyFromRows(rows),
	); err != nil {
		return fmt.Errorf("copy into stage: %w", err)
	}

	// Merge: insert or update on (pool_id). All non-key columns refresh
	// from the freshly-fetched row (this is the "current view" cache).
	// Note: last_succeeded_polled_at is bumped to match last_polled on every
	// successful upsert; last_failed_polled_at is left as-is (we only ever
	// touch it from the failure path via MarkPoolAttempted).
	mergeSQL := fmt.Sprintf(`
INSERT INTO %s (
  pool_id, meta_url, meta_hash, meta_json,
  last_polled, last_pool_update_id, is_valid,
  last_succeeded_polled_at, consecutive_failures, next_attempt_at
)
SELECT
  pool_id, meta_url, meta_hash, meta_json,
  last_polled, last_pool_update_id, is_valid,
  last_polled, 0, NULL
FROM %s
ON CONFLICT (pool_id) DO UPDATE SET
  meta_url                   = EXCLUDED.meta_url,
  meta_hash                  = EXCLUDED.meta_hash,
  meta_json                  = EXCLUDED.meta_json,
  last_polled                = EXCLUDED.last_polled,
  last_pool_update_id        = EXCLUDED.last_pool_update_id,
  is_valid                   = EXCLUDED.is_valid,
  last_succeeded_polled_at   = EXCLUDED.last_succeeded_polled_at,
  consecutive_failures       = 0,
  next_attempt_at            = NULL;
`, tableIdent, stageIdent)
	if _, err := tx.Exec(ctx, mergeSQL); err != nil {
		return fmt.Errorf("merge into %s: %w", tableIdent, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit upsert tx: %w", err)
	}
	return nil
}

// MarkPoolAttempted records that we attempted (succeeded or failed) for a
// given (pool_id, last_pool_update_id). For successful attempts, the upsert
// path handles both timestamp updates; this method only deals with failures
// to avoid round-tripping through the upsert batch.
//
// Failure-mode behaviour:
//   - bumps last_polled and last_failed_polled_at to now
//   - increments consecutive_failures
//   - schedules next_attempt_at = now() + base * 2^(consecutive_failures-1),
//     capped at base * 2^10
//   - if no row exists yet (brand-new pool, first attempt failed), creates
//     a placeholder row with the URL but NULL meta_hash / meta_json and
//     is_valid = FALSE. Subsequent successful fetches populate the payload
//     via UpsertBatch.
func (p *Pool) MarkPoolAttempted(ctx context.Context, poolID, lastPoolUpdateID int64, metaURL string, succeeded bool, baseSeconds int) error {
	tableIdent := pgx.Identifier{p.Schema(), "pool_offchain_metadata"}.Sanitize()
	if succeeded {
		// Successful path goes through UpsertBatch (which already resets
		// consecutive_failures=0 and next_attempt_at=NULL). This method
		// is a no-op for success so callers can use it uniformly.
		return nil
	}
	if baseSeconds < 1 {
		baseSeconds = 60
	}
	// UPSERT: on first failure (no row yet) create a placeholder row with
	// the URL but no payload; on subsequent failures update the existing
	// row in place. The meta_hash / meta_json columns stay NULL until a
	// successful UpsertBatch populates them.
	sql := fmt.Sprintf(`
INSERT INTO %s (
  pool_id, meta_url, last_polled, last_failed_polled_at, last_pool_update_id,
  is_valid, consecutive_failures, next_attempt_at
)
VALUES (
  $1, $2, now(), now(), $3,
  FALSE, 1, now() + make_interval(secs => $4::bigint * pow(2, 0)::bigint)
)
ON CONFLICT (pool_id) DO UPDATE SET
  last_polled           = now(),
  last_failed_polled_at = now(),
  consecutive_failures   = %s.consecutive_failures + 1,
  next_attempt_at       = now() + make_interval(secs => $4::bigint * pow(2, GREATEST(0, LEAST(%s.consecutive_failures, 10)))::bigint)
`, tableIdent, tableIdent, tableIdent)
	_, err := p.Exec(ctx, sql, poolID, metaURL, lastPoolUpdateID, int64(baseSeconds))
	return err
}

// AdjustMetaBytesCheckConstraint was removed when the meta_bytes column was
// dropped from grest.pool_offchain_metadata (the daemon stores meta_json + the
// 32-byte meta_hash, never the raw bytes). See §schema in
// openspec/specs/pool-offchain-metadata-cache/spec.md.

// FetchError is the payload passed to InsertFetchError. Fields map 1:1 to
// <schema>.pool_offchain_fetch_error columns.
type FetchError struct {
	PoolID         int64
	PMRID          int64
	MetaURL        string
	HTTPStatusCode int    // 0 = no HTTP response (DNS / TLS / IP-block / etc.)
	ErrorClass     string // stable machine-readable category
	ErrorMessage   string // human-readable detail
	ResponseBody   []byte // truncated server response (first ~4 KiB)
}

// InsertFetchError writes a single error row to <schema>.pool_offchain_fetch_error.
// Body is truncated to bodyPreviewBytes (zero means "don't store the body").
// Used by the fetcher to record every failed poll-attempt so operators can
// answer "why is this pool missing?" without scraping logs.
func (p *Pool) InsertFetchError(ctx context.Context, e FetchError, bodyPreviewBytes int) error {
	schemaIdent := pgx.Identifier{p.Schema()}.Sanitize()
	if bodyPreviewBytes < 0 {
		bodyPreviewBytes = 0
	}
	preview := e.ResponseBody
	if bodyPreviewBytes > 0 && len(preview) > bodyPreviewBytes {
		preview = preview[:bodyPreviewBytes]
	}
	sql := fmt.Sprintf(`
INSERT INTO %s.pool_offchain_fetch_error
  (pool_id, pmr_id, meta_url, http_status_code, error_class, error_message, response_preview)
VALUES ($1, $2, $3, $4, $5, $6, $7)
`, schemaIdent)
	var status *int
	if e.HTTPStatusCode != 0 {
		s := e.HTTPStatusCode
		status = &s
	}
	_, err := p.Exec(ctx, sql,
		e.PoolID,
		e.PMRID,
		e.MetaURL,
		status,
		e.ErrorClass,
		e.ErrorMessage,
		preview,
	)
	return err
}

// -----------------------------------------------------------------------------
// Migrations
// -----------------------------------------------------------------------------

// RunMigrations applies any pending schema migrations. The daemon ships with
// the SQL files embedded under migrations/*.sql; the literal token "{schema}"
// in those files is replaced with the configured schema name before execution.
//
// For dev overrides, set KOIOS_MIGRATIONS_DIR to a directory of plain SQL
// files; the same {schema} substitution applies.
func RunMigrations(ctx context.Context, dbURL, schema string) error {
	if dbURL == "" {
		return errors.New("dbURL is empty")
	}
	if schema == "" {
		schema = "grest"
	}
	if err := validateSchemaIdent(schema); err != nil {
		return err
	}

	// Bootstrap: ensure the schema exists BEFORE constructing the migrator.
	// Without this, golang-migrate's CREATE TABLE schema_migrations fails
	// because the target schema does not yet exist (the schema is normally
	// created by migration 0001). The CREATE is idempotent.
	if err := ensureSchema(dbURL, schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	srcFS, dir, err := buildMigrationsSource(schema)
	if err != nil {
		return fmt.Errorf("build migration source: %w", err)
	}
	if dir != "" {
		defer os.RemoveAll(dir)
	}

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return fmt.Errorf("parse db url: %w", err)
	}
	driver, err := migratepgx.WithInstance(stdlib.OpenDB(*cfg.ConnConfig), &migratepgx.Config{
		// Pin the schema_migrations bookkeeping table into the same schema as
		// the daemon's table (no default-search_path / no creation in public).
		MigrationsTable: "schema_migrations",
		SchemaName:      schema,
	})
	if err != nil {
		return fmt.Errorf("create migrate driver: %w", err)
	}
	migSrc, err := migrateiofs.New(srcFS, "migrations")
	if err != nil {
		return fmt.Errorf("new iofs source: %w", err)
	}
	defer migSrc.Close()
	m, err := migrate.NewWithInstance("iofs", migSrc, "postgres", driver)
	if err != nil {
		return fmt.Errorf("new migrator: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// ensureSchema creates the configured schema if it doesn't exist. Safe to
// call repeatedly (uses IF NOT EXISTS).
func ensureSchema(dbURL, schema string) error {
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return err
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	sql := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`,
		pgx.Identifier{schema}.Sanitize())
	_, err = pool.Exec(context.Background(), sql)
	return err
}

// buildMigrationsSource resolves the migration source. Returns (fs.FS, tmpdir).
// When tmpdir is non-empty the caller is responsible for removing it.
func buildMigrationsSource(schema string) (fs.FS, string, error) {
	if dir := osGetenvFunc("KOIOS_MIGRATIONS_DIR"); dir != "" {
		return renderMigrationsToTempDir(os.DirFS(dir), schema, dir)
	}
	return renderMigrationsToTempDir(migrationsFS, schema, "")
}

// renderMigrationsToTempDir walks the supplied FS, replaces {schema} in every
// .sql file, and writes the result to a tempdir (or to a baseDir override).
// Returns an OS-DirFS that the migrate.iofs source can drive directly with
// the "migrations" prefix.
func renderMigrationsToTempDir(src fs.FS, schema string, baseDir string) (fs.FS, string, error) {
	var out string
	if baseDir != "" {
		out = baseDir
		if err := os.MkdirAll(out, 0o755); err != nil {
			return nil, "", err
		}
	} else {
		var err error
		out, err = os.MkdirTemp("", "pool-meta-fetcher-mig-*")
		if err != nil {
			return nil, "", err
		}
	}
	migSubdir := out + "/migrations"
	if err := os.MkdirAll(migSubdir, 0o755); err != nil {
		return nil, "", err
	}

	// Read entries; try both layouts (migrations/ subdir and flat).
	listed, err := fs.ReadDir(src, "migrations")
	if err != nil {
		listed, err = fs.ReadDir(src, ".")
		if err != nil {
			return nil, "", err
		}
	}
	var names []string
	for _, e := range listed {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	if len(names) == 0 {
		return nil, "", errors.New("no .sql migrations found")
	}
	sort.Strings(names)
	for i, name := range names {
		var body []byte
		body, err = fs.ReadFile(src, "migrations/"+name)
		if err != nil {
			body, err = fs.ReadFile(src, name)
			if err != nil {
				return nil, "", fmt.Errorf("read %s: %w", name, err)
			}
		}
		rendered := substituteSchema(string(body), schema)
		// Strip any pre-existing numeric prefix from the source name (so we
		// can renumber deterministically); preserve the remainder.
		// golang-migrate's filename regex requires ".up." or ".down." before
		// the extension — ensure our output keeps ".up.sql" suffix.
		stripped := name
		if k := strings.IndexByte(stripped, '_'); k > 0 && k <= 6 {
			stripped = stripped[k+1:]
		}
		// Keep ".up" in the migration name so the regex matches.
		// Allow source filenames to be either "0001_name.up.sql" or
		// "0001_a.up.sql" (our sort-stable a/b/c suffix style).
		if !strings.Contains(stripped, ".up.") && !strings.HasSuffix(stripped, ".up.sql") {
			stripped = strings.TrimSuffix(stripped, ".sql") + ".up.sql"
		}
		newName := fmt.Sprintf("%05d_%s", i+1, stripped)
		if err := os.WriteFile(migSubdir+"/"+newName, []byte(rendered), 0o644); err != nil {
			return nil, "", fmt.Errorf("write %s: %w", newName, err)
		}
	}
	return os.DirFS(out), out, nil
}

// schemaIdentRE is exported for testing.
var schemaIdentRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateSchemaIdent(s string) error {
	if !schemaIdentRE.MatchString(s) {
		return fmt.Errorf("invalid schema ident %q (must match %s)", s, schemaIdentRE)
	}
	return nil
}

// substituteSchema replaces literal "{schema}" and "grest." (only as schema
// prefix, not as part of any token) with the configured schema name. Conservative
// substitution: looks for the token `grest.` at identifier boundaries.
func substituteSchema(src, schema string) string {
	src = strings.ReplaceAll(src, "{schema}", schema)
	// Replace leading "grest." schema prefix in qualified names.
	// We match word boundaries so we don't touch identifiers that happen to
	// contain "grest" as a substring (none in Koios codebase, but be safe).
	return reLeadingGrest.ReplaceAllString(src, schema+".")
}

var reLeadingGrest = regexp.MustCompile(`(^|[^A-Za-z0-9_])grest\.`)

// buildMigrationsSource picks the migration directory to use. Embedded
// migrations/*.sql ship with the binary; KOIOS_MIGRATIONS_DIR overrides for dev.

func existsDir(p string) bool {
	if p == "" {
		return false
	}
	fi, err := osStatFunc(p)
	return err == nil && fi.IsDir()
}
