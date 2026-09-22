# Capability: pool-offchain-metadata-cache

## Purpose

Persist the *current* off-chain pool metadata for every Cardano stake pool in a single
grest-owned table. One row per pool, upserted in place on every successful fetch by
`koios-pool-meta-fetcher`. The on-chain declared hash is **not** enforced; the fetched
hash is recorded (informational only). No live-verification column is exposed in the
RPC API; consumers JOIN `pom.meta_hash = pmr.hash` themselves if they want.

This capability is the storage backend; populating it is the responsibility of
`pool-offchain-fetcher`. Reading it is the responsibility of `pool-rpc-repointing`.

## Requirements

### Requirement: schema

The system SHALL provide a table `grest.pool_offchain_metadata` with the following columns:

| Column | SQL type | Constraints |
|---|---|---|
| `id` | `BIGSERIAL` | `PRIMARY KEY` |
| `pool_id` | `BIGINT` | `NOT NULL`, `UNIQUE`, `REFERENCES public.pool_hash(id) ON DELETE CASCADE` |
| `meta_url` | `VARCHAR(256)` | `NOT NULL` |
| `meta_hash` | `BYTEA` | (nullable; CHECK `meta_hash IS NULL OR OCTET_LENGTH(meta_hash) = 32`) |
| `meta_json` | `JSONB` | (nullable) |
| `last_polled` | `TIMESTAMPTZ` | `NOT NULL` |
| `last_succeeded_polled_at` | `TIMESTAMPTZ` | `NOT NULL DEFAULT now()` |
| `last_failed_polled_at` | `TIMESTAMPTZ` | (nullable; NULL until the first failure) |
| `last_pool_update_id` | `BIGINT` | `NOT NULL`, `REFERENCES public.pool_update(id) ON DELETE CASCADE` |
| `is_valid` | `BOOLEAN` | `NOT NULL DEFAULT TRUE` |
| `consecutive_failures` | `INT` | `NOT NULL DEFAULT 0` |
| `next_attempt_at` | `TIMESTAMPTZ` | (nullable; NULL while in the success bucket; set to `now() + failed_pool_retry_seconds * 2^(consecutive_failures - 1)` after a failure) |

The daemon stores `meta_hash` (Blake2b-256 of the raw response body at fetch time,
32 bytes) and `meta_json` (the parsed payload round-tripped through `json.Marshal`,
what the RPCs return). It does **not** store the raw bytes — the live table's role is
to serve the parsed metadata, and the byte-level identity is captured by `meta_hash`.

`meta_hash` and `meta_json` are nullable so that a row can be created on the first
attempted fetch of a brand-new pool even when that attempt fails (see the
"first attempt fails" scenario below). On any successful fetch, both are populated
and `is_valid` is set to TRUE; the row is upserted in place. A row whose `is_valid`
is FALSE and whose `meta_hash`/`meta_json` are NULL exists solely to track a failed
first attempt — it should not be returned to RPC consumers.

#### Scenario: row is upserted on successful fetch

- **WHEN** the fetcher downloads pool X's metadata, parses it, and writes successfully
- **THEN** a row exists in `grest.pool_offchain_metadata` with
  - `pool_id = X.id`
  - `meta_hash = blake2b_256(raw_response_body)`
  - `meta_json = json_marshal(parsed_payload)`
  - `is_valid = TRUE`
  - `last_polled = now()`
  - `last_succeeded_polled_at = now()`
  - `consecutive_failures = 0`, `next_attempt_at = NULL`

#### Scenario: existing row is overwritten when upstream pool_update changes

- **WHEN** pool X submits a new pool_update with a new `meta_id`, the fetcher downloads
  the new URL, and writes successfully
- **THEN** the existing row's `meta_url`, `meta_hash`, `meta_json`, `last_polled`,
  `last_pool_update_id`, `is_valid`, `last_succeeded_polled_at` are updated in place.
  `consecutive_failures` is reset to 0 and `next_attempt_at` is set to NULL.
  No second row is created (enforced by the UNIQUE constraint on `pool_id`).

#### Scenario: refresh fails but the existing successful row is preserved

- **WHEN** pool X has an existing successful row (with `is_valid = TRUE`,
  `last_succeeded_polled_at` set, `meta_hash`/`meta_json` populated) and the daemon
  re-polls it but the fetch fails (HTTP 5xx, transport timeout, JSON validation
  error, IP-block, etc.)
- **THEN**
  - the existing row's `meta_url`, `meta_hash`, `meta_json`, `is_valid`, and
    `last_succeeded_polled_at` are **left unchanged**. Consumers reading the live
    table continue to see the last known-good metadata.
  - `last_polled` and `last_failed_polled_at` are set to `now()`.
  - `consecutive_failures` is incremented by 1.
  - `next_attempt_at` is set to `now() + failed_pool_retry_seconds * 2^(consecutive_failures - 1)`,
    clamped at `2^10 * failed_pool_retry_seconds`.
  - a new row is appended to `grest.pool_offchain_fetch_error` with
    `error_class`, `error_message`, `response_preview` (where applicable), and
    `http_status_code` (NULL for transport-class errors). The error table
    accumulates one row per failed attempt; `consecutive_failures` on the live row
    is the current streak since the last successful fetch.

#### Scenario: first attempt of a brand-new pool fails

- **WHEN** pool X has no row in `grest.pool_offchain_metadata` and the daemon's
  first fetch attempt fails (any error class)
- **THEN**
  - a placeholder row is created with
    - `pool_id = X.id`
    - `meta_url = the URL that was attempted`
    - `meta_hash = NULL`, `meta_json = NULL`
    - `is_valid = FALSE`
    - `last_polled = now()`, `last_failed_polled_at = now()`
    - `last_succeeded_polled_at = now()` (the row's default; distinguishes the row
      from a pre-existing failed-state row that has been retried)
    - `consecutive_failures = 1`
    - `next_attempt_at = now() + failed_pool_retry_seconds`
  - this ensures the discovery query picks the pool back up on the failure
    cadence (`next_attempt_at <= now()`) rather than waiting a full
    `poll_interval_seconds` for the success-cadence re-fetch predicate
  - a row is also appended to `grest.pool_offchain_fetch_error` as in the
    refresh-failure scenario above
  - RPC consumers that filter on `is_valid = TRUE` (or on `meta_json IS NOT NULL`)
    will not see the placeholder; ops dashboards reading the row directly see
    the URL and the failure timestamp

#### Scenario: placeholder row is filled by a later successful fetch

- **WHEN** the pool with the placeholder row (`is_valid = FALSE`, `meta_hash`/
  `meta_json` NULL) succeeds on a later fetch
- **THEN** the existing row is updated in place via `UpsertBatch`:
  `meta_url`, `meta_hash`, `meta_json`, `is_valid = TRUE`,
  `last_succeeded_polled_at = now()`, `consecutive_failures = 0`,
  `next_attempt_at = NULL`. No second row is created.

### Requirement: indexes

The system SHALL create the following indexes:

- UNIQUE INDEX on `(pool_id)` — already enforced by the column constraint
- INDEX on `(last_polled DESC)` — supports the "pools not fetched in N minutes" ops query
- INDEX on `(last_succeeded_polled_at)` — drives the normal-cadence re-fetch predicate
- PARTIAL INDEX on `(last_failed_polled_at)` WHERE `last_failed_polled_at IS NOT NULL`
  — drives "pools with recent failures" ops queries
- PARTIAL INDEX on `(next_attempt_at)` WHERE `next_attempt_at IS NOT NULL` — drives the
  failure-cadence re-fetch predicate
- INDEX on `(last_pool_update_id)` — supports JOINs back to chain provenance

### Requirement: access-control

The table SHALL be:
- owned by the new role `grest_owner` (created in `files/grest/rpc/db-scripts/basics.sql`),
- `GRANT SELECT ON` to `web_anon` (PostgREST consumers),
- `GRANT ALL ON` to `grest_owner` (daemon).
- `GRANT USAGE ON SEQUENCE grest.pool_offchain_metadata_id_seq TO grest_owner`.

#### Scenario: web_anon can read but not write

- **WHEN** a PostgREST request `SELECT meta_json FROM grest.pool_offchain_metadata WHERE
  pool_id = 42` is executed
- **THEN** it returns the row
- AND **WHEN** the same role issues `INSERT INTO grest.pool_offchain_metadata (...)`
- **THEN** the database raises `permission denied`

### Requirement: storage-budget

The cached payload per row is bounded only by the row size of the JSONB column (Postgres
TOAST-compressed; effectively bounded by `fetch.validation.max_body_bytes` on the wire
side, since the daemon rejects larger bodies before parsing). No per-row CHECK constraint
is needed; the daemon enforces size limits at fetch time before INSERT/UPDATE.

### Requirement: fetch-error-log

The system SHALL provide a table `grest.pool_offchain_fetch_error` that records every
failed poll-attempt. The live `pool_offchain_metadata` row is **preserved on failure**
(see §schema scenarios); this table is the per-attempt audit log used by operators
to answer "why is this pool missing?".

| Column | SQL type | Constraints |
|---|---|---|
| `id` | `BIGSERIAL` | `PRIMARY KEY` |
| `pool_id` | `BIGINT` | `NOT NULL`, `REFERENCES public.pool_hash(id) ON DELETE CASCADE` |
| `pmr_id` | `BIGINT` | `NOT NULL` (snapshot of `public.pool_metadata_ref.id` at fetch time; **no FK** — the audit log must survive db-sync pruning of the on-chain ref) |
| `meta_url` | `VARCHAR(256)` | `NOT NULL` (snapshot of the attempted URL; **no FK** — the URL on-chain may change later) |
| `http_status_code` | `INT` | (nullable; NULL for transport-class errors — DNS, TLS, IP-block, timeout) |
| `error_class` | `VARCHAR(64)` | `NOT NULL` — stable machine-readable category (`timeout`, `dns_failure`, `tls_failure`, `ip_blocked`, `http_status`, `size_violation`, `content_type_rejected`, `json_validation`, …) |
| `error_message` | `TEXT` | `NOT NULL` — human-readable detail |
| `response_preview` | `BYTEA` | (nullable; first `fetch.validation.max_body_bytes` bytes of the body, NULL when no body was read) |
| `attempted_at` | `TIMESTAMPTZ` | `NOT NULL DEFAULT now()` |

`pool_id` cascades with the chain-side `public.pool_hash` row: if the pool is
deregistered from db-sync, its audit history is removed. `pmr_id` and `meta_url`
are deliberately **decoupled** from `public.pool_metadata_ref`: the audit log must
survive db-sync pruning of stale on-chain metadata refs (which happens periodically)
and capture the URL as it was at the time of failure, not its current on-chain value.

One row is appended per failed attempt. The daemon writes nothing to this table when
a fetch succeeds. The table is **append-only** — there is no UPDATE/DELETE in the
daemon code path. Operators are expected to prune old rows on a schedule of their
choosing (out of scope for this spec).

`consecutive_failures` on the live row is the current streak since the last success;
`SELECT count(*) FROM grest.pool_offchain_fetch_error WHERE pool_id = X` is the lifetime
failure count; `SELECT error_class, count(*) … GROUP BY error_class` is the per-class
breakdown.

### Requirement: provenance-pointer

`last_pool_update_id` SHALL always correspond to the most recent `public.pool_update` row
that declared the URL / hash the fetcher is currently storing for this pool. Consumers can
JOIN through this column back to `public.pool_update` → `public.pool_metadata_ref` to get
the on-chain declared `url` and `hash`.

#### Scenario: provenance join

- **WHEN** a consumer wants to verify whether the current fetched hash equals the
  on-chain declared hash
- **THEN** the consumer JOINs `grest.pool_offchain_metadata pomet ON pomet.pool_id = $1`
  with `public.pool_update pu ON pu.id = pomet.last_pool_update_id` and
  `public.pool_metadata_ref pmr ON pmr.id = pu.meta_id`
- **AND** the comparison `pomet.meta_hash = pmr.hash` yields TRUE iff the SPO's currently
  declared hash matches the actual fetched hash
