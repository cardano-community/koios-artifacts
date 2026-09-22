# Capability: pool-offchain-fetcher

## Purpose

A long-running Go daemon (`koios-pool-meta-fetcher`) that periodically fetches the
off-chain metadata JSON declared by each Cardano stake pool's most recent registration
certificate, validates it against configurable rules (size, content-type, JSON shape,
hash), and upserts the result into `grest.pool_offchain_metadata`. Written in Go to
remove the operational friction of `cardano-smash-server` (Haskell, hardcoded limits,
SMASH policy baggage, no per-pool tuning).

## Requirements

### Requirement: process-lifecycle

The daemon SHALL:
- read its config from `--config PATH` (default `/etc/koios/pool-meta-fetcher.yaml`),
- open a database connection pool on startup,
- validate the config (see `pool-meta-fetcher-config` capability),
- start the health HTTP server,
- enter the poll loop on a single `time.Ticker`,
- respond to SIGINT / SIGTERM with a graceful shutdown (see Requirement: graceful-shutdown),
- handle SIGHUP by re-reading the config from disk and applying tunables (see
  Requirement: live-reload).

#### Scenario: clean startup
- **WHEN** the daemon is launched with a valid config and a reachable database
- **THEN** it logs `event=ready`, exits no later than 5 seconds, and `/readyz` returns
  200 OK with body `{"status":"ready"}`

#### Scenario: invalid config
- **WHEN** the daemon is launched with `--config /nonexistent.yaml`
- **THEN** it logs `level=error event=config_load_failed path=/nonexistent.yaml reason="no
  such file"` and exits with code 2 within 1 second

### Requirement: poll-cycle

The daemon SHALL loop on a fixed cadence (`fetch.poll_interval_seconds`, default 300). On
each tick it SHALL:
1. Run the work-queue query (see Requirement: pool-discovery) to enumerate pools needing
   work, applying the per-pool `min_poll_interval_seconds` floor.
2. Push the work items into a bounded channel of size
   `fetch.work_queue.batch_size * fetch.work_queue.concurrency`.
3. Spawn `fetch.work_queue.concurrency` workers that pull work, perform HTTP GET, apply
   validators, and produce a result (success or failure).
4. Aggregate results and perform batched UPSERT into
   `grest.pool_offchain_metadata` in a single transaction at the end of the cycle.
5. Emit a structured `event=cycle_summary` log line at end of cycle with:
   `succeeded`, `failed`, `skipped`, `duration_ms` (integer counters).
   Successful/failed/successful counts reflect the worker outcomes for this cycle.

#### Scenario: high queue
- **WHEN** the work-queue query returns 500 rows and `concurrency = 16`
- **THEN** the daemon processes them in 32 batches of 16 each, never exceeding 16
  in-flight requests

### Requirement: pool-discovery

On each tick, the daemon SHALL execute (with `{schema}` substituted for the
configured schema name):

```sql
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
LEFT JOIN {schema}.pool_offchain_metadata pom ON pom.pool_id = ph.id
WHERE pmr.url IS NOT NULL
  AND (
       pom.pool_id IS NULL                                                                  -- brand-new pool (never attempted)
    OR pom.last_pool_update_id IS DISTINCT FROM lp.pu_id                                     -- on-chain change
    -- last successful poll is older than poll_interval_seconds (the normal cycle)
    OR (pom.next_attempt_at IS NULL
        AND pom.last_succeeded_polled_at < now() - make_interval(secs => $1::bigint))
    -- last attempt failed; MarkPoolAttempted scheduled next_attempt_at for the
    -- exponential-backoff window. Re-pick up the pool when its cooldown ends.
    -- Covers both: (a) refresh failures on existing successful rows (where the
    -- placeholder in pool_offchain_metadata carries next_attempt_at but the
    -- previous successful payload is preserved), and (b) brand-new pools whose
    -- first fetch attempt failed (where a placeholder row was created on the
    -- previous cycle).
    OR (pom.next_attempt_at IS NOT NULL AND pom.next_attempt_at <= now())
  )
LIMIT $2;
```

`ph.view` (bech32 pool ID) is returned so the fetcher can enforce
`pool_id_allow_list` / `pool_id_deny_list` against the correct identifier (the
work-queue id alone is the internal `pool_hash.id`, not human-meaningful).

#### Failure retry path (intentional divergence from the canonical spec query)

The spec query in the OpenSpec revision of 2025 used
`grest.pool_offchain_metadata.last_polled < now() - poll_interval` as the
single "needs work" predicate. The implementation splits that into two:

- **Successful-path predicate**: `last_succeeded_polled_at < now() - poll_interval`.
  This counts only successful fetches toward the floor, so a chain of failures
  does not delay the eventual successful poll by `n * poll_interval`.
- **Failure-path predicate**: `next_attempt_at <= now()`. The
  `MarkPoolAttempted` SQL writes `next_attempt_at = now() + base * 2^n` after
  each failure (see Requirement: retry-policy), and the discovery query picks the
  pool back up when that timestamp elapses.

The net effect is that failed pools retry on the exponential backoff schedule
(unaffected by `poll_interval_seconds`), while successful pools are refreshed
on the normal `poll_interval_seconds` cadence.

#### Scenario: brand-new pool
- **WHEN** a pool's most recent pool_update is at the canonical `pu.id` and no row exists
  in `grest.pool_offchain_metadata` for it
- **THEN** it appears in the work queue

#### Scenario: recently fetched pool, no chain change
- **WHEN** a pool's `last_succeeded_polled_at` is less than `poll_interval_seconds` ago and
  `last_pool_update_id` matches the latest pool_update id and `next_attempt_at IS NULL`
- **THEN** it is NOT in the work queue

#### Scenario: SPO changed their URL
- **WHEN** a pool submits a new pool_update with a new `meta_id`
- **THEN** `last_pool_update_id` differs from `pu.id`, so the pool appears in the work queue

#### Scenario: failed pool inside backoff window
- **WHEN** a pool has `consecutive_failures = 3` and `next_attempt_at` is in the future
- **THEN** it is NOT in the work queue (backoff window is in effect)

### Requirement: hash-capture-informational

For each pool in the work queue, the daemon SHALL:
1. Download the URL declared in `pool_metadata_ref.url` via HTTPS GET.
2. Compute `blake2b_256(raw_response_body)` using `golang.org/x/crypto/blake2b` Size=256
   (configurable via `fetch.validation.hash.algorithm; "none"` disables).
3. Apply all other validation rules (size, content-type, JSON shape) — see
   `pool-meta-fetcher-config`.
4. Store the computed `meta_hash` in the corresponding row column, regardless of whether
   it equals the on-chain declared hash.
5. If `fetch.validation.hash.log_mismatches: true`, log mismatches as an informational
   event (`event=hash_mismatch pool_id=X declared=Y actual=Z level=info`) ONCE per
   cycle per pool (deduplicated).
6. There is NO live-verification column in the public RPC API. Consumers wanting to
   compare fetched vs declared hash do so themselves by JOINing
   `pom.meta_hash = pmr.hash` through `last_pool_update_id`.

#### Scenario: hash mismatch is informational, not a gate
- **WHEN** the downloaded bytes' Blake2b-256 hash differs from `pool_metadata_ref.hash`
  AND all other validators pass
- **THEN** the row IS upserted with `meta_hash = actual_hash` and `is_valid = TRUE`.
  An info-level hash_mismatch event is emitted (deduplicated per cycle).

#### Scenario: body too large
- **WHEN** response body exceeds `fetch.validation.max_body_bytes`
- **THEN** the daemon does NOT upsert; logs `level=warn event=size_exceeded pool_id=X
  size=Y limit=Z`; increments per-pool retry counter

#### Scenario: malformed JSON
- **WHEN** `json.Unmarshal` fails
- **THEN** the daemon does NOT upsert; logs `level=warn event=json_parse_failed pool_id=X
  err=Y`; increments per-pool retry counter

### Requirement: connection-security

The daemon SHALL:
- Block outgoing HTTPS connections to **any** of the following when `fetch.private_ip_policy
  = block`:
  - IPv4 RFC1918 (10/8, 172.16/12, 192.168/16)
  - IPv4 loopback (127/8) and broadcast (255.255.255.255)
  - IPv4 link-local (169.254/16) and unspecified (0.0.0.0)
  - IPv4 multicast (224/4) — also blocks SSRF to metadata services like
    169.254.169.254
  - IPv6 loopback (::1), unspecified (::), link-local (fe80::/10), unique-local
    (fc00::/7), multicast (ff00::/8)
- Implement blocking at connect time via a custom `http.Transport.DialContext` that:
  1. Resolves the URL host via `net.DefaultResolver`.
  2. Checks every resolved IP against the deny list.
  3. Aborts with `event=ip_blocked pool_id=X host=Y ip=Z` if any resolved IP is denied.
  4. Connects using the vetted address (DNS-rebind safe).
- Verify TLS certificates by default; `fetch.http.skip_tls_verify: true` allowed only for
  testing (daemon logs a `level=warn event=tls_verify_disabled` once at startup).
- Apply a per-request HTTP timeout (`fetch.http.timeout_seconds`, default 30) and a
  separate connect timeout (`fetch.http.connect_timeout_seconds`, default 10).

#### Scenario: private IP rejected
- **WHEN** the URL host resolves to `127.0.0.1` and `private_ip_policy = block`
- **THEN** the daemon aborts the request before any TCP connect, classifies the
  error as `ip_blocked`, and persists a row to `{schema}.pool_offchain_fetch_error`
  with `error_class = 'ip_blocked'`. The per-pool retry counter is incremented and
  the next attempt is scheduled per the retry-policy curve.

#### Scenario: DNS rebind attempt
- **WHEN** a host resolves to `93.184.216.34` at the API check and `127.0.0.1` at connect
  time
- **THEN** the daemon catches the second resolution inside `DialContext`, detects
  `127.0.0.1` is private, and aborts

### Requirement: graceful-shutdown

The daemon SHALL handle SIGINT / SIGTERM by:
1. Stopping the poll loop ticker (no new work enqueued).
2. Cancelling the cycle context, which terminates in-flight HTTP requests via
   the `http.Client.Timeout` and the per-cycle timeout. Rows whose fetch
   completed but whose batched UPSERT was not yet committed are discarded (the
   next discovery tick will re-pick them up via `last_pool_update_id`).
3. Closing the database connection pool cleanly.
4. Stopping the health HTTP server.
5. Exiting with code 0.

### Requirement: health-endpoints

The daemon SHALL expose on `fetch.health.listen_addr:fetch.health.listen_port`:
- `GET /healthz` — always 200 OK with body `{"status":"ok"}` while the process is alive
- `GET /readyz` — 200 OK if the last DB ping succeeded within
  `fetch.health.readyz_db_grace_seconds`, else 503 with body
  `{"status":"degraded","reason":"db_unreachable","last_ping_age_seconds":N}`
- `GET /debug/pools` — registered but returns 404 unless
  `fetch.work_queue.expose_debug_endpoint: true`. When enabled, returns JSON of
  the most recent cycle summary plus `recent_failures` (per-pool retry state with
  `attempts` and `next_delay_s`). The 404 response when disabled prevents the
  route from leaking existence.

#### Scenario: database unreachable
- **WHEN** the most recent DB ping is older than `readyz_db_grace_seconds`
- **THEN** `/readyz` returns 503

### Requirement: live-reload

The daemon SHALL handle SIGHUP by:
1. Re-reading the config file from disk.
2. Applying tunables that do not require a restart: `poll_interval_seconds`,
   `min_poll_interval_seconds`, `failed_pool_retry_seconds`, `concurrency`,
   `batch_size`, `write_batch_size`, `expose_debug_endpoint`,
   `pool_id_allow_list` / `pool_id_deny_list`, `max_redirects`, `user_agent`,
   `extra_headers`, all `fetch.validation.*` knobs, `fetch.private_ip_policy`,
   `fetch.private_ip_allow_list`, `fetch.retry.max_attempts`,
   `fetch.pools_overrides`, and all `logging.*` knobs.
3. Logging a diff of changed values: `event=config_reloaded changed=[...]`.
4. NOT applying tunables that require a restart: `database_url`, all
   `database.pool_*` knobs, `health.listen_addr`, `health.listen_port`,
   `network_name`, `schema_name`. These changes are logged but ignored; the
   operator must restart the daemon.

#### Scenario: SIGHUP with valid updated config
- **WHEN** the daemon is running and the operator updates `poll_interval_seconds: 300 →
  600` in the config file and sends SIGHUP
- **THEN** the next tick occurs 600 seconds after the previous tick, and the daemon logs
  `event=config_reloaded changed=[poll_interval_seconds: 300 → 600]`

### Requirement: retry-policy

The daemon SHALL implement per-pool retry counters persisted in
`grest.pool_offchain_metadata.consecutive_failures`. After each failed fetch the
daemon sets `next_attempt_at = now() + base * 2^consecutive_failures`, where
`base = fetch.poll.failed_pool_retry_seconds` (default 60 s) and
`consecutive_failures` is read pre-increment in the same SQL UPDATE.

The exponent is clamped to `[0, 10]` (`LEAST(consecutive_failures, 10)`), so the
maximum delay is `base * 2^10` (= `1024 * base`, about 17 hours at the default
60 s base). The discovery query picks the pool back up when
`next_attempt_at <= now()`. A successful fetch (`UpsertBatch`) resets
`consecutive_failures = 0` and `next_attempt_at = NULL`, returning the pool to
the normal `poll_interval_seconds` cadence.

`MarkPoolAttempted` is an UPSERT: if no row exists yet (brand-new pool whose
first attempt failed), it creates a placeholder row with the URL but NULL
`meta_hash` / `meta_json` and `is_valid = FALSE`. This ensures the discovery
query picks the pool back up on the failure cadence (via the
`next_attempt_at <= now()` predicate) rather than waiting a full
`poll_interval_seconds` for the success-cadence predicate. A subsequent
successful fetch fills the payload and flips `is_valid` to TRUE via
`UpsertBatch`. See `pool-offchain-metadata-cache` §schema for the full
placeholder-row contract.

`fetch.retry.max_attempts` (default 10) is an in-memory cap: once
`consecutive_failures >= max_attempts` the pool is skipped at the start of each
cycle and not retried until the next successful fetch (or daemon restart).

#### Scenario: pool fails 3 times then succeeds
- **WHEN** pool X fails 3 consecutive fetches then succeeds on the 4th, with
  `failed_pool_retry_seconds = 60`
- **THEN** the backoff delays were 60s, 120s, 240s (n=0,1,2). If the first
  failure created a placeholder row in `grest.pool_offchain_metadata`
  (`meta_hash`/`meta_json` NULL, `is_valid = FALSE`), the 4th successful fetch
  populates the payload and sets `is_valid = TRUE`, `consecutive_failures = 0`,
  `next_attempt_at = NULL` via `UpsertBatch`.

### Requirement: per-pool-overrides

The daemon SHALL support `fetch.pools_overrides` (list of `{match, overrides}` records)
for fine-tuning individual pools. Supported override keys per pool:
- `max_body_bytes`
- `timeout_seconds`
- `user_agent`
- `private_ip_policy` (override global)
- `enabled` (boolean — override global `fetch.enabled` per pool)

#### Scenario: matched override applied
- **WHEN** `pools_overrides` contains
  `{match: "pool1abc...exact_bech32", overrides: {max_body_bytes: 1048576}}`
- **THEN** pool `pool1abc...` accepts bodies up to 1 MiB; all other pools use the
  global default
