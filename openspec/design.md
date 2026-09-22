# Design

## 1. Single table, current state only

One row per pool, upserted in place. Matches the user's "save only current" requirement.
Mirrors db-sync's `public.off_chain_pool_data` semantics (one row per `(pool_id, pmr_id)`
tuple, with `pmr_id` provenance tracked via `last_pool_update_id`).

Why upsert-in-place rather than versioned:
- API consumers (everything in `grest.pool_*`) want the *current* view, not history.
- Disk cost is constant (~3000 pools × ~8 KiB avg = ~24 MiB).
- Auditing is delegated to `last_polled` + on-chain joinability through `last_pool_update_id`.

## 2. No hash enforcement at write time — and no live-verification column in the RPCs

The user's stated goal is to capture the *current* raw off-chain payload. Enforcing hash-match
at write time would discard SPOs who briefly misconfigure their infrastructure (deploy a buggy
metadata file, then fix it within the poll cycle).

By storing unconditionally, the API consumers see what the SPO is *actually* serving.

**We do NOT expose a live-verification column** (e.g. `meta_hash_match` on `grest.pool_info`).
That would conflate the data-fetch story with a verification story and grow the API surface.
Operators wanting to know whether the current fetched hash matches the on-chain declared hash
can JOIN themselves:

```sql
SELECT (pom.meta_hash = pmr.hash) AS hash_matches
FROM grest.pool_offchain_metadata pom
JOIN public.pool_update pu ON pu.id = pom.last_pool_update_id
JOIN public.pool_metadata_ref pmr ON pmr.id = pu.meta_id
WHERE pom.pool_id = $1;
```

We still **log** hash mismatches (info-level, deduplicated per cycle) so operators can monitor
drift.

## 3. `is_valid` semantics

`is_valid = TRUE` when the most recent fetch attempt produced a payload that:
- returned HTTP 2xx,
- had a body size within `[min_body_bytes, max_body_bytes]`,
- had an allowlisted `Content-Type` (see config spec), and
- decoded as valid JSON.

`is_valid = FALSE` is only set when we have to create a row from a *partial* success. In the
typical case, a failed fetch does NOT touch the existing row — the last known-good stays in
place. The `is_valid` column on `grest.pool_offchain_metadata` reflects the validity of *this
row's contents*, not the network health of the pool.

The `is_valid` flag is **orthogonal to hash agreement**. A row can have `is_valid = TRUE`
with a mismatched `meta_hash` — we still stored the payload, the JSON parses, etc. Hash
mismatches are info-logged but don't flip `is_valid`.

## 4. Body size cap raised from SMASH's 512 B by default

SMASH hardcodes 512 bytes (`OffChain/Http.hs:82`). Real-world pool JSON is typically 1–8 KiB
but can be larger (long descriptions, multiple social URLs).

Default proposed: **524288 bytes (512 KiB)**. Configurable both up and down. 512 B stays as
the absolute minimum allowed (so operators can re-impose SMASH parity if needed).

A PostgreSQL CHECK constraint enforces the cap at the DB level so a misbehaving daemon
can't exceed it.

## 5. Hash algorithm: Blake2b-256 (NOT SHA-256)

Cardano's `PoolMetadataHash` is Blake2b keyed-variant, 32 bytes (`cardano-crypto` +
`OffChain/Http.hs:60`). The Go implementation uses `golang.org/x/crypto/blake2b` with
`Size = 256`.

## 6. Private IP blocking at connect time (DNS-rebind safe)

SMASH blocks RFC1918 / loopback / link-local ranges by resolving the URL host first and
checking the resolved IP before `connect`. We mirror this using a custom
`http.Transport.DialContext` that:
1. Resolves the host via `net.DefaultResolver`.
2. Checks every resolved IP against the deny list (RFC1918, loopback, link-local,
   unique-local, multicast, unspecified).
3. Aborts with `event=ip_blocked` if any resolved IP is denied.
4. Connects using the vetted address.

Override: `allow_private_urls: true` (for local cluster testing; mirrors
`--allow-private-offchain-urls`).

## 7. Polling cadence: 432000 seconds (5 days) default for successful pools

Mirrors db-sync default. `poll_interval_seconds` config (default 300). The daemon ticks on
a single `time.Ticker`; per-pool work is parallelised by a worker pool sized via
`concurrency`.

**Minimum poll interval per pool**: `min_poll_interval_seconds` (default 60) — even if the
on-chain `pool_update` advances, individual pools won't be re-fetched more frequently than
this. Prevents tight-loop hammering when an SPO's node spams updates.

## 8. Pool discovery: query on every tick

```sql
SELECT
  ph.id,
  pmr.url,
  pmr.hash,
  pu.id AS pu_id
FROM public.pool_update pu
JOIN public.pool_hash   ph  ON ph.id  = pu.hash_id
JOIN public.pool_metadata_ref pmr ON pmr.id = pu.meta_id
WHERE pu.id = (SELECT MAX(id) FROM public.pool_update WHERE hash_id = pu.hash_id)
  AND pu.active_epoch_no <= (SELECT MAX(epoch_no) FROM public.epoch_param)
  -- Exclude completely retired-without-metadata pools
  AND pmr.url IS NOT NULL
  -- "needs work" filter:
  AND (
    NOT EXISTS (SELECT 1 FROM grest.pool_offchain_metadata WHERE pool_id = ph.id)
    OR grest.pool_offchain_metadata.last_pool_update_id IS DISTINCT FROM pu.id
    OR grest.pool_offchain_metadata.last_polled < now() - INTERVAL '1 second' * poll_interval_seconds
  )
```

This naturally re-fetches pools whose URL/hash changed AND pools whose row has gone stale.

## 9. Backoff strategy

Per-pool in-memory retry counter with exponential backoff:

```
delay(n) = min(
  backoff_max_seconds,
  backoff_initial_seconds + 2^n * 60
)
where n = retry_count
```

Default: `backoff_initial_seconds = 30`, `backoff_max_seconds = 86400` (24h), `max_retries
= 10`. After `max_retries`, the pool is excluded from the work queue until the daemon
restarts (operator decision).

We do NOT persist retry state across restarts in v1 — the daemon will re-encounter failed
pools on its next tick. This matches the user's "keep it simple" preference. (Upgrade path
for v2: optional `grest.pool_offchain_retry_state` table.)

## 10. Health endpoint (NOT metrics)

A separate `health` module exposes `GET /healthz` and `GET /readyz` on a configurable port
(default 8081). No Prometheus. Returns:
- `/healthz` → 200 OK with `{"status":"ok"}` while the process is alive.
- `/readyz` → 200 OK if last DB ping succeeded within `readyz_db_grace_seconds`, else 503.

The user explicitly said no metrics; health only.

## 11. Database access: pgx/v5 + pgxpool

Binary protocol, connection pool. Statements prepared at startup. No ORM. Migrations are
plain SQL files executed by `golang-migrate/migrate/v4`.

## 12. Configuration file format: YAML

Mirrors db-sync's convention (which uses YAML/JSON via `Yaml.decodeEither'`). Required
fields: `database_url`, `network_name`. Optional fields all have safe defaults (see
`specs/pool-meta-fetcher-config/spec.md`).

## 13. Deployment artifact

- Single static Go binary (`make build`) — strip+upx-compress optional via `make dist`.
- Multi-stage `Dockerfile` (distroless base).
- systemd unit (`koios-pool-meta-fetcher.service`) with `Restart=on-failure`.
- CI builds both, pushes to GHCR on tagged release.

## 14. Rollout ordering

1. Apply SQL migration creating `grest.pool_offchain_metadata` (additive — no breaking
   changes yet, existing RPCs continue reading `public.off_chain_pool_data`).
2. Deploy the Go daemon, let it populate the new table. Both data sources coexist.
3. Deploy the RPC repointing migration (now reads from new table). Existing API
   contract is preserved; only the internal data source changes.
4. Set `insert_options.offchain_pool_data: disable` in db-sync config.
5. After one full polling cycle (~10 min), `public.off_chain_pool_data` is unused by gRest.
   db-sync keeps writing to it but it's safe to leave for a grace period.
6. Rollback procedure is the reverse of the above; the new table can be dropped only
   after the RPCs are repointed back to `public.off_chain_pool_data`.

## 15. Painpoints of db-sync SMASH fetcher — addressed by which config knob

This is the brainstorm-friendly section. Every row is a concrete operator pain observed in
production with `cardano-db-sync` 13.7.x's off-chain fetcher, paired with the new config
knob that fixes it. The right-hand column deliberately *over-engineers* the surface area
on purpose — we are not bound by the SMASH constraints anymore and want operators to have
full control.

| # | Painpoint in db-sync/SMASH | Reference | Knob in `specs/pool-meta-fetcher-config` |
|---|---|---|---|
| P1 | Body size hardcoded to 512 B, rejecting large valid metadata | `OffChain/Http.hs:82` | `fetch.validation.max_body_bytes` (default 524288) + DB CHECK constraint |
| P2 | Content-Type allowlist is hardcoded with no way to extend | `OffChain/Http.hs:201-209` | `fetch.validation.allowed_content_types` (list) + `fetch.validation.allow_text_html_with_brace` |
| P3 | Successful pools cycle every `poll_interval_seconds` (default 432000 = 5 days, matching Cardano mainnet epoch cadence). Failed pools get a fast retry cadence via `failed_pool_retry_seconds` (default 60s), with exponential backoff per row (`base * 2^(count-1)`, capped at 2^10 * base). | `OffChain.hs:308` | `fetch.poll_interval_seconds` + `fetch.failed_pool_retry_seconds` + per-row `next_attempt_at` in `grest.pool_offchain_metadata` |
| P4 | Backoff curve is hand-coded in Haskell; can't tune `30 + 2^n * 60` | `OffChain/FetchQueue.hs:30-35` | `fetch.retry.backoff_initial_seconds`, `fetch.retry.backoff_max_seconds`, `fetch.retry.max_attempts` |
| P5 | No HTTP timeout configured; relies on http-client default (~30s) | `OffChain/Http.hs:84` | `fetch.http.timeout_seconds`, `fetch.http.connect_timeout_seconds` |
| P6 | User-Agent fixed and unmemorable (`cardano-db-sync/...`) | `OffChain/Http.hs:73` | `fetch.http.user_agent` (default `koios-pool-meta-fetcher/<version>`) |
| P7 | Batch size hardcoded to 100 work items per cycle | `OffChain.hs:92` | `fetch.work_queue.batch_size`, `fetch.work_queue.max_inflight` |
| P8 | Concurrency is 1 thread (single `runFetchOffChainPoolThread`) | `DbSync.hs:257-266` | `fetch.work_queue.concurrency` (default 16) |
| P9 | `--allow-private-offchain-urls` is global; cannot allow per-pool | doc/command-line-options.md:15 | `fetch.private_ip_policy` (`block` \| `allow_all` \| `allow_list`) + `fetch.private_ip_allow_list` |
| P10 | No min-body check; HTTP 200 + 0-byte body counts as success | (implicit) | `fetch.validation.min_body_bytes` |
| P11 | No JSON structural validation; trusts SPO to send valid CIP-6 | (implicit) | `fetch.validation.json.required_fields`, `fetch.validation.json.max_depth`, `fetch.validation.json.max_string_length`, `fetch.validation.json.allow_additional_fields` |
| P12 | Hash mismatch is silently logged; no operator-visible counter | `OffChain/Http.hs:60-71` | Aggregate log line per cycle: successes, hash_mismatches, size_violations, etc. (structlog) |
| P13 | No way to exclude known-problematic pools from the work queue | (absent) | `fetch.work_queue.pool_id_allow_list` / `pool_id_deny_list` (bech32 or hex) |
| P14 | No per-pool override (e.g., longer timeout for one specific pool) | (absent) | `fetch.pools_overrides` — list of `{match: pool_id_pattern, overrides: {...}}` |
| P15 | All-or-nothing disabled via single config key | `Config/Types.hs:190-208` | `fetch.enabled` (bool) + per-cycle dynamic enable/disable via SIGHUP |
| P16 | Hash algorithm hardcoded to Blake2b-256 with no migration path | `OffChain/Http.hs:60` | `fetch.validation.hash.algorithm` (`blake2b-256` default; **future**: `sha256` for non-Cardano standards) |
| P17 | Failed-fetch retry counter is persisted *per pool* in db-sync; couples retry state to db schema | `OffChain/FetchQueue.hs:25-35` | In-memory retry state (per-pool `sync.Map`); resets on daemon restart. Optional upgrade path: `fetch.retry.persist_state: true` (v2). |
| P18 | SMASH's own policy endpoints (`/delist`, `/tickers`) are part of the same service surface, hard to disable | `cardano-smash-server/src/Cardano/SMASH/Server/Api.hs:96-119` | **Removed entirely**. This proposal does not implement SMASH. Operators needing delisting use other tooling. |
| P19 | No way to override the body cap **per pool** | (absent) | `fetch.pools_overrides[*].max_body_bytes` |
| P20 | Successful fetch does NOT store the hash, only the JSON round-trip | `OffChain/Http.hs:78` | DB column `meta_hash` carries the actual fetched hash; daemon computes it unconditionally |
| P21 | `last_offchain_meta_pmr_id` is decoupled from `pool_update.id` | db-sync schema | `last_pool_update_id` in our table is the canonical provenance pointer |
| P22 | Internal worker queue holds up to 1000 entries forever if never drained | `Api.hs:366` | Bounded channel in Go (`make(chan work, N)`); never blocks the producer indefinitely |
| P23 | No way to throttle DB writes; bulk insert every 10k blocks | `Block.hs:140-146` | Per-cycle batched transaction; configurable batch size via `fetch.work_queue.write_batch_size` |
| P24 | No observability of which pools are currently in retry backoff | (absent) | `/debug/pools` endpoint (optional, behind `fetch.work_queue.expose_debug_endpoint: true`) — returns JSON of in-flight + recently-failed pools |
| P25 | Migrations not declarative; locked to db-sync release schedule | schema/ | golang-migrate with versioned SQL files; CI runs migration up+down on every PR |
| P26 | Single config key `offchain_pool_data: disable` requires db-sync restart | config docs | `fetch.enabled` reads from disk on SIGHUP; no restart needed |

This matrix IS the user-facing value proposition. The user already said the upstream issues
"hasn't worked out well" — these are the concrete things we now get to fix.

## 16. Resolved decisions (from brainstorm)

The cluster-by-cluster review yielded the following resolutions. Recording them so
the implementation phase doesn't re-litigate.

- **C1 (poll/queue sizing)**: keep all defaults as written in
  `pool-meta-fetcher-config/spec.md`. The Go implementation SHALL NOT hardcode any of
  these values — every default must be expressible in the YAML.
- **C2 (size/JSON validation)**: defaults locked, hard ceiling tightened to 5 MiB
  (`5242880`).
- **C3 (deny_list/retry/match style)**: deny_list warn at 100 entries stays (user said
  "OK to use dbsync defaults to begin with" — db-sync has no equivalent, so we keep the
  proposal defaults). Default match_style = `exact`. Retry state in-memory only in v1
  (`fetch.retry.persist_state = false`).
- **C4 (hash behaviour)**: **major change**. The `meta_hash_match` column on
  `grest.pool_info` is REMOVED. No live-verification surface is added to any public RPC.
  The `meta_hash` DB column is retained (low cost, useful for ops) but consumers compare
  against `pmr.hash` themselves if they want. The `fetch.validation.hash.enforce_match`
  knob is replaced by `fetch.validation.hash.log_mismatches` (default `true`) — purely
  informational logging.
- **C5 (/debug/pools exposure)**: not registered when `expose_debug_endpoint = false` —
  probe gets 404 (safe).
- **C6 (additions)**: no backoff jitter, no syslog/journald destination, no per-network
  overrides, no TLS client cert, no total-table-size cap.

### Remaining open questions (smaller, can be deferred)

These are minor implementation choices that don't affect the spec-level contract. They
can be resolved when each is implemented:

1. **`grest_owner` role**: separate role (proposal) vs reuse `authenticator`. We default
   to creating `grest_owner` (least privilege). Final call at PR review.
2. **CHECK constraint coupling**: hardcode `OCTET_LENGTH(meta_bytes) <= 5242880` in the
   migration, and `ALTER` on startup if `max_body_bytes` differs (the user-facing
   config knob). Implemented in `internal/storage/postgres.go`.
3. **Bcrypt for `application_users.csv`**: SMASH uses cleartext + comparison. We're
   not re-implementing SMASH auth — n/a.
