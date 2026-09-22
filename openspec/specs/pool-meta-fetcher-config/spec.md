# Capability: pool-meta-fetcher-config

## Purpose

A single YAML file (`pool-meta-fetcher.yaml`) that controls every tunable knob of the
`koios-pool-meta-fetcher` daemon. The schema below is the user-visible surface area for
operator negotiation — every field addresses a specific pain in `cardano-db-sync` 13.7.x's
off-chain fetcher (`cardano-smash-server` + `Cardano/DbSync/OffChain/*`). Painpoint
references (P1–P26) map to §15 of `design.md`.

The schema is deliberately **over-engineered** relative to db-sync: we are no longer bound
by its constraints and want operators to have full control.

## Requirements

### Requirement: file-format

The daemon SHALL accept the config path via `--config PATH` (default
`/etc/koios/pool-meta-fetcher.yaml`). The file SHALL be YAML 1.2, parsed strictly
(`yaml.v3:UnmarshalStrict`). Unknown top-level keys SHALL be rejected at startup with a
clear error pointing at the offending line.

#### Scenario: unknown key
- **WHEN** the config contains `fetch.totally_made_up_knob: 42`
- **THEN** the daemon logs `level=error event=config_validation_failed
  reason="unknown field" path=fetch.totally_made_up_knob` and exits with code 2

### Requirement: required-top-level-keys

The following top-level keys SHALL be required (no defaults — must be set explicitly):

| Key | Type | Description |
|---|---|---|
| `database_url` | string | PostgreSQL connection string (URL or libpq form). Used by both `pgxpool` and `golang-migrate`. |
| `network_name` | enum | One of `mainnet`, `preprod`, `preview`, `sanchonet`. Used only for logging context. |

A config missing either SHALL cause the daemon to exit with code 2 within 1 second.

### Requirement: top-level-structure

The full top-level structure SHALL be:

```yaml
database_url:                # required
network_name:                # required, enum

fetch:
  enabled:                  # bool
  poll:                     # poll cadence
  work_queue:               # concurrency, batch sizing, allow/deny lists
  http:                     # HTTP transport tuning
  validation:               # response validators
  private_ip_policy:        # enum: block|allow_all|allow_list
  private_ip_allow_list:    # []string
  retry:                    # backoff curve
  pools_overrides:          # per-pool tuning (list)

database:                   # pgxpool tuning
health:                     # /healthz + /readyz
logging:                    # level + format
```

#### Scenario: minimum valid config
- **WHEN** the config is exactly:
  ```yaml
  database_url: postgres://grest_owner:xxx@localhost:5432/cexplorer
  network_name: mainnet
  ```
- **THEN** the daemon starts with all defaults applied, logs the resolved config at
  `level=debug`, and runs the poll loop

### Requirement: fetch-enabled

| Field | Type | Default | Painpoint |
|---|---|---|---|
| `fetch.enabled` | bool | `true` | P15 |

When `false`, the daemon exits with code 0 immediately after config load (skipping DB
connect, poll loop, etc). This is the "stop fetching" knob — distinct from `kill -STOP` at
the OS level because it also stops the DB poll query.

#### Scenario: soft disable
- **WHEN** the operator sets `fetch.enabled: false` and restarts the daemon
- **THEN** the daemon logs `event=disabled reason=fetch.enabled=false`, does NOT connect
  to the DB, and exits 0 after `health.listen_port` is bound (so `/healthz` keeps
  reporting OK while fetching is off)

### Requirement: fetch-poll

| Field | Type | Default | Range | Painpoint | Notes |
|---|---|---|---|---|---|
| `fetch.poll.poll_interval_seconds` | int | `300` | `> 0` | P3 | Master tick — 5 min matches db-sync |
| `fetch.poll.min_poll_interval_seconds` | int | `60` | `> 0`, `<= poll_interval_seconds` | P3 | Per-pool floor — even fresh updates won't re-fetch faster than this |
| `fetch.poll.failed_pool_retry_seconds` | int | `60` | `> 0`, `<= poll_interval_seconds` | P4 | Base interval for the failed-pool exponential backoff curve. Doubles each consecutive failure (see §fetch-retry). |

#### Scenario: fresh-update throttling
- **WHEN** pool X submits a new pool_update, the daemon picks it up on the next tick,
  fetches it, writes the row, and 30 seconds later another pool_update from X arrives
- **THEN** the second pool_update is in the work queue but the daemon skips it (last
  fetch was only 30 s ago, below `min_poll_interval_seconds = 60`)

#### Scenario: validation
- **WHEN** `poll_interval_seconds: 100` and `min_poll_interval_seconds: 200`
- **THEN** the daemon exits 2 with `error=invalid_config field=fetch.poll.min_poll_interval_seconds
  reason="must be <= poll_interval_seconds (got 200 > 100)"`

### Requirement: fetch-work-queue

| Field | Type | Default | Painpoint | Notes |
|---|---|---|---|---|
| `fetch.work_queue.concurrency` | int | `16` | P8 | Parallel HTTP workers |
| `fetch.work_queue.batch_size` | int | `100` | P7 | DB query batch size |
| `fetch.work_queue.max_inflight` | int | `200` | P22 | Hard cap on bounded channel — never exceeds this even with backlog |
| `fetch.work_queue.write_batch_size` | int | `50` | P23 | UPSERT batch size per cycle |
| `fetch.work_queue.pool_id_allow_list` | []string | `[]` | P13 | Pool bech32s only — empty means "all pools eligible" |
| `fetch.work_queue.pool_id_deny_list` | []string | `[]` | P13 | Excluded pools — applied after allow_list |
| `fetch.work_queue.expose_debug_endpoint` | bool | `false` | P24 | If true, register `/debug/pools` |

#### Scenario: excluded pool
- **WHEN** `pool_id_deny_list: ["pool1abc...bech32", "pool1def...bech32"]`
- **THEN** the work-queue query filters these out and the daemon never requests them,
  logging them once at startup at `level=info event=pools_excluded count=2`

#### Scenario: oversized deny_list
- **WHEN** `pool_id_deny_list` has > 100 entries
- **THEN** the daemon logs `level=warn event=large_deny_list count=150
  hint="consider if this should be a pools_override match instead"` but still accepts
  the config

#### Scenario: allow-then-deny
- **WHEN** `pool_id_allow_list: ["pool1abc..."]` AND `pool_id_deny_list: ["pool1abc..."]`
- **THEN** the deny_list wins (the pool is excluded); the daemon logs
  `event=config_overlap pool_id=pool1abc...`

### Requirement: fetch-http

| Field | Type | Default | Painpoint | Notes |
|---|---|---|---|---|
| `fetch.http.timeout_seconds` | int | `30` | P5 | Total per-request timeout |
| `fetch.http.connect_timeout_seconds` | int | `10` | P5 | TCP+TLS handshake only |
| `fetch.http.max_redirects` | int | `3` | (related) | Cap on 3xx hops |
| `fetch.http.user_agent` | string | `koios-pool-meta-fetcher/<version>` | P6 | Sent in `User-Agent` header |
| `fetch.http.skip_tls_verify` | bool | `false` | (related) | If true, TLS verification is disabled (use only for testing). |
| `fetch.http.tls_min_version` | string | `"1.2"` | (related) | One of `1.2`, `1.3` |
| `fetch.http.http2` | bool | `true` | (related) | Enable HTTP/2 |
| `fetch.http.proxy_url` | string | `""` | (related) | Optional HTTP proxy (e.g. `http://proxy:8080`) |
| `fetch.http.extra_headers` | map[string]string | `{}` | (related) | Extra headers to attach (e.g. `Authorization`) |

#### Scenario: timeout exceeded
- **WHEN** an HTTP request stalls for 35 seconds with `timeout_seconds: 30`
- **THEN** the daemon cancels the request, logs `event=http_timeout host=Y duration_ms=30000`,
  increments per-pool retry counter, and treats this like any other failure

### Requirement: fetch-validation

| Field | Type | Default | Painpoint | Notes |
|---|---|---|---|---|
| `fetch.validation.min_body_bytes` | int | `16` | P10 | Below this, body considered empty / suspicious |
| `fetch.validation.max_body_bytes` | int | `524288` | P1 | Hard ceiling (512 KiB) — also enforced by DB CHECK |
| `fetch.validation.allowed_content_types` | []string | `["application/json", "application/ld+json", "text/plain", "application/octet-stream", "binary/octet-stream", "application/binary"]` | P2 | Content-Type allowlist (case-insensitive) |
| `fetch.validation.allow_text_html_with_brace` | bool | `true` | P2 | If true and content-type is `text/html` AND body starts with `{`, accept (legacy SPO workaround) |
| `fetch.validation.follow_cross_host_redirects` | bool | `false` | (related) | If false, 3xx to a different host aborts the request |

#### Scenario: oversized body
- **WHEN** a pool's response body is 600 000 bytes (> max_body_bytes 524288)
- **THEN** the daemon aborts the read, logs `event=size_exceeded pool_id=X size=600000
  limit=524288`, increments retry counter, treats as failure

#### Scenario: text/html legacy accepted
- **WHEN** a pool responds with Content-Type `text/html` and body starts with `{`
  (the legacy workaround) and `allow_text_html_with_brace: true`
- **THEN** the JSON parses and the row is upserted

#### Scenario: disallowed content type
- **WHEN** a pool responds with Content-Type `text/xml`
- **THEN** the daemon logs `event=content_type_rejected pool_id=X content_type=text/xml`
  and treats as failure (no upsert)

### Requirement: fetch-validation-json

| Field | Type | Default | Painpoint | Notes |
|---|---|---|---|---|
| `fetch.validation.json.required_fields` | []string | `["name", "description", "ticker", "homepage"]` | P11 | Top-level fields that must be present and non-empty (CIP-6 minimum) |
| `fetch.validation.json.max_depth` | int | `8` | P11 | JSON nesting depth cap |
| `fetch.validation.json.max_string_length` | int | `8192` | P11 | Per-field string length cap |
| `fetch.validation.json.max_keys_per_object` | int | `128` | P11 | Per-object key count cap |
| `fetch.validation.json.max_array_length` | int | `256` | P11 | Per-array length cap |
| `fetch.validation.json.allow_additional_fields` | bool | `true` | P11 | If false, missing `required_fields` is a failure |

#### Scenario: CIP-6 minimum check
- **WHEN** a pool's JSON is `{ "name": "StakeCo", "ticker": "STAKE" }`
  (missing `description`, `homepage`) and `required_fields` includes all four
- **THEN** the request fails validation, logs `event=json_missing_required
  fields=[description,homepage]`, increments retry counter

#### Scenario: JSON too deep
- **WHEN** a pool's JSON has a nested array 10 levels deep and `max_depth: 8`
- **THEN** the request fails validation, logs `event=json_depth_exceeded depth=10 limit=8`

### Requirement: fetch-validation-hash

| Field | Type | Default | Painpoint | Notes |
|---|---|---|---|---|
| `fetch.validation.hash.algorithm` | enum | `"blake2b-256"` | P16 | One of `blake2b-256` (Cardano native), `none` (disable hash computation entirely) |
| `fetch.validation.hash.log_mismatches` | bool | `true` | (design §2) | If true, hash mismatches against `pool_metadata_ref.hash` are logged ONCE per cycle at `info` level (informational; not enforced) |

This captures a `meta_hash` row column for every successful fetch. We do NOT expose a
verification column in the public RPCs (no `meta_hash_match`). The on-disk hash is
purely informational and available to operators via DB inspection or a future ops
endpoint; consumers compare against `public.pool_metadata_ref.hash` themselves if they
want. See `design.md` §2.

#### Scenario: user disables hash computation
- **WHEN** `hash.algorithm: "none"`
- **THEN** the daemon does not compute Blake2b-256 (saves ~1 µs/pool). The `meta_hash`
  column is filled with 32 zero bytes. `log_mismatches` is implicitly false.

#### Scenario: hash mismatch (informational only)
- **WHEN** a pool's JSON hashes to Z but `pool_metadata_ref.hash` is Y and
  `log_mismatches: true`
- **THEN** the row IS upserted (with `meta_hash = Z`); `event=hash_mismatch
  declared=Y actual=Z level=info` is logged ONCE per cycle (deduplicated per pool to
  avoid log spam)

### Requirement: fetch-private-ip-policy

| Field | Type | Default | Painpoint | Notes |
|---|---|---|---|---|
| `fetch.private_ip_policy` | enum | `"block"` | P9 | One of `block`, `allow_all`, `allow_list` |
| `fetch.private_ip_allow_list` | []string (pool bech32) | `[]` | P9 | Pools allowed to hit private IPs (only honored when policy is `allow_list`) |

#### Scenario: SSRF to metadata service blocked
- **WHEN** a pool's URL resolves to `169.254.169.254` (AWS metadata service)
- **THEN** the daemon aborts at `DialContext`, logs `event=ip_blocked
  ip=169.254.169.254 reason=link_local`

#### Scenario: per-pool allow
- **WHEN** `private_ip_policy: allow_list` and
  `private_ip_allow_list: ["pool1abc..."]` (a known test pool on the LAN)
- **THEN** pool `pool1abc...` is allowed to resolve to `192.168.x.x`; all other pools
  still get blocked

#### Scenario: allow_all override
- **WHEN** `private_ip_policy: allow_all` (operator chose this for dev)
- **THEN** the daemon logs ONCE at startup `level=warn event=private_ip_policy_allow_all
  reason="explicit operator choice — production deployments must use 'block'"`
  and allows all RFC1918 traffic

### Requirement: fetch-retry

| Field | Type | Default | Painpoint | Notes |
|---|---|---|---|---|
| `fetch.retry.max_attempts` | int | `10` | (design §9) | Per-pool consecutive failure cap. Implemented in-memory; excluded pools are skipped in `dispatch()` once `consecutive_failures >= max_attempts`. |
| `fetch.retry.persist_state` | bool | `false` | P17 | Reserved for v2 (would persist retry counters to `grest.pool_offchain_retry_state`). Currently has no effect. |

The backoff curve for failed pools is driven by **`fetch.poll.failed_pool_retry_seconds`**
(the base) and the row-level `consecutive_failures` counter. After each failure the
daemon sets

```
next_attempt_at = now() + base * 2^consecutive_failures
```

where `consecutive_failures` is read pre-increment in the same SQL UPDATE, so the
first failure schedules `base * 2^0 = base` seconds out. The discovery query picks
the pool back up when `next_attempt_at <= now()`.

The exponent is clamped to `[0, 10]` so the maximum delay is `base * 2^10` (about
17 hours at the default 60 s base). A successful fetch (`UpsertBatch`) resets
`consecutive_failures = 0` and `next_attempt_at = NULL`, returning the pool to
the normal `poll_interval_seconds` cadence.

#### Scenario: validation
- **WHEN** `failed_pool_retry_seconds: 100` and `poll_interval_seconds: 50`
- **THEN** the daemon exits 2 with `error=invalid_config
  field=fetch.poll.failed_pool_retry_seconds reason="must be <= poll_interval_seconds (got 100 > 50)"`

### Requirement: fetch-pools-overrides

| Field | Type | Default | Notes |
|---|---|---|---|
| `fetch.pools_overrides` | []object | `[]` | List of `{match, overrides}` records |

Each record:
```yaml
- match: "pool1abc...exact_or_glob"
  overrides:
    max_body_bytes: 1048576       # 1 MiB — e.g., for a pool with long descriptions
    timeout_seconds: 60
    user_agent: "koios-pool-meta-fetcher (override for pool1abc...)"
    private_ip_policy: allow_all  # per-pool override
    enabled: true                 # override global fetch.enabled
```

`match` matches against the pool bech32 ID. Default matching is exact string; if
`fetch.pools_overrides_match_style: glob` is set at the top level, glob patterns
(`pool1abc*`, `pool1?bc*`) are honored.

Override keys SHALL be a strict subset of the global fetch keys; unknown override keys
are rejected at startup.

#### Scenario: exact-match override
- **WHEN** `pools_overrides: [{match: "pool1abc...", overrides: {max_body_bytes: 1048576}}]`
- **THEN** only `pool1abc...` accepts bodies up to 1 MiB; all other pools use the
  global default

#### Scenario: glob override
- **WHEN** `pools_overrides_match_style: glob` and
  `pools_overrides: [{match: "pool1test*", overrides: {timeout_seconds: 120}}]`
- **THEN** any pool bech32 starting with `pool1test...` gets the longer timeout

### Requirement: database-pool

| Field | Type | Default |
|---|---|---|
| `database.pool_max_conns` | int32 | `10` |
| `database.pool_min_conns` | int32 | `1` |
| `database.pool_max_conn_lifetime` | duration string | `"1h"` |
| `database.pool_max_conn_idle_time` | duration string | `"30m"` |
| `database.statement_timeout_ms` | int | `60000` |

(Standard pgxpool knobs; durations use Go's `time.ParseDuration` syntax — `"1h30m"`,
`"45s"`, etc.)

### Requirement: health

| Field | Type | Default |
|---|---|---|
| `health.listen_addr` | string | `"0.0.0.0"` |
| `health.listen_port` | int | `8081` |
| `health.readyz_db_grace_seconds` | int | `60` |

#### Scenario: readyz degraded
- **WHEN** the most recent DB ping is older than `readyz_db_grace_seconds`
- **THEN** `/readyz` returns 503 with body
  `{"status":"degraded","reason":"db_unreachable","last_ping_age_seconds":120}`
  and `curl --fail http://localhost:8081/readyz` exits non-zero

### Requirement: logging

| Field | Type | Default | Notes |
|---|---|---|---|
| `logging.level` | enum | `"info"` | One of `debug`, `info`, `warn`, `error` |
| `logging.format` | enum | `"json"` | One of `json` (structured), `text` (human-readable) |
| `logging.add_source` | bool | `false` | If true, include `source.file:line` in each log line (debugging) |

The daemon SHALL use the standard library `log/slog` package.

#### Scenario: debug logging
- **WHEN** `logging.level: debug`
- **THEN** every HTTP request is logged with method, URL, status, duration_ms,
  bytes_read, and pool_id (when applicable)

### Requirement: validation-rules

The daemon SHALL validate the config at startup. Validation rules (full list):

1. `database_url` is non-empty and parseable by `pgxpool.ParseConfig`.
2. `network_name ∈ {mainnet, preprod, preview, sanchonet}`.
3. `fetch.poll.poll_interval_seconds > 0`.
4. `fetch.poll.min_poll_interval_seconds > 0` AND `<= poll_interval_seconds`.
4a. `fetch.poll.failed_pool_retry_seconds > 0` AND `<= poll_interval_seconds`.
5. `fetch.work_queue.concurrency >= 1`.
6. `fetch.work_queue.batch_size >= 1`.
7. `fetch.work_queue.max_inflight >= concurrency`.
8. `fetch.work_queue.write_batch_size >= 1`.
9. `fetch.http.timeout_seconds > 0` AND `>= fetch.http.connect_timeout_seconds`.
10. `fetch.http.connect_timeout_seconds > 0`.
11. `fetch.http.max_redirects >= 0`.
12. `fetch.http.tls_min_version ∈ {"1.2", "1.3"}`.
13. `fetch.validation.min_body_bytes > 0` AND `< max_body_bytes`.
14. `fetch.validation.max_body_bytes > 0` AND `<= 5242880` (5 MiB hard ceiling).
15. `fetch.validation.allowed_content_types` non-empty.
16. `fetch.validation.json.max_depth > 0`.
17. `fetch.validation.json.max_string_length > 0`.
18. `fetch.validation.hash.algorithm ∈ {"blake2b-256", "none"}`.
18a. `fetch.validation.hash.log_mismatches` is bool (no further constraints).
19. `fetch.private_ip_policy ∈ {"block", "allow_all", "allow_list"}`.
20. `fetch.retry.max_attempts >= 1`.
21. `database.pool_max_conns >= 1`.
22. `database.pool_min_conns >= 0` AND `<= pool_max_conns`.
23. `database.statement_timeout_ms > 0`.
24. `health.listen_port` in `[1, 65535]`.
25. `health.readyz_db_grace_seconds > 0`.
26. `logging.level ∈ {"debug", "info", "warn", "error"}`.
27. `logging.format ∈ {"json", "text"}`.
28. `fetch.pools_overrides_match_style ∈ {"exact", "glob"}` if set.
29. Every entry in `fetch.pools_overrides[*].overrides` has keys drawn from the
    allowlist: `{max_body_bytes, timeout_seconds, user_agent, private_ip_policy,
    enabled}`.

#### Scenario: invalid
- **WHEN** any single rule fails
- **THEN** the daemon logs the offending field, value, expected rule, and exits 2

### Requirement: live-reload

The daemon SHALL handle SIGHUP by re-reading and re-validating the config. Only the
following fields SHALL be hot-reloadable:

- `fetch.poll.poll_interval_seconds`
- `fetch.poll.min_poll_interval_seconds`
- `fetch.poll.failed_pool_retry_seconds`
- `fetch.work_queue.concurrency` (next tick uses new value)
- `fetch.work_queue.batch_size`
- `fetch.work_queue.write_batch_size`
- `fetch.work_queue.expose_debug_endpoint`
- `fetch.work_queue.pool_id_allow_list`, `pool_id_deny_list`
- `fetch.http.timeout_seconds`, `connect_timeout_seconds`, `max_redirects`,
  `user_agent`, `extra_headers`
- `fetch.validation.*` (all sub-fields)
- `fetch.private_ip_policy`, `private_ip_allow_list`
- `fetch.retry.max_attempts`, `fetch.retry.persist_state` (v2 reserved)
- `fetch.pools_overrides`, `fetch.pools_overrides_match_style`
- `logging.level`, `logging.format`, `logging.add_source`

Fields NOT hot-reloadable (require full restart):
- `database_url` — connection pool cannot be half-replaced
- `database.pool_*` — pgxpool doesn't support live reconfigure
- `health.listen_addr`, `health.listen_port` — server cannot be unbound safely
- `network_name` — affects logging only but treated as restart-required

On a SIGHUP, the daemon SHALL log a diff: `event=config_reloaded
changed=["poll_interval_seconds: 300 → 600", ...]`.

#### Scenario: SIGHUP with non-reloadable change
- **WHEN** the operator changes `database_url` and sends SIGHUP
- **THEN** the daemon logs `level=warn event=config_reload_skipped
  reason=non_reloadable_field field=database_url old=... new=...` and continues with the
  old value

## Examples

### Minimal (use all defaults)

```yaml
database_url: postgres://grest_owner:secret@localhost:5432/cexplorer?sslmode=disable
network_name: mainnet
```

### Production mainnet

```yaml
database_url: postgres://grest_owner:secret@db.internal:5432/cexplorer
network_name: mainnet

fetch:
  enabled: true
  poll:
    poll_interval_seconds: 300           # 5 min (matches db-sync default)
    min_poll_interval_seconds: 60        # don't re-fetch same pool more than once a minute
    failed_pool_retry_seconds: 60        # base for exponential backoff (see §fetch-retry)
  work_queue:
    concurrency: 32                 # mainnet has ~3000 pools; bump from default 16
    batch_size: 250
    max_inflight: 500
    write_batch_size: 100
    expose_debug_endpoint: true      # for ops visibility
  http:
    timeout_seconds: 30
    connect_timeout_seconds: 10
    max_redirects: 3
    user_agent: "koios-pool-meta-fetcher/1.0 (+https://koios.rest)"
  validation:
    min_body_bytes: 32
    max_body_bytes: 524288           # 512 KiB
    allowed_content_types: ["application/json", "application/ld+json", "text/plain"]
    allow_text_html_with_brace: true
    json:
      required_fields: ["name", "description", "ticker", "homepage"]
      max_depth: 8
      max_string_length: 8192
      max_keys_per_object: 128
      max_array_length: 256
      allow_additional_fields: true
    hash:
      algorithm: blake2b-256
      log_mismatches: true            # informational only; mismatches logged at info level
  private_ip_policy: block
  retry:
    max_attempts: 10                    # in-memory cap; excludes a pool once consecutive_failures >= max_attempts
    persist_state: false                # v1: retry state is in the grest.pool_offchain_metadata row
  pools_overrides:
    pools_overrides_match_style: exact
    - match: "pool1verylargepool...bech32"
      overrides:
        max_body_bytes: 1048576       # 1 MiB for one specific pool
        timeout_seconds: 60

database:
  pool_max_conns: 10
  pool_min_conns: 2
  pool_max_conn_lifetime: 1h
  pool_max_conn_idle_time: 15m
  statement_timeout_ms: 30000

health:
  listen_addr: "127.0.0.1"            # localhost only in production
  listen_port: 8081
  readyz_db_grace_seconds: 60

logging:
  level: info
  format: json
  add_source: false
```

### Dev / staging (private IPs allowed)

```yaml
database_url: postgres://grest_owner:dev@localhost:5432/cexplorer
network_name: preview

fetch:
  private_ip_policy: allow_all        # tests against local SPO node
  pools_overrides:
    - match: "pool1localdev...bech32"
      overrides:
        max_body_bytes: 5242880       # 5 MiB cap for one pool during testing
        timeout_seconds: 120

health:
  listen_addr: "0.0.0.0"

logging:
  level: debug
  format: text
  add_source: true
```

## Cross-references

- §15 of `design.md` lists each painpoint (P1–P26) → this spec is the resolution surface.
- `pool-offchain-fetcher/spec.md` consumes these fields and references them by name.
- `pool-offchain-metadata-cache/spec.md` is the storage target the daemon writes to.
