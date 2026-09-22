# Tasks

## Phase 0 — OpenSpec scaffolding (this PR)

- [x] Create `openspec/` directory in `koios-artifacts`
- [x] Write `proposal.md`, `tasks.md`, `design.md`
- [x] Write capability `spec.md` files under `openspec/specs/`:
  - `pool-offchain-metadata-cache/spec.md`
  - `pool-offchain-fetcher/spec.md`
  - `pool-meta-fetcher-config/spec.md`
  - `pool-rpc-repointing/spec.md`
- [ ] Validate via `openspec validate add-pool-offchain-fetcher --strict` (once tool installed)
- [ ] Brainstorm config painpoints and lock schema (user review checkpoint)

## Phase 1 — SQL migrations (`files/grest/rpc/`)

- [ ] Create `00_cached_tables/pool_offchain_metadata.sql`
  - DDL for `grest.pool_offchain_metadata` (id, pool_id, meta_url, meta_hash, meta_json,
    meta_bytes, last_polled, last_pool_update_id, is_valid)
  - CHECK constraint on `OCTET_LENGTH(meta_bytes)`
  - Indexes (`last_polled DESC`, partial on `is_valid = FALSE`, on `last_pool_update_id`)
  - GRANTs to `web_anon` (SELECT) and a new role `grest_owner` (ALL)
- [ ] Add `grest_owner` role + ALTER DEFAULT PRIVILEGES in `db-scripts/basics.sql`
- [ ] Modify `pool/pool_info.sql` — LEFT JOIN new table for `meta_json`; **no new columns**
- [ ] Modify `pool/pool_list.sql` (×2 overloads) — `ticker` from `meta_json->>'ticker'`
- [ ] Modify `pool/pool_metadata.sql` — LEFT JOIN new table for `meta_json`
- [ ] Note: `pool/pool_updates.sql` is intentionally NOT modified (historical data source)
- [ ] Migration runbook (`MIGRATION.md`): step-by-step rollout / rollback

## Phase 2 — Go daemon skeleton (`files/grest/pool-meta-fetcher/`)

- [ ] `go mod init github.com/cardano-community/koios-artifacts/pool-meta-fetcher`
- [ ] `cmd/pool-meta-fetcher/main.go` — flag parsing, signal handling, lifecycle
- [ ] `internal/config/config.go` — YAML loader, defaults, cross-field validation
- [ ] `internal/storage/postgres.go` — pgxpool, prepared statements, upsert, work-queue query
- [ ] `internal/health/health.go` — `/healthz`, `/readyz` server

## Phase 3 — Fetcher implementation

- [ ] `internal/ipfilter/ipfilter.go` — DNS-resolve + deny list + custom `DialContext`
- [ ] `internal/fetcher/pool.go` — per-pool work item, in-memory retry counter, backoff
- [ ] `internal/fetcher/fetcher.go` — cycle loop, worker pool, batched result aggregation
- [ ] Hash validation using `golang.org/x/crypto/blake2b` (Size=256)
- [ ] Configurable response validators: size, content-type, JSON required fields, depth,
  string length, hash check (toggle)
- [ ] Per-pool override support (`pools_overrides` list in config)

## Phase 4 — Observability & ops

- [ ] Structured logging (`log/slog`, JSON or text, configurable)
- [ ] Dockerfile (multi-stage, distroless base)
- [ ] systemd unit (`systemd/koios-pool-meta-fetcher.service`)
- [ ] README.md with install/run/debug/log interpretation
- [ ] Makefile targets: `build`, `test`, `lint`, `docker`, `run`, `migrate`

## Phase 5 — Tests

- [ ] Unit tests for config validation (every default, every override, every invalid input)
- [ ] Unit tests for IP filter (RFC1918 + IPv6 ULAs + DNS-rebind scenario)
- [ ] Unit tests for hash validator (positive + negative vectors from cardano-db-sync tests)
- [ ] Unit tests for JSON validators (missing fields, oversized strings, depth limit)
- [ ] Integration test: docker-compose with PostgreSQL, run daemon against fixture pool data
- [ ] Golden-file tests for response shapes (`specs/createspecs.py --check` must pass)
- [ ] Regression: existing schemathesis suite in `tests/` must pass against repointed RPCs

## Phase 6 — Docs & CI

- [ ] Update `AGENT.md` `00_cached_tables` section to reference new table
- [ ] Add `## Pool metadata fetching` section to `README.md` and a sub-doc
- [ ] Note: operators must set `insert_options.offchain_pool_data: disable` in db-sync config
- [ ] Add Go build lane to `.github/workflows/`

## Phase 7 — Validation gates (must all pass before merge)

- [ ] `openspec validate add-pool-offchain-fetcher --strict` passes
- [ ] All unit + integration tests green
- [ ] `go test -race ./...` passes
- [ ] `golangci-lint run` passes
- [ ] `python3 specs/createspecs.py --check` passes (response shapes unchanged)
- [ ] `pytest -q tests/test_pool_account_endpoints.py` passes against a live instance
- [ ] Manual: spin up docker-compose stack, observe daemon populate `grest.pool_offchain_metadata`
  for a fixture pool set within one poll cycle
