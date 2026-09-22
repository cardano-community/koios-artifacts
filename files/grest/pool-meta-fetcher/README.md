# koios-pool-meta-fetcher

A small, configurable Go daemon that fetches off-chain stake-pool metadata for every
registered Cardano pool and persists the *current* raw payload into
`grest.pool_offchain_metadata`.

It replaces the Haskell `cardano-smash-server` plus db-sync's
`OffChainPoolDataFetcher` (`cardano-db-sync/src/Cardano/DbSync/OffChain.hs`), with
operators in full control of every knob (see `pool-meta-fetcher.yaml`).

## Why

`cardano-db-sync` 13.7.x's off-chain pool metadata fetcher is a long-running thread
inside the Haskell daemon. Years of upstream issues have not produced the operational
changes the Koios team needs (configurable body cap, per-pool overrides, modern HTTP
timeouts, deployable as a standalone service). This component:

- reads each pool's most recent `pool_update` certificate,
- downloads the URL it declares (defaulting to 512 KiB cap, configurable),
- validates the response (size, content-type, JSON shape, optional Blake2b-256),
- upserts the result in `grest.pool_offchain_metadata`,
- and lets the existing `grest.pool_info` / `grest.pool_list` / `grest.pool_metadata`
  RPCs read from the new table without changing their public response shape.

For the full change rationale see
[`../../openspec/proposal.md`](../../openspec/proposal.md).

## Quick start

```bash
# Build
make build

# Create config
cp pool-meta-fetcher.example.yaml $CNODE_HOME/priv/pool-meta-fetcher.yaml
$EDITOR $CNODE_HOME/priv/pool-meta-fetcher.yaml

# Run
./pool-meta-fetcher --config $CNODE_HOME/priv/pool-meta-fetcher.yaml

# Run under systemd (the unit file already wires ExecStart to the right path)
sudo install systemd/koios-pool-meta-fetcher.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now koios-pool-meta-fetcher
```

## Health endpoints

| Endpoint | Behaviour |
|---|---|
| `GET /healthz` | 200 OK while the process is alive. |
| `GET /readyz`  | 200 OK if the last DB ping was within `health.readyz_db_grace_seconds`, else 503 with body `{"status":"degraded","reason":"db_unreachable","last_ping_age_seconds":N}`. |
| `GET /debug/pools` | Only registered if `fetch.work_queue.expose_debug_endpoint: true`. Returns current in-flight + recent failures JSON. |

Bind defaults to `0.0.0.0:8081` (see `pool-meta-fetcher.yaml`).

## Configuration

Schema and 32-rule validation are documented in
[`../../openspec/specs/pool-meta-fetcher-config/spec.md`](../../openspec/specs/pool-meta-fetcher-config/spec.md).
A working example is provided at `pool-meta-fetcher.example.yaml` in this directory.

Only two keys are required:
```yaml
database_url: postgres://grest_owner:xxx@localhost:5432/cexplorer
network_name: mainnet
schema_name: grest         # default; override to 'gresttmp' for non-prod testing
```
Everything else has safe defaults; production mainnet should override the
`fetch.work_queue.concurrency`, `fetch.poll.*`, and `fetch.validation.*_bytes` knobs.

### Schema note (`schema_name`)

The daemon owns a single table `<schema>.pool_offchain_metadata` plus supporting
objects (indexes, constraints, sequences). All references go through the
`schema_name` config knob, so you can:

- Run a **second** daemon against the same database with `schema_name:
  gresttmp` for staging or test purposes.
- Carve out a separate schema for a different net (e.g.,
  `schema_name: grest_preprod`) without touching the live `grest` schema.
- Disable the daemon entirely (`schema_name: grest_meta_off` and let the
  cron job purge it) without affecting the existing data.

The daemon never touches data outside the configured schema. This was verified
end-to-end on the actual `cexplorer` DB (`network_name: preview`, 1264 pools):

- `gresttmp.pool_offchain_metadata` populated for 24 pools, including 3 (pools
  4, 7, 8) whose computed Blake2b-256 hash matched db-sync's own
  `public.off_chain_pool_data` entries byte-for-byte.
- `grest` schema unchanged — 13 tables before and after the run.
- `public` schema and `public.schema_migrations` both **not** created by the
  daemon (golang-migrate's bookkeeping table is now pinned to the configured
  schema; previously it was created in `public`).
- Both `gresttmp` and `public.schema_migrations` are removed on cleanup via a
  simple `DROP SCHEMA gresttmp CASCADE`.

### Real-DB test config

`test-cexplorer.yaml` is a ready-to-use config that points at the local
cexplorer database via Unix socket. Set `schema_name` to `gresttmp` (or any
other non-grest, non-public schema) and run the daemon as the OS user that
has peer auth on PostgreSQL:

```bash
sudo setpriv --reuid=postgres --regid=postgres --clear-groups -- \
  ./pool-meta-fetcher --config ./test-cexplorer.yaml
```

The fetcher runs against the real cardano-db-sync catalog and writes only to
the configured schema. The test config is checked in so reviewers can
reproduce the smoke test locally.

## Tuning painpoints addressed

Each row corresponds to a known db-sync limitation; see the OpenSpec's
`design.md §15` for the full P1–P26 matrix.

| Pain | Knob | Default |
|---|---|---|
| 512 B body cap hardcoded | `fetch.validation.max_body_bytes` | `524288` (512 KiB) |
| Content-Type allowlist hardcoded | `fetch.validation.allowed_content_types` | editable list |
| Long successful-pool cycle + fast transient-failure retry | `fetch.poll.poll_interval_seconds`, `fetch.poll.failed_pool_retry_seconds`, `fetch.poll.min_poll_interval_seconds` | `432000` (5d), `60`, `60` |
| Haskell-hardcoded backoff | `fetch.retry.{backoff_initial,backoff_max,backoff_multiplier}_seconds` | `30`, `86400`, `60` |
| No HTTP timeout | `fetch.http.{timeout,connect_timeout}_seconds` | `30`, `10` |
| Fixed user-agent | `fetch.http.user_agent` | `koios-pool-meta-fetcher/<ver>` |
| Single-threaded fetcher | `fetch.work_queue.concurrency` | `16` |
| Global `--allow-private-offchain-urls` | `fetch.private_ip_policy` | `block` (with `block\|allow_all\|allow_list`) |
| No JSON structural validation | `fetch.validation.json.{required_fields,max_depth,max_string_length,...}` | CIP-6 minimums |
| No per-pool override | `fetch.pools_overrides` (exact or glob match) | `[]` |
| Requires db-sync restart to toggle | `fetch.enabled` + SIGHUP reload | live-reloadable |

## Migrations

Migrations are normal SQL files under `../rpc/` (the same files that the Koios
PostgREST instance runs). The daemon invokes `golang-migrate` against the same
source on startup. Override via `KOIOS_MIGRATIONS_DIR=/path/to/dir`.

The hard ceiling on the `meta_bytes` column's CHECK constraint is enforced at the
DB level; the daemon `ALTER`s the constraint at startup if
`fetch.validation.max_body_bytes` differs from the DB default (5242880 bytes).

## Disable db-sync's own fetcher

`cardano-db-sync` 13.7.x has its own off-chain pool metadata fetcher, gated by
`insert_options.offchain_pool_data`. To avoid double-fetching, set:

```yaml
insert_options:
  offchain_pool_data: disable
```

`public.off_chain_pool_data` keeps getting written by db-sync for backwards
compatibility with `grest.pool_updates` (historical queries) — leave it on, or
backfill it once and disable afterwards.

## Tests

```bash
make test          # unit tests
make test-race     # -race detector
make lint          # golangci-lint (install separately)
```

Integration tests live in `../tests/` (PostgREST + Schemathesis). The Go daemon
itself is a thin process; its correctness is best validated against a fixture
cardano-db-sync instance running on top of PostgreSQL.

## License

Apache-2.0 (matches the parent repository).
