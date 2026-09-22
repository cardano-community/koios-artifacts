# Proposal: add-pool-offchain-fetcher

## Why

`cardano-db-sync` 13.7.x ships `cardano-smash-server` (`~/git/cardano-db-sync/cardano-smash-server`)
as a Haskell-based off-chain pool metadata fetcher, gated by `insert_options.offchain_pool_data`.
Years of upstream issues against it have not produced the operational improvements the Koios team
needs (better tuning surface, faster failure recovery, simpler deployment, no SMASH policy bloat).

We want to:

1. **Replace** the Haskell fetcher with a small Go daemon we own.
2. **Stop** relying on `public.off_chain_pool_data` (which depends on db-sync's runtime
   setting, is gated behind a config flag, and silently breaks when the flag is off).
3. Instead, persist the *current* raw metadata in `grest.pool_offchain_metadata` — one row
   per pool, including the `meta_hash` of what we fetched (for live verification).
4. **Repoint** the existing Koios RPCs (`grest.pool_info`, `grest.pool_list`,
   `grest.pool_metadata`) to read `meta_json` from the new grest table. Public API
   contract is unchanged (no new columns, no type changes); only the data source moves.

## What changes

| Capability | Δ | Summary |
|---|---|---|
| `pool-offchain-metadata-cache` | **ADDED** | New `grest.pool_offchain_metadata` table (id, pool_id, meta_url, meta_hash, meta_json, meta_bytes, last_polled, last_pool_update_id, is_valid). |
| `pool-offchain-fetcher` | **ADDED** | Go daemon `koios-pool-meta-fetcher`: poll loop, worker pool, Blake2b-256 hashing, private-IP blocking, health endpoints. |
| `pool-meta-fetcher-config` | **ADDED** | YAML configuration schema — addresses the dbsync/SMASH painpoints with tunable knobs. |
| `pool-rpc-repointing` | **MODIFIED** | `pool_info`/`pool_list`/`pool_metadata` JOINs the new table; no API shape change. |

## Impact

- **API**: zero breaking changes. `meta_json`, `meta_url`, `meta_hash`, `ticker` columns keep
  their meaning and shape. No new columns are added. Source-of-joins moves internally only.
- **Ops**: operators disable db-sync's own pool fetcher
  (`insert_options.offchain_pool_data: disable`) and run the new daemon under systemd
  (or in Docker).
- **DB**: adds one table (~3000 rows on mainnet, ~24 MiB at 8 KiB avg payload).
- **Out of scope**:
  - SMASH's policy endpoints (`/delist`, `/tickers`, `/policies`, `/errors`, BasicAuth
    admin surface) — grest layer never exposed these; we do not add them.
  - Historical fetches (only current; overwrites in place). `grest.pool_updates` is
    therefore NOT repointed — it continues to read `public.off_chain_pool_data` for
    historical `meta_json` lookups.
  - On-chain pool update tracker (still owned by db-sync → `pool_info_cache`).
  - Cardano ledger / consensus logic — pure URL fetcher.
