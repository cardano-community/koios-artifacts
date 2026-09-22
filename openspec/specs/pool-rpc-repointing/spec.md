# Capability: pool-rpc-repointing

## Purpose

Repoint the existing off-chain metadata joins in the pool-related RPCs
(`grest.pool_info`, `grest.pool_list`, `grest.pool_metadata`) from
`public.off_chain_pool_data` to the new `grest.pool_offchain_metadata` table produced
by `pool-offchain-fetcher`. Preserve the public-facing response shape exactly
(`meta_json`, `meta_url`, `meta_hash`, `ticker` columns keep their meaning and types).
**No new columns are added.**

This is the final capability in the rollout chain. See `design.md` §14 for ordering.

## MODIFIED Requirements

### Requirement: grest-pool-info-meta-json-repointed

The function `grest.pool_info(_pool_bech32_ids text[])` SHALL source its `meta_json`
output column by LEFT JOINing `grest.pool_offchain_metadata AS pom ON pom.pool_id =
pic.pool_hash_id`. The existing join to `public.off_chain_pool_data AS ocpd ON
api.meta_id = ocpd.pmr_id` SHALL be removed.

`meta_url` and `meta_hash` SHALL continue to be sourced from `public.pool_metadata_ref`
JOINed via `public.pool_update` (i.e. reflecting the **on-chain declared** URL/hash,
not what was actually fetched).

When `pom` is NULL (no row for the pool), `meta_json` SHALL be NULL.

#### Scenario: cached metadata
- **WHEN** `_pool_bech32_ids` contains pool X and a row exists in
  `grest.pool_offchain_metadata` for X
- **THEN** the response row for X contains:
  - `meta_url` = `pmr.url` (on-chain declared)
  - `meta_hash` = `pmr.hash` (on-chain declared)
  - `meta_json` = `pom.meta_json` (fetched payload, regardless of hash agreement)

#### Scenario: no cached metadata
- **WHEN** `_pool_bech32_ids` contains pool Y and no row exists in
  `grest.pool_offchain_metadata` for Y
- **THEN** the response row for Y contains:
  - `meta_url` = `pmr.url` (or NULL if pool has never declared one)
  - `meta_hash` = `pmr.hash` (or NULL)
  - `meta_json` = NULL

#### Scenario: response shape compatibility
- **WHEN** an old client from before this change sends the same request
- **THEN** every previously-existing column has the same value and type. No 4xx/5xx
  errors. The response shape is byte-identical to the pre-change shape.

### Requirement: grest-pool-info-no-new-columns

The RETURNS TABLE for `grest.pool_info` SHALL NOT gain any new columns. No rename,
removal, or type change is allowed on existing columns either. The only accepted diff
is the source of `meta_json` (changed from `ocpd.json` to `pom.meta_json`) and the
removal of the `public.off_chain_pool_data` JOIN.

#### Scenario: zero new columns
- **WHEN** the change is applied
- **THEN** `grest.pool_info` returns the same 29 columns in the same order with the
  same types as before

### Requirement: grest-pool-metadata-repointed

The function `grest.pool_metadata(_pool_bech32_ids text[] DEFAULT NULL)` SHALL source
its `meta_json` output column by LEFT JOINing `grest.pool_offchain_metadata AS pom ON
pom.pool_id = ph.id` instead of `public.off_chain_pool_data AS ocpd ON ocpd.pool_id =
ph.id`.

`meta_url` and `meta_hash` SHALL continue to be sourced from `public.pool_metadata_ref`
JOINed via `public.pool_update` (UNCHANGED). The DISTINCT ON / ORDER BY clauses SHALL
be preserved. No new columns SHALL be added.

#### Scenario: parity with old shape
- **WHEN** `_pool_bech32_ids` is non-NULL
- **THEN** every previously-existing column (`pool_id_bech32`, `meta_url`, `meta_hash`,
  `meta_json`) has the same type and meaning. The DISTINCT ON behaviour (latest
  `pmr.registered_tx_id` first) is preserved.

### Requirement: grest-pool-list-ticker-repointed

Both overloads of `grest.pool_list(...)` (`_lovelace_numeric boolean DEFAULT NULL` and
`_lovelace_numeric boolean`) SHALL source the `ticker varchar` column from
`grest.pool_offchain_metadata.meta_json->>'ticker'` (when the row exists) instead of
`public.off_chain_pool_data.ticker_name`. When `pom` is NULL, `ticker` SHALL be NULL.

#### Scenario: ticker from fetched payload
- **WHEN** pool X has a row in `grest.pool_offchain_metadata` and `pom.meta_json` has
  `{"ticker": "ABC", ...}`
- **THEN** the `ticker` column for X equals `'ABC'`

#### Scenario: no cached metadata
- **WHEN** pool Y has no row in `grest.pool_offchain_metadata`
- **THEN** `ticker` for Y is NULL (no longer falling back to `ocpd.ticker_name`)

## REMOVED Requirements

### Requirement: removal-of-public-off-chain-pool-data-joins

After this capability rolls out and a single full poll cycle has elapsed (~10 minutes),
no `grest.*` RPC used for **current-state** queries SHALL reference
`public.off_chain_pool_data` in its JOINs. The following files SHALL be updated:

- `files/grest/rpc/pool/pool_info.sql`
- `files/grest/rpc/pool/pool_list.sql`
- `files/grest/rpc/pool/pool_metadata.sql`

#### Scenario: reference check
- **WHEN** the rollout completes
- **THEN** `grep -l 'off_chain_pool_data' files/grest/rpc/pool/pool_info.sql
  files/grest/rpc/pool/pool_list.sql files/grest/rpc/pool/pool_metadata.sql` returns
  zero matches

### Requirement: pool-updates-out-of-scope

The function `grest.pool_updates(...)` SHALL continue to source `meta_json` from
`public.off_chain_pool_data`. It is intentionally **out of scope** for this proposal
because it returns **historical** metadata at the time of each registration cert, while
`grest.pool_offchain_metadata` only stores the **current** payload.

Consumers calling `grest.pool_updates` with a focus on `meta_json` for recent
registrations will see NULLs once db-sync's off-chain fetcher is disabled. Two
acceptable resolutions for operators:

1. Keep `insert_options.offchain_pool_data: enable` in db-sync (only the new
   `grest.pool_offchain_metadata` table is read by the current-state RPCs; db-sync
   continues to write to its own table for backwards-compatible historical queries).
2. Disable db-sync's fetcher AND backfill `public.off_chain_pool_data` from
   `grest.pool_offchain_metadata` on rollout (one-time migration script, out of scope
   here).

The default rollout plan in `design.md` §14 keeps db-sync's fetcher enabled for
graceful coexistence.

#### Scenario: pool_updates reads ocpd (unchanged)
- **WHEN** `grest.pool_updates(_pool_bech32)` is called and db-sync OCPD is still
  enabled
- **THEN** historical `meta_json` values are returned, identical to pre-change
  behaviour

### Requirement: no-hash-verification-rpc

There SHALL be NO public column in any `grest.*` RPC that surfaces a live
`fetched-vs-declared` hash comparison. Specifically:
- `grest.pool_info` SHALL NOT gain a `meta_hash_match` column (or any equivalent).
- `grest.pool_metadata` SHALL NOT gain a `meta_hash_match` column.
- `grest.pool_list` SHALL NOT gain a `meta_hash_match` column.

If a future capability chooses to expose such a check, it SHALL be a brand-new RPC
(e.g. `grest.pool_metadata_status`) rather than a column addition to existing RPCs.
This deliberate restriction keeps the API surface stable and prevents accidental
coupling of the verification story to the data-fetch story.

#### Scenario: post-rollout api surface
- **WHEN** the rollout is complete
- **THEN** `git diff main -- files/grest/rpc/pool/` shows only changes to source-of-joins
  for `meta_json` / `meta_url` / `meta_hash` / `ticker` columns, with zero new columns
  added to any RETURNS TABLE
