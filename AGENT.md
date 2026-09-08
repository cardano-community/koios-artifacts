Koios gREST (grest) RPC Functions Documentation

## Overview

This repository contains PostgreSQL functions that expose a public REST API (via PostgREST) for querying Cardano blockchain data. The functions are stored in the `grest` schema and serve as the backend for the Koios API, providing efficient, stateless blockchain queries.

It also contains openapi specs in specs/results folder.

### Key Architecture Principles

- **Stateless Design**: Each PostgREST instance can serve queries independently
- **Performance Optimized**: Extensive use of cached tables to minimize query time
- **Data Source**: Queries data from the `public` schema (populated by cardano-db-sync), which is documented at https://raw.githubusercontent.com/IntersectMBO/cardano-db-sync/13.7.0.1/doc/schema.md
- **CIP Standards**: Implements CIP-5, CIP-67, CIP-129 for address/asset/governance standards

---

## Directory Structure

```/dev/null/structure.md#L1-18
.github/workflows/          # Folder for github workflows
files/grest/rpc/
├── 000_utilities/          # Helper functions (CIP conversions, validators)
├── 00_cached_tables/       # Performance cache tables and update functions
├── 01_blockchain/          # Basic blockchain info (tip, genesis, totals)
├── 02_indexes/             # Additional database indexes
├── account/                # Stake account queries
├── address/                # Payment address queries
├── assets/                 # Native asset (NFT/FT) queries
├── blocks/                 # Block information queries
├── db-scripts/             # Database initialization scripts
├── epoch/                  # Epoch information queries
├── governance/             # Governance (DRep, Committee, Proposals)
├── pool/                   # Stake pool queries
├── script/                 # Smart contract script queries
└── transactions/           # Transaction queries
specs/ 
├── results/                # Output network-specific openapi specs that will be used by API layer
│   ├── koiosapi-mainnet.yaml
│   ├── koiosapi-preview.yaml
│   ├── koiosapi-preprod.yaml
├── fragments/              # Domain-split source fragments (paths, schemas, parameters, etc.)
├── examples/fixtures.yaml  # Per-network example values for parameters and request bodies
├── template.yaml           # OpenAPI skeleton merged by createspecs.py
├── networks.yaml           # Network registry and output file mapping
├── createspecs.py          # Python program to merge fragments and generate results/
└── README.md               # Contributor guide for maintaining specs
tests/                      # Run  manual schemathesis tests against instances serving postgrest API to validate specs
├── setup-tests.sh          # Script to setup python and echo schemathesis test command
└── not_empty_response.py   # A test case for schemathesis to ensure results returned are not empty
```

---

## 000_utilities

Utility functions for Cardano address/credential conversions and validations.

### CIP-129 Functions

CIP-129 defines new bech32 prefixes for Constitutional Committee and DRep credentials.

```/dev/null/cip129.md#L1-12
| Function | Description |
|----------|-------------|
| `grest.cip129_cc_hot_to_hex()` | Converts CC Hot Credential to hex |
| `grest.cip129_cc_hot_has_script()` | Checks if CC Hot credential is a script |
| `grest.cip129_hex_to_cc_hot()` | Converts hex to CC Hot credential |
| `grest.cip129_cc_cold_to_hex()` | Converts CC Cold Credential to hex |
| `grest.cip129_cc_cold_has_script()` | Checks if CC Cold credential is a script |
| `grest.cip129_hex_to_cc_cold()` | Converts hex to CC Cold credential |
| `grest.cip129_drep_id_to_hex()` | Converts DRep ID to hex |
| `grest.cip129_drep_id_has_script()` | Checks if DRep ID is a script |
| `grest.cip129_hex_to_drep_id()` | Converts hex to DRep ID |
| `grest.cip129_from_gov_action_id()` | Parses Governance Action ID |
| `grest.cip129_to_gov_action_id()` | Creates Governance Action ID |
```

### CIP-5 Functions

Standard bech32 address prefixes (CIP-0005).

```/dev/null/cip5.md#L1-3
| Function | Description |
|----------|-------------|
| `grest.cip5_hex_to_stake_addr()` | Converts hex to stake address |
```

### CIP-67 Functions

Asset name labeling standard for NFT/FT classification.

```/dev/null/cip67.md#L1-3
| Function | Description |
|----------|-------------|
| `grest.cip67_label()` | Returns CIP-67 label (100, 222, 333, 444, 500) |
| `grest.cip67_strip_label()` | Strips CIP-67 label prefix from asset name |
```

### Other Utilities

```/dev/null/utilities.md#L1-6
| Function | Description |
|----------|-------------|
| `grest.cli_protocol_params()` | Returns current protocol parameters |
| `grest.is_dangling_delegation()` | Checks if delegation is invalidated by pool retirement |
| `grest.has_security_param()` | Validates gov action contains security parameters |
| `grest.pg_cardano_version()` | Returns pg_cardano extension version |
```

---

## 00_cached_tables

Performance-critical cached tables that store pre-computed data for fast queries.

### Cache Tables

```/dev/null/cached_tables.md#L1-18
| Table | Description |
|-------|-------------|
| `grest.epoch_active_stake_cache` | Total active stake per epoch |
| `grest.asset_cache_control` | Controlled list of tracked policies |
| `grest.asset_tx_out_cache` | Cached asset UTXO data |
| `grest.asset_info_cache` | Asset metadata (supply, mint count, burn count) |
| `grest.asset_registry_cache` | Token registry metadata |
| `grest.epoch_info_cache` | Epoch statistics (fees, rewards, tx count) |
| `grest.pool_history_cache` | Historical pool performance data |
| `grest.pool_info_cache` | Current pool parameters and status |
| `grest.stake_distribution_cache` | Account stake distribution |
```

### Update Functions

```/dev/null/cache_updates.md#L1-12
| Function | Description |
|----------|-------------|
| `grest.active_stake_cache_update()` | Updates epoch active stake cache |
| `grest.asset_info_cache_update()` | Updates asset metadata cache |
| `grest.asset_registry_cache_update()` | Updates token registry cache |
| `grest.asset_txo_cache_update()` | Updates asset UTXO cache |
| `grest.epoch_info_cache_update()` | Updates epoch statistics cache |
| `grest.pool_history_cache_update()` | Updates pool history (procedure) |
| `grest.update_pool_info_cache()` | Updates pool info cache (procedure) |
| `grest.update_stake_distribution_cache()` | Updates stake distribution (procedure) |
```

---

## 01_blockchain

Basic blockchain information endpoints.

```/dev/null/blockchain.md#L1-12
| Function | Description |
|----------|-------------|
| `grest.era_summaries()` | Get era summaries per CIP-0059 |
| `grest.genesis()` | Get blockchain genesis parameters |
| `grest.param_updates()` | Get protocol parameter updates |
| `grest.reserve_withdrawals()` | MIR withdrawals from reserves |
| `grest.tip()` | Get chain tip (latest block) |
| `grest.totals()` | Get circulating supply, treasury, rewards |
| `grest.treasury_withdrawals()` | MIR withdrawals from treasury |
```

---

## account

Stake account queries. These functions query by stake address (not payment address).

```/dev/null/account.md#L1-20
| Function | Description |
|----------|-------------|
| `grest.account_addresses()` | Get all payment addresses associated with stake address |
| `grest.account_assets()` | Get native asset holdings |
| `grest.account_history()` | **[DEPRECATED]** Get active stake history |
| `grest.account_info()` | Get full account details (balance, delegation, rewards) |
| `grest.account_info_cached()` | Get account info from cache (faster) |
| `grest.account_list()` | List all stake addresses |
| `grest.account_reward_history()` | Get complete rewards history (incl. MIR) |
| `grest.account_rewards()` | **[DEPRECATED]** Get rewards history |
| `grest.account_stake_history()` | Get epoch-wise active stake |
| `grest.account_txs()` | Get transactions for stake address |
| `grest.account_update_history()` | Get registration/delegation history |
| `grest.account_updates()` | Get updates (registration, delegation, withdrawals) |
| `grest.account_utxos()` | Get UTXO set for stake address |
```

### Account Info Response Example

```json
/dev/null/account_example.json#L1-18
{
  "stake_address": "stake1...",
  "status": "registered",
  "delegated_pool": "pool1...",
  "delegated_drep": "drep1...",
  "total_balance": "5000000",
  "utxo": "3000000",
  "rewards": "1500000",
  "withdrawals": "0",
  "rewards_available": "1500000",
  "deposit": "2000000",
  "reserves": "0",
  "treasury": "0",
  "proposal_refund": "0"
}
```

---

## address

Payment address queries (not stake addresses).

```/dev/null/address.md#L1-10
| Function | Description |
|----------|-------------|
| `grest.address_assets()` | Get assets at address |
| `grest.address_info()` | Get address balance and UTXO set |
| `grest.address_list()` | List all used addresses |
| `grest.address_outputs()` | Get transaction outputs |
| `grest.address_txs()` | Get transactions for address |
| `grest.address_utxos()` | Get UTXO set for address |
| `grest.credential_txs()` | Get transactions by payment credential |
| `grest.credential_utxos()` | Get UTXOs by payment credential |
```

---

## assets

Native asset (NFT/FT) queries.

```/dev/null/assets.md#L1-20
| Function | Description |
|----------|-------------|
| `grest.asset_addresses()` | Get addresses holding an asset |
| `grest.asset_history()` | Get mint/burn history |
| `grest.asset_info()` | Get asset metadata (supply, decimals, metadata) |
| `grest.asset_list()` | List all native assets on chain |
| `grest.asset_nft_address()` | Get current owner of NFT |
| `grest.asset_summary()` | Get asset statistics (transactions, holders) |
| `grest.asset_token_registry()` | Get token registry metadata |
| `grest.asset_txs()` | Get transactions involving asset |
| `grest.asset_utxos()` | Get UTXOs containing asset |
| `grest.policy_asset_addresses()` | Get addresses for all assets in policy |
| `grest.policy_asset_info()` | Get info for all assets in policy |
| `grest.policy_asset_list()` | List all assets under a policy |
| `grest.policy_asset_mints()` | Get mint/burn details for policy |

### CIP-68 Support

The asset functions support CIP-68 token standard with CIP-67 label detection:
- **Label 100**: Reference/Metadata tokens
- **Label 222**: NFT
- **Label 333**: Fungible Token (FT)
- **Label 444**: Rich FT (RFT)
- **Label 500**: Royalty NFT

---

## blocks

Block information and queries.

```/dev/null/blocks.md#L1-10
| Function | Description |
|----------|-------------|
| `grest.block_info()` | Get detailed block information by hash |
| `grest.block_tx_info()` | Get transaction details for blocks |
| `grest.block_txs()` | Get transaction hashes in blocks |
| `grest.blocks()` | List all blocks (paginated) |
```

### Block Info Response

```json
/dev/null/block_info_example.json#L1-24
{
  "hash": "abc123...",
  "epoch_no": 400,
  "era": "Babbage",
  "abs_slot": 100000000,
  "epoch_slot": 43200,
  "block_height": 9000000,
  "block_size": 15000,
  "block_time": 1700000000,
  "tx_count": 50,
  "vrf_key": "vrf_vk1...",
  "pool": "pool1...",
  "proto_major": 8,
  "proto_minor": 0,
  "total_output": "5000000000",
  "total_fees": "2500000",
  "num_confirmations": 100,
  "parent_hash": "def456...",
  "child_hash": "ghi789..."
}
```

---

## epoch

Epoch-related queries.

```/dev/null/epoch.md#L1-8
| Function | Description |
|----------|-------------|
| `grest.epoch_block_protocols()` | Get block protocol distribution |
| `grest.epoch_info()` | Get epoch information and statistics |
| `grest.epoch_params()` | Get epoch protocol parameters |
| `grest.epoch_summary_corrections_update()` | Internal: Fix epoch data inconsistencies |

---

## governance

Governance queries for Cardano Conway era (CIP-1694).

```/dev/null/governance.md#L1-30
| Function | Description |
|----------|-------------|
| `grest.committee_info()` | Get current committee members |
| `grest.committee_votes()` | Get committee votes |
| `grest.drep_delegators()` | Get DRep delegators |
| `grest.drep_epoch_summary()` | Get DRep epoch summary |
| `grest.drep_info()` | Get DRep information |
| `grest.drep_list()` | List all registered DReps |
| `grest.drep_metadata()` | Get DRep metadata |
| `grest.drep_updates()` | Get DRep registration updates |
| `grest.drep_votes()` | Get DRep votes |
| `grest.drep_voting_power_history()` | Get DRep voting power history |
| `grest.drep_history()` | Alias for voting power history |
| `grest.pool_votes()` | Get SPO votes |
| `grest.pool_voting_power_history()` | Get SPO voting power |
| `grest.proposal_list()` | List all governance proposals |
| `grest.proposal_votes()` | Get votes on proposal |
| `grest.proposal_voting_summary()` | Get voting summary |
| `grest.vote_list()` | List all votes |
| `grest.voter_proposal_list()` | Get proposals voted by voter |

### Governance Response Types

**DRep Info:**
```json
/dev/null/drep_info_example.json#L1-12
{
  "drep_id": "drep1...",
  "hex": "...",
  "has_script": false,
  "drep_status": "registered",
  "deposit": "500000000",
  "active": true,
  "expires_epoch_no": 500,
  "amount": "1000000000",
  "meta_url": "https://...",
  "meta_hash": "..."
}
```

**Proposal Types:**
- `InfoAction`
- `HardForkInitiation`
- `ParameterChange`
- `TreasuryWithdrawals`
- `NewCommittee`
- `NoConfidence`
- `Auto-abstain` DReps

---

## pool

Stake pool queries.

```/dev/null/pool.md#L1-30
| Function | Description |
|----------|-------------|
| `grest.pool_blocks()` | Get blocks minted by pool |
| `grest.pool_calidus_keys()` | List valid Calidus keys |
| `grest.pool_delegators()` | Get current delegators |
| `grest.pool_delegators_list()` | Get delegator list (brief) |
| `grest.pool_delegators_history()` | Get delegator history |
| `grest.pool_groups()` | Get pool group assignments |
| `grest.pool_history()` | Get pool performance history |
| `grest.pool_info()` | Get pool details |
| `grest.pool_invalid_delegators()` | Get invalid delegators for epoch |
| `grest.pool_list()` | List all pools |
| `grest.pool_metadata()` | Get pool metadata |
| `grest.pool_owner_history()` | Get owner pledge history |
| `grest.pool_registrations()` | Get pool registrations |
| `grest.pool_relays()` | Get pool relay info |
| `grest.pool_retirements()` | Get pool retirements |
| `grest.pool_stake_snapshot()` | Get stake snapshots (Mark/Set/Go) |
| `grest.pool_updates()` | Get pool updates |

### Pool Status Values

- `registered`: Pool is active
- `retiring`: Pool announced retirement
- `retired`: Pool fully retired

---

## script

Smart contract script queries.

```/dev/null/script.md#L1-12
| Function | Description |
|----------|-------------|
| `grest.datum_info()` | Get datum information |
| `grest.native_script_list()` | List native (timelock/multisig) scripts |
| `grest.plutus_script_list()` | List Plutus scripts |
| `grest.reference_script_utxos()` | Get reference script UTXOs |
| `grest.script_info()` | Get script details |
| `grest.script_redeemers()` | Get script redeemers |
| `grest.script_utxos()` | Get UTXOs at script addresses |

---

## transactions

Transaction queries.

```/dev/null/transactions.md#L1-20
| Function | Description |
|----------|-------------|
| `grest.tx_by_metalabel()` | Get txs by metadata label |
| `grest.tx_cbor()` | Get raw transaction CBOR |
| `grest.tx_info()` | Get transaction details |
| `grest.tx_metadata()` | Get transaction metadata |
| `grest.tx_metalabels()` | List all metadata labels |
| `grest.tx_outs_epoch()` | Get outputs for epoch |
| `grest.tx_status()` | Get transaction confirmations |
| `grest.tx_treasury_donations_epoch()` | Get treasury donations |
| `grest.tx_utxos()` | Get transaction inputs/outputs |
| `grest.utxo_info()` | Get UTXO details |

### Transaction Info Options

The `tx_info()` function accepts optional boolean parameters:
- `_inputs`: Include transaction inputs
- `_metadata`: Include metadata
- `_assets`: Include minted/burned assets
- `_withdrawals`: Include withdrawals
- `_certs`: Include certificates
- `_scripts`: Include scripts
- `_bytecode`: Include Plutus bytecode
- `_governance`: Include governance votes

---

## db-scripts

Database initialization and management scripts.

### Key Setup Functions

```/dev/null/db_setup.md#L1-30
-- Schema creation
CREATE SCHEMA grest;
CREATE SCHEMA grestv0;

-- Users
CREATE ROLE web_anon;
CREATE ROLE authenticator;

-- Control table (tracks cache state)
CREATE TABLE grest.control_table (
  key text PRIMARY KEY,
  last_value text NOT NULL,
  artifacts text
);

-- Genesis parameters
CREATE TABLE grest.genesis (
  networkmagic,
  networkid,
  activeslotcoeff,
  ...
);

-- Era mapping (CIP-0059)
CREATE TABLE grest.era_map (
  phase, era, protocol_major, protocol_minor,
  ledger_protocol, consensus_mechanism, notes
);
```

---

## 02_indexes

Additional database indexes for query optimization beyond vanilla db-sync.

```/dev/null/indexes.md#L1-40
-- Unique constraints
CREATE UNIQUE INDEX unique_ada_pots ON public.ada_pots (block_id);
CREATE UNIQUE INDEX unique_delegation ON public.delegation (tx_id, cert_index);
CREATE UNIQUE INDEX unique_ma_tx_mint ON public.ma_tx_mint (ident, tx_id);
CREATE UNIQUE INDEX unique_pool_update ON public.pool_update (registered_tx_id, cert_index);
-- ... and more

-- Performance indexes
CREATE INDEX idx_ma_tx_out_ident ON ma_tx_out (ident) INCLUDE (tx_out_id, quantity);
CREATE INDEX idx_address_address ON address USING hash (address);
CREATE INDEX idx_voting_procedure_tx_id ON voting_procedure (tx_id DESC);
```

---

## Cache Update Workflow

The system uses a cron-based approach to keep caches updated:

```/dev/null/cache_workflow.md#L1-20
1. Check if update is needed (epoch/block height difference)
2. Acquire advisory lock (prevent concurrent updates)
3. Update cache table
4. Update control_table with last processed position
5. Release lock

-- Key control table keys:
-- - last_active_stake_validated_epoch
-- - asset_info_cache_last_tx_id
-- - pool_info_cache_last_block_height
-- - stake_distribution_lbh
-- - epoch_info_cache_last_updated
```

---

## Access Control

```/dev/null/access_control.md#L1-15
-- web_anon has SELECT on grest schema
GRANT USAGE ON SCHEMA grest TO web_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA grest TO web_anon;

-- PostgREST configuration
-- authenticator role handles authentication
-- web_anon role is the default role for unauthenticated requests
```

---

## Usage via PostgREST

Once deployed, access functions like:

```bash
/dev/null/curl_examples.sh#L1-8
# Get account info
curl "https://api.koios.rest/api/v1/account_info?_stake_addresses=[\"stake1...\"]"

# Get pool info
curl "https://api.koios.rest/api/v1/pool_info?_pool_bech32_ids=[\"pool1...\"]"

# Get asset info
curl "https://api.koios.rest/api/v1/asset_info?_asset_policy=abc...&_asset_name=MYTOKEN"
```

---

## Performance Notes

- Cached tables are updated asynchronously via cron jobs
- Some functions have both cached and non-cached versions
- Account info uses `stake_distribution_cache` for speed
- Asset queries use `asset_info_cache` and `asset_tx_out_cache`
- Pool history uses `pool_history_cache` for historical data
