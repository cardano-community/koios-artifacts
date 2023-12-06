-- VIEWS

DROP VIEW IF EXISTS grestv0.account_list;

CREATE VIEW grestv0.account_list AS
SELECT stake_address.view AS id
FROM
  stake_address;

DROP VIEW IF EXISTS grestv0.asset_list;

CREATE VIEW grestv0.asset_list AS
SELECT
  ENCODE(ma.policy, 'hex') AS policy_id,
  ENCODE(ma.name, 'hex') AS asset_name,
  ma.fingerprint
FROM
  public.multi_asset AS ma
ORDER BY ma.policy, ma.name;

DROP VIEW IF EXISTS grestv0.asset_token_registry;

CREATE VIEW grestv0.asset_token_registry AS
SELECT
  asset_policy AS policy_id,
  asset_name,
  name AS asset_name_ascii,
  ticker,
  description,
  url,
  decimals,
  logo
FROM
  grest.asset_registry_cache;

DROP VIEW IF EXISTS grestv0.blocks;

CREATE VIEW grestv0.blocks AS
SELECT
  ENCODE(b.hash::bytea, 'hex') AS hash,
  b.epoch_no AS epoch_no,
  b.slot_no AS abs_slot,
  b.epoch_slot_no AS epoch_slot,
  b.block_no AS block_height,
  b.size AS block_size,
  EXTRACT(EPOCH FROM b.time)::integer AS block_time,
  b.tx_count,
  b.vrf_key,
  ph.view AS pool,
  b.proto_major,
  b.proto_minor,
  b.op_cert_counter
FROM block AS b
LEFT JOIN slot_leader AS sl ON b.slot_leader_id = sl.id
LEFT JOIN pool_hash AS ph ON sl.pool_hash_id = ph.id
ORDER BY b.id DESC;
