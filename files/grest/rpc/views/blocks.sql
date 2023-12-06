DROP VIEW IF EXISTS grest.blocks;

CREATE VIEW grest.blocks AS
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

COMMENT ON VIEW grest.blocks IS 'Get detailed information about all blocks (paginated - latest first)';
