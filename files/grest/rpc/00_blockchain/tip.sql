CREATE OR REPLACE FUNCTION grest.tip()
RETURNS TABLE (
  hash text,
  era varchar,
  epoch_no word31type,
  abs_slot word63type,
  epoch_slot word31type,
  block_height word31type,
  block_no word31type,
  block_time integer
)
LANGUAGE sql STABLE
AS $$
  SELECT
    ENCODE(b.hash::bytea, 'hex') AS block_hash,
    b.epoch_no AS epoch_no,
    em.era,
    b.slot_no AS abs_slot,
    b.epoch_slot_no AS epoch_slot,
    b.block_no AS block_height,
    b.block_no AS block_no,
    EXTRACT(EPOCH FROM b.time)::integer
  FROM public.block AS b
  LEFT JOIN public.epoch_param AS ep ON ep.epoch_no = b.epoch_no
  LEFT JOIN grest.era_map em ON ep.protocol_major::text = em.protocol_major::text AND ep.protocol_minor::text = em.protocol_minor::text
  ORDER BY b.id DESC
  LIMIT 1;
$$;

COMMENT ON FUNCTION grest.tip IS 'Get the tip info about the latest block seen by chain'; -- noqa: LT01
