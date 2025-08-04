CREATE OR REPLACE FUNCTION grest.era_summaries()
RETURNS TABLE (
  era varchar,
  protocol_major word31type,
  protocol_minor word31type,
  ledger_protocol varchar,
  consensus_mechanism varchar,
  notes varchar,
  epoch_no word31type,
  first_block_time numeric,
  first_block_hash text
)
LANGUAGE sql STABLE
AS $$
  SELECT DISTINCT ON (em.era,em.protocol_major,em.protocol_minor)
    em.era,
    em.protocol_major,
    em.protocol_minor,
    em.ledger_protocol,
    em.consensus_mechanism,
    em.notes,
    ei.epoch_no,
    ei.i_first_block_time,
    ei.p_block_hash
  FROM grest.era_map AS em
  LEFT JOIN public.epoch_param AS ep ON ep.protocol_major::text = em.protocol_major::text AND ep.protocol_minor::text = em.protocol_minor::text
  LEFT JOIN grest.epoch_info_cache AS ei ON ep.epoch_no = ei.epoch_no
  ORDER BY em.protocol_major,em.protocol_minor
  ;
$$;

COMMENT ON FUNCTION grest.era_summaries IS 'Get a summary for each era as per CIP-0059'; -- noqa: LT01
