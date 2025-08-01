CREATE OR REPLACE FUNCTION grest.epoch_block_protocols(_epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  proto_major word31type,
  proto_minor word31type,
  era varchar,
  blocks bigint
)
LANGUAGE plpgsql
AS $$
BEGIN
  IF _epoch_no IS NOT NULL THEN
    RETURN QUERY
      SELECT
        b.proto_major,
        b.proto_minor,
        em.era,
        count(b.*)
      FROM public.block AS b
        LEFT JOIN public.epoch_param AS ep ON ep.epoch_no = b.epoch_no
        LEFT JOIN grest.era_map AS em ON ep.protocol_major::text = em.protocol_major::text AND ep.protocol_minor::text = em.protocol_minor::text
      WHERE b.epoch_no = _epoch_no::word31type
      GROUP BY
        b.proto_major, b.proto_minor;
  ELSE
    RETURN QUERY
      SELECT
        b.proto_major,
        b.proto_minor,
        em.era,
        count(b.*)
      FROM public.block AS b
        LEFT JOIN public.epoch_param AS ep ON ep.epoch_no = b.epoch_no
        LEFT JOIN grest.era_map AS em ON ep.protocol_major::text = em.protocol_major::text AND ep.protocol_minor::text = em.protocol_minor::text
      WHERE b.epoch_no = (SELECT MAX(no) FROM epoch)
      GROUP BY
        b.proto_major, b.proto_minor;
  END IF;
END;
$$;

COMMENT ON FUNCTION grest.epoch_block_protocols IS 'Get the information about block protocol distribution in epoch'; -- noqa: LT01
