CREATE OR REPLACE FUNCTION grest.pool_bls_coverage(_epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  epoch_no word31type,
  active_stake text,
  pool_count bigint,
  bls_pool_count bigint,
  bls_stake text,
  bls_stake_share numeric,
  leios_committee_size word64type,
  leios_quorum_stake_threshold double precision,
  committee_bls_pool_count bigint,
  committee_bls_stake_share numeric
)
LANGUAGE sql STABLE
AS $$
  WITH
    _epoch AS (
      SELECT COALESCE(_epoch_no, (SELECT MAX(ep.epoch_no) FROM public.epoch_param AS ep))::word31type AS no
    ),
    -- Active stake in an epoch is the mark snapshot that pool_stat records for the epoch before, as in /pool_info.
    -- A pool counts as keyed if the registration in force for the epoch declares a BLS key.
    _pools AS (
      SELECT
        ps.stake,
        ph.hash_raw,
        COALESCE((
          SELECT pu.bls_key IS NOT NULL
          FROM public.pool_update AS pu
          WHERE pu.hash_id = ps.pool_hash_id
            AND pu.active_epoch_no <= (SELECT no FROM _epoch)
          ORDER BY pu.id DESC
          LIMIT 1
        ), FALSE) AS has_bls_key
      FROM public.pool_stat AS ps
        INNER JOIN public.pool_hash AS ph ON ph.id = ps.pool_hash_id
      WHERE ps.epoch_no = (SELECT no FROM _epoch) - 1
        AND ps.stake > 0
    ),
    -- The Leios committee rule on ledger master: the leios_committee_size pools with the most stake,
    -- ties broken by ascending pool id. A seat without a usable key cannot count towards quorum.
    _ranked AS (
      SELECT
        p.stake,
        p.has_bls_key,
        ROW_NUMBER() OVER (ORDER BY p.stake DESC, p.hash_raw ASC) AS seat
      FROM _pools AS p
    )
  SELECT
    e.no,
    COALESCE(SUM(r.stake), 0)::text,
    COUNT(r.seat),
    COUNT(r.seat) FILTER (WHERE r.has_bls_key),
    COALESCE(SUM(r.stake) FILTER (WHERE r.has_bls_key), 0)::text,
    ROUND(COALESCE(SUM(r.stake) FILTER (WHERE r.has_bls_key), 0) / NULLIF(SUM(r.stake), 0), 6),
    ep.leios_committee_size,
    ep.leios_quorum_stake_threshold,
    COUNT(r.seat) FILTER (WHERE r.has_bls_key AND r.seat <= ep.leios_committee_size),
    ROUND(COALESCE(SUM(r.stake) FILTER (WHERE r.has_bls_key AND r.seat <= ep.leios_committee_size), 0) / NULLIF(SUM(r.stake), 0), 6)
  FROM _epoch AS e
    LEFT JOIN public.epoch_param AS ep ON ep.epoch_no = e.no
    LEFT JOIN _ranked AS r ON TRUE
  GROUP BY e.no, ep.leios_committee_size, ep.leios_quorum_stake_threshold;
$$;

COMMENT ON FUNCTION grest.pool_bls_coverage IS 'How much of an epoch''s active stake is held by pools with a BLS key, overall and within the estimated Leios committee'; --noqa: LT01
