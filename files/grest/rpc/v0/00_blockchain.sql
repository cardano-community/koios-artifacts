-- NETWORK

CREATE OR REPLACE FUNCTION grestv0.genesis()
RETURNS TABLE (
  networkmagic varchar,
  networkid varchar,
  activeslotcoeff varchar,
  updatequorum varchar,
  maxlovelacesupply varchar,
  epochlength varchar,
  systemstart integer,
  slotsperkesperiod varchar,
  slotlength varchar,
  maxkesrevolutions varchar,
  securityparam varchar,
  alonzogenesis varchar
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT * FROM grest.genesis();
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.param_updates()
RETURNS TABLE (
  tx_hash text,
  block_height word31type,
  block_time integer,
  epoch_no word31type,
  data jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT *
    FROM grest.param_updates();
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.tip()
RETURNS TABLE (
  hash text,
  epoch_no word31type,
  abs_slot word63type,
  epoch_slot word31type,
  block_no word31type,
  block_time integer
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT *
  FROM grest.tip();
END;
$$;

CREATE OR REPLACE FUNCTION grestv0.totals(_epoch_no numeric DEFAULT NULL)
RETURNS TABLE (
  epoch_no word31type,
  circulation text,
  treasury text,
  reward text,
  supply text,
  reserves text
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
    SELECT * FROM grest.totals(_epoch_no);
END;
$$;
