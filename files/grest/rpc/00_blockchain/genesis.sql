CREATE OR REPLACE FUNCTION grest.genesis()
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
LANGUAGE sql STABLE
AS $$
  SELECT
    g.networkmagic,
    g.networkid,
    g.activeslotcoeff,
    g.updatequorum,
    g.maxlovelacesupply,
    g.epochlength,
    EXTRACT(EPOCH FROM g.systemstart::timestamp)::integer,
    g.slotsperkesperiod,
    g.slotlength,
    g.maxkesrevolutions,
    g.securityparam,
    g.alonzogenesis
  FROM grest.genesis AS g;
$$;
