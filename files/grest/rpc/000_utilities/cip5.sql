-- CIP References
-- 0005: Common bech32 prefixes https://cips.cardano.org/cip/CIP-0005
-- 0019: Cardano Addresses https://cips.cardano.org/cip/CIP-0019

CREATE OR REPLACE FUNCTION grest.cip5_hex_to_stake_addr(_raw bytea)
RETURNS text
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF _raw IS NULL THEN
   RETURN NULL;
  ELSE
    RETURN cardano.tools_shelley_address_build(
      ''::bytea,
      FALSE,
      SUBSTRING(_raw FROM 2),
      FALSE,
      SUBSTRING(ENCODE(_raw, 'hex') from 2 for 1)::integer
      )::text;
  END IF;
END;
$$;
