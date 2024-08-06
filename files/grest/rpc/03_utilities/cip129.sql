-- Binary format
--     1 byte     variable length
--      <------> <------------------->
--     ┌────────┬─────────────────────┐
--     │ header │        key      │
--     └────────┴─────────────────────┘
--         🔎
--         ╎          7 6 5 4 3 2 1 0
--         ╎         ┌─┬─┬─┬─┬─┬─┬─┬─┐
--         ╰╌╌╌╌╌╌╌╌ |t│t│t│t│c│c│c│c│
--                   └─┴─┴─┴─┴─┴─┴─┴─┘
--
-- Key Type (`t t t t . . . .`)          | Key
-- ---                                   | ---
-- `0000....`                            | CC Hot
-- `0001....`                            | CC Cold
-- `0010....`                            | DRep
--
-- Credential Type (`. . . . c c c c`)   | Semantic
-- ---                                   | ---
-- `....0010`                            | Key Hash
-- `....0011`                            | Script Hash

CREATE OR REPLACE FUNCTION grest.cip129_cc_hot_to_hex(_cc_hot text)
RETURNS bytea
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF LENGTH(_cc_hot) = 60 THEN
    RETURN substring(b32_decode(_cc_hot) from 2);
  ELSE
    RETURN b32_decode(_cc_hot);
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION grest.cip129_hex_to_cc_hot(_raw bytea, _is_script boolean)
RETURNS text
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF _is_script THEN
    RETURN b32_encode('cc_hot', ('\x03'::bytea || _raw)::text);
  ELSE
    RETURN b32_encode('cc_hot', ('\x02'::bytea || _raw)::text);
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION grest.cip129_cc_cold_to_hex(_cc_cold text)
RETURNS bytea
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF LENGTH(_cc_cold) = 61 THEN
    RETURN substring(b32_decode(_cc_cold) from 2);
  ELSE
    RETURN b32_decode(_cc_cold);
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION grest.cip129_hex_to_cc_cold(_raw bytea, _is_script boolean)
RETURNS text
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF _is_script THEN
    RETURN b32_encode('cc_cold', ('\x13'::bytea || _raw)::text);
  ELSE
    RETURN b32_encode('cc_cold', ('\x12'::bytea || _raw)::text);
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION grest.cip129_drep_id_to_hex(_drep_id text)
RETURNS bytea
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF LENGTH(_drep_id) = 58 THEN
    RETURN substring(b32_decode(_drep_id) from 2);
  ELSE
    RETURN b32_decode(_drep_id);
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION grest.cip129_hex_to_drep_id(_raw bytea, _is_script boolean)
RETURNS text
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF _is_script THEN
    RETURN b32_encode('drep', ('\x23'::bytea || _raw)::text);
  ELSE
    RETURN b32_encode('drep', ('\x22'::bytea || _raw)::text);
  END IF;
END;
$$;

COMMENT ON FUNCTION grest.cip129_cc_hot_to_hex IS 'Returns binary hex from Constitutional Committee Hot Credential ID in old or new (CIP-129) format'; -- noqa: LT01
COMMENT ON FUNCTION grest.cip129_hex_to_cc_hot IS 'Returns Constitutional Committee Hot Credential ID in CIP-129 format from raw binary hex'; -- noqa: LT01
COMMENT ON FUNCTION grest.cip129_cc_cold_to_hex IS 'Returns binary hex from Constitutional Committee Cold Credential ID in old or new (CIP-129) format'; -- noqa: LT01
COMMENT ON FUNCTION grest.cip129_hex_to_cc_cold IS 'Returns Constitutional Committee Cold Credential ID in CIP-129 format from raw binary hex'; -- noqa: LT01
COMMENT ON FUNCTION grest.cip129_drep_id_to_hex IS 'Returns binary hex from DRep Credential ID in old or new (CIP-129) format'; -- noqa: LT01
COMMENT ON FUNCTION grest.cip129_hex_to_drep_id IS 'Returns DRep Credential ID in CIP-129 format from raw binary hex'; -- noqa: LT01
