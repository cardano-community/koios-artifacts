CREATE OR REPLACE FUNCTION grest.asset_summary(_asset_policy text, _asset_name text DEFAULT '')
RETURNS TABLE (
  policy_id text,
  asset_name text,
  fingerprint character varying,
  total_transactions bigint,
  staked_wallets bigint,
  unstaked_addresses bigint,
  addresses bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
  _asset_policy_decoded bytea;
  _asset_name_decoded bytea;
  _asset_id int;
BEGIN
  SELECT DECODE(_asset_policy, 'hex') INTO _asset_policy_decoded;
  SELECT DECODE(
    CASE WHEN _asset_name IS NULL THEN ''
    ELSE _asset_name
    END,
    'hex'
  ) INTO _asset_name_decoded;
  SELECT id INTO _asset_id
  FROM multi_asset AS ma
  WHERE ma.policy = _asset_policy_decoded
    AND ma.name = _asset_name_decoded;
  RETURN QUERY
    with _asset_utxos AS (
      SELECT
        txo.tx_id AS tx_id,
        txo.id AS tx_out_id,
        txo.index AS tx_out_idx,
        txo.address AS address,
        txo.stake_address_id AS sa_id
      FROM ma_tx_out AS mto
      INNER JOIN tx_out AS txo ON txo.id = mto.tx_out_id
      WHERE mto.ident = _asset_id
        AND txo.consumed_by_tx_in_id IS NULL)

    SELECT
      _asset_policy,
      _asset_name,
      ma.fingerprint,
      (
        SELECT COUNT(DISTINCT(txo.tx_id))
        FROM ma_tx_out mto
        INNER JOIN tx_out txo ON txo.id = mto.tx_out_id
        WHERE ident = _asset_id
      ) AS total_transactions,
      (
        SELECT COUNT(DISTINCT(_asset_utxos.sa_id))
        FROM _asset_utxos
        WHERE _asset_utxos.sa_id IS NOT NULL
      ) AS staked_wallets,
      (
        SELECT COUNT(DISTINCT(_asset_utxos.address))
        FROM _asset_utxos
        WHERE _asset_utxos.sa_id IS NULL
      ) AS unstaked_addresses,
      (
        SELECT COUNT(DISTINCT(_asset_utxos.address))
        FROM _asset_utxos
      ) AS addresses
    FROM multi_asset AS ma
    WHERE ma.id = _asset_id;
  END;
$$;

COMMENT ON FUNCTION grest.asset_summary IS 'Get the summary of an asset (total transactions exclude minting/total wallets include only wallets with asset balance)'; -- noqa: LT01
