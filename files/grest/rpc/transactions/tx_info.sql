CREATE OR REPLACE FUNCTION grest.tx_info(
  _tx_hashes text [],
  _inputs boolean DEFAULT false,
  _metadata boolean DEFAULT false,
  _assets boolean DEFAULT false,
  _withdrawals boolean DEFAULT false,
  _certs boolean DEFAULT false,
  _scripts boolean DEFAULT false,
  _bytecode boolean DEFAULT false,
  _governance boolean DEFAULT false
)
RETURNS TABLE (
  tx_hash text,
  block_hash text,
  block_height word31type,
  epoch_no word31type,
  epoch_slot word31type,
  absolute_slot word63type,
  tx_timestamp integer,
  tx_block_index word31type,
  tx_size word31type,
  total_output text,
  fee text,
  treasury_donation text,
  deposit text,
  invalid_before text,
  invalid_after text,
  collateral_inputs jsonb,
  collateral_output jsonb,
  reference_inputs jsonb,
  inputs jsonb,
  outputs jsonb,
  withdrawals jsonb,
  assets_minted jsonb,
  metadata jsonb,
  certificates jsonb,
  native_scripts jsonb,
  plutus_contracts jsonb,
  voting_procedures jsonb,
  proposal_procedures jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
  _tx_hashes_bytea      bytea[];
  _tx_id_list           bigint[];
BEGIN
  -- convert input _tx_hashes array into bytea array
  SELECT INTO _tx_hashes_bytea ARRAY_AGG(hashes_bytea)
  FROM (
    SELECT DECODE(hashes_hex, 'hex') AS hashes_bytea
    FROM UNNEST(_tx_hashes) AS hashes_hex
  ) AS tmp;
  -- all tx ids
  SELECT INTO _tx_id_list ARRAY_AGG(id)
  FROM (
    SELECT id
    FROM  tx
    WHERE tx.hash = ANY(_tx_hashes_bytea)
  ) AS tmp;

  RETURN QUERY (
    WITH
      -- limit by last known block, also join with block only once
      _all_tx AS (
        SELECT
          tx.id,
          tx.hash               AS tx_hash,
          b.hash                AS block_hash,
          b.block_no            AS block_height,
          b.epoch_no            AS epoch_no,
          b.epoch_slot_no       AS epoch_slot,
          b.slot_no             AS absolute_slot,
          b.time                AS tx_timestamp,
          tx.block_index        AS tx_block_index,
          tx.size               AS tx_size,
          tx.out_sum            AS total_output,
          tx.fee,
          tx.treasury_donation,
          tx.deposit,
          tx.invalid_before,
          tx.invalid_hereafter  AS invalid_after
        FROM tx
          INNER JOIN block AS b ON tx.block_id = b.id
        WHERE tx.id = ANY(_tx_id_list)
      ),

      _all_collateral_inputs AS (
        SELECT
          collateral_tx_in.tx_in_id           AS tx_id,
          tx_out.address                      AS payment_addr_bech32,
          ENCODE(tx_out.payment_cred, 'hex')  AS payment_addr_cred,
          sa.view                             AS stake_addr,
          ENCODE(tx.hash, 'hex')              AS tx_hash,
          tx_out.index                        AS tx_index,
          tx_out.value::text                  AS value,
          tx_out.data_hash                    AS datum_hash,
          (CASE WHEN ma.policy IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'policy_id', ENCODE(ma.policy, 'hex'),
                'asset_name', ENCODE(ma.name, 'hex'),
                'fingerprint', ma.fingerprint,
                'decimals', aic.decimals,
                'quantity', mto.quantity::text
              )
            END
          ) AS asset_list,
          (CASE WHEN tx_out.inline_datum_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'bytes', ENCODE(datum.bytes, 'hex'),
                'value', datum.value
              )
            END
          ) AS inline_datum,
          (CASE WHEN tx_out.reference_script_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'hash', ENCODE(script.hash, 'hex'),
                'bytes', ENCODE(script.bytes, 'hex'),
                'value', script.json,
                'type', script.type::text,
                'size', script.serialised_size
              )
            END
          ) AS reference_script
        FROM collateral_tx_in
          INNER JOIN tx_out ON tx_out.tx_id = collateral_tx_in.tx_out_id
            AND tx_out.index = collateral_tx_in.tx_out_index
          INNER JOIN tx ON tx_out.tx_id = tx.id
          LEFT JOIN stake_address AS sa ON tx_out.stake_address_id = sa.id
          LEFT JOIN ma_tx_out AS mto ON _assets IS TRUE AND mto.tx_out_id = tx_out.id
          LEFT JOIN multi_asset AS ma ON _assets IS TRUE AND ma.id = mto.ident
          LEFT JOIN grest.asset_info_cache AS aic ON _assets IS TRUE AND aic.asset_id = ma.id
          LEFT JOIN datum ON _scripts IS TRUE AND datum.id = tx_out.inline_datum_id
          LEFT JOIN script ON _scripts IS TRUE AND script.id = tx_out.reference_script_id
        WHERE
          (_inputs IS TRUE AND _scripts IS TRUE)
          AND collateral_tx_in.tx_in_id = ANY(_tx_id_list)
      ),

      _all_reference_inputs AS (
        SELECT
          reference_tx_in.tx_in_id            AS tx_id,
          tx_out.address                      AS payment_addr_bech32,
          ENCODE(tx_out.payment_cred, 'hex')  AS payment_addr_cred,
          sa.view                             AS stake_addr,
          ENCODE(tx.hash, 'hex')              AS tx_hash,
          tx_out.index                        AS tx_index,
          tx_out.value::text                  AS value,
          tx_out.data_hash                    AS datum_hash,
          (CASE WHEN ma.policy IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'policy_id', ENCODE(ma.policy, 'hex'),
                'asset_name', ENCODE(ma.name, 'hex'),
                'fingerprint', ma.fingerprint,
                'decimals', aic.decimals,
                'quantity', mto.quantity::text
              )
            END
          ) AS asset_list,
          (CASE WHEN tx_out.inline_datum_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'bytes', ENCODE(datum.bytes, 'hex'),
                'value', datum.value
              )
            END
          ) AS inline_datum,
          (CASE WHEN tx_out.reference_script_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'hash', ENCODE(script.hash, 'hex'),
                'bytes', ENCODE(script.bytes, 'hex'),
                'value', script.json,
                'type', script.type::text,
                'size', script.serialised_size
              )
            END
          ) AS reference_script
        FROM reference_tx_in
          INNER JOIN tx_out ON tx_out.tx_id = reference_tx_in.tx_out_id
            AND tx_out.index = reference_tx_in.tx_out_index
          INNER JOIN tx ON tx_out.tx_id = tx.id
          LEFT JOIN stake_address AS sa ON tx_out.stake_address_id = sa.id
          LEFT JOIN ma_tx_out AS mto ON _assets IS TRUE AND mto.tx_out_id = tx_out.id
          LEFT JOIN multi_asset AS ma ON _assets IS TRUE AND ma.id = mto.ident
          LEFT JOIN grest.asset_info_cache AS aic ON _assets IS TRUE AND aic.asset_id = ma.id
          LEFT JOIN datum ON _scripts IS TRUE AND datum.id = tx_out.inline_datum_id
          LEFT JOIN script ON _scripts IS TRUE AND script.id = tx_out.reference_script_id
        WHERE
          (_inputs IS TRUE AND _scripts IS TRUE)
          AND reference_tx_in.tx_in_id = ANY(_tx_id_list)
      ),

      _all_inputs AS (
        SELECT
          tx_out.consumed_by_tx_id           AS tx_id,
          tx_out.address                     AS payment_addr_bech32,
          ENCODE(tx_out.payment_cred, 'hex') AS payment_addr_cred,
          sa.view                            AS stake_addr,
          ENCODE(tx.hash, 'hex')             AS tx_hash,
          tx_out.index                       AS tx_index,
          tx_out.value::text                 AS value,
          tx_out.data_hash                   AS datum_hash,
          (CASE WHEN ma.policy IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'policy_id', ENCODE(ma.policy, 'hex'),
                'asset_name', ENCODE(ma.name, 'hex'),
                'fingerprint', ma.fingerprint,
                'decimals', aic.decimals,
                'quantity', mto.quantity::text
              )
            END
          ) AS asset_list,
          (CASE WHEN tx_out.inline_datum_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'bytes', ENCODE(datum.bytes, 'hex'),
                'value', datum.value
              )
            END
          ) AS inline_datum,
          (CASE WHEN tx_out.reference_script_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'hash', ENCODE(script.hash, 'hex'),
                'bytes', ENCODE(script.bytes, 'hex'),
                'value', script.json,
                'type', script.type::text,
                'size', script.serialised_size
              )
            END
          ) AS reference_script
        FROM tx_out
          INNER JOIN tx ON tx_out.tx_id = tx.id
          LEFT JOIN stake_address AS sa ON tx_out.stake_address_id = sa.id
          LEFT JOIN ma_tx_out AS mto ON _assets IS TRUE AND mto.tx_out_id = tx_out.id
          LEFT JOIN multi_asset AS ma ON _assets IS TRUE AND ma.id = mto.ident
          LEFT JOIN grest.asset_info_cache AS aic ON _assets IS TRUE AND aic.asset_id = ma.id
          LEFT JOIN datum ON _scripts IS TRUE AND datum.id = tx_out.inline_datum_id
          LEFT JOIN script ON _scripts IS TRUE AND script.id = tx_out.reference_script_id
        WHERE _inputs IS TRUE
          AND tx_out.consumed_by_tx_id = ANY(_tx_id_list)
      ),

      _all_collateral_outputs AS (
        SELECT
          tx_out.tx_id,
          tx_out.address                      AS payment_addr_bech32,
          ENCODE(tx_out.payment_cred, 'hex')  AS payment_addr_cred,
          sa.view                             AS stake_addr,
          ENCODE(tx.hash, 'hex')              AS tx_hash,
          tx_out.index                        AS tx_index,
          tx_out.value::text                  AS value,
          tx_out.data_hash                    AS datum_hash,
          (CASE WHEN tx_out.inline_datum_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'bytes', ENCODE(datum.bytes, 'hex'),
                'value', datum.value
              )
            END
          ) AS inline_datum,
          (CASE WHEN tx_out.reference_script_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'hash', ENCODE(script.hash, 'hex'),
                'bytes', ENCODE(script.bytes, 'hex'),
                'value', script.json,
                'type', script.type::text,
                'size', script.serialised_size
              )
            END
          ) AS reference_script,
          REPLACE(tx_out.multi_assets_descr,'fromList ','')::jsonb AS asset_descr
        FROM
          collateral_tx_out AS tx_out
          INNER JOIN tx ON tx_out.tx_id = tx.id
          LEFT JOIN stake_address AS sa ON tx_out.stake_address_id = sa.id
          LEFT JOIN datum ON _scripts IS TRUE AND datum.id = tx_out.inline_datum_id
          LEFT JOIN script ON _scripts IS TRUE AND script.id = tx_out.reference_script_id
        WHERE _scripts IS TRUE
          AND tx_out.tx_id = ANY(_tx_id_list)
      ),

      _all_outputs AS (
        SELECT
          tx_out.tx_id,
          tx_out.address                      AS payment_addr_bech32,
          ENCODE(tx_out.payment_cred, 'hex')  AS payment_addr_cred,
          sa.view                             AS stake_addr,
          ENCODE(tx.hash, 'hex')              AS tx_hash,
          tx_out.index                        AS tx_index,
          tx_out.value::text                  AS value,
          tx_out.data_hash                    AS datum_hash,
          (CASE WHEN ma.policy IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'policy_id', ENCODE(ma.policy, 'hex'),
                'asset_name', ENCODE(ma.name, 'hex'),
                'fingerprint', ma.fingerprint,
                'decimals', aic.decimals,
                'quantity', mto.quantity::text
              )
            END
          ) AS asset_list,
          (CASE WHEN tx_out.inline_datum_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'bytes', ENCODE(datum.bytes, 'hex'),
                'value', datum.value
              )
            END
          ) AS inline_datum,
          (CASE WHEN tx_out.reference_script_id IS NULL THEN NULL
            ELSE
              JSONB_BUILD_OBJECT(
                'hash', ENCODE(script.hash, 'hex'),
                'bytes', ENCODE(script.bytes, 'hex'),
                'value', script.json,
                'type', script.type::text,
                'size', script.serialised_size
              )
            END
          ) AS reference_script
        FROM tx_out
          INNER JOIN tx ON tx_out.tx_id = tx.id
          LEFT JOIN stake_address AS sa ON tx_out.stake_address_id = sa.id
          LEFT JOIN ma_tx_out AS mto ON _assets IS TRUE AND mto.tx_out_id = tx_out.id
          LEFT JOIN multi_asset AS ma ON _assets IS TRUE AND ma.id = mto.ident
          LEFT JOIN grest.asset_info_cache AS aic ON _assets IS TRUE AND aic.asset_id = ma.id
          LEFT JOIN datum ON _scripts IS TRUE AND datum.id = tx_out.inline_datum_id
          LEFT JOIN script ON _scripts IS TRUE AND script.id = tx_out.reference_script_id
        WHERE tx_out.tx_id = ANY(_tx_id_list)
      ),

      _all_withdrawals AS (
        SELECT
          tx_id,
          JSONB_AGG(data) AS list
        FROM (
          SELECT
            w.tx_id,
            JSONB_BUILD_OBJECT(
              'amount', w.amount::text,
              'stake_addr', sa.view
            ) AS data
          FROM withdrawal AS w
            INNER JOIN stake_address AS sa ON w.addr_id = sa.id
          WHERE _withdrawals IS TRUE
            AND w.tx_id = ANY(_tx_id_list)
        ) AS tmp
        GROUP BY tx_id
      ),

      _all_mints AS (
        SELECT
          tx_id,
          JSONB_AGG(data) AS list
        FROM (
          SELECT
            mtm.tx_id,
            JSONB_BUILD_OBJECT(
              'policy_id', ENCODE(ma.policy, 'hex'),
              'asset_name', ENCODE(ma.name, 'hex'),
              'fingerprint', ma.fingerprint,
              'decimals', COALESCE(aic.decimals, 0),
              'quantity', mtm.quantity::text
            ) AS data
          FROM ma_tx_mint AS mtm
            INNER JOIN multi_asset AS ma ON ma.id = mtm.ident
            LEFT JOIN grest.asset_info_cache AS aic ON aic.asset_id = ma.id
          WHERE _assets IS TRUE
            AND mtm.tx_id = ANY(_tx_id_list)
        ) AS tmp
        GROUP BY tx_id
      ),

      _all_metadata AS (
        SELECT
          tx_id,
          JSONB_OBJECT_AGG(
            tm.key::text,
            tm.json
          ) AS list
        FROM tx_metadata AS tm
        WHERE _metadata IS TRUE
          AND tm.tx_id = ANY(_tx_id_list)
        GROUP BY tx_id
      ),

      _all_certs AS (
        SELECT
          tx_id,
          JSONB_AGG(data) AS list
        FROM (
          SELECT
            sr.tx_id,
            JSONB_BUILD_OBJECT(
              'index', sr.cert_index,
              'type', 'stake_registration',
              'info', JSONB_BUILD_OBJECT(
                'stake_address', sa.view,
                'deposit', sr.deposit::text
              )
            ) AS data
          FROM public.stake_registration AS sr
            INNER JOIN public.stake_address AS sa ON sa.id = sr.addr_id
          WHERE _certs IS TRUE
            AND sr.tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT
            sd.tx_id,
            JSONB_BUILD_OBJECT(
              'index', sd.cert_index,
              'type', 'stake_deregistration',
              'info', JSONB_BUILD_OBJECT(
                'stake_address', sa.view
              )
            ) AS data
          FROM public.stake_deregistration AS sd
            INNER JOIN public.stake_address AS sa ON sa.id = sd.addr_id
          WHERE _certs IS TRUE
            AND sd.tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT
            d.tx_id,
            JSONB_BUILD_OBJECT(
              'index', d.cert_index,
              'type', 'pool_delegation',
              'info', JSONB_BUILD_OBJECT(
                'stake_address', sa.view,
                'pool_id_bech32', ph.view,
                'pool_id_hex', ENCODE(ph.hash_raw, 'hex')
              )
            ) AS data
          FROM public.delegation AS d
            INNER JOIN public.stake_address AS sa ON sa.id = d.addr_id
            INNER JOIN public.pool_hash AS ph ON ph.id = d.pool_hash_id
          WHERE _certs IS TRUE
            AND d.tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT
            t.tx_id,
            JSONB_BUILD_OBJECT(
              'index', t.cert_index,
              'type', 'treasury_MIR',
              'info', JSONB_BUILD_OBJECT(
                'stake_address', sa.view,
                'amount', t.amount::text
              )
            ) AS data
          FROM public.treasury AS t
            INNER JOIN public.stake_address AS sa ON sa.id = t.addr_id
          WHERE _certs IS TRUE
            AND t.tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT
            r.tx_id,
            JSONB_BUILD_OBJECT(
              'index', r.cert_index,
              'type', 'reserve_MIR',
              'info', JSONB_BUILD_OBJECT(
                'stake_address', sa.view,
                'amount', r.amount::text
              )
            ) AS data
          FROM public.reserve AS r
            INNER JOIN public.stake_address AS sa ON sa.id = r.addr_id
          WHERE _certs IS TRUE
            AND r.tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT
            pt.tx_id,
            JSONB_BUILD_OBJECT(
              'index', pt.cert_index,
              'type', 'pot_transfer',
              'info', JSONB_BUILD_OBJECT(
                'treasury', pt.treasury::text,
                'reserves', pt.reserves::text
              )
            ) AS data
          FROM public.pot_transfer AS pt
          WHERE _certs IS TRUE
            AND pt.tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT DISTINCT ON (pp.registered_tx_id)
            -- SELECT DISTINCT below because there are multiple entries for each signing key of a given transaction
            pp.registered_tx_id AS tx_id,
            JSONB_BUILD_OBJECT(
              'index', NULL, -- cert_index not stored in param_proposal table
              'type', 'param_proposal',
              'info', JSONB_STRIP_NULLS(TO_JSONB(pp.*)) - array['id','registered_tx_id','epoch_no']
            ) AS data
          FROM public.param_proposal AS pp
            INNER JOIN cost_model AS cm ON cm.id = pp.cost_model_id
          WHERE _certs IS TRUE
            AND pp.registered_tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT
            pr.announced_tx_id AS tx_id,
            JSONB_BUILD_OBJECT(
              'index', pr.cert_index,
              'type', 'pool_retire',
              'info', JSONB_BUILD_OBJECT(
                'pool_id_bech32', ph.view,
                'pool_id_hex', ENCODE(ph.hash_raw, 'hex'),
                'retiring epoch', pr.retiring_epoch
              )
            ) AS data
          FROM public.pool_retire AS pr
            INNER JOIN public.pool_hash AS ph ON ph.id = pr.hash_id
          WHERE _certs IS TRUE
            AND pr.announced_tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT
            pu.registered_tx_id AS tx_id,
            JSONB_BUILD_OBJECT(
              'index', pu.cert_index,
              'type', 'pool_update',
              'info', JSONB_BUILD_OBJECT(
                'pool_id_bech32', ph.view,
                'pool_id_hex', ENCODE(ph.hash_raw, 'hex'),
                'active_epoch_no', pu.active_epoch_no,
                'vrf_key_hash', ENCODE(pu.vrf_key_hash, 'hex'),
                'margin', pu.margin,
                'fixed_cost', pu.fixed_cost::text,
                'pledge', pu.pledge::text,
                'reward_addr', sa.view,
                'owners', JSONB_AGG(po.view),
                'relays', JSONB_AGG(JSONB_BUILD_OBJECT (
                  'ipv4', pr.ipv4,
                  'ipv6', pr.ipv6,
                  'dns', pr.dns_name,
                  'srv', pr.dns_srv_name,
                  'port', pr.port
                )),
                'meta_url', pmr.url,
                'meta_hash', ENCODE(pmr.hash, 'hex')
              )
            ) AS data
          FROM public.pool_update AS pu
            LEFT JOIN public.pool_hash AS ph ON pu.hash_id = ph.id
            LEFT JOIN public.stake_address AS sa ON pu.reward_addr_id = sa.id
            LEFT JOIN (
                SELECT po1.pool_update_id, sa1.view
                FROM public.pool_owner AS po1
                  LEFT JOIN public.stake_address AS sa1 ON sa1.id = po1.addr_id
              ) AS po ON pu.id = po.pool_update_id
            LEFT JOIN public.pool_relay AS pr ON pu.id = pr.update_id
            LEFT JOIN public.pool_metadata_ref AS pmr ON pu.meta_id = pmr.id
          WHERE _certs IS TRUE
            AND pu.registered_tx_id = ANY(_tx_id_list)
          GROUP BY pu.registered_tx_id, pu.cert_index, ph.view, ph.hash_raw, pu.active_epoch_no, pu.vrf_key_hash, pu.margin, pu.fixed_cost, pu.pledge, sa.view, pmr.url, pmr.hash
          --
          UNION ALL
          --
          SELECT
            dv.tx_id,
            JSONB_BUILD_OBJECT(
              'index', dv.cert_index,
              'type', 'vote_delegation',
              'info', JSONB_BUILD_OBJECT(
                'stake_address', sa.view,
                'drep_id', dh.view,
                'drep_hex', ENCODE(dh.raw, 'hex')
              )
            ) AS data
          FROM public.delegation_vote AS dv
            INNER JOIN public.drep_hash AS dh ON dh.id = dv.drep_hash_id
            INNER JOIN public.stake_address AS sa ON sa.id = dv.addr_id
          WHERE _certs IS TRUE
            AND dv.tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT
            dr.tx_id,
            JSONB_BUILD_OBJECT(
              'index', dr.cert_index,
              'type', 'drep_registration',
              'info', JSONB_BUILD_OBJECT(
                'drep_id', dh.view,
                'drep_hex', ENCODE(dh.raw, 'hex'),
                'deposit', dr.deposit::text,
                'meta_url', va.url,
                'meta_hash', va.data_hash
              )
            ) AS data
          FROM public.drep_registration AS dr
            INNER JOIN public.drep_hash AS dh ON dh.id = dr.drep_hash_id
            LEFT JOIN public.voting_anchor AS va ON va.id = dr.voting_anchor_id
          WHERE _certs IS TRUE
            AND dr.tx_id = ANY(_tx_id_list)
            AND dr.deposit IS NOT NULL
            AND dr.deposit >= 0
          --
          UNION ALL
          --
          SELECT
            dr.tx_id,
            JSONB_BUILD_OBJECT(
              'index', dr.cert_index,
              'type', 'drep_update',
              'info', JSONB_BUILD_OBJECT(
                'drep_id', dh.view,
                'drep_hex', ENCODE(dh.raw, 'hex'),
                'meta_url', va.url,
                'meta_hash', va.data_hash
              )
            ) AS data
          FROM public.drep_registration AS dr
            INNER JOIN public.drep_hash AS dh ON dh.id = dr.drep_hash_id
            LEFT JOIN public.voting_anchor AS va ON va.id = dr.voting_anchor_id
          WHERE _certs IS TRUE
            AND dr.tx_id = ANY(_tx_id_list)
            AND dr.deposit IS NULL
          --
          UNION ALL
          --
          SELECT
            dr.tx_id,
            JSONB_BUILD_OBJECT(
              'index', dr.cert_index,
              'type', 'drep_retire',
              'info', JSONB_BUILD_OBJECT(
                'drep_id', dh.view,
                'drep_hex', ENCODE(dh.raw, 'hex')
              )
            ) AS data
          FROM public.drep_registration AS dr
            INNER JOIN public.drep_hash AS dh ON dh.id = dr.drep_hash_id
          WHERE _certs IS TRUE
            AND dr.tx_id = ANY(_tx_id_list)
            AND dr.deposit IS NOT NULL
            AND dr.deposit < 0
          --
          UNION ALL
          --
          SELECT
            cr.tx_id,
            JSONB_BUILD_OBJECT(
              'index', cr.cert_index,
              'type', 'committee_hot_auth',
              'info', JSONB_BUILD_OBJECT(
                'cc_cold_hex', ENCODE(ch_cold.raw, 'hex'),
                'cc_cold_has_script', ch_cold.has_script,
                'cc_hot_hex', ENCODE(ch_hot.raw, 'hex'),
                'cc_hot_has_script', ch_hot.has_script
              )
            ) AS data
          FROM public.committee_registration AS cr
            INNER JOIN public.committee_hash AS ch_cold ON ch_cold.id = cr.cold_key_id
            INNER JOIN public.committee_hash AS ch_hot ON ch_hot.id = cr.hot_key_id
          WHERE _certs IS TRUE
            AND cr.tx_id = ANY(_tx_id_list)
          --
          UNION ALL
          --
          SELECT
            cdr.tx_id,
            JSONB_BUILD_OBJECT(
              'index', cdr.cert_index,
              'type', 'committee_resign',
              'info', JSONB_BUILD_OBJECT(
                'cc_cold_hex', ENCODE(ch.raw, 'hex'),
                'cc_cold_has_script', ch.has_script,
                'meta_url', va.url,
                'meta_hash', va.data_hash
              )
            ) AS data
          FROM public.committee_de_registration AS cdr
            INNER JOIN public.committee_hash AS ch ON ch.id = cdr.cold_key_id
            LEFT JOIN public.voting_anchor AS va ON va.id = cdr.voting_anchor_id
          WHERE _certs IS TRUE
            AND cdr.tx_id = ANY(_tx_id_list)
        ) AS tmp
        GROUP BY tx_id
      ),

      _all_native_scripts AS (
        SELECT
          tx_id,
          JSONB_AGG(data) AS list
        FROM (
          SELECT
            script.tx_id,
            JSONB_BUILD_OBJECT(
              'script_hash', ENCODE(script.hash, 'hex'),
              'script_json', script.json
            ) AS data
          FROM script
          WHERE _scripts IS TRUE
            AND script.tx_id = ANY(_tx_id_list)
            AND script.type = 'timelock'
        ) AS tmp
        GROUP BY tx_id
      ),

      _all_plutus_contracts AS (
        SELECT
          tx_id,
          JSONB_AGG(data) AS list
        FROM (
          WITH
            all_redeemers AS (
              SELECT
                redeemer.id,
                redeemer.tx_id,
                redeemer.purpose,
                redeemer.fee,
                redeemer.unit_steps,
                redeemer.unit_mem,
                rd.hash AS rd_hash,
                rd.value AS rd_value,
                script.hash AS script_hash,
                CASE WHEN _bytecode IS TRUE THEN
                  script.bytes
                END AS script_bytes,
                script.serialised_size AS script_serialised_size,
                tx.valid_contract
              FROM redeemer
                INNER JOIN tx ON redeemer.tx_id = tx.id
                INNER JOIN redeemer_data AS rd ON rd.id = redeemer.redeemer_data_id
                INNER JOIN script ON redeemer.script_hash = script.hash
              WHERE _scripts IS TRUE
                AND redeemer.tx_id = ANY(_tx_id_list)
            ),

            _all_inputs_sorted AS (
              SELECT
                ROW_NUMBER () OVER (
                  PARTITION BY ai.tx_id
                  ORDER BY ai.tx_hash, ai.tx_index
                ) - 1 AS sorted_index,
                ai.*
              FROM (
                SELECT DISTINCT ON (_ai.tx_hash, _ai.tx_index)
                  _ai.tx_id,
                  _ai.tx_hash,
                  _ai.tx_index,
                  _ai.payment_addr_bech32 as address,
                  _ai.datum_hash
                from _all_inputs as _ai
              ) as ai
            ),

            spend_redeemers AS (
              SELECT DISTINCT ON (redeemer.id)
                redeemer.id,
                ais.address,
                ais.tx_hash,
                ais.tx_index,
                ind.hash AS ind_hash,
                ind.value AS ind_value
              FROM redeemer
              INNER JOIN _all_inputs_sorted AS ais ON ais.tx_id = redeemer.tx_id AND ais.sorted_index = redeemer.index
              INNER JOIN datum AS ind ON ind.hash = ais.datum_hash
              WHERE _scripts IS TRUE
                AND redeemer.tx_id = ANY(_tx_id_list)
                AND redeemer.purpose = 'spend'
            )

          SELECT
            ar.tx_id,
            JSONB_BUILD_OBJECT(
              'address',
                CASE
                  WHEN ar.purpose = 'spend' THEN
                    (SELECT address FROM spend_redeemers AS sr WHERE sr.id = ar.id)
                END,
              'spends_input',
                CASE
                  WHEN ar.purpose = 'spend' THEN
                    (
                      SELECT JSONB_BUILD_OBJECT(
                        'tx_hash', sr.tx_hash,
                        'tx_index', sr.tx_index
                      )
                      FROM spend_redeemers AS sr
                      WHERE sr.id = ar.id
                    )
                END,
              'script_hash', ENCODE(ar.script_hash, 'hex'),
              'bytecode',
                CASE
                  WHEN _bytecode IS TRUE THEN
                    ENCODE(ar.script_bytes, 'hex')
                END,
              'size', ar.script_serialised_size,
              'valid_contract', ar.valid_contract,
              'input', JSONB_BUILD_OBJECT(
                'redeemer', JSONB_BUILD_OBJECT(
                  'purpose', ar.purpose,
                  'fee', ar.fee::text,
                  'unit', JSONB_BUILD_OBJECT(
                    'steps', ar.unit_steps::text,
                    'mem', ar.unit_mem::text
                  ),
                  'datum', JSONB_BUILD_OBJECT(
                    'hash', ENCODE(ar.rd_hash, 'hex'),
                    'value', ar.rd_value
                  )
                ),
                'datum', CASE WHEN ar.purpose = 'spend' THEN (
                    SELECT JSONB_BUILD_OBJECT(
                      'hash', ENCODE(sr.ind_hash, 'hex'),
                      'value', sr.ind_value
                    )
                    FROM spend_redeemers AS sr WHERE sr.id = ar.id
                  ) END
              )
            ) AS data
          FROM all_redeemers AS ar
          WHERE _scripts IS TRUE
        ) AS tmp
        GROUP BY tx_id
      ),

      _all_voting_procedures AS (
        SELECT
          tx_id,
          JSONB_AGG(data) AS list
        FROM (
          SELECT
            vp.tx_id,
            JSONB_BUILD_OBJECT(
              'proposal_tx_hash', ENCODE(tx.hash, 'hex'),
              'proposal_index', gap.index,
              'voter_role', vp.voter_role,
              'voter', COALESCE(ENCODE(ch.raw, 'hex'), dh.view, ph.view),
              'voter_hex', COALESCE(ENCODE(ch.raw, 'hex'), ENCODE(dh.raw, 'hex'), ENCODE(ph.hash_raw, 'hex')),
              'vote', vp.vote
            ) AS data
          FROM voting_procedure AS vp
            INNER JOIN public.gov_action_proposal AS gap ON vp.gov_action_proposal_id = gap.id
            INNER JOIN public.tx ON gap.tx_id = tx.id
            LEFT JOIN public.drep_hash AS dh ON vp.drep_voter = dh.id
            LEFT JOIN public.pool_hash AS ph ON vp.pool_voter = ph.id
            LEFT JOIN public.committee_hash AS ch ON vp.committee_voter = ch.id
          WHERE _governance IS TRUE
            AND vp.tx_id = ANY(_tx_id_list)
        ) AS tmp
        GROUP BY tx_id
      ),

      _all_proposal_procedures AS (
        SELECT
          tx_id,
          JSONB_AGG(data) AS list
        FROM (
          SELECT
            gap.tx_id,
            JSONB_BUILD_OBJECT(
              'index', gap.index,
              'type', gap.type,
              'description', gap.description,
              'deposit', gap.deposit::text,
              'return_address', sa.view,
              'expiration', gap.expiration,
              'meta_url', va.url,
              'meta_hash', ENCODE(va.data_hash, 'hex'),
              'withdrawal', CASE
                WHEN tw.id IS NULL THEN NULL
                ELSE
                  JSONB_BUILD_OBJECT(
                    'stake_address', (
                      SELECT sa2.view
                      FROM stake_address AS sa2
                      WHERE sa2.id = tw.stake_address_id
                    ),
                    'amount', tw.amount::text
                  )
              END,
              'param_proposal', CASE
                WHEN pp.id IS NULL THEN NULL
                ELSE ( SELECT JSONB_STRIP_NULLS(TO_JSONB(pp.*)) - array['id','registered_tx_id','epoch_no'] )
              END
            ) AS data
          FROM gov_action_proposal AS gap
          INNER JOIN public.stake_address AS sa ON gap.return_address = sa.id
          LEFT JOIN public.treasury_withdrawal AS tw ON gap.id = tw.gov_action_proposal_id
          LEFT JOIN public.param_proposal AS pp ON gap.param_proposal = pp.id
          LEFT JOIN public.cost_model AS cm ON cm.id = pp.cost_model_id
          LEFT JOIN public.voting_anchor AS va ON gap.voting_anchor_id = va.id
          WHERE _governance IS TRUE
            AND gap.tx_id = ANY(_tx_id_list)
        ) AS tmp
        GROUP BY tx_id
      )

    SELECT
      ENCODE(atx.tx_hash, 'hex'),
      ENCODE(atx.block_hash, 'hex'),
      atx.block_height,
      atx.epoch_no,
      atx.epoch_slot,
      atx.absolute_slot,
      EXTRACT(EPOCH FROM atx.tx_timestamp)::integer,
      atx.tx_block_index,
      atx.tx_size,
      atx.total_output::text,
      atx.fee::text,
      atx.treasury_donation::text,
      atx.deposit::text,
      atx.invalid_before::text,
      atx.invalid_after::text,
      COALESCE((
        SELECT JSONB_AGG(tx_collateral_inputs)
        FROM (
          SELECT
            JSONB_BUILD_OBJECT(
              'payment_addr', JSONB_BUILD_OBJECT(
                'bech32', payment_addr_bech32,
                'cred', payment_addr_cred
              ),
              'stake_addr', stake_addr,
              'tx_hash', aci.tx_hash,
              'tx_index', tx_index,
              'value', value,
              'datum_hash', ENCODE(datum_hash, 'hex'),
              'inline_datum', inline_datum,
              'reference_script', reference_script,
              'asset_list', COALESCE(JSONB_AGG(asset_list) FILTER (WHERE asset_list IS NOT NULL), JSONB_BUILD_ARRAY())
            ) AS tx_collateral_inputs
          FROM _all_collateral_inputs AS aci
          WHERE (_inputs IS TRUE AND _scripts IS TRUE) AND aci.tx_id = atx.id
          GROUP BY payment_addr_bech32, payment_addr_cred, stake_addr, aci.tx_hash, tx_index, value, datum_hash, inline_datum, reference_script
        ) AS tmp
      ), JSONB_BUILD_ARRAY()),
      (
        SELECT
          JSONB_BUILD_OBJECT(
            'payment_addr', JSONB_BUILD_OBJECT(
              'bech32', payment_addr_bech32,
              'cred', payment_addr_cred
            ),
            'stake_addr', stake_addr,
            'tx_hash', aco.tx_hash,
            'tx_index', tx_index,
            'value', value,
            'datum_hash', ENCODE(datum_hash, 'hex'),
            'inline_datum', inline_datum,
            'reference_script', reference_script,
            'asset_list', asset_descr
          ) AS tx_collateral_outputs
        FROM _all_collateral_outputs AS aco
        WHERE _scripts IS TRUE AND aco.tx_id = atx.id
        GROUP BY payment_addr_bech32, payment_addr_cred, stake_addr, aco.tx_hash, tx_index, value, datum_hash, inline_datum, reference_script, asset_descr
        LIMIT 1 -- there can only be one collateral output
      ),
      COALESCE((
        SELECT JSONB_AGG(tx_reference_inputs)
        FROM (
          SELECT
            JSONB_BUILD_OBJECT(
              'payment_addr', JSONB_BUILD_OBJECT(
                'bech32', payment_addr_bech32,
                'cred', payment_addr_cred
              ),
              'stake_addr', stake_addr,
              'tx_hash', ari.tx_hash,
              'tx_index', tx_index,
              'value', value,
              'datum_hash', ENCODE(datum_hash, 'hex'),
              'inline_datum', inline_datum,
              'reference_script', reference_script,
              'asset_list', COALESCE(JSONB_AGG(asset_list) FILTER (WHERE asset_list IS NOT NULL), JSONB_BUILD_ARRAY())
            ) AS tx_reference_inputs
          FROM _all_reference_inputs AS ari
          WHERE (_inputs IS TRUE AND _scripts IS TRUE) AND ari.tx_id = atx.id
          GROUP BY payment_addr_bech32, payment_addr_cred, stake_addr, ari.tx_hash, tx_index, value, datum_hash, inline_datum, reference_script
        ) AS tmp
      ), JSONB_BUILD_ARRAY()),
      COALESCE((
        SELECT JSONB_AGG(tx_inputs)
        FROM (
          SELECT
            JSONB_BUILD_OBJECT(
              'payment_addr', JSONB_BUILD_OBJECT(
                'bech32', payment_addr_bech32,
                'cred', payment_addr_cred
              ),
              'stake_addr', stake_addr,
              'tx_hash', ai.tx_hash,
              'tx_index', tx_index,
              'value', value,
              'datum_hash', ENCODE(datum_hash, 'hex'),
              'inline_datum', inline_datum,
              'reference_script', reference_script,
              'asset_list', COALESCE(JSONB_AGG(asset_list) FILTER (WHERE asset_list IS NOT NULL), JSONB_BUILD_ARRAY())
            ) AS tx_inputs
          FROM _all_inputs AS ai
          WHERE _inputs IS TRUE AND ai.tx_id = atx.id
          GROUP BY payment_addr_bech32, payment_addr_cred, stake_addr, ai.tx_hash, tx_index, value, datum_hash, inline_datum, reference_script
        ) AS tmp
      ), JSONB_BUILD_ARRAY()),
      COALESCE((
        SELECT JSONB_AGG(tx_outputs)
        FROM (
          SELECT
            JSONB_BUILD_OBJECT(
              'payment_addr', JSONB_BUILD_OBJECT(
                'bech32', payment_addr_bech32,
                'cred', payment_addr_cred
              ),
              'stake_addr', stake_addr,
              'tx_hash', ao.tx_hash,
              'tx_index', tx_index,
              'value', value,
              'datum_hash', ENCODE(datum_hash, 'hex'),
              'inline_datum', inline_datum,
              'reference_script', reference_script,
              'asset_list', COALESCE(JSONB_AGG(asset_list) FILTER (WHERE asset_list IS NOT NULL), JSONB_BUILD_ARRAY())
            ) AS tx_outputs
          FROM _all_outputs AS ao
          WHERE ao.tx_id = atx.id
          GROUP BY payment_addr_bech32, payment_addr_cred, stake_addr, ao.tx_hash, tx_index, value, datum_hash, inline_datum, reference_script
        ) AS tmp
      ), JSONB_BUILD_ARRAY()),
      COALESCE((SELECT aw.list FROM _all_withdrawals AS aw WHERE _withdrawals IS TRUE AND aw.tx_id = atx.id), JSONB_BUILD_ARRAY()),
      COALESCE((SELECT ami.list FROM _all_mints AS ami WHERE _assets IS TRUE AND ami.tx_id = atx.id), JSONB_BUILD_ARRAY()),
      COALESCE((SELECT ame.list FROM _all_metadata AS ame WHERE _metadata IS TRUE AND ame.tx_id = atx.id), NULL),
      COALESCE((SELECT ac.list FROM _all_certs AS ac WHERE _certs IS TRUE AND ac.tx_id = atx.id), JSONB_BUILD_ARRAY()),
      COALESCE((SELECT ans.list FROM _all_native_scripts AS ans WHERE _scripts IS TRUE AND ans.tx_id = atx.id), JSONB_BUILD_ARRAY()),
      COALESCE((SELECT apc.list FROM _all_plutus_contracts AS apc WHERE _scripts IS TRUE AND apc.tx_id = atx.id), JSONB_BUILD_ARRAY()),
      COALESCE((SELECT avp.list FROM _all_voting_procedures AS avp WHERE _governance IS TRUE AND avp.tx_id = atx.id), JSONB_BUILD_ARRAY()),
      COALESCE((SELECT app.list FROM _all_proposal_procedures AS app WHERE _governance IS TRUE AND app.tx_id = atx.id), JSONB_BUILD_ARRAY())
    FROM _all_tx AS atx
    WHERE atx.id = ANY(_tx_id_list)
  );

END;
$$;

COMMENT ON FUNCTION grest.tx_info IS 'Get information about transactions.'; -- noqa: LT01
