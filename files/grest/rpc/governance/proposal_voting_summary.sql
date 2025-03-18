CREATE OR REPLACE FUNCTION grest.proposal_voting_summary(_proposal_id text)
RETURNS TABLE (
  proposal_type text,
  epoch_no integer,
  drep_yes_votes_cast integer,
  drep_active_yes_vote_power text,
  drep_yes_vote_power text,
  drep_yes_pct numeric,
  drep_no_votes_cast integer,
  drep_active_no_vote_power text,
  drep_no_vote_power text,
  drep_no_pct numeric,
  drep_abstain_votes_cast integer,
  drep_active_abstain_vote_power text,
  drep_always_no_confidence_vote_power text,
  drep_always_abstain_vote_power text,
  pool_yes_votes_cast integer,
  pool_active_yes_vote_power text,
  pool_yes_vote_power text,
  pool_yes_pct numeric,
  pool_no_votes_cast integer,
  pool_active_no_vote_power text,
  pool_no_vote_power text,
  pool_no_pct numeric,
  pool_abstain_votes_cast integer,
  pool_active_abstain_vote_power text,
  pool_passive_always_abstain_votes_assigned integer,
  pool_passive_always_abstain_vote_power text,
  pool_passive_always_no_confidence_votes_assigned integer,
  pool_passive_always_no_confidence_vote_power text,
  committee_yes_votes_cast integer,
  committee_yes_pct numeric,
  committee_no_votes_cast integer,
  committee_no_pct numeric,
  committee_abstain_votes_cast integer
)
LANGUAGE plpgsql
AS $$
DECLARE
  proposal text[];
  proposal_id integer;
BEGIN

  SELECT INTO proposal grest.cip129_from_gov_action_id(_proposal_id);

  SELECT gap.id INTO proposal_id
    FROM gov_action_proposal AS gap
      LEFT JOIN tx AS t ON gap.tx_id = t.id
    WHERE t.hash = DECODE(proposal[1], 'hex') AND gap.index = proposal[2]::smallint;

  RETURN QUERY (
    WITH
      latest_votes AS (
        SELECT * FROM voting_procedure AS vp
        WHERE vp.gov_action_proposal_id = proposal_id
          AND NOT EXISTS (SELECT 1 FROM voting_procedure AS vp2
                          WHERE vp2.gov_action_proposal_id = vp.gov_action_proposal_id
                          AND vp2.id > vp.id
                          AND vp2.voter_role = vp.voter_role
                          AND coalesce(vp2.drep_voter, vp2.pool_voter, vp2.committee_voter) = 
                              coalesce(vp.drep_voter, vp.pool_voter, vp.committee_voter))
      ),
      proposal_epoch_data AS (
        SELECT
          gap.id AS gov_action_proposal_id,
          gap.type AS proposal_type,
          expired_epoch,
          ratified_epoch,
          dropped_epoch,
          (coalesce(ratified_epoch, expired_epoch, dropped_epoch, ( SELECT MAX(no) FROM epoch))) AS epoch_of_interest
        FROM gov_action_proposal AS gap 
        WHERE proposal_id = gap.id
      ),
      tot_drep_power AS (
        SELECT ped.gov_action_proposal_id, SUM(amount) AS tot_drep_power 
        FROM drep_distr AS dd 
          INNER JOIN proposal_epoch_data AS ped ON dd.epoch_no = epoch_of_interest
        GROUP BY ped.gov_action_proposal_id
      ),
      -- voting power for drep that's been inactive for too long will be treated as abstain
      inactive_drep_power AS (
        SELECT ped.gov_action_proposal_id, SUM(amount) AS inactive_drep_power 
        FROM drep_distr AS dd 
          INNER JOIN proposal_epoch_data AS ped ON dd.epoch_no = epoch_of_interest
          AND dd.active_until is not NULL AND dd.active_until < epoch_of_interest
          AND NOT EXISTS (SELECT 1 FROM voting_procedure vp INNER JOIN tx t on vp.tx_id = t.id INNER JOIN block b on b.id = t.block_id AND b.epoch_no = epoch_of_interest AND vp.voter_role = 'DRep' and vp.drep_voter = dd.hash_id)        
        GROUP BY ped.gov_action_proposal_id
      ),
      active_prop_drep_votes AS (
        SELECT
          ped.gov_action_proposal_id,
          coalesce(SUM(amount),0) AS active_drep_vote_total,
          vote,
          COUNT(*) AS active_drep_votes_cast
        FROM proposal_epoch_data AS ped 
          INNER JOIN latest_votes AS vp ON vp.voter_role = 'DRep' AND vp.gov_action_proposal_id = ped.gov_action_proposal_id
          LEFT JOIN drep_distr AS dd ON vp.drep_voter = dd.hash_id AND dd.epoch_no = ped.epoch_of_interest
          WHERE NOT EXISTS 
          (
            SELECT 1 
            FROM drep_registration AS dr 
            WHERE dr.drep_hash_id = vp.drep_voter
              AND deposit < 0 -- deregistration tx
              AND dr.tx_id > vp.tx_id -- submitted after vote tx
              AND dr.tx_id < -- but before last tx of epoch preceding ratification/expiry/drop epoch or last tx of current epoch
                ( 
                  SELECT i_last_tx_id 
                  FROM grest.epoch_info_cache AS eic
                  WHERE eic.epoch_no = 
                    (coalesce(ped.ratified_epoch, ped.expired_epoch, ped.dropped_epoch, (SELECT MAX(no) + 1 FROM epoch)) - 1)
                )
          )
        GROUP BY ped.gov_action_proposal_id, vote
      ),
      always_no_conf_data AS (
        SELECT
          ped.gov_action_proposal_id,
          amount AS always_no_conf
        FROM proposal_epoch_data AS ped
          INNER JOIN drep_distr AS dd ON dd.epoch_no = ped.epoch_of_interest
          INNER JOIN drep_hash AS dh ON dh.view = 'drep_always_no_confidence' AND dd.hash_id = dh.id
      ),
      always_abstain_data AS (
        SELECT
          ped.gov_action_proposal_id,
          amount AS always_abstain 
        FROM proposal_epoch_data AS ped
          INNER JOIN drep_distr AS dd ON dd.epoch_no = ped.epoch_of_interest
          INNER JOIN drep_hash AS dh ON dh.view = 'drep_always_abstain' AND dd.hash_id = dh.id
      ),
      tot_pool_power AS (
        SELECT
          ped.gov_action_proposal_id,
          SUM(voting_power) AS tot_pool_power 
        FROM proposal_epoch_data AS ped
          INNER JOIN pool_stat ON pool_stat.epoch_no = ped.epoch_of_interest
        GROUP BY ped.gov_action_proposal_id, pool_stat.epoch_no
      ),
      active_prop_pool_votes AS (
        SELECT
          ped.gov_action_proposal_id,
          SUM(voting_power) AS active_pool_vote_total,
          vote,
          COUNT(*) AS pool_votes_cast
        FROM proposal_epoch_data AS ped
          INNER JOIN latest_votes AS vp ON vp.voter_role = 'SPO' AND vp.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN pool_stat ON vp.pool_voter = pool_stat.pool_hash_id AND pool_stat.epoch_no = ped.epoch_of_interest
        GROUP BY ped.gov_action_proposal_id, vote
      ),
      -- below snippet is cut down version of pool_info endpoint sql, not sure how easy it was to re-use?
      _all_non_voted_pool_info AS (
        SELECT DISTINCT ON (pic.pool_hash_id)
          pic.pool_hash_id,
          pic.update_id,
          cardano.bech32_encode('pool', ph.hash_raw) AS pool_id_bech32,
          ph.hash_raw
        FROM grest.pool_info_cache AS pic
          -- short-circuit non-voted pool data for proposals where SPO cannot vote - TODO parameter change logic, hopefully not too ugly
          INNER JOIN proposal_epoch_data ped ON ped.proposal_type NOT IN ('TreasuryWithdrawals', 'NewConstitution')
          INNER JOIN pool_stat ps ON ps.pool_hash_id = pic.pool_hash_id AND ps.epoch_no = ped.epoch_of_interest -- AND ps.voting_power is not null
          INNER JOIN public.pool_hash AS ph ON ph.id = pic.pool_hash_id
          INNER JOIN pool_update AS pu ON pu.id = pic.update_id AND pu.active_epoch_no <= ped.epoch_of_interest
          INNER JOIN public.stake_address AS sa ON pu.reward_addr_id = sa.id
            AND EXISTS (SELECT NULL FROM delegation_vote dv WHERE dv.addr_id = sa.id) -- reward address delegated to drep at least once?
        WHERE NOT EXISTS ( -- exclude all pools that voted for this proposal
          SELECT NULL
          FROM proposal_epoch_data AS ped
            INNER JOIN latest_votes AS VP ON vp.voter_role = 'SPO' AND vp.gov_action_proposal_id = ped.gov_action_proposal_id
        			AND vp.pool_voter = pic.pool_hash_id)
        ORDER BY
          pic.pool_hash_id,
          pic.tx_id DESC
      ),
      passive_prop_pool_votes AS (
      	SELECT
      	ped.gov_action_proposal_id,
        (CASE WHEN dh.view = 'drep_always_abstain' THEN 'Abstain' ELSE 'No' END) AS vote, -- else = drep_always_no_confidence currently
        SUM(pstat.voting_power) passive_pool_vote_total,
        COUNT(*) AS pool_votes_cast
        FROM _all_non_voted_pool_info AS api
        INNER JOIN proposal_epoch_data AS ped ON true
        INNER JOIN public.pool_update AS pu ON pu.id = api.update_id
        INNER JOIN public.stake_address AS sa ON pu.reward_addr_id = sa.id
        INNER JOIN delegation_vote AS dv on dv.addr_id = sa.id
          AND dv.tx_id = (SELECT MAX(tx_id) FROM delegation_vote dv2 WHERE dv2.addr_id = sa.id -- get the details of most recent vote delegation for this addr before last tx of epoch-of-interest
          AND tx_id <= -- TODO - maybe calculate these once off in the early CTE and use it from there
            (CASE
              WHEN (SELECT MAX(no) FROM epoch) = ped.epoch_of_interest THEN 
                (SELECT t.id FROM tx AS t ORDER BY id DESC LIMIT 1) 
              ELSE
                (SELECT MAX(i_last_tx_id) FROM grest.epoch_info_cache AS eic WHERE eic.epoch_no <= ped.epoch_of_interest)
            END)
      )
	    INNER JOIN drep_hash AS dh on dh.id = dv.drep_hash_id
	        and dh.view like 'drep_always%'
	    INNER JOIN pool_stat AS pstat ON api.pool_hash_id = pstat.pool_hash_id
	        and pstat.epoch_no = ped.epoch_of_interest
	    GROUP BY ped.gov_action_proposal_id, dh.view
	  ),
      committee_votes AS (
        SELECT
          ped.gov_action_proposal_id,
          vote,
          COUNT(*) AS committee_votes_cast
        FROM proposal_epoch_data AS ped 
          INNER JOIN latest_votes AS vp ON vp.voter_role = 'ConstitutionalCommittee'
          -- TODO: add logic to only count valid committee member votes, need a way to get committee ids for a given epoch...

          INNER JOIN committee_registration cr ON cr.hot_key_id = vp.committee_voter 
          INNER JOIN committee_member cm ON cr.cold_key_id = cm.committee_hash_id 
          INNER JOIN committee c ON c.id = cm.committee_id 
            AND c.id = (SELECT id FROM committee 
              WHERE 
              (gov_action_proposal_id IN
                (
                  SELECT id 
                  FROM gov_action_proposal 
                  WHERE enacted_epoch IS NOT null AND enacted_epoch <= ped.epoch_of_interest AND type = 'NewCommittee' 
                  ORDER BY id DESC
                  LIMIT 1
                )
              OR gov_action_proposal_id IS null)
          ORDER BY id DESC LIMIT 1)

          AND vp.gov_action_proposal_id = ped.gov_action_proposal_id
        GROUP BY ped.gov_action_proposal_id, vote
      ),
      tot_committee_size AS (
        SELECT 
          ped.gov_action_proposal_id,
          count(cm.id) AS committee_size
        FROM epoch_state AS epstate
          INNER JOIN proposal_epoch_data AS ped on epstate.epoch_no = ped.epoch_of_interest
          INNER JOIN committee AS c on epstate.committee_id = c.id
          INNER JOIN committee_member AS cm on cm.committee_id = c.id
        GROUP BY ped.gov_action_proposal_id
      ),
      combined_data AS (
        SELECT 
          ped.gov_action_proposal_id,
          ped.proposal_type,
          ped.epoch_of_interest,
          tot_drep_power,
          inactive_drep_power,
          always_no_conf,
          always_abstain,
          committee_size,
          tot_pool_power
        FROM proposal_epoch_data AS ped
          INNER JOIN tot_drep_power ON tot_drep_power.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN inactive_drep_power ON inactive_drep_power.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN always_no_conf_data ON always_no_conf_data.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN always_abstain_data ON always_abstain_data.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN tot_pool_power ON tot_pool_power.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN tot_committee_size on tot_committee_size.gov_action_proposal_id = ped.gov_action_proposal_id
      )

    SELECT
      y.proposal_type::text AS proposal_type,
      y.epoch_of_interest AS epoch_no,
      y.drep_yes_votes_cast::integer,
      y.drep_yes_vote_power::text AS drep_active_yes_vote_power,
      (CASE
        WHEN y.proposal_type IN ('NoConfidence') THEN y.drep_yes_vote_power + y.drep_no_confidence_vote_power
        ELSE y.drep_yes_vote_power
       END)::text AS drep_yes_vote_power,
   	  (CASE
        WHEN y.proposal_type IN ('NoConfidence') THEN ROUND((y.drep_yes_vote_power + drep_no_confidence_vote_power) * 100 / y.drep_non_abstain_total, 2)
        ELSE ROUND(y.drep_yes_vote_power * 100 / y.drep_non_abstain_total, 2) 
      END) AS drep_yes_pct,
      y.drep_no_votes_cast::integer,
      y.drep_no_vote_power::text AS drep_active_no_vote_power,
      (CASE
        WHEN y.proposal_type IN ('NoConfidence') THEN (y.drep_non_abstain_total - y.drep_yes_vote_power - y.drep_no_confidence_vote_power)
      	ELSE (y.drep_non_abstain_total - y.drep_yes_vote_power)
      END)::text AS drep_no_vote_power,
      (CASE
        WHEN y.proposal_type IN ('NoConfidence') THEN ROUND((y.drep_non_abstain_total - y.drep_yes_vote_power - y.drep_no_confidence_vote_power) * 100 / y.drep_non_abstain_total, 2)
        ELSE ROUND((y.drep_non_abstain_total - y.drep_yes_vote_power) * 100 / y.drep_non_abstain_total, 2)
       END) AS drep_no_pct,
      (SELECT COALESCE(SUM(active_drep_votes_cast), 0)::integer 
      FROM active_prop_drep_votes WHERE vote = 'Abstain') 
      AS drep_abstain_votes_cast,
      y.drep_abstain_vote_power::text AS drep_active_abstain_vote_power,
      y.drep_no_confidence_vote_power::text AS drep_always_no_confidence_vote_power,
      y.drep_always_abstain_vote_power::text AS drep_always_abstain_vote_power,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        ELSE y.pool_yes_votes_cast
      END)::integer AS pool_yes_votes_cast,
      y.pool_yes_vote_power::text AS pool_active_yes_vote_power,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        WHEN y.proposal_type IN ('NoConfidence') THEN y.pool_yes_vote_power + y.pool_passive_always_no_confidence_vote_power
        ELSE y.pool_yes_vote_power
      END)::text AS pool_yes_vote_power,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        WHEN y.proposal_type IN ('NoConfidence') THEN ROUND((y.pool_yes_vote_power + y.pool_passive_always_no_confidence_vote_power) * 100 / y.pool_non_abstain_total, 2)
        ELSE ROUND(y.pool_yes_vote_power * 100 / y.pool_non_abstain_total, 2)
      END) AS pool_yes_pct,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        ELSE y.pool_no_votes_cast
      END)::integer AS pool_no_votes_cast,
      y.pool_no_vote_power::text AS pool_active_no_vote_power,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        WHEN y.proposal_type IN ('NoConfidence') THEN (y.pool_non_abstain_total - y.pool_yes_vote_power - y.pool_passive_always_no_confidence_vote_power)
        ELSE (y.pool_non_abstain_total - y.pool_yes_vote_power)
      END)::text AS pool_no_vote_power,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        WHEN y.proposal_type IN ('NoConfidence') THEN  ROUND((y.pool_non_abstain_total - y.pool_yes_vote_power - y.pool_passive_always_no_confidence_vote_power) * 100 / y.pool_non_abstain_total, 2)
        ELSE ROUND((y.pool_non_abstain_total - y.pool_yes_vote_power) * 100 / y.pool_non_abstain_total, 2)
      END) AS pool_no_pct,
      (SELECT COALESCE(SUM(pool_votes_cast), 0)::integer FROM active_prop_pool_votes WHERE vote = 'Abstain') AS pool_abstain_votes_cast,
      y.pool_abstain_vote_power::text AS pool_active_abstain_vote_power,
      y.pool_passive_always_abstain_votes_assigned::integer,
      y.pool_passive_always_abstain_vote_power::text,
      y.pool_passive_always_no_confidence_votes_assigned::integer,
      y.pool_passive_always_no_confidence_vote_power::text,
      y.committee_yes_votes_cast::integer,
      (CASE
        WHEN y.proposal_type IN ('NoConfidence', 'NewCommittee') THEN 0
        ELSE ROUND((y.committee_yes_votes_cast * 100 / y.committee_non_abstain_total), 2)
      END) AS committee_yes_pct,
      (CASE
        WHEN y.proposal_type IN ('NoConfidence', 'NewCommittee') THEN 0
        ELSE y.committee_no_votes_cast
      END)::integer AS committee_no_votes_cast,
      (CASE
        WHEN y.proposal_type IN ('NoConfidence', 'NewCommittee') THEN 0
        ELSE ROUND((committee_non_abstain_total - y.committee_yes_votes_cast) * 100 / y.committee_non_abstain_total, 2)
      END) AS committee_no_pct,
      (SELECT COALESCE(SUM(committee_votes_cast), 0)::integer FROM committee_votes WHERE vote = 'Abstain') AS committee_abstain_votes_cast
    FROM 
      (
        SELECT  
          c1.*,
          ( SELECT coalesce(SUM(active_drep_votes_cast), 0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS drep_yes_votes_cast,
          ( SELECT coalesce(SUM(active_drep_vote_total), 0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS drep_yes_vote_power,
          ( SELECT coalesce(SUM(active_drep_votes_cast), 0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
          ) AS drep_no_votes_cast,
          ( SELECT coalesce(SUM(active_drep_vote_total), 0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
          ) AS drep_no_vote_power,
          always_no_conf AS drep_no_confidence_vote_power,
          always_abstain AS drep_always_abstain_vote_power,
          ( SELECT coalesce(SUM(active_drep_vote_total),0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Abstain'
          ) AS drep_abstain_vote_power,
          tot_drep_power - inactive_drep_power - always_abstain - (
            SELECT coalesce(SUM(active_drep_vote_total), 0)
            FROM active_prop_drep_votes AS c3
            WHERE c3.gov_action_proposal_id = c1.gov_action_proposal_id AND c3.vote = 'Abstain'
          ) AS drep_non_abstain_total,
          ( SELECT coalesce(SUM(active_pool_vote_total), 0)
            FROM active_prop_pool_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS pool_yes_vote_power,
          ( SELECT coalesce(SUM(active_pool_vote_total), 0)
            FROM active_prop_pool_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
          ) AS pool_no_vote_power,
          ( SELECT coalesce(SUM(active_pool_vote_total), 0)
            FROM active_prop_pool_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Abstain'
          ) AS pool_abstain_vote_power,
          tot_pool_power - ( SELECT coalesce(SUM(active_pool_vote_total), 0)
            FROM active_prop_pool_votes c3
            WHERE c3.gov_action_proposal_id = c1.gov_action_proposal_id AND c3.vote = 'Abstain'
          ) - ( SELECT COALESCE(SUM(passive_pool_vote_total), 0)
          	FROM passive_prop_pool_votes p3 
          	WHERE p3.gov_action_proposal_id = c1.gov_action_proposal_id AND p3.vote = 'Abstain'
          ) AS pool_non_abstain_total,
          ( SELECT COALESCE(SUM(passive_pool_vote_total), 0)
          		FROM passive_prop_pool_votes p3 
          		WHERE p3.gov_action_proposal_id = c1.gov_action_proposal_id AND p3.vote = 'Abstain'
          ) AS pool_passive_always_abstain_vote_power,
          ( SELECT COALESCE(SUM(pool_votes_cast), 0)
            FROM passive_prop_pool_votes p3 
          	WHERE p3.gov_action_proposal_id = c1.gov_action_proposal_id AND p3.vote = 'Abstain'
          ) AS pool_passive_always_abstain_votes_assigned,
          ( SELECT COALESCE(SUM(passive_pool_vote_total), 0)
          	FROM passive_prop_pool_votes p3 
          	WHERE p3.gov_action_proposal_id = c1.gov_action_proposal_id AND p3.vote = 'No'
          ) AS pool_passive_always_no_confidence_vote_power,
          ( SELECT COALESCE(SUM(pool_votes_cast), 0)
          	FROM passive_prop_pool_votes p3 
          	WHERE p3.gov_action_proposal_id = c1.gov_action_proposal_id AND p3.vote = 'No'
          ) AS pool_passive_always_no_confidence_votes_assigned,
          ( SELECT coalesce(SUM(pool_votes_cast), 0)
            FROM active_prop_pool_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS pool_yes_votes_cast,
          ( SELECT coalesce(SUM(pool_votes_cast), 0)
            FROM active_prop_pool_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
          ) AS pool_no_votes_cast,
          ( SELECT coalesce(SUM(committee_votes_cast), 0)
            FROM committee_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS committee_yes_votes_cast,
          ( SELECT coalesce(SUM(committee_votes_cast), 0)
            FROM committee_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
          ) AS committee_no_votes_cast,
          committee_size - (
            SELECT coalesce(SUM(committee_votes_cast), 0)
            FROM committee_votes AS c3
            WHERE c3.gov_action_proposal_id = c1.gov_action_proposal_id AND c3.vote = 'Abstain'
          ) AS committee_non_abstain_total
        FROM combined_data AS c1
      ) AS y
    ORDER BY 1 DESC
  );
END;
$$;

COMMENT ON FUNCTION grest.proposal_voting_summary IS 'Get a summary of votes cast on specified governance action'; -- noqa: LT01
