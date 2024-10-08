
CREATE OR REPLACE FUNCTION grest.proposal_voting_summary(_proposal_id text)
RETURNS TABLE (
  proposal_type text,
  epoch_no integer,
  drep_yes_votes_cast integer,
  drep_yes_vote_power lovelace,
  drep_yes_pct numeric,
  drep_no_votes_cast integer,
  drep_no_vote_power lovelace,
  drep_no_pct numeric,
  drep_abstain_votes_cast integer,
  pool_yes_votes_cast integer,
  pool_yes_vote_power lovelace,
  pool_yes_pct numeric,
  pool_no_votes_cast integer,
  pool_no_vote_power lovelace,
  pool_no_pct numeric,
  pool_abstain_votes_cast integer,
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
BEGIN

  SELECT INTO proposal grest.cip129_from_gov_action_id(_proposal_id);

  RETURN QUERY (
    WITH
      latest_votes AS (
        SELECT * FROM voting_procedure vp
        WHERE NOT EXISTS (SELECT 1 FROM voting_procedure vp2
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
        FROM gov_action_proposal gap 
        INNER JOIN tx t ON gap.tx_id = t.id AND t.hash = DECODE(proposal[1], 'hex') AND gap.index = proposal[2]::smallint
      ),
      tot_drep_power AS (
        SELECT ped.gov_action_proposal_id, SUM(amount) AS tot_drep_power 
        FROM drep_distr AS dd 
          INNER JOIN proposal_epoch_data AS ped ON dd.epoch_no = epoch_of_interest
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
            FROM drep_registration as dr 
            WHERE dr.drep_hash_id = vp.drep_voter
            -- deregistration tx
            AND deposit < 0 
            -- submitted after vote tx
            AND dr.tx_id > vp.tx_id 
            -- but before last tx of epoch preceding ratification/expiry/drop epoch or last tx of current epoch
            AND dr.tx_id < 
              ( 
                SELECT i_last_tx_id 
                FROM grest.epoch_info_cache as eic
                WHERE eic.epoch_no = 
                  (coalesce(ped.ratified_epoch, ped.expired_epoch, ped.dropped_epoch, (SELECT max(no) + 1 FROM epoch)) - 1)
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
          -- if hard fork initiation, then need to use full SPO voting power otherwise just voted SPO power
          WHERE ((ped.proposal_type = 'HardForkInitiation') or EXISTS (SELECT 1 FROM latest_votes vp where vp.voter_role = 'SPO' 
          AND vp.gov_action_proposal_id = ped.gov_action_proposal_id AND vp.pool_voter = pool_stat.pool_hash_id)) 
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
      committee_votes AS (
        SELECT
          ped.gov_action_proposal_id,
          vote,
          COUNT(*) AS committee_votes_cast
        FROM proposal_epoch_data AS ped 
          INNER JOIN latest_votes AS vp ON vp.voter_role = 'ConstitutionalCommittee'
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
          always_no_conf,
          always_abstain,
          committee_size,
          tot_pool_power
        FROM proposal_epoch_data AS ped
          INNER JOIN tot_drep_power ON tot_drep_power.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN always_no_conf_data ON always_no_conf_data.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN always_abstain_data ON always_abstain_data.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN tot_pool_power ON tot_pool_power.gov_action_proposal_id = ped.gov_action_proposal_id
          INNER JOIN tot_committee_size on tot_committee_size.gov_action_proposal_id = ped.gov_action_proposal_id
      )

    SELECT
      y.proposal_type::text AS proposal_type,
      y.epoch_of_interest AS epoch_no,
      y.drep_yes_votes_cast::integer,
      y.drep_yes_vote_power::lovelace,
      ROUND(y.drep_yes_vote_power * 100 / y.drep_non_abstain_total, 2) AS drep_yes_pct,
      y.drep_no_votes_cast::integer,
      (y.drep_non_abstain_total - y.drep_yes_vote_power)::lovelace AS drep_no_vote_power,
      ROUND((y.drep_non_abstain_total - y.drep_yes_vote_power) * 100 / y.drep_non_abstain_total, 2) AS drep_no_pct,
      (SELECT COALESCE(SUM(active_drep_votes_cast), 0)::integer FROM active_prop_drep_votes WHERE vote = 'Abstain') AS drep_abstain_votes_cast,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        ELSE y.pool_yes_votes_cast
      END)::integer AS pool_yes_votes_cast,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        ELSE y.pool_yes_vote_power
      END)::lovelace AS pool_yes_vote_power,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        ELSE ROUND(y.pool_yes_vote_power * 100 / y.pool_non_abstain_total, 2)
      END) AS pool_yes_pct,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        ELSE y.pool_no_votes_cast
      END)::integer AS pool_no_votes_cast,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        ELSE (y.pool_non_abstain_total - y.pool_yes_vote_power)
      END)::lovelace AS pool_no_vote_power,
      (CASE
        WHEN y.proposal_type IN ('ParameterChange', 'TreasuryWithdrawals', 'NewConstitution') THEN 0
        ELSE ROUND((y.pool_non_abstain_total - y.pool_yes_vote_power) * 100 / y.pool_non_abstain_total, 2)
      END) AS pool_no_pct,
      (SELECT COALESCE(SUM(pool_votes_cast), 0)::integer FROM active_prop_pool_votes WHERE vote = 'Abstain') AS pool_abstain_votes_cast,
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
          (
            SELECT coalesce(SUM(active_drep_votes_cast), 0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS drep_yes_votes_cast,
          (
            SELECT coalesce(SUM(active_drep_vote_total), 0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS drep_yes_vote_power,
          (
            SELECT coalesce(SUM(active_drep_votes_cast), 0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
          ) AS drep_no_votes_cast,
          (
            SELECT coalesce(SUM(active_drep_vote_total), 0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
          ) + always_no_conf AS drep_no_vote_power,
          (
            SELECT coalesce(SUM(active_drep_vote_total),0)
            FROM active_prop_drep_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Abstain'
          ) + always_abstain AS drep_abstain_vote_power,
          tot_drep_power - always_abstain - (
            SELECT coalesce(SUM(active_drep_vote_total), 0)
            FROM active_prop_drep_votes AS c3
            WHERE c3.gov_action_proposal_id = c1.gov_action_proposal_id AND c3.vote = 'Abstain'
          ) AS drep_non_abstain_total,
          (
            SELECT coalesce(SUM(active_pool_vote_total), 0)
            FROM active_prop_pool_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS pool_yes_vote_power,
          tot_pool_power - (
            SELECT coalesce(SUM(active_pool_vote_total), 0)
            FROM active_prop_pool_votes c3
            WHERE c3.gov_action_proposal_id = c1.gov_action_proposal_id AND c3.vote = 'Abstain'
          ) AS pool_non_abstain_total,
          (
            SELECT coalesce(SUM(pool_votes_cast), 0)
            FROM active_prop_pool_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS pool_yes_votes_cast,
          (
            SELECT coalesce(SUM(pool_votes_cast), 0)
            FROM active_prop_pool_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
          ) AS pool_no_votes_cast,
          (
            SELECT coalesce(SUM(committee_votes_cast), 0)
            FROM committee_votes AS c2
            WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
          ) AS committee_yes_votes_cast,
          (
            SELECT coalesce(SUM(committee_votes_cast), 0)
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

COMMENT ON FUNCTION grest.proposal_votes IS 'Get a summary of votes cast on specified governance action'; -- noqa: LT01
