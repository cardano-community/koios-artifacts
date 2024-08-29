CREATE OR REPLACE FUNCTION grest.proposal_voting_summary(_proposal_id text)
RETURNS TABLE (
  epoch_no integer,
  drep_yes_votes_cast integer,
  drep_yes_vote_power lovelace,
  drep_yes_pct numeric,
  drep_no_votes_cast integer,
  drep_no_vote_power lovelace,
  drep_no_pct numeric,
  pool_yes_votes_cast integer,
  pool_yes_vote_power lovelace,
  pool_yes_pct numeric,
  pool_no_votes_cast integer,
  pool_no_vote_power lovelace,
  pool_no_pct numeric
)
LANGUAGE plpgsql
AS $$
DECLARE
  proposal text[];
BEGIN

  SELECT INTO proposal grest.cip129_from_gov_action_id(_proposal_id);

RETURN QUERY (
  WITH
    proposal_epoch_data AS (
      SELECT
        gap.id as gov_action_proposal_id,
        expired_epoch,
        ratified_epoch,
        (coalesce(expired_epoch, ratified_epoch, ( SELECT MAX(no) FROM epoch))) AS epoch_of_interest
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
        SUM(amount) AS active_drep_vote_total,
        vote,
        COUNT(*) AS active_drep_votes_cast
      FROM proposal_epoch_data AS ped 
        INNER JOIN voting_procedure AS vp ON vp.voter_role = 'DRep' AND vp.gov_action_proposal_id = ped.gov_action_proposal_id
        INNER JOIN drep_distr AS dd ON vp.drep_voter = dd.hash_id AND dd.epoch_no = ped.epoch_of_interest
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
        INNER JOIN voting_procedure AS vp ON vp.voter_role = 'SPO' AND vp.gov_action_proposal_id = ped.gov_action_proposal_id
        INNER JOIN pool_stat ON vp.pool_voter = pool_stat.pool_hash_id AND pool_stat.epoch_no = ped.epoch_of_interest
      GROUP BY ped.gov_action_proposal_id, vote
    ),
    combined_data AS (
      SELECT 
        activedrep.gov_action_proposal_id,
        ped.epoch_of_interest,
        active_drep_vote_total,
        active_drep_votes_cast,
        pool_votes_cast,
        active_pool_vote_total,
        activedrep.vote,
        tot_drep_power,
        anc.always_no_conf,
        aab.always_abstain,
        tot_pool_power
      FROM proposal_epoch_data AS ped
        INNER JOIN active_prop_drep_votes AS activedrep ON activedrep.gov_action_proposal_id = ped.gov_action_proposal_id
        INNER JOIN tot_drep_power AS tdp ON tdp.gov_action_proposal_id = ped.gov_action_proposal_id
        INNER JOIN always_no_conf_data AS anc ON anc.gov_action_proposal_id = ped.gov_action_proposal_id
        INNER JOIN always_abstain_data AS aab ON aab.gov_action_proposal_id = ped.gov_action_proposal_id
        INNER JOIN tot_pool_power AS tpp ON tpp.gov_action_proposal_id = ped.gov_action_proposal_id
        LEFT OUTER JOIN active_prop_pool_votes AS activepool ON activedrep.gov_action_proposal_id = activepool.gov_action_proposal_id
    ) 
  SELECT
    y.epoch_of_interest AS epoch_no,
    y.drep_yes_votes_cast::integer,
    y.drep_yes_vote_power::lovelace,
    ROUND(y.drep_yes_vote_power * 100 / y.drep_non_abstain_total, 2) AS drep_yes_pct,
    y.drep_no_votes_cast::integer,
    (y.drep_non_abstain_total - y.drep_yes_vote_power)::lovelace AS drep_no_vote_power,
    ROUND((y.drep_non_abstain_total - y.drep_yes_vote_power) * 100 / y.drep_non_abstain_total, 2) AS drep_no_pct,
    y.pool_yes_votes_cast::integer,
    y.pool_yes_vote_power::lovelace,
    ROUND(y.pool_yes_vote_power * 100 / y.pool_non_abstain_total, 2) AS pool_yes_pct,
    y.pool_no_votes_cast::integer,
    (y.pool_non_abstain_total - y.pool_yes_vote_power)::lovelace AS pool_no_vote_power,
    ROUND((y.pool_non_abstain_total - y.pool_yes_vote_power) * 100 / y.pool_non_abstain_total, 2) AS pool_no_pct
  FROM 
    (
      SELECT distinct 
        gov_action_proposal_id, 
        epoch_of_interest,
        always_abstain, 
        always_no_conf,
        (
          SELECT coalesce(SUM(active_drep_votes_cast), 0)
          FROM combined_data AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
        ) AS drep_yes_votes_cast,
        (
          SELECT coalesce(SUM(active_drep_vote_total), 0)
          FROM combined_data AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
        ) AS drep_yes_vote_power,
        (
          SELECT coalesce(SUM(active_drep_votes_cast), 0)
          FROM combined_data AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
        ) AS drep_no_votes_cast,
        (
          SELECT coalesce(SUM(active_drep_vote_total), 0)
          FROM combined_data AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
        ) + always_no_conf AS drep_no_vote_power,
        (
          SELECT coalesce(SUM(active_drep_vote_total),0)
          FROM combined_data AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Abstain'
        ) + always_abstain AS drep_abstain_vote_power,
        tot_drep_power - always_abstain - 
        (
          SELECT coalesce(SUM(active_drep_vote_total), 0)
          FROM combined_data AS c3
          WHERE c3.gov_action_proposal_id = c1.gov_action_proposal_id AND c3.vote = 'Abstain'
        ) AS drep_non_abstain_total,
        (
          SELECT coalesce(SUM(active_pool_vote_total), 0)
          FROM combined_data AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
        ) AS pool_yes_vote_power,
        tot_pool_power - (
          SELECT coalesce(SUM(active_pool_vote_total), 0)
          FROM combined_data c3
          WHERE c3.gov_action_proposal_id = c1.gov_action_proposal_id AND c3.vote = 'Abstain'
        ) AS pool_non_abstain_total,
        (
          SELECT coalesce(SUM(pool_votes_cast), 0)
          FROM combined_data AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
        ) as pool_yes_votes_cast,
        (
          SELECT coalesce(SUM(pool_votes_cast), 0)
          FROM combined_data AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
        ) as pool_no_votes_cast,
        tot_drep_power,
        tot_pool_power
      FROM combined_data AS c1
    ) AS y
  ORDER BY 1 DESC
);
END;
$$;
