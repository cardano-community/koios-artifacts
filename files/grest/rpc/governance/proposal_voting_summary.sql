create or replace function grest.proposal_voting_summary(_proposal_id text)
returns table (
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
  pool_no_pct numeric,
  committee_yes_votes_cast integer,
  committee_yes_pct numeric,
  committee_no_votes_cast integer,
  committee_no_pct numeric
)
language plpgsql
as $$
DECLARE
  proposal text[];
BEGIN

  SELECT INTO proposal grest.cip129_from_gov_action_id(_proposal_id);

RETURN QUERY (
  WITH
    proposal_epoch_data AS (
      SELECT
        gap.id as gov_action_proposal_id,
        gap.type as proposal_type,
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
        coalesce(SUM(amount),0) AS active_drep_vote_total,
        vote,
        COUNT(*) AS active_drep_votes_cast
      FROM proposal_epoch_data AS ped 
        INNER JOIN voting_procedure AS vp ON vp.voter_role = 'DRep' AND vp.gov_action_proposal_id = ped.gov_action_proposal_id
        LEFT JOIN drep_distr AS dd ON vp.drep_voter = dd.hash_id AND dd.epoch_no = ped.epoch_of_interest
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
    committee_votes AS (
      SELECT
        ped.gov_action_proposal_id,
        vote,
        COUNT(*) AS committee_votes_cast
      FROM proposal_epoch_data AS ped 
        INNER JOIN voting_procedure AS vp ON vp.voter_role = 'ConstitutionalCommittee'
        AND vp.gov_action_proposal_id = ped.gov_action_proposal_id
        AND NOT EXISTS (select null from voting_procedure vp2 where vp2.gov_action_proposal_id = vp.gov_action_proposal_id
                        and vp2.committee_voter = vp.committee_voter and vp2.id > vp.id)
      GROUP BY ped.gov_action_proposal_id, vote
    ),
    tot_committee_size AS (
      SELECT 
        ped.gov_action_proposal_id,
        count(cm.id) as committee_size
        FROM epoch_state epstate
        INNER JOIN proposal_epoch_data ped on epstate.epoch_no = ped.epoch_of_interest
        INNER JOIN committee c on epstate.committee_id = c.id
        INNER JOIN committee_member cm on cm.committee_id = c.id
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
    y.epoch_of_interest AS epoch_no,
    y.drep_yes_votes_cast::integer,
    y.drep_yes_vote_power::lovelace,
    ROUND(y.drep_yes_vote_power * 100 / y.drep_non_abstain_total, 2) AS drep_yes_pct,
    y.drep_no_votes_cast::integer,
    (y.drep_non_abstain_total - y.drep_yes_vote_power)::lovelace AS drep_no_vote_power,
    ROUND((y.drep_non_abstain_total - y.drep_yes_vote_power) * 100 / y.drep_non_abstain_total, 2) AS drep_no_pct,
    (case when y.proposal_type = 'NewConstitution' then null else
    y.pool_yes_votes_cast end)::integer,
    (case when y.proposal_type = 'NewConstitution' then null else
    y.pool_yes_vote_power end)::lovelace,
    (case when y.proposal_type = 'NewConstitution' then null else
    ROUND(y.pool_yes_vote_power * 100 / y.pool_non_abstain_total, 2) end) AS pool_yes_pct,
    (case when y.proposal_type = 'NewConstitution' then null else
    y.pool_no_votes_cast end)::integer,
    (case when y.proposal_type = 'NewConstitution' then null else
    (y.pool_non_abstain_total - y.pool_yes_vote_power) end)::lovelace AS pool_no_vote_power,
    (case when y.proposal_type = 'NewConstitution' then null else
    ROUND((y.pool_non_abstain_total - y.pool_yes_vote_power) * 100 / y.pool_non_abstain_total, 2) end) AS pool_no_pct,
    y.committee_yes_votes_cast::integer,
    (case when y.proposal_type = 'NewCommittee' then null else
    ROUND((y.committee_yes_votes_cast * 100 / y.committee_non_abstain_total), 2) end) as committee_yes_pct,
    (case when y.proposal_type = 'NewCommittee' then null else
    y.committee_no_votes_cast end)::integer,
    (case when y.proposal_type = 'NewCommittee' then null else
    ROUND((committee_non_abstain_total - y.committee_yes_votes_cast) * 100 / y.committee_non_abstain_total, 2) end) as committee_no_pct 
  FROM 
    (
      SELECT distinct 
        gov_action_proposal_id,
        proposal_type,
        epoch_of_interest,
        always_abstain, 
        always_no_conf,
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
        tot_drep_power - always_abstain - 
        (
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
        ) as pool_yes_votes_cast,
        (
          SELECT coalesce(SUM(pool_votes_cast), 0)
          FROM active_prop_pool_votes AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
        ) as pool_no_votes_cast,
        (
          SELECT coalesce(SUM(committee_votes_cast), 0)
          FROM committee_votes AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'Yes'
        ) as committee_yes_votes_cast,
        (
          SELECT coalesce(SUM(committee_votes_cast), 0)
          FROM committee_votes AS c2
          WHERE c2.gov_action_proposal_id = c1.gov_action_proposal_id AND c2.vote = 'No'
        ) as committee_no_votes_cast,
        committee_size -  
        (
          SELECT coalesce(SUM(committee_votes_cast), 0)
          FROM committee_votes AS c3
          WHERE c3.gov_action_proposal_id = c1.gov_action_proposal_id AND c3.vote = 'Abstain'
        ) AS committee_non_abstain_total,
        tot_drep_power,
        tot_pool_power
      FROM combined_data AS c1
    ) AS y
  ORDER BY 1 DESC
);
END;
$$;
