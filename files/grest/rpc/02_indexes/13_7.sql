CREATE INDEX IF NOT EXISTS idx_voting_procedure_drep_voter ON voting_procedure (drep_voter) WHERE drep_voter IS NOT NULL;

CREATE INDEX IF NOT EXISTS delegation_vote_addr_id_idx ON public.delegation_vote (addr_id, tx_id);
