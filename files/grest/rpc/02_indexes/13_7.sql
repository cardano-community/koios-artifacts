CREATE INDEX IF NOT EXISTS idx_voting_procedure_drep_voter ON voting_procedure (drep_voter) WHERE drep_voter IS NOT NULL;

CREATE INDEX IF NOT EXISTS delegation_vote_addr_id_idx ON public.delegation_vote (addr_id, tx_id);

CREATE INDEX IF NOT EXISTS idx_redeemer_script_hash ON public.redeemer USING btree (script_hash);

CREATE INDEX IF NOT EXISTS idx_drep_registration_drep_hash_id ON public.drep_registration USING btree (drep_hash_id, tx_id DESC);

CREATE INDEX IF NOT EXISTS idx_drep_distr_epoch_no ON public.drep_distr USING btree (epoch_no);

CREATE INDEX IF NOT EXISTS idx_committee_registration_cold_key_id ON public.committee_registration USING btree (cold_key_id, tx_id DESC);
CREATE INDEX IF NOT EXISTS idx_committee_registration_hot_key_id ON public.committee_registration USING btree (hot_key_id);
CREATE INDEX IF NOT EXISTS idx_committee_de_registration_cold_key_id ON public.committee_de_registration USING btree (cold_key_id, tx_id DESC);

CREATE INDEX IF NOT EXISTS idx_voting_procedure_pool_voter ON public.voting_procedure USING btree (pool_voter) WHERE pool_voter IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_voting_procedure_committee_voter ON public.voting_procedure USING btree (committee_voter) WHERE committee_voter IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_voting_procedure_gov_action_proposal_id ON public.voting_procedure USING btree (gov_action_proposal_id);

CREATE INDEX IF NOT EXISTS idx_delegation_vote_tx_id ON public.delegation_vote USING btree (tx_id);
