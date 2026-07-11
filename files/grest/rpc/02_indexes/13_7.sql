CREATE INDEX IF NOT EXISTS idx_voting_procedure_drep_voter ON voting_procedure (drep_voter) WHERE drep_voter IS NOT NULL;
