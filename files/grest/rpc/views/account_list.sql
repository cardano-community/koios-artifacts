DROP VIEW IF EXISTS grest.account_list;

CREATE VIEW grest.account_list AS
SELECT stake_address.view AS id
FROM stake_address;

COMMENT ON VIEW grest.account_list IS 'Get a list of all accounts';
