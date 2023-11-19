CREATE TABLE IF NOT EXISTS grest.asset_cache_control AS (
  SELECT policy, name FROM multi_asset LIMIT 0
);

CREATE TABLE IF NOT EXISTS grest.asset_tx_out_cache AS (
  SELECT id AS ma_id, id AS txo_id, quantity FROM ma_tx_out AS mto LIMIT 0
);
CREATE INDEX IF NOT EXISTS idx_atoc_txoid ON grest.asset_tx_out_cache USING btree (txo_id);

INSERT INTO grest.asset_cache_control VALUES (DECODE('13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f','hex'),DECODE('4d494e53574150','hex'));
INSERT INTO grest.asset_cache_control VALUES (DECODE('92292852e3820cfbe99874b284fdf2befbddb38e070cf3512009a60a','hex'),DECODE('436f6c6f72506561726c','hex'));
INSERT INTO grest.asset_cache_control VALUES (DECODE('29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6','hex'),DECODE('4d494e','hex'));
INSERT INTO grest.asset_cache_control VALUES (DECODE('de9b756719341e79785aa13c164e7fe68c189ed04d61c9876b2fe53f','hex'),DECODE('4d7565736c69537761705f414d4d','hex'));
INSERT INTO grest.asset_cache_control VALUES (DECODE('f43a62fdc3965df486de8a0d32fe800963589c41b38946602a0dc535','hex'),DECODE('41474958','hex'));
INSERT INTO grest.asset_cache_control VALUES (DECODE('a0028f350aaabe0545fdcb56b039bfb08e4bb4d8c4d7c3c7d481c235','hex'),DECODE('484f534b59','hex'));
INSERT INTO grest.asset_cache_control VALUES (DECODE('682fe60c9918842b3323c43b5144bc3d52a23bd2fb81345560d73f63','hex'),DECODE('4e45574d','hex'));
INSERT INTO grest.asset_cache_control VALUES (DECODE('af2e27f580f7f08e93190a81f72462f153026d06450924726645891b','hex'),DECODE('44524950','hex'));
INSERT INTO grest.asset_cache_control VALUES (DECODE('10a49b996e2402269af553a8a96fb8eb90d79e9eca79e2b4223057b6','hex'),DECODE('4745524f','hex'));
INSERT INTO grest.asset_cache_control VALUES (DECODE('1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e','hex'),DECODE('776f726c646d6f62696c65746f6b656e','hex'));