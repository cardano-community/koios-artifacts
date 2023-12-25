CREATE TABLE IF NOT EXISTS grest.asset_cache_control AS (
  SELECT policy FROM multi_asset LIMIT 0
);

CREATE TABLE IF NOT EXISTS grest.asset_tx_out_cache AS (
  SELECT
    id AS ma_id,
    id AS txo_id,
    quantity
  FROM ma_tx_out
  LIMIT 0
);

CREATE INDEX IF NOT EXISTS idx_atoc_txoid ON grest.asset_tx_out_cache USING btree (txo_id DESC);

CREATE INDEX IF NOT EXISTS idx_atoc_maid ON grest.asset_tx_out_cache USING btree (ma_id) INCLUDE (txo_id, quantity);

DELETE FROM grest.asset_cache_control;
INSERT INTO grest.asset_cache_control VALUES (DECODE('a0028f350aaabe0545fdcb56b039bfb08e4bb4d8c4d7c3c7d481c235', 'hex')); -- HOSKY
INSERT INTO grest.asset_cache_control VALUES (DECODE('af2e27f580f7f08e93190a81f72462f153026d06450924726645891b', 'hex')); -- DRIP
INSERT INTO grest.asset_cache_control VALUES (DECODE('29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6', 'hex')); -- MIN
INSERT INTO grest.asset_cache_control VALUES (DECODE('9a9693a9a37912a5097918f97918d15240c92ab729a0b7c4aa144d77', 'hex')); -- SUNDAE
INSERT INTO grest.asset_cache_control VALUES (DECODE('13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f', 'hex')); -- MINSWAP
INSERT INTO grest.asset_cache_control VALUES (DECODE('c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073', 'hex')); -- WINGRIDERS
INSERT INTO grest.asset_cache_control VALUES (DECODE('1d7f33bd23d85e1a25d87d86fac4f199c3197a2f7afeb662a0f34e1e', 'hex')); -- worldmobiletoken
INSERT INTO grest.asset_cache_control VALUES (DECODE('682fe60c9918842b3323c43b5144bc3d52a23bd2fb81345560d73f63', 'hex')); -- NEWM
INSERT INTO grest.asset_cache_control VALUES (DECODE('6ac8ef33b510ec004fe11585f7c5a9f0c07f0c23428ab4f29c1d7d10', 'hex')); -- MELD
INSERT INTO grest.asset_cache_control VALUES (DECODE('884892bcdc360bcef87d6b3f806e7f9cd5ac30d999d49970e7a903ae', 'hex')); -- PAVIA
INSERT INTO grest.asset_cache_control VALUES (DECODE('279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f', 'hex')); -- SNEK
INSERT INTO grest.asset_cache_control VALUES (DECODE('f43a62fdc3965df486de8a0d32fe800963589c41b38946602a0dc535', 'hex')); -- AGIX
INSERT INTO grest.asset_cache_control VALUES (DECODE('92292852e3820cfbe99874b284fdf2befbddb38e070cf3512009a60a', 'hex')); -- *Pearl
INSERT INTO grest.asset_cache_control VALUES (DECODE('de9b756719341e79785aa13c164e7fe68c189ed04d61c9876b2fe53f', 'hex')); -- MuesliSwap_AMM
INSERT INTO grest.asset_cache_control VALUES (DECODE('10a49b996e2402269af553a8a96fb8eb90d79e9eca79e2b4223057b6', 'hex')); -- GERO
INSERT INTO grest.asset_cache_control VALUES (DECODE('750900e4999ebe0d58f19b634768ba25e525aaf12403bfe8fe130501', 'hex')); -- BOOK
INSERT INTO grest.asset_cache_control VALUES (DECODE('e38748c08c510a4a5d712922a0f91269b8446ac565068f653c517475', 'hex')); -- preprod KUt1
INSERT INTO grest.asset_cache_control VALUES (DECODE('602866d30452bf3ea0af2d6b4007389eed5542d2572808cba3eb991f', 'hex')); -- preprod tokenA
INSERT INTO grest.asset_cache_control VALUES (DECODE('af6c50cb85c8df17f539437c01b405ab9b62b03140d872e787d7a279', 'hex')); -- preprod tokenB
INSERT INTO grest.asset_cache_control VALUES (DECODE('c462512684cf5a5ee0b176326c724d5879a37a4977d3bf1e4edc39f6', 'hex')); -- preview mTOSI BLUE/GREEN/PURPLE/RAINBOW/RED/YELLOW
-- INSERT INTO grest.asset_cache_control VALUES (DECODE('', 'hex')); -- 
