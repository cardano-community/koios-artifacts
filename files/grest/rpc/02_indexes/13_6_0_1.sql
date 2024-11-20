CREATE INDEX IF NOT EXISTS idx_address_address ON address USING hash (address);
CREATE INDEX IF NOT EXISTS idx_tx_out_stake_address_id ON tx_out(stake_address_id) ;
CREATE INDEX IF NOT EXISTS idx_tx_out_address_id ON tx_out(address_id) ;
CREATE INDEX IF NOT EXISTS idx_address_stake_address_id ON address(stake_address_id) ;
