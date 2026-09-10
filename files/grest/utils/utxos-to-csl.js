#!/usr/bin/env node
// utxos-to-csl.mjs — convert cardano-cli query-utxo JSON to CSL hex for /auth/register
//
// Usage:
//   cardano-cli conway query utxo --address $(cat payment.addr) --mainnet --output-json \
//     | node utxos-to-csl.mjs > /tmp/utxos.json
//
// Outputs { utxos: ["<csl-hex>", ...], changeAddr: "<csl-hex>" } as JSON.
// Pipe straight into jq for /auth/register:
//   jq -n --argjson u "$(... | node utxos-to-csl.mjs)" '{ tx: $u }'
//
// Reads CSL from <repo>/auth-worker/node_modules (this repo's layout) or
// from a standard package-name resolution if installed elsewhere.
//
// Pure-ADA UTXOs only — UTXOs with native tokens or reference scripts are skipped.

import fs from 'node:fs'

const loadCSL = async () => {
  try {
    const localPath = new URL('../auth-worker/node_modules/@emurgo/cardano-serialization-lib-nodejs/cardano_serialization_lib.js', import.meta.url)
    return await import(localPath.href)
  } catch { /* fall through */ }
  return await import('@emurgo/cardano-serialization-lib-nodejs')
}

const CSL = await loadCSL()

// Read JSON from stdin.
const chunks = []
for await (const chunk of process.stdin) { chunks.push(chunk) }
const raw = JSON.parse(Buffer.concat(chunks).toString('utf8'))
if (!Array.isArray(raw)) {
  process.stderr.write('expected a JSON array of UTXOs from cardano-cli --output-json\n')
  process.exit(1)
}

const utxos = []
let changeAddr = ''
for (const u of raw) {
  // Pure-ADA only: skip anything with multi-asset, datum, or reference script.
  const v = u.value ?? {}
  const hasAssets = Object.keys(v).some(k => k !== 'lovelace')
  if (hasAssets || u.inlineDatum || u.referenceScript) continue

  const input  = CSL.TransactionInput.new(
    CSL.TransactionHash.from_hex(u.txHash),
    u.outputIndex,
  )
  const output = CSL.TransactionOutput.new(
    CSL.Address.from_bech32(u.address),
    CSL.Value.from_json(JSON.stringify({ coin: String(v.lovelace) })),
  )
  const ref = CSL.TransactionUnspentOutput.new(input, output)

  utxos.push(ref.to_hex())
  if (!changeAddr) changeAddr = CSL.Address.from_bech32(u.address).to_hex()
}

process.stdout.write(JSON.stringify({ utxos, changeAddr }, null, 2) + '\n')

if (utxos.length === 0) {
  process.stderr.write('warning: no pure-ADA UTXOs found in input\n')
}
