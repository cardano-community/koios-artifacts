#!/usr/bin/env node
// sign-proof.mjs — produce the JSON body for POST /auth/proof
// Usage: node sign-proof.mjs <rewardAddress> <stake.skey>
// Pure Node 18+, no dependencies.

import fs from 'node:fs'
import crypto from 'node:crypto'

const [, , rewardAddress, skeyPath] = process.argv
if (!rewardAddress || !skeyPath) {
  process.stderr.write('usage: node sign-proof.mjs <rewardAddress> <stake.skey>\n')
  process.exit(2)
}

// Read 32-byte Ed25519 seed from cardano-cli's stake.skey (JSON + cborHex)
// or from a raw 32/64/96-byte hex file.
const raw = fs.readFileSync(skeyPath, 'utf8').trim()
const seed = raw.startsWith('{')
  ? Buffer.from(JSON.parse(raw).cborHex.slice(4), 'hex')           // strip "5820" CBOR tag
  : Buffer.from(raw, 'hex').subarray(0, 32)

// Wrap the raw seed in PKCS#8 DER so Node's crypto accepts it.
const PKCS8_ED25519_PREFIX = Buffer.from('302e020100300506032b657004220420', 'hex')
const priv = crypto.createPrivateKey({ key: Buffer.concat([PKCS8_ED25519_PREFIX, seed]), format: 'der', type: 'pkcs8' })
const pub  = crypto.createPublicKey(priv)

const now = Math.floor(Date.now() / 1000)
const message = `Koios Auth Proof\nAddress: ${rewardAddress}\nTimestamp: ${now}`
const sig = crypto.sign(null, Buffer.from(message, 'utf8'), priv)
const publicKey = Buffer.from(pub.export({ format: 'jwk' }).x, 'base64url').toString('hex')

process.stdout.write(JSON.stringify({ rewardAddress, publicKey, message, signature: sig.toString('hex') }, null, 2) + '\n')
