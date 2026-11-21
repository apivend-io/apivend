# Client compatibility

Which x402 clients have been run against this gateway, and what happened.
A row is added only after the client actually paid a route; "not tested" means
exactly that.

How these were run: `make e2e` starts the gateway behind a real Envoy with a
fake chain node and a mock facilitator, then pays with each client (CI runs
it on every pull request). That proves the **protocol exchange** (challenge,
payment header, verification, response header). Rows marked "live" were also
paid on the dev cluster (k3s, Envoy Gateway) with real test USDC on Base
Sepolia and settled on chain.

| Client | Version | Language | Async route | Sync route | Tested | Notes |
|---|---|---|---|---|---|---|
| [`@x402/fetch`](https://www.npmjs.com/package/@x402/fetch) + `@x402/evm` | 2.31.0 | TypeScript | Pass | Pass | 2026-11-17 | Reference client. Signs with viem 2.58.0. Decodes the async `PAYMENT-RESPONSE` (`success: true`, empty `transaction`) without error. |
| [`github.com/x402-foundation/x402/go/v2`](https://github.com/x402-foundation/x402/tree/main/go) (exact EVM client) | v2.31.0 | Go | Pass | Pass | 2026-11-20 | x402 Foundation's Go SDK, `hack/compat/go` (own module). In `make e2e`; **live** on the dev cluster (async served, empty transaction decoded; sync served with the transaction hash). |
| `github.com/x402-foundation/x402/go/v2` batch-settlement client | v2.31.0 | Go | Pass (deposit, then vouchers) | - | 2026-11-20 | `hack/compat/go -batch`. **Live** on the dev cluster: deposit settled, two voucher-only requests, claimed and paid out (see BATCH-SETTLEMENT.md). |
| [`x402`](https://pypi.org/project/x402/) (`requests` client, `ExactEvmScheme`) | 2.28.0 | Python | Pass | Pass | 2026-11-20 | x402 Foundation's Python SDK, `hack/compat/python`. In `make e2e` (needs `python3 -m venv`); **live** on the dev cluster, same results as Go. |
| `x402-pay` (this repository) | - | Go | Pass | Pass | 2026-11-17 | Demo client; shares no verification code path with the filter beyond the hashing primitives. `-as` pays as a contract wallet. |
| Coinbase Smart Wallet v1.1 (viem `toCoinbaseSmartAccount`) | viem 2.58.2 | TypeScript | Pass | - | 2026-11-19 | **Live**: deployed and undeployed (ERC-6492) wallets, key and passkey owners; settled by x402.org, PayAI and `direct`. |
| EIP-1271 contract wallet (minimal, owner key) | - | - | Pass | - | 2026-11-19 | **Live** with `x402-pay -as`. |
| `x402-fetch` (v1 protocol) | 1.2.0 | TypeScript | Not tested | Not tested | - | Speaks protocol v1 (`X-PAYMENT`). The gateway answers v2 only. |
| x402 Java SDK (`org.x402:x402`) | 1.0.0-SNAPSHOT | Java | Not tested | Not tested | - | Unpublished, and protocol v1 only (`X-PAYMENT`, `X-PAYMENT-RESPONSE`) at x402 commit `971e64f`. |
| Rust clients | - | Rust | Not tested | Not tested | - | |

What the `@x402/fetch` run establishes:

- The 402 challenge is parseable by the reference client, including
  `extra.name` / `extra.version` for the EIP-712 domain.
- A signature produced by an independent implementation (viem) passes the
  filter's verification, including its low-s and `v` in {27, 28} rules.
- The client accepts both forms of `PAYMENT-RESPONSE`.

What it does not establish: behaviour in a browser (CORS), other networks than
Base Sepolia's parameters, and anything about settlement.

To add a client: put a script in `hack/compat/` that pays the URLs given on
its command line and exits non-zero on failure, call it from `hack/e2e.sh`
next to the `@x402/fetch` step, and add the row with the version you ran.

Clients checked against the x402 Foundation repository (commit `971e64f`,
2026-11-19): it ships v2 clients in TypeScript, Go and Python (all three in
`make e2e` now) and a Java SDK that is still protocol v1.

## Test vectors and error codes

- The x402 repository has **no test-vector set for EVM `exact`** (the only
  vector sets in its specs are for Cardano). The specification's example
  signature is in our tests (`evm.TestSpecExampleVector`). The SDKs' EVM
  tests are facilitator internals around mocks, not portable payload
  vectors, so the reference clients themselves, in CI and live, are the
  compatibility evidence.
- Refusal reasons follow the specification's section 9 list exactly
  (`insufficient_funds`, `invalid_exact_evm_payload_authorization_valid_after`,
  `..._valid_before`, `..._value_mismatch`, `invalid_exact_evm_payload_signature`,
  `invalid_exact_evm_payload_recipient_mismatch`). The reference SDKs use
  further codes of their own (for example `invalid_exact_evm_signature`,
  `invalid_exact_evm_nonce_already_used`) that the specification does not
  define; the gateway keeps the specification's words, and adds its own only
  where the specification has none (`payment_already_used`, `exposure_limit`).
