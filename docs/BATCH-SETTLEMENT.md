# Batch settlement (design)

Status: works end to end on Base Sepolia (2026-11-20), deposits, vouchers,
claims, payouts and cooperative refunds, through CDP and PayAI, and in
`make e2e` (the Go client against the fake node's channel ledger and the
mock facilitator). Each request paid by voucher gets a `batch_charged`
receipt (the settler drains `x402:ch:charges`), which the demo counts as
answered and as revenue. Not built: claims through our own `direct`
backend.

## Refunds and closing a channel

A payer asks for its unused escrow back with a `refund` payload (a
zero-charge voucher: its ceiling is the charged total; `amount` for a
partial refund). The filter:

1. closes the channel to new vouchers at once (`closing` in Redis), and
   refuses a refund larger than balance minus charged;
2. queues a `batch-refund` job (queue-only: nothing held from the payer's
   wallet) and waits;
3. the settler completes it: the payer's refund voucher becomes the claim of
   everything charged, then our ClaimBatch and Refund signatures (refund
   nonce read from the chain); a facilitator submits claimWithSignature then
   refundWithSignature;
4. answers 200 directly, never calling the API, with the refund transaction
   and amount; a fully refunded channel is forgotten, a partial one reopens,
   a failed refund reopens it.

A payer's timed withdrawal (seen within a minute by the mirror, or at the
next claim round) also closes the channel: vouchers are refused from then
on, and the claim job claims what was charged before the delay ends.
Receipts: `refund_served` / `refund_settled` / `refund_unpaid`.

Live (dev cluster, CDP, 2026-11-20): partial refund 0.10
(`0xae949c67075b6b32f7d30f6e5d67adcb7aa3fd537722288231603887dd33ac99`), then
the remaining 0.88 (`0x2270c8ee8534f7c2d0566165c1ba36141753e621fd960f0ed11592ef6aef6bb6`);
the payer got back everything except the 0.02 it spent. CDP also ran a full
deposit, claim and payout (`0xd213ef23…`, `0xdd25338b…`, `0xf4bf1540…`).

## Live on Base Sepolia (dev cluster, 2026-11-20)

The x402 Foundation's Go client (v2.31.0, `hack/compat/go -batch`) paid the
dev route `prod/search` (0.01 USDC) three times:

| Step | Transaction | Backend |
|---|---|---|
| Deposit 1.00 USDC (first request served, 200) | `0x4f553b5ac34563a5fa73f9644489e2537020c2a9d5c8d2d0ec047c94793aa408` (block 61180619) | PayAI |
| Requests 2 and 3: vouchers only (cumulative 20000, 30000), no transaction | - | - |
| Claim 30000 (one transaction, our ClaimBatch signature) | `0x93dddb3a03b54fbbebf53385a322bcfafbc715ecfa003093f15dfccd29cfc2fe` | PayAI |
| Payout to the seller | `0x5c2c28d09bcb0b190fa8aa465186837b9016490b6448fc18f95be63ca6a4a9ba` | PayAI |

Payer 19.242 to 18.242 USDC (the deposit), seller 0.522 to 0.552 (exactly
the three charges), channel on chain: balance 1.00, claimed 0.03. Receipts:
`deposit_served`, `deposit_settled`, `batch_claimed` 30000. x402.org lists
batch-settlement on Base Sepolia but failed all three transactions
(`invalid_batch_settlement_evm_{deposit,claim,settle}_transaction_failed`);
the settler moved each to PayAI.

Operational: Helm does not upgrade CRDs (apply `deploy/crds` by hand), and
the controller needs a restart after a CRD gains a field.

## Why

An `exact` payment is one on-chain transfer per request: about 86,000 gas,
roughly $0.001 on Base. That is as much as a $0.001 call earns. With the x402
`batch-settlement` scheme (x402 `specs/schemes/batch-settlement/`, EVM
binding v1.1):

- the payer deposits into a channel of the `x402BatchSettlement` contract
  once (`0x4020074e9dF2ce1deE5A9C1b5c3f541D02a10003`, same address on every
  chain);
- for each request, the payer signs a cumulative voucher ("this channel may
  be claimed up to N");
- the receiver side claims many channels in one transaction and sweeps the
  funds to the seller with `settle(receiver, token)`.

What we checked on 2026-11-20:

- The contract is deployed on Base and Base Sepolia, and its views
  (`channels`, `pendingWithdrawals`, `refundNonce`) answer as expected.
- CDP advertises the scheme on Base and Base Sepolia. Its `extra` offers its
  own `receiverAuthorizer` and `delegatedRefund: true`, but no
  facilitator-managed vouchers.
- x402.org advertises it on Base Sepolia with no `extra`.
- The TypeScript, Go and Python client SDKs implement it.

## Decisions

1. **Self-managed vouchers.** The gateway keeps each channel's state:
   - the charged total;
   - the latest signed voucher;
   - the mirrored balance and claimed total.

   This state lives in its own Redis, in Lua scripts like every other
   payer-state transition, and the gateway checks vouchers locally (EIP-712,
   no RPC for an EOA authorizer). No facilitator is called on the request
   path for vouchers. Neither facilitator offers to manage vouchers anyway.
2. **Our own receiver authorizer key, not a facilitator's.** The channel
   config is immutable. With CDP's authorizer, every channel would be tied to
   CDP. With ours, any backend can relay our signed `claimWithSignature`
   (CDP, x402.org, PayAI, or `direct` with our gas key), so failover keeps
   working. The key can only claim vouchers to the seller's address or
   refund unclaimed escrow to the payer. It is not a payer key and cannot
   pay anyone. It lives with the settler, one key per environment.
3. **The seller is the receiver.** `ChannelConfig.receiver` is the route's
   `payTo`, and `settle` is permissionless and transfers straight to it, so
   the hosted gateway stays non-custodial.
4. **Deposits are settled before the first request is served.** A deposit is
   an on-chain transfer into escrow, made through a facilitator (`/settle`
   `type: deposit`), like a sync route. Requests after it are vouchers and
   answer at local speed.

## Phases

- **B1, done.** `engine/evm/batch.go` computes:
  - the channel id;
  - the voucher, refund and claim-batch digests;
  - the deposit nonce and the `receiveWithAuthorization` digest;
  - the encoding of the three views.

  All of it matches the x402 Foundation Go SDK (`testdata/batch_sdk.json`).
- **B2a, done.** A PaymentPolicy offers `schemes: [exact, batch-settlement]`
  per accepted asset; the controller adds our receiver authorizer and delay
  (`-batch-receiver-authorizer`, `-batch-withdraw-delay`, chart
  `controller.batch`). The filter verifies voucher payloads
  (`internal/verify/batch.go`, rules 1-6, contract-wallet vouchers through
  EIP-1271), holds the channel in Redis (`engine/payerstate/channels.go`:
  in step, within escrow, one request at a time, mirrored from the chain at
  most a minute old), commits on a 2xx and answers with `chargedAmount` and
  the channel snapshot; an out-of-step voucher gets the corrective 402.
  Charged channels are queued in `x402:ch:unclaimed` for B3. Deposits and
  refunds are refused (`invalid_batch_settlement_evm_payload_type`) until
  B2b. Not yet: a receipt row per charged request.
- **B2b, done.** A `deposit` payload: the ERC-3009 `receiveWithAuthorization`
  to the deposit collector is verified locally (payer's key or EIP-1271,
  nonce bound to the channel, amount signed, valid long enough), the payer's
  funds are held like a payment (balance, replay), and the deposit is queued
  as a sync job: the settler hands the client's own payload to facilitators
  that list `batch-settlement` (`/settle` `type: deposit`; `direct` does
  not). The filter waits (the controller gives batch routes the sync
  message timeout), mirrors the channel once the deposit is visible, then
  holds the voucher and serves. `PAYMENT-RESPONSE` carries the deposit
  transaction and amount. Deposit receipts are `deposit_served` /
  `deposit_settled` / `deposit_unpaid`, so revenue (status `settled`) never
  counts escrow. Not confirmed in time: 503 with `retry-after`, the deposit
  stays queued. Permit2 deposits refused.
- **B3, done.** `settle.Claimer` in the settler (`-batch-authorizer-key`,
  chart `settler.batchClaims`), every `-claim-interval` (5 min): lists the
  queued channels, re-reads each from the chain (out-of-band claims,
  withdrawals), and claims a channel once its unclaimed charges reach
  `-claim-min-amount` (0.10 USDC), a withdrawal is pending, or it waited an
  hour. One ClaimBatch per (network, provider, seller, token), at most 50
  rows, signed with our key and sent as `/settle` `type: claim` through the
  route's provider backends in order, with the settler's health checks;
  then `type: settle` pays the claimed funds out to the seller. Receipts:
  one `batch_claimed` row per channel (the claimed delta, the claim
  transaction). A failed payout loses nothing: claimed funds wait in the
  contract for the seller. Not yet: submitting claims ourselves through
  `direct` (needs the tuple ABI), a receipt per charged request, the demo's
  revenue counting `batch_claimed`, and PayAI's out-of-credits refusal
  treated as a backend outage.
- **B2, filter (original plan).**
  - Price table: a route may also accept `batch-settlement`. The 402 offers
    `exact` and `batch-settlement` side by side, with `receiverAuthorizer`,
    `withdrawDelay` and `minDeposit`.
  - `voucher` payloads: check the channel config (hash, token, receiver,
    authorizer, delay); check the signature; reserve atomically, in one Lua
    script, that `maxClaimable == charged + price` and
    `maxClaimable <= balance`; on a 2xx, commit `charged += price`.
  - `deposit` payloads: deposit through the settler's backends, then serve.
  - Corrective 402 when the cumulative amount is out of step.
- **B3, settler.**
  - A claim job per receiver and token. It runs on a schedule, when the
    unclaimed amount passes a threshold, and at once when a timed withdrawal
    starts (vouchers left unclaimed after the delay are lost).
  - It signs the ClaimBatch with the receiver authorizer key, submits it
    through the backend list, then calls `settle`.
  - Receipts: per-request `charged` rows, and `claimed` and `settled` rows
    with the transaction.
- **B4.** Cooperative refunds; the Go SDK client in `make e2e`; live on Base
  Sepolia through CDP and x402.org; then one mainnet channel.

## Open

- Defaults for `withdrawDelay` (proposed 1 day), `minDeposit` (proposed 100
  times the price) and the claim threshold.
- Whether `exact` remains offered next to `batch-settlement` on cheap routes
  (proposed: yes; clients choose).
- How a seller sees money that is charged but not yet claimed (the dashboard,
  hosted H2).
