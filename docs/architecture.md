# Architecture

The gateway is three programs and two stores next to Envoy:

| Part | What it does | On the request path |
|---|---|---|
| `x402-filter` | Envoy external processor (gRPC). Answers 402, verifies payments, reserves funds, charges on 2xx | Yes |
| Redis | Reservations, used authorizations, the settlement queue, cached balances | Yes |
| `x402-settler` | Settles charged payments, writes receipts | No |
| `x402-controller` | Turns `PaymentPolicy` and `SettlementProvider` into the price table, the providers and Envoy Gateway configuration | No |
| PostgreSQL | Receipts, append-only (optional) | No |

The filter is an external processor rather than an authorization filter
because it needs both directions: the request to challenge and verify, the
response to decide whether to charge.

## One paid request

1. A request to a priced route arrives without a `PAYMENT-SIGNATURE` header.
   The filter answers `402` with the terms (network, asset, amount, payee,
   validity) in `PAYMENT-REQUIRED`, repeated in the body.
2. The caller signs an EIP-3009 `transferWithAuthorization` for exactly those
   terms and sends the request again with it.
3. The filter checks the terms, the validity window and the signature itself;
   no facilitator call is on the request path. Then, in one Redis script, it
   reserves the amount: the authorization must be unused, the payer's balance
   less their other reservations must cover it, and the payer must be under
   the route's cap on unsettled payments.
4. The payment header is removed and the request goes to your API with
   `x-x402-payer` set to the payer's address. A value the caller sent in that
   header is overwritten.
5. Your API answers. On a 2xx, the reservation becomes a charge and a
   settlement job is queued, in one Redis script. On anything else, or when
   the request is aborted, the reservation is released: nobody is charged,
   and the same authorization can be used again. With the answer check on,
   a 2xx that may be JSON is held until its body has arrived; an obviously
   unusable one is released and replaced by a 503 (see the install guide).
6. The settler takes the job with the nearest deadline (authorizations
   expire), settles it, writes the receipt and lowers the payer's unsettled
   total.

With `settlement.mode: sync`, step 5 waits for step 6: the response leaves
only once the payment has settled, with the transaction in
`PAYMENT-RESPONSE`. If settlement fails, the caller gets 402 instead of the
response.

In async mode (the default), `PAYMENT-RESPONSE` says `success: true` with an
empty `transaction`: the transfer has not been sent yet. The x402
specification describes settling before responding; responding first keeps a
paid request as fast as an unpaid one, and the cap on unsettled payments
bounds what an API owner can lose to a payer whose settlement then fails.
That payer is blocked for an hour.

## Where balances come from

The filter needs a payer's USDC balance the first time it sees them, and
keeps it current afterwards. It asks an ordered list of sources and records,
on each decision, which source and which block it was based on:

- **RPC** (`-rpc network=url[,url...]`): reads the chain directly, polls for
  new blocks and follows transfers and used authorizations of known payers.
  A balance increase can be required to age a few blocks before it counts.
- **A hosted payer-state service** (`-hosted-state URL`), optional: the same
  facts over gRPC, each with its block. When it is configured, RPC is the
  fallback: the filter asks RPC after a third of its time budget if the
  hosted service has not answered.

A source that cannot answer leads to the next one or to a refusal, never to
a decision on old data. Reservations, used authorizations and the queue
always stay in the gateway's own Redis.

## Settlement

A `SettlementProvider` lists backends by priority:

- **Facilitators**: any x402 facilitator URL, with no credentials, a bearer
  token or Coinbase CDP's signed requests. The settler asks each one which
  networks and schemes it supports, and does not send it payments it says
  it cannot handle.
- **Direct**: the settler simulates the transfer, signs a transaction with a
  gas-only key and sends it through its own RPC endpoints. It refuses to pay
  more gas than the backend's `maxFee`.

A backend that fails twice in a row is skipped for a cool-down that grows
from 10 seconds to 2 minutes, then probed with one payment. A refusal (an
emptied wallet, a bad signature) is an answer, not a failure: it is not
retried elsewhere. Before retrying a payment, the settler asks the chain
whether the authorization has already been used, so a payment is never sent
twice and never recorded as unpaid when it went through.

## Batch settlement

With the `batch-settlement` scheme, a payer deposits once into a payment
channel and then pays each request with a signed voucher: no transfer per
request. The settler claims the vouchers in batches and pays the total out
to the payee. Refunds of what is left go back to the payer. See
[BATCH-SETTLEMENT.md](BATCH-SETTLEMENT.md).

## Failing closed

| Failure | What happens |
|---|---|
| The filter is unreachable | Envoy refuses priced requests (the controller sets `failOpen: false`) |
| Redis is down | Priced requests are refused |
| Chain data unavailable | Refused, unless the route sets `onStateUnavailable: allow` |
| A payer drains their wallet between payment and settlement | Bounded by the per-payer cap; the payer is blocked after a failed settlement |
| A facilitator is down | The next backend settles |

## Kubernetes

The controller watches `PaymentPolicy` and `SettlementProvider` and writes:

- the price table, a ConfigMap the filters read through the Kubernetes API,
  so every replica sees a change within about a second;
- the providers, a Secret the settler reads;
- for each priced route, an Envoy Gateway `EnvoyExtensionPolicy` that sends
  the route through the filter, with a message timeout long enough for a
  first balance read (and for settlement on sync routes).

Deleting a policy makes its route free again; the filter matches routes by
Envoy's route name, so a policy only ever prices its own route.

## Records

- **Receipts**: one row per payment in PostgreSQL: route, payer, amount,
  network, the source and block behind the decision, the backend that
  settled it and the transaction.
- **Metrics**: requests by route and decision, settlement attempts by
  backend and outcome, queue depth and nearest deadline, unsettled amounts by
  route, chain read latency. Payer addresses are never metric labels.

## Security

- No payer key is ever in the cluster. The direct backend's key pays gas
  only.
- Every value from a client is untrusted: headers are parsed strictly, with
  size limits, and never logged unescaped.
- An authorization names its payee and amount, so it cannot be redirected,
  and its nonce is recorded as used before the request is served.
- RPC URLs often carry API keys: they come from Secrets and are never
  logged.
