# apivend

An open-source [x402](https://www.x402.org) payment gateway for Envoy and
Kubernetes. Put a price on an HTTP route, and callers pay for each request in
USDC: no account, no API key, no change to the API behind it.

```yaml
apiVersion: apivend.io/v1alpha1
kind: PaymentPolicy
metadata: { name: search, namespace: prod }
spec:
  targetRef: { group: gateway.networking.k8s.io, kind: HTTPRoute, name: search }
  price: { amount: "0.01", currency: USD }
  accepts:
    - { network: base, asset: USDC, payTo: "0xYourWallet" }
```

A request without payment gets `402 Payment Required` with the terms. The
caller signs a USDC authorization and sends the request again; the gateway
verifies it, passes the request to your API and charges only if the API
answered with a 2xx. Payments go from the caller's wallet straight to yours.

## What it does

- **Verifies payments locally.** Signatures are checked in the gateway, and
  each payment is reserved in Redis before your API sees the request, so a
  balance cannot be spent twice and a payment cannot be replayed.
- **Charges only for answers.** A reservation becomes a charge only when your
  API answers 2xx. Errors, timeouts and aborted requests cost the caller
  nothing.
- **Settles after the response, or before it.** By default the response goes
  out first and the payment settles about a second later, with a cap on what
  any one payer can owe in the meantime. A route can instead settle first and
  return the transaction in `PAYMENT-RESPONSE`.
- **Settles through more than one service.** Settlement goes through x402
  facilitators in your order of preference (for example x402.org, Coinbase's
  CDP, PayAI), skips one that is down, and can fall back to settling with your
  own gas key.
- **Accepts smart wallets.** Besides ordinary wallets: contract wallets
  (EIP-1271) and smart wallets not yet deployed (ERC-6492, from factories you
  allow).
- **Fails closed.** If the filter, Redis or chain data is unavailable, a
  priced request is refused rather than served for free, unless a route opts
  out.
- **Keeps records.** One receipt per payment in PostgreSQL, Prometheus
  metrics, alert rules and a Grafana dashboard.
- **Holds no payer keys.** Nothing in the cluster can move a payer's money.

Networks: Base and Base Sepolia, USDC. Schemes: `exact`, and
`batch-settlement` (payment channels: one deposit, then signed vouchers,
claimed in batches). See [docs/BATCH-SETTLEMENT.md](docs/BATCH-SETTLEMENT.md).

## Status

**v0.3: early.** The gateway has served and settled real payments on Base
Sepolia and on Base mainnet, behind Envoy Gateway on Kubernetes. It has not
run at volume yet. What has been tested, and against what, is in
[docs/status.md](docs/status.md).

Clients that have paid through it: the x402 Foundation's TypeScript
(`@x402/fetch`), Go and Python clients, on every pull request. See
[docs/COMPATIBILITY.md](docs/COMPATIBILITY.md).

## Install

On Kubernetes with Envoy Gateway, the Helm chart installs the filter, the
settler and the controller:

```sh
helm install x402 oci://ghcr.io/apivend-io/charts/x402 --version 0.3.0 \
  -n x402-system --create-namespace \
  --set redis.url=redis://redis.x402-system.svc:6379/0
```

Then create a `SettlementProvider` and a `PaymentPolicy`. The full guide,
including standalone Envoy without Kubernetes, is in
[docs/install.md](docs/install.md).

## How it fits together

```
caller ──> Envoy ──────────────> your API
             │  ext_proc
             v
         x402-filter ──> Redis <── x402-settler ──> facilitator or chain
             ^                          │
             │ price table              v
       x402-controller              PostgreSQL (receipts)
       (PaymentPolicy, SettlementProvider)
```

[docs/architecture.md](docs/architecture.md) explains each part and the rules
it follows.

## Develop

Requires Go 1.26. Tests that need Redis or PostgreSQL start their own servers
and skip when the binaries are not installed.

```sh
make test   # vet, then all tests with the race detector
make e2e    # a real Envoy (Docker), Redis, a fake chain and a mock facilitator,
            # paid with the x402 Foundation's clients
```

`hack/e2e.sh` is also the shortest complete example of wiring every part
together by hand.

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md); commits
are signed off under the Developer Certificate of Origin. To report a
security issue, see [SECURITY.md](SECURITY.md).

## Licence

Apache License 2.0, see [LICENSE](LICENSE). "apivend" is a trademark; see
[TRADEMARKS.md](TRADEMARKS.md) for how to use the name.
