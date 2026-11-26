# Install

Two ways to run the gateway:

- **Kubernetes with Envoy Gateway** (below). The controller turns
  `PaymentPolicy` and `SettlementProvider` resources into the filter's price
  table, the settler's providers and the Envoy configuration.
- **Standalone Envoy** (at the end), with the price table and providers as
  files.

Start on Base Sepolia with test USDC; move a route to Base once it works.

## Kubernetes

### What you need

- A Kubernetes cluster with the Gateway API and
  [Envoy Gateway](https://gateway.envoyproxy.io). Tested with Envoy Gateway
  v1.10.0 on k3s v1.37 and on kind.
- Redis 7, one instance (not Redis Cluster), with persistence on and no
  eviction: it holds payments that have not settled yet.
  `hack/dev-cluster/redis.yaml` is a minimal example.
- A chain RPC endpoint per network you accept, from any provider. Optional
  but recommended: without one, the gateway cannot check a payer's balance
  before serving.
- Optional: PostgreSQL, for receipts. Without it, receipts go to the
  settler's log.

### 1. RPC endpoints

RPC URLs often contain an API key, so they come from a Secret. Several URLs
for one network are tried in order:

```sh
kubectl create namespace x402-system
kubectl -n x402-system create secret generic x402-rpc \
  --from-literal=base-sepolia='https://sepolia.base.org'
```

### 2. The chart

```sh
helm install x402 oci://ghcr.io/apivend-io/charts/x402 --version 0.6.0 \
  -n x402-system \
  --set redis.url=redis://redis.x402-system.svc:6379/0 \
  --set 'rpc[0].network=base-sepolia' \
  --set 'rpc[0].secretName=x402-rpc' \
  --set 'rpc[0].secretKey=base-sepolia'
```

This installs the CRDs, two filter replicas, the settler and the controller.
For production, give Redis a password and pass its URL through a Secret:
see [Securing Redis](#securing-redis).
For receipts, add `--set postgres.existingSecret.name=<secret>` with the DSN
under the key `dsn`. Every setting is described in the chart's
`values.yaml`.

### 3. A provider and a price

A `SettlementProvider` says how payments settle; it is cluster-wide. A
`PaymentPolicy` prices one HTTPRoute in its own namespace:

```yaml
apiVersion: apivend.io/v1alpha1
kind: SettlementProvider
metadata: { name: default }
spec:
  backends:
    - { type: facilitator, url: "https://x402.org/facilitator", priority: 1 }
---
apiVersion: apivend.io/v1alpha1
kind: PaymentPolicy
metadata: { name: search, namespace: prod }
spec:
  targetRef: { group: gateway.networking.k8s.io, kind: HTTPRoute, name: search }
  description: Search API
  price: { amount: "0.01", currency: USD }
  accepts:
    - { network: base-sepolia, asset: USDC, payTo: "0xYourWallet" }
  settlement: { mode: async, providerRef: { name: default }, maxUnsettledPerPayer: "1.00" }
```

`deploy/example/k8s/payment-policy.yaml` shows every field with comments.
The policy is ready when it is `Programmed`:

```sh
kubectl -n prod get paymentpolicy search
```

Deleting the policy makes the route free again.

### 4. Try it

Without a payment the route answers 402:

```sh
curl -i https://your-gateway/search
```

To pay, use any x402 client, or `x402-pay` from this repository with a
throwaway key holding test USDC
([Circle's faucet](https://faucet.circle.com) gives some on Base Sepolia):

```sh
X402_PRIVATE_KEY=0x... go run ./cmd/x402-pay https://your-gateway/search
```

### Base mainnet

Set `network: base` on the policy and settle through a facilitator that
supports Base mainnet. For Coinbase's CDP facilitator, create an API key in
the CDP portal, store its key file as a Secret in `x402-system`, and point a
provider at it:

```sh
kubectl -n x402-system create secret generic x402-cdp-key --from-file=key.json=cdp-api-key.json
```

```yaml
apiVersion: apivend.io/v1alpha1
kind: SettlementProvider
metadata: { name: cdp }
spec:
  backends:
    - type: facilitator
      url: https://api.cdp.coinbase.com/platform/v2/x402
      auth: cdp
      credentialsRef: { name: x402-cdp-key, key: key.json }
      priority: 1
```

`go run ./hack/cdpcheck -key cdp-api-key.json` checks a key against the
facilitator without paying anything.

A provider can list several backends; the settler tries them in priority
order and skips one that keeps failing. The last one can be `type: direct`:
the settler then submits the transfer itself, paying gas from a key you give
it. That key pays gas only and cannot move anyone's USDC; direct settlement
skips the compliance screening some facilitators do.

### Choices per route

- **`settlement.mode`**: `async` (the default) answers first and settles
  right after; `maxUnsettledPerPayer` caps what one payer can owe meanwhile,
  and `maxUnsettledPerRoute` what the route may have unsettled across all
  payers (past it, 503 with `Retry-After` until settlements catch up; the
  chart's `controller.defaultMaxUnsettledPerRoute` sets it for policies that
  do not).
  `sync` holds the response until the payment has settled and returns the
  transaction in `PAYMENT-RESPONSE`; if settlement fails, the caller gets 402
  instead of the response.
- **`risk.onStateUnavailable`**: `deny` (the default) refuses priced
  requests when Redis or chain data cannot be reached; `allow` serves them
  unpaid.
- **`discovery`**: describes the route for x402 directories and agents. Off
  until `enabled: true`; some directories list a route after its first
  settled payment, with no review.

### Buyer protection

`--set filter.answerCheck=watch` checks every paid 2xx answer that may be
JSON before charging and counts the verdict in
`x402_answer_checks_total{route,verdict}`, still charging everything.
Run it for a while and look at the verdicts; then `enforce` stops charging
for the unusable ones (empty, `not_json`, `html`, `error_only`,
`missing_fields`), which callers receive as `503` `unusable_answer`.

The filter gets the answer's body through ext_proc's mode override, which
Envoy honours only when the filter's config allows it: the controller's
policies set `allowModeOverride: true`, and `deploy/envoy/envoy.yaml` sets
`allow_mode_override: true`. Any `EnvoyExtensionPolicy` or Envoy config of
your own that runs the filter needs it too. Without it the body never
reaches the filter, answers are charged unchecked after they were sent
(`verdict="no_body"`), and the filter logs a warning for each.

If your API's normal answer looks like an error object, describe it in the
route's `discovery` output example: such answers are then charged.

### Securing Redis

Redis holds the payments in flight: the funds held while a request is
served, the nonces already used, and the settlement queue. Anyone who can
write to it can drop payments that were served but not settled, or replay
one. Treat it as part of the payment path:

- **A password, and a user per role.** The filter and settler need data
  commands, Lua scripts and blocking list commands, nothing else. Backups
  need only a snapshot. With Redis ACLs (here as an ACL file):

  ```
  user default off
  user x402 on >long-random-password ~* &* +@all -@admin -@dangerous
  user backup on >another-password +sync +psync +replconf +ping
  ```

  Put the URL in a Secret and point the chart at it, instead of
  `--set redis.url`:

  ```sh
  kubectl -n x402-system create secret generic x402-redis \
    --from-literal=url='redis://x402:long-random-password@redis.x402-system.svc:6379/0'
  helm upgrade x402 ... --set redis.existingSecret.name=x402-redis
  ```

  Snapshots then use `redis-cli --user backup --rdb <file>`.
- **A memory ceiling, never eviction.** Set `maxmemory` with
  `maxmemory-policy noeviction`. Evicting a hold or a nonce would mean an
  unpaid answer or a replay; refusing writes makes the filter refuse paid
  requests instead (it fails closed).
- **Persistence.** `appendonly yes` with `appendfsync everysec`: a restart
  loses at most about a second.
- **Its own instance.** If Envoy Gateway's global rate limiting uses Redis,
  give it a separate one: its counters are disposable (eviction is fine
  there), and its pods then never reach the payments.
- **A network policy** that lets only the filter and settler pods (and your
  backup job) connect.

## Watching it

- Metrics on each pod (`/metrics`; the chart adds Prometheus scrape
  annotations).
- Alert rules: `deploy/monitoring/alerts.yaml`. The most important one fires
  when a queued payment is close to its authorization's expiry.
- A Grafana dashboard: `deploy/monitoring/dashboard.json`.
- Receipts in PostgreSQL, one row per payment, with the transaction and the
  block behind each decision.

## Standalone Envoy

Without Kubernetes, run the filter and the settler next to Envoy and give
them files:

- `deploy/envoy/envoy.yaml`: Envoy with the filter as an external processor
- `deploy/example/routes.yaml`: the price table (`x402-filter -config`)
- `deploy/example/providers.yaml`: the providers (`x402-settler -providers`)

Both reload their file when it changes. `hack/e2e.sh` runs exactly this
setup, with every flag, and is the reference for it.
