# Changelog

## v0.2.0

Most of this release comes from a security review of a production
deployment before it opened to the public: ways a request could be served
without being paid for, found and closed. Upgrade notes at the end.

### Fixed: served without payment

- **OPTIONS requests** reached the upstream unpaid. Browser CORS preflights
  (OPTIONS with `Origin` and `Access-Control-Request-Method`) are now
  answered by the filter itself (204, with the CORS headers x402 clients
  need); any other OPTIONS request is priced like the route's other
  methods.
- **A cancelled authorization counted as settled.** USDC reports a
  cancelled EIP-3009 authorization the same way as a used one; a payer
  could cancel theirs and be served. The settler now requires the
  authorization's transfer on chain; a cancellation is unpaid and blocks
  the payer.
- **A nonce spent on another transfer counted as payment.** A payer could
  sign a second authorization with the same nonce (say, 1 unit to their own
  address) and have it settled first, by any facilitator, right after the
  gateway served an async answer: the nonce's AuthorizationUsed log was
  taken as proof of this payment. The settler now requires the token's
  Transfer logged right after it in the same transaction, from the payer to
  the route's payTo for exactly the price; anything else is unpaid and
  blocks the payer. Found by an outside tester.
- **An answer was served after its payment had expired.** If the
  authorization ran out while the upstream worked, or the hold could not
  be committed, the response still went out. It is now withheld (402, or
  503 when the store failed). Authorizations must have 30 seconds left
  (was 10).
- **Connection upgrades** (WebSocket) released the payment without serving
  anything chargeable. They are refused on paid routes (400).
- **A rejected PaymentPolicy left its route free.** The controller now
  keeps the filter on the route and the filter answers 503 until the policy
  is fixed or deleted (deleting a policy still frees its route).
- **Routes that must be priced** can be listed for the filter
  (`filter.refuseUnpricedRoutes`, patterns of Envoy route names): before
  their prices load, they are refused instead of passed through.
- **A made-up `X-X402-Payer` header** passed through on unpriced requests;
  it is now removed from every request the filter sees.

### Fixed: other

- After a facilitator **refused** a payment, the settler tried the next
  one. Only an unavailable facilitator fails over now.
- Contract-wallet signature checks (EIP-1271, ERC-6492) made one RPC call
  per junk signature from a fresh address. They now spend from a budget
  (`-contract-wallet-checks-per-second`, default 10); past it the payment
  gets 503 and may be retried. Payments signed by the payer's own key never
  use it.
- Merging a change no longer restarts every pod: `helm.sh/chart` (which
  carries Flux's per-commit chart version) is out of the pod templates.
- The settler retries its first PostgreSQL
  connection for 30 seconds instead of exiting.

### New

- **`settlement.maxUnsettledPerRoute`**: what a route may have served and
  not yet settled, across all payers. Payer addresses are free, so the
  per-payer cap alone does not bound an async route. Past it the route
  answers 503 with `Retry-After` until settlements catch up; nothing is
  held. The chart's `controller.defaultMaxUnsettledPerRoute` sets it for
  async routes that do not (never below a route's price).
- Smart-wallet signatures over any hash (`verify.ChainChecker.VerifyHash`),
  for sign-in flows: EIP-1271, and ERC-6492 for wallets from an allowed
  factory.

### Upgrading from v0.1

- Apply the CRDs first (`kubectl apply -f deploy/crds/`): Helm does not
  upgrade them, and `maxUnsettledPerRoute` is new.
- Upgrade the controller and the filter together (one chart release): the
  price table may now carry a `refuse` list that a v0.1 filter cannot
  read.
- Behaviour you may notice: plain OPTIONS requests now answer 402 on paid
  routes; upgrades answer 400; routes of rejected policies answer 503.
