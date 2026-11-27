# Changelog

## v0.7.0

### Changed

- **The defaults are now the settings we run in production.** The answer
  check is on: `-answer-check` (chart: `filter.answerCheck`) defaults to
  `enforce`, where it was `off`. Set it to `off` to keep the old behaviour.
  It needs Envoy's ext_proc mode override, which `deploy/envoy/envoy.yaml`
  and the controller's policies already allow. An unquoted `off` in your
  values (YAML reads it as `false`) still means off.

### Added

- **A repeated paid request is not charged twice.** A paid GET answer that
  the answer check held, on a `sync` route once its payment has settled, is
  kept for `-redeliver-window` (env `X402_REDELIVER_WINDOW`, chart
  `filter.redeliverWindow`; default `60s`, at most `1h`, `0` turns it off).
  When the same payer sends the same request (method, host, path and query,
  and the headers that commonly pick the answer: `Authorization`,
  `Proxy-Authorization`, `Cookie`, `X-API-Key`, `Accept`,
  `Accept-Language`, `Accept-Encoding`, `Origin`) again within it, with a
  new valid payment, the filter returns the kept answer with its original
  `PAYMENT-RESPONSE` and `x-x402-redelivered: true`; the API is not called
  and the new payment is never reserved or charged. This is for a buyer
  whose connection dropped after the charge: x402 clients sign a new
  payment on retry rather than resend the old one. Only GET; only answers
  held whole (JSON up to 256 KiB: the answer check's limits apply); only
  `exact` payments; only settled ones (an answer served before settlement
  may yet go unpaid, so async routes and sync waits that time out keep
  nothing); only `200` answers to requests without `Range` or a
  `Content-Length` body, and not when the answer's `Vary` names a header
  outside the list above. The payment the answer was charged to, resent, is
  accepted too (a payment used for another request is not); since a settled
  payment is public on chain, turn redelivery off if answers depend on who
  paid (see the install guide). A repeat that arrives while the first call
  is still being answered (a client that timed out) waits for it, up to the
  route's settlement wait, instead of being charged too (at most 8 per
  request and 256 per filter replica wait at once). Kept answers are in the
  gateway's Redis until the window ends. Metric:
  `x402_requests_total{decision="redelivered"}`.
- `x402-check` checks replay protection with the used payment on another
  request (still `402` `payment_already_used`), and accepts a redelivered
  answer for the same request. An older `x402-check` reports redelivery as
  a failed replay check. Its overspend payments each use their own query.

## v0.6.0

### Added

- **Buyer protection: an answer is checked before it is charged.** With
  `-answer-check` (chart: `filter.answerCheck`) set to `enforce`, the
  filter holds a paid 2xx answer that may be JSON and looks at its form
  before charging. An answer that is empty, does not parse (cut off), is
  an HTML page where JSON was expected, is an object of only error fields
  (`{"error":"upstream timed out"}`), or lacks at least half of the fields
  the route reliably returns is not charged: the reservation is released
  and the caller gets `503` with `{"error":"unusable_answer","reason":...}`.
  The answer itself is never passed on. Content is never judged: an empty
  list or "not found" in the normal structure is an answer and is charged.
  `watch` runs the same check and only counts the verdicts; `off` (the
  default) charges on the headers as before. JSON is expected when the
  answer says `application/json` (or `+json`) or the route's discovery
  output is JSON; answers over 256 KiB (`-answer-check-max-bytes`), streams
  and other content types are charged unchecked. The check needs Envoy's
  ext_proc mode override (see the install guide). Metrics:
  `x402_answer_checks_total{route,verdict}` and
  `x402_answer_hold_seconds{part}` (about a millisecond in our production).
- With the check on, a paid request asks the API for `gzip` or no
  compression, so the answer can be read: proxies such as Cloudflare offer
  `br` and `zstd` on the caller's behalf.

### Changed

- **A paid HEAD request is refused before payment.** It would be charged
  for headers only: `405`, nothing reserved. An unpaid HEAD still gets its
  402 with the terms.
- **The controller's policies allow mode override** (`allowModeOverride`),
  and so does `deploy/envoy/envoy.yaml`. Nothing changes while the answer
  check is off.

## v0.5.0

### Changed

- **A payment to oneself is refused up front.** When the payer is the
  route's `payTo` (an exact authorization from that address, or a
  batch-settlement channel paid by it), the gateway answers 402 with
  `error: self_send_not_allowed` before anything is reserved or the API
  is called. Facilitators refuse to settle such a transfer, so before
  this the API answered and the payment was then recorded as unpaid.
- **The demo page shows its payment record without JavaScript.** The
  newest receipts, the totals per network and the counters are rendered
  into the HTML, so agents and other readers that fetch the page as text
  see them; the script replaces them with the live ones as before.

## v0.4.0

### Changed

- **Required query parameters are checked before payment.** A route whose
  policy marks a query parameter `required` (in `discovery.queryParams`)
  now refuses a paid request without it: 400, before the payment is
  looked at, so nothing is reserved or charged. An unpaid request still
  gets its 402, since directories find routes by calling the bare URL;
  the 402's `error` names the missing parameter. A batch-settlement
  refund, which never calls the API, needs no parameters.
- **The bazaar extension describes the output.** A route with
  `discovery.output.example` now publishes a JSON Schema inferred from it
  (types, properties, array items) instead of an empty one: directories
  such as Coinbase's Bazaar count the output schema towards a listing's
  quality. When the schema would take the extension past its 16 KB limit,
  the route is published without it rather than refused; an example over
  16 KB is refused.

### Added

- **`chain.RPC.UseClient`** sends a client's JSON-RPC calls through an
  HTTP client of the caller's choosing, for callers whose network policy
  allows only guarded connections.

## v0.3.0

### Fixed

- **Client headers in the gateway's namespaces reached the upstream.** A
  client could send `X-Apivend-*` or `X-X402-*` headers (other than the
  payer header, which the filter already replaced), and a backend that
  trusts them would believe them. Every request the filter forwards now
  loses them, along with a client's `X-Forwarded-Host` and anything from
  Cloudflare Access in front of the gateway: every `Cf-Access-*` header and
  its session cookies (other cookies are kept). Found by an outside tester
  and an independent code review.
- **A 503 for a payment that could not be verified** (the chain unreachable,
  or contract-wallet checks over their budget, which is per filter replica)
  now says when to try again: `Retry-After: 2`.
- **Rollouts could drop requests.** Envoy keeps sending to a pod for a
  moment after Kubernetes takes it out of its Service. The filter and the
  demo now wait before stopping (`rolloutDrainSeconds`, default 5; on
  Kubernetes 1.30 or newer, 0 turns it off).

### Documentation

- **Securing Redis** in `docs/install.md`: a user per role with Redis
  ACLs, the URL from a Secret (`redis.existingSecret`), a memory ceiling
  without eviction, persistence, a separate Redis for Envoy Gateway's rate
  limiter, and a network policy.

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
