# Status

What has been tested, and against what. "Real" means the actual software ran
in the test: a real Envoy, a real Redis, a real chain. A gap is listed as a
gap.

v0.3 has served and settled real payments on Base Sepolia and Base mainnet,
behind Envoy Gateway on Kubernetes. It has not run at volume.

| Part | Tested against | Not yet tested |
|---|---|---|
| Filter | A real Envoy 1.34 in `make e2e` on every pull request: challenge, payment, replay refused, sync mode, route matching. Envoy Gateway v1.10.0 on k3s and kind: payments verified and served in both modes, refusals on insufficient balance, sync mode returning 402 when settlement is refused | Volume |
| Clients | The x402 Foundation's TypeScript (`@x402/fetch` 2.31.0), Go (v2.31.0) and Python (2.28.0) clients, on every pull request and on a live cluster. See [COMPATIBILITY.md](COMPATIBILITY.md) | Browsers, Rust, Java (still protocol v1) |
| Signatures | The x402 specification's example; signatures made by viem through `@x402/fetch`; both USDC signing domains checked against the chain; payments settled on Base Sepolia and Base mainnet | Multi-owner smart wallets |
| Smart wallets | A minimal EIP-1271 wallet and Coinbase Smart Wallets (deployed, and undeployed through ERC-6492; key and passkey owners) paid and settled on Base Sepolia | |
| Payer state | Real Redis 7 with concurrent filter replicas; a payer able to afford 2 of 10 concurrent requests got exactly 2 served | Redis failover; recovery from a crash |
| Balances | Public Base Sepolia RPC endpoints, with reads on the request path inside the default 300 ms. The hosted source killed in the middle of traffic: every payment then decided from RPC, none on old data | A paid RPC provider |
| Settlement | The x402.org, CDP and PayAI facilitators on Base Sepolia; CDP on Base mainnet. An unreachable facilitator skipped after two failures and payments settled through the next one. Direct settlement with our own gas key on Base Sepolia (about 86,000 gas each) | Volume |
| Batch settlement | Live on Base Sepolia with the x402 Foundation's Go client: a deposit, requests paid by voucher, a claim paid out to the payee, partial and full refunds. See [BATCH-SETTLEMENT.md](BATCH-SETTLEMENT.md) | Base mainnet; many channels in one claim |
| Controller | A real API server (k3s v1.37): policies reach `Programmed`, Envoy Gateway accepts what it generates, deleting a policy frees its route within a second, both filter replicas pick up a price change within a millisecond of each other | |
| Receipts | Real PostgreSQL 16, with rows for real payments | Reconciliation against the chain (not built) |
| Helm chart | `helm lint` on every pull request; installs and upgrades on k3s; the kind smoke test installs it on every pull request that changes the gateway | PostgreSQL and hosted-source values on a fresh install |
| Alerts, dashboard | The files parse | `promtool check rules`; a Grafana import |

## Not built yet

- Networks other than Base and Base Sepolia, and assets other than USDC.
- A daily reconciliation of receipts against transfers on the chain.
- Exports of receipts and a webhook per settled payment.
- Tracing.
- Websocket log subscriptions (balances are polled).
