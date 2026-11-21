# Contributing

Thank you for helping. Bug reports, fixes, client compatibility results and
documentation are all welcome.

## Before you start

For anything larger than a small fix, open an issue first and describe what
you want to change. Changes to how payments are verified, reserved or settled
get a careful review; it saves you work to agree on the approach first.

## Making a change

```sh
make hooks   # once per clone: gofmt and go vet before each push
make test    # vet, then all tests with the race detector
make e2e     # a real Envoy in Docker, paid by the x402 Foundation's clients
```

Tests that need Redis or PostgreSQL start their own servers and skip when the
binaries are not installed; set `X402_REQUIRE_TEST_SERVERS=1` to make a
missing server a failure instead. After editing `api/v1alpha1`, run
`make manifests`; after editing `api/proto`, run `make proto`.

A few rules the code depends on:

- Payment paths fail closed. Do not add a fallback that serves a priced
  request when verification, Redis or chain data is unavailable, other than
  through a route's explicit `onStateUnavailable: allow`.
- Every change to a payer's state is one Redis script. Do not split a check
  and a write across round trips.
- Never label a metric with a payer address, and never log RPC URLs or
  client-supplied values unescaped.
- Add a test with every fix and every feature.

To add a client to the compatibility table, see
[docs/COMPATIBILITY.md](docs/COMPATIBILITY.md).

## Sign-off

Every commit is signed off under the
[Developer Certificate of Origin](https://developercert.org): you certify
that you wrote the change, or have the right to submit it under this
project's licence. Add the line with `git commit -s`:

```
Signed-off-by: Your Name <you@example.com>
```

A pull request whose commits are not all signed off cannot be merged. There
is no contributor licence agreement.

## Licence

By contributing, you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE), like the rest of the project.
