# Security

This gateway decides whether a request has been paid for, so a flaw in it can
cost the people who run it money. Please report one privately first.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting: the **Report a vulnerability**
button under this repository's **Security** tab. Please do not open a public
issue or pull request for a security problem.

Include what you found, how to reproduce it, and what an attacker could do
with it. You will get an answer within three working days. We will keep you
informed while we fix it, and credit you in the advisory if you wish.

## What counts

Anything that lets someone:

- get a priced response without paying, or pay less than the price;
- use one payment more than once, or make the gateway charge for a request
  it did not serve;
- move or redirect a payer's or a payee's funds;
- read secrets the gateway holds (facilitator credentials, RPC URLs, the
  direct settlement key);
- make the gateway serve priced requests when it should fail closed.

Denial of service through sheer volume is out of scope; a request that costs
the gateway far more work than it costs the sender is in scope.

## Supported versions

Fixes go into the latest release. Before 1.0, older releases do not get
fixes.
