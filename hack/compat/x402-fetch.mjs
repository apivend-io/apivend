// Pays priced routes with the reference TypeScript client, @x402/fetch, and
// reports what it made of the gateway's answers.
//
//   X402_PRIVATE_KEY=0x... node x402-fetch.mjs URL [URL...]
//
// Prints one JSON line per URL; exits non-zero if any payment was not served.
import { wrapFetchWithPaymentFromConfig, decodePaymentResponseHeader } from "@x402/fetch";
import { ExactEvmScheme } from "@x402/evm";
import { privateKeyToAccount } from "viem/accounts";

const key = process.env.X402_PRIVATE_KEY;
const urls = process.argv.slice(2);
if (!key || urls.length === 0) {
  console.error("usage: X402_PRIVATE_KEY=0x... node x402-fetch.mjs URL [URL...]");
  process.exit(2);
}

const pay = wrapFetchWithPaymentFromConfig(fetch, {
  schemes: [{ network: "eip155:*", client: new ExactEvmScheme(privateKeyToAccount(key)) }],
});

let failed = false;
for (const url of urls) {
  const out = { client: "@x402/fetch", url };
  try {
    const res = await pay(url, { method: "GET" });
    out.status = res.status;
    const header = res.headers.get("PAYMENT-RESPONSE");
    out.paymentResponseHeader = header !== null;
    if (header) {
      // Whether the client can decode the header, including the async form
      // (success: true with an empty transaction).
      try {
        out.paymentResponse = decodePaymentResponseHeader(header);
      } catch (e) {
        out.decodeError = String(e);
        failed = true;
      }
    }
    await res.arrayBuffer();
    if (res.status !== 200) failed = true;
  } catch (e) {
    out.error = String(e);
    failed = true;
  }
  console.log(JSON.stringify(out));
}
process.exit(failed ? 1 : 0);
