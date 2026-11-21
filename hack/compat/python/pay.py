"""Pays priced routes with the x402 Foundation's Python client (the x402
package, requests transport) and reports what it made of the gateway's
answers.

    X402_PRIVATE_KEY=0x... python pay.py URL [URL...]

Prints one JSON line per URL; exits non-zero if any payment was not served.
"""

import json
import os
import sys

from eth_account import Account
from x402 import x402ClientSync
from x402.http.clients import x402_requests
from x402.http.utils import decode_payment_response_header
from x402.mechanisms.evm.exact import ExactEvmScheme
from x402.mechanisms.evm.signers import EthAccountSigner


def main() -> int:
    key = os.environ.get("X402_PRIVATE_KEY")
    urls = sys.argv[1:]
    if not key or not urls:
        print("usage: X402_PRIVATE_KEY=0x... python pay.py URL [URL...]", file=sys.stderr)
        return 2
    client = x402ClientSync()
    client.register("eip155:*", ExactEvmScheme(signer=EthAccountSigner(Account.from_key(key))))
    session = x402_requests(client)

    failed = False
    for url in urls:
        out = {"client": "x402 (python)", "url": url}
        try:
            resp = session.get(url, timeout=30)
            out["status"] = resp.status_code
            header = resp.headers.get("PAYMENT-RESPONSE")
            out["paymentResponseHeader"] = header is not None
            if header:
                # Whether the client can decode it, including the async form
                # (success: true, empty transaction).
                try:
                    out["paymentResponse"] = decode_payment_response_header(header).model_dump(by_alias=True, exclude_none=True)
                except Exception as e:  # noqa: BLE001
                    out["decodeError"] = repr(e)
                    failed = True
            if resp.status_code != 200:
                failed = True
        except Exception as e:  # noqa: BLE001
            out["error"] = repr(e)
            failed = True
        print(json.dumps(out))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
