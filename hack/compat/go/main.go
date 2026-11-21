// Pays priced routes with the x402 Foundation's Go client
// (github.com/x402-foundation/x402/go/v2) and reports what it made of the
// gateway's answers. Its own module, so the gateway never depends on it.
//
//	X402_PRIVATE_KEY=0x... go run . URL [URL...]
//	X402_PRIVATE_KEY=0x... X402_RPC=https://... go run . -batch URL [URL...]
//
// -batch pays with the batch-settlement scheme instead of exact: a deposit
// into a payment channel on the first request, signed vouchers after. It
// reads channel state through X402_RPC (never printed).
// Prints one JSON line per URL; exits non-zero if any payment was not served.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/ethereum/go-ethereum/ethclient"
	x402 "github.com/x402-foundation/x402/go/v2"
	x402http "github.com/x402-foundation/x402/go/v2/http"
	batch "github.com/x402-foundation/x402/go/v2/mechanisms/evm/batch-settlement/client"
	evm "github.com/x402-foundation/x402/go/v2/mechanisms/evm/exact/client"
	evmsigners "github.com/x402-foundation/x402/go/v2/signers/evm"
)

func main() {
	useBatch := flag.Bool("batch", false, "pay with batch-settlement (channel deposit, then vouchers)")
	salt := flag.String("salt", "0", "batch channel salt (a new value opens a new channel)")
	refund := flag.Bool("refund", false, "with -batch: ask for a cooperative refund of the channel behind each URL instead of paying")
	refundAmount := flag.String("refund-amount", "", "with -refund: partial refund in atomic units (default: everything left)")
	flag.Parse()
	key := os.Getenv("X402_PRIVATE_KEY")
	if key == "" || flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: X402_PRIVATE_KEY=0x... go run . [-batch] URL [URL...]")
		os.Exit(2)
	}
	client := x402.Newx402Client()
	if *useBatch {
		rpc := os.Getenv("X402_RPC")
		if rpc == "" {
			fmt.Fprintln(os.Stderr, "-batch needs X402_RPC")
			os.Exit(2)
		}
		ec, err := ethclient.DialContext(context.Background(), rpc)
		if err != nil {
			fmt.Fprintln(os.Stderr, "rpc: connection failed") // the URL may embed a key: never print it
			os.Exit(2)
		}
		signer, err := evmsigners.NewClientSignerFromPrivateKeyWithClient(key, ec)
		if err != nil {
			fmt.Fprintln(os.Stderr, "signer:", err)
			os.Exit(2)
		}
		scheme := batch.NewBatchSettlementEvmScheme(signer, &batch.BatchSettlementEvmSchemeOptions{Salt: *salt})
		if *refund {
			failed := false
			for _, url := range flag.Args() {
				out := map[string]any{"client": "x402-foundation/x402/go/v2", "url": url, "refund": true}
				sr, err := scheme.Refund(context.Background(), url, &batch.RefundOptions{Amount: *refundAmount})
				if err != nil {
					out["error"] = err.Error()
					failed = true
				} else {
					out["paymentResponse"] = sr
				}
				b, _ := json.Marshal(out)
				fmt.Println(string(b))
			}
			if failed {
				os.Exit(1)
			}
			return
		}
		client.Register("eip155:*", scheme)
	} else {
		signer, err := evmsigners.NewClientSignerFromPrivateKey(key)
		if err != nil {
			fmt.Fprintln(os.Stderr, "signer:", err)
			os.Exit(2)
		}
		client.Register("eip155:*", evm.NewExactEvmScheme(signer, nil))
	}
	hc := x402http.Newx402HTTPClient(client)
	pay := x402http.WrapHTTPClientWithPayment(http.DefaultClient, hc)

	failed := false
	for _, url := range flag.Args() {
		out := map[string]any{"client": "x402-foundation/x402/go/v2", "url": url}
		resp, err := pay.Get(url)
		if err != nil {
			out["error"] = err.Error()
			failed = true
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			out["status"] = resp.StatusCode
			headers := map[string]string{}
			for k := range resp.Header {
				headers[k] = resp.Header.Get(k)
			}
			if h := resp.Header.Get("PAYMENT-RESPONSE"); h != "" {
				out["paymentResponseHeader"] = true
				// Whether the client can decode it, including the async form
				// (success: true, empty transaction).
				if sr, err := hc.GetPaymentSettleResponse(headers); err != nil {
					out["decodeError"] = err.Error()
					failed = true
				} else {
					out["paymentResponse"] = sr
				}
			}
			if resp.StatusCode != 200 {
				failed = true
			}
		}
		b, _ := json.Marshal(out)
		fmt.Println(string(b))
	}
	if failed {
		os.Exit(1)
	}
}
