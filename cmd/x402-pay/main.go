// x402-pay is a minimal x402 v2 client for demos and smoke tests: it requests
// a URL and, when challenged with a 402, signs an EIP-3009 payment and retries.
//
// The key is read from X402_PRIVATE_KEY. Use a throwaway testnet key.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/payer"
	"apivend.io/apivend/internal/x402"
)

// asWallet: -as, a contract wallet the key owns (EIP-1271).
var asWallet string

func main() {
	method := flag.String("X", "GET", "HTTP method")
	flag.StringVar(&asWallet, "as", "", "pay from this contract wallet (EIP-1271), signing with X402_PRIVATE_KEY as its owner")
	host := flag.String("host", "", "override the Host header")
	var headers headerFlags
	flag.Var(&headers, "H", "extra request header \"Name: value\" (repeatable); X402_HEADERS adds one per line, for values that should not appear in the process list")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: X402_PRIVATE_KEY=0x... x402-pay [-X METHOD] [-host HOST] [-H 'Name: value'] URL")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	for _, line := range strings.Split(os.Getenv("X402_HEADERS"), "\n") {
		if strings.TrimSpace(line) != "" {
			if err := headers.Set(line); err != nil {
				fmt.Fprintln(os.Stderr, "x402-pay: X402_HEADERS:", err)
				os.Exit(2)
			}
		}
	}
	if err := run(*method, flag.Arg(0), *host, headers); err != nil {
		fmt.Fprintln(os.Stderr, "x402-pay:", err)
		os.Exit(1)
	}
}

// headerFlags collects -H "Name: value" flags, e.g. a Cloudflare Access
// service token for a gated route.
type headerFlags [][2]string

func (h *headerFlags) String() string { return fmt.Sprint(len(*h), " headers") }

func (h *headerFlags) Set(v string) error {
	name, value, ok := strings.Cut(v, ":")
	name = strings.TrimSpace(name)
	if !ok || name == "" {
		return fmt.Errorf("header %q: want \"Name: value\"", name)
	}
	*h = append(*h, [2]string{name, strings.TrimSpace(value)})
	return nil
}

func run(method, url, host string, headers headerFlags) error {
	client := &http.Client{Timeout: 30 * time.Second}
	do := func(sig string) (*http.Response, []byte, error) {
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			return nil, nil, err
		}
		if host != "" {
			req.Host = host
		}
		for _, h := range headers {
			req.Header.Add(h[0], h[1])
		}
		if sig != "" {
			req.Header.Set("PAYMENT-SIGNATURE", sig)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, nil, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return resp, body, err
	}

	resp, body, err := do("")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusPaymentRequired {
		fmt.Fprintf(os.Stderr, "< %s (no payment requested)\n", resp.Status)
		os.Stdout.Write(body)
		return nil
	}

	var pr x402.PaymentRequired
	if err := x402.DecodeHeader(resp.Header.Get("PAYMENT-REQUIRED"), &pr); err != nil {
		return fmt.Errorf("402 without a usable PAYMENT-REQUIRED header: %w", err)
	}
	fmt.Fprintf(os.Stderr, "< 402 Payment Required: %s\n", compact(pr.Accepts))

	key := os.Getenv("X402_PRIVATE_KEY")
	if key == "" {
		return fmt.Errorf("payment required; set X402_PRIVATE_KEY to pay")
	}
	w, err := payer.NewWallet(key)
	if err != nil {
		return err
	}
	if asWallet != "" {
		a, err := evm.ParseAddress(asWallet)
		if err != nil {
			return fmt.Errorf("-as: %w", err)
		}
		w.As = &a
	}
	payment, err := w.Pay(&pr, time.Now())
	if err != nil {
		return err
	}
	sig, err := x402.EncodeHeader(payment)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "> paying %s atomic units on %s from %s\n", payment.Accepted.Amount, payment.Accepted.Network, w.Payer())

	resp, body, err = do(sig)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "< %s\n", resp.Status)
	if h := resp.Header.Get("PAYMENT-RESPONSE"); h != "" {
		var sr x402.SettlementResponse
		if err := x402.DecodeHeader(h, &sr); err == nil {
			fmt.Fprintf(os.Stderr, "< PAYMENT-RESPONSE: %s\n", compact(sr))
		}
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		var again x402.PaymentRequired
		if x402.DecodeHeader(resp.Header.Get("PAYMENT-REQUIRED"), &again) == nil {
			return fmt.Errorf("payment refused: %s", again.Error)
		}
	}
	os.Stdout.Write(body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("request failed: %s", resp.Status)
	}
	return nil
}

func compact(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
