// x402-check runs the demo's acceptance checks against a live priced route:
// the challenge is well formed, a payment is served, a replayed payment is
// refused, and concurrent payments beyond the payer's balance are not all
// served. It exits non-zero if any check fails, so it can gate a deploy.
//
// The payer key is read from X402_PRIVATE_KEY. It runs outside the cluster:
// no payer key belongs inside it.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"apivend.io/apivend/internal/payer"
	"apivend.io/apivend/internal/x402"
)

var client = &http.Client{Timeout: 60 * time.Second}

func main() {
	var (
		url         = flag.String("url", "", "priced route to check")
		replay      = flag.Bool("replay", true, "check that a used payment is refused")
		overspend   = flag.Int("overspend", 0, "send this many concurrent payments and check that not all are served")
		expectServe = flag.Int("expect-served", -1, "with -overspend: exactly this many must be served (-1: at least one and fewer than all)")
		pause       = flag.Duration("pause", 2*time.Second, "with -overspend: wait this long first, so the earlier payment has settled and no longer holds funds")
		free        = flag.Bool("expect-free", false, "check instead that the route is NOT priced (after its PaymentPolicy was deleted)")
		wait        = flag.Duration("wait", 0, "keep retrying the first check for this long (policy changes take a moment to propagate)")
	)
	flag.Parse()
	if *url == "" {
		fmt.Fprintln(os.Stderr, "usage: X402_PRIVATE_KEY=0x... x402-check -url URL [-overspend N -expect-served K] | -expect-free")
		os.Exit(2)
	}
	failed := false
	check := func(name string, err error) {
		if err != nil {
			failed = true
			fmt.Printf("FAIL %s: %v\n", name, err)
			return
		}
		fmt.Printf("ok   %s\n", name)
	}
	retry := func(f func() error) error {
		deadline := time.Now().Add(*wait)
		for {
			err := f()
			if err == nil || time.Now().After(deadline) {
				return err
			}
			time.Sleep(2 * time.Second)
		}
	}

	if *free {
		check("route is free", retry(func() error {
			code, _, _, err := get(*url, "")
			if err != nil {
				return err
			}
			if code == http.StatusPaymentRequired {
				return fmt.Errorf("still answers 402")
			}
			return nil
		}))
		exit(failed)
	}

	var terms x402.PaymentRequired
	check("unpaid request gets a decodable 402", retry(func() error {
		code, hdr, _, err := get(*url, "")
		if err != nil {
			return err
		}
		if code != http.StatusPaymentRequired {
			return fmt.Errorf("status %d, want 402", code)
		}
		if err := x402.DecodeHeader(hdr.Get("PAYMENT-REQUIRED"), &terms); err != nil {
			return fmt.Errorf("PAYMENT-REQUIRED: %w", err)
		}
		if terms.X402Version != 2 || len(terms.Accepts) == 0 {
			return fmt.Errorf("challenge has version %d and %d offers", terms.X402Version, len(terms.Accepts))
		}
		return nil
	}))
	if failed {
		exit(true)
	}

	wallet, err := payer.NewWallet(os.Getenv("X402_PRIVATE_KEY"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "X402_PRIVATE_KEY:", err)
		os.Exit(2)
	}
	sign := func() (string, error) {
		p, err := wallet.Pay(&terms, time.Now())
		if err != nil {
			return "", err
		}
		return x402.EncodeHeader(p)
	}

	sig, err := sign()
	check("payment can be signed for the offered terms", err)
	if err != nil {
		exit(true)
	}
	code, hdr, _, err := get(*url, sig)
	check("paid request is served", func() error {
		if err != nil {
			return err
		}
		if code != http.StatusOK {
			return fmt.Errorf("status %d (%s)", code, refusal(hdr))
		}
		var sr x402.SettlementResponse
		if err := x402.DecodeHeader(hdr.Get("PAYMENT-RESPONSE"), &sr); err != nil || !sr.Success {
			return fmt.Errorf("no successful PAYMENT-RESPONSE")
		}
		return nil
	}())

	if *replay {
		// The same payment for another request: a gateway that keeps paid
		// answers for a repeat (redelivery) keys them on the request, so
		// this must be refused whatever the gateway does with a repeat.
		code, hdr, _, err := get(withQuery(*url, "x402check=replay"), sig)
		check("replayed payment is refused as payment_already_used", func() error {
			if err != nil {
				return err
			}
			if code != http.StatusPaymentRequired {
				return fmt.Errorf("status %d, want 402", code)
			}
			if r := refusal(hdr); r != "payment_already_used" {
				return fmt.Errorf("refused as %q", r)
			}
			return nil
		}())
		// The same payment for the same request: refused, or the kept
		// answer again (marked, and never a second charge).
		code, hdr, _, err = get(*url, sig)
		check("repeated payment is refused or redelivered", func() error {
			if err != nil {
				return err
			}
			switch {
			case code == http.StatusPaymentRequired && refusal(hdr) == "payment_already_used":
			case code == http.StatusOK && hdr.Get("x-x402-redelivered") == "true":
			default:
				return fmt.Errorf("status %d (%s)", code, refusal(hdr))
			}
			return nil
		}())
	}

	if *overspend > 0 {
		time.Sleep(*pause)
		sigs := make([]string, *overspend)
		for i := range sigs {
			if sigs[i], err = sign(); err != nil {
				check("sign overspend payments", err)
				exit(true)
			}
		}
		var mu sync.Mutex
		served, insufficient, other := 0, 0, map[string]int{}
		var wg sync.WaitGroup
		for i, s := range sigs {
			wg.Add(1)
			go func(i int, s string) {
				defer wg.Done()
				// Each its own request, so none is a repeat of another.
				code, hdr, _, err := get(withQuery(*url, fmt.Sprintf("x402check=%d", i)), s)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err != nil:
					other[err.Error()]++
				case code == http.StatusOK:
					served++
				case code == http.StatusPaymentRequired && (refusal(hdr) == x402.ErrInsufficientFunds || refusal(hdr) == "exposure_limit"):
					insufficient++
				default:
					other[fmt.Sprintf("%d %s", code, refusal(hdr))]++
				}
			}(i, s)
		}
		wg.Wait()
		check(fmt.Sprintf("%d concurrent payments: only the affordable number is served", *overspend), func() error {
			if len(other) > 0 {
				return fmt.Errorf("unexpected outcomes: %v", other)
			}
			if *expectServe >= 0 && served != *expectServe {
				return fmt.Errorf("%d served, %d refused; want exactly %d served", served, insufficient, *expectServe)
			}
			if *expectServe < 0 && (served == 0 || served == *overspend) {
				return fmt.Errorf("%d of %d served; the payer's balance should cover some but not all", served, *overspend)
			}
			return nil
		}())
		fmt.Printf("     served %d, refused %d\n", served, insufficient)
	}
	exit(failed)
}

// withQuery adds a query parameter to u.
func withQuery(u, kv string) string {
	if strings.Contains(u, "?") {
		return u + "&" + kv
	}
	return u + "?" + kv
}

func exit(failed bool) {
	if failed {
		os.Exit(1)
	}
	os.Exit(0)
}

func get(url, sig string) (int, http.Header, []byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	if sig != "" {
		req.Header.Set("PAYMENT-SIGNATURE", sig)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, resp.Header, body, nil
}

// refusal extracts why a 402 was returned.
func refusal(h http.Header) string {
	var pr x402.PaymentRequired
	if x402.DecodeHeader(h.Get("PAYMENT-REQUIRED"), &pr) == nil {
		return pr.Error
	}
	var sr x402.SettlementResponse
	if x402.DecodeHeader(h.Get("PAYMENT-RESPONSE"), &sr) == nil {
		return sr.ErrorReason
	}
	return ""
}
