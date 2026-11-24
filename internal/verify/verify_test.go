package verify_test

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/payer"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/verify"
	"apivend.io/apivend/internal/x402"
)

const (
	payerKey = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
	otherKey = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"
	payTo    = "0x209693Bc6afc0C5328bA36FaF03C514EF312287C"
)

var now = time.Unix(1_800_000_000, 0)

func offers(t *testing.T) []verify.Offer {
	t.Helper()
	table, err := pricing.Build(pricing.Config{Routes: []pricing.RouteConfig{{
		Name:    "search",
		Match:   pricing.MatchConfig{PathPrefix: "/search"},
		Price:   pricing.PriceConfig{Amount: "0.01", Currency: "USD"},
		Accepts: []pricing.AcceptConfig{{Network: "base-sepolia", Asset: "USDC", PayTo: payTo}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return table.Match("", "/search", "").Offers
}

// signed returns a valid payment after letting mutate adjust what gets signed.
func signed(t *testing.T, key string, mutate func(*evm.Authorization)) *x402.PaymentPayload {
	t.Helper()
	w, err := payer.NewWallet(key)
	if err != nil {
		t.Fatal(err)
	}
	o := offers(t)[0]
	auth := evm.Authorization{
		From:        w.Addr,
		To:          o.PayTo,
		Value:       new(big.Int).Set(o.Amount),
		ValidAfter:  big.NewInt(now.Unix() - 60),
		ValidBefore: big.NewInt(now.Unix() + 60),
		Nonce:       [32]byte{1, 2, 3},
	}
	if mutate != nil {
		mutate(&auth)
	}
	p, err := w.Sign(o.Wire, x402.ResourceInfo{URL: "https://api.example.com/search"}, auth, o.Domain)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// editPayload rewrites the scheme payload after signing.
func editPayload(t *testing.T, p *x402.PaymentPayload, edit func(*x402.ExactEVMPayload)) {
	t.Helper()
	var ex x402.ExactEVMPayload
	if err := json.Unmarshal(p.Payload, &ex); err != nil {
		t.Fatal(err)
	}
	edit(&ex)
	p.Payload, _ = json.Marshal(ex)
}

func TestVerifyAccepts(t *testing.T) {
	p := signed(t, payerKey, nil)
	got, verr := verify.Verify(context.Background(), p, offers(t), now, verify.Options{MinRemaining: 10 * time.Second})
	if verr != nil {
		t.Fatal(verr)
	}
	w, _ := payer.NewWallet(payerKey)
	if got.Payer != w.Addr {
		t.Fatalf("payer = %s", got.Payer)
	}
	if got.ValidBefore.Unix() != now.Unix()+60 {
		t.Fatalf("validBefore = %v", got.ValidBefore)
	}

	// Lowercase addresses in the echoed terms are fine.
	p.Accepted.Asset = strings.ToLower(p.Accepted.Asset)
	p.Accepted.PayTo = strings.ToLower(p.Accepted.PayTo)
	if _, verr := verify.Verify(context.Background(), p, offers(t), now, verify.Options{}); verr != nil {
		t.Fatalf("lowercase terms rejected: %v", verr)
	}
}

func TestVerifyRejects(t *testing.T) {
	stranger, _ := payer.NewWallet(otherKey)

	cases := []struct {
		name      string
		payload   func() *x402.PaymentPayload
		reason    string
		malformed bool
	}{
		{"wrong version", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			p.X402Version = 1
			return p
		}, x402.ErrInvalidVersion, true},
		{"wrong scheme", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			p.Accepted.Scheme = "upto"
			return p
		}, x402.ErrInvalidScheme, false},
		{"permit2", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			p.Accepted.Extra = map[string]any{"assetTransferMethod": "permit2"}
			return p
		}, x402.ErrInvalidScheme, false},
		{"wrong network", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			p.Accepted.Network = "eip155:1"
			return p
		}, x402.ErrInvalidNetwork, false},
		{"cheaper terms echoed", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			p.Accepted.Amount = "1"
			return p
		}, x402.ErrInvalidRequirements, false},
		{"underpaid", func() *x402.PaymentPayload {
			return signed(t, payerKey, func(a *evm.Authorization) { a.Value = big.NewInt(9999) })
		}, x402.ErrValueMismatch, false},
		{"overpaid", func() *x402.PaymentPayload {
			return signed(t, payerKey, func(a *evm.Authorization) { a.Value = big.NewInt(10001) })
		}, x402.ErrValueMismatch, false},
		{"paid to someone else", func() *x402.PaymentPayload {
			return signed(t, payerKey, func(a *evm.Authorization) { a.To = stranger.Addr })
		}, x402.ErrRecipientMismatch, false},
		{"paid by the seller to itself", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			editPayload(t, p, func(ex *x402.ExactEVMPayload) { ex.Authorization.From = payTo })
			return p
		}, x402.ErrSelfSend, false},
		{"not yet valid", func() *x402.PaymentPayload {
			return signed(t, payerKey, func(a *evm.Authorization) { a.ValidAfter = big.NewInt(now.Unix() + 5) })
		}, x402.ErrValidAfter, false},
		{"expired", func() *x402.PaymentPayload {
			return signed(t, payerKey, func(a *evm.Authorization) { a.ValidBefore = big.NewInt(now.Unix() - 1) })
		}, x402.ErrValidBefore, false},
		{"expires too soon to settle", func() *x402.PaymentPayload {
			return signed(t, payerKey, func(a *evm.Authorization) { a.ValidBefore = big.NewInt(now.Unix() + 5) })
		}, x402.ErrValidBefore, false},
		{"signed by someone else", func() *x402.PaymentPayload {
			p := signed(t, otherKey, nil)
			w, _ := payer.NewWallet(payerKey)
			editPayload(t, p, func(ex *x402.ExactEVMPayload) { ex.Authorization.From = w.Addr.String() })
			return p
		}, x402.ErrSignature, false},
		{"value raised after signing", func() *x402.PaymentPayload {
			p := signed(t, payerKey, func(a *evm.Authorization) { a.Value = big.NewInt(1) })
			editPayload(t, p, func(ex *x402.ExactEVMPayload) { ex.Authorization.Value = "10000" })
			return p
		}, x402.ErrSignature, false},
		{"signature for another chain's token", func() *x402.PaymentPayload {
			w, _ := payer.NewWallet(payerKey)
			o := offers(t)[0]
			d := o.Domain
			d.ChainID = big.NewInt(8453)
			auth := evm.Authorization{From: w.Addr, To: o.PayTo, Value: o.Amount,
				ValidAfter: big.NewInt(0), ValidBefore: big.NewInt(now.Unix() + 60)}
			p, _ := w.Sign(o.Wire, x402.ResourceInfo{}, auth, d)
			return p
		}, x402.ErrSignature, false},
		{"contract-wallet signature", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			editPayload(t, p, func(ex *x402.ExactEVMPayload) { ex.Signature += "00" })
			return p
		}, x402.ErrSignature, false},
		{"garbage nonce", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			editPayload(t, p, func(ex *x402.ExactEVMPayload) { ex.Authorization.Nonce = "0x01" })
			return p
		}, x402.ErrInvalidPayload, true},
		{"negative value", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			editPayload(t, p, func(ex *x402.ExactEVMPayload) { ex.Authorization.Value = "-10000" })
			return p
		}, x402.ErrInvalidPayload, true},
		{"missing payload", func() *x402.PaymentPayload {
			p := signed(t, payerKey, nil)
			p.Payload = nil
			return p
		}, x402.ErrInvalidPayload, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, verr := verify.Verify(context.Background(), tc.payload(), offers(t), now, verify.Options{MinRemaining: 10 * time.Second})
			if verr == nil {
				t.Fatal("accepted")
			}
			if verr.Reason != tc.reason || verr.Malformed != tc.malformed {
				t.Fatalf("got %q (malformed=%v), want %q (malformed=%v)", verr.Reason, verr.Malformed, tc.reason, tc.malformed)
			}
		})
	}
}
