// Package payer is the client side of x402: it turns a 402 challenge into a
// signed payment. The gateway itself never uses it on the request path; it
// backs the x402-pay demo client and the tests.
package payer

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/x402"
)

// Wallet signs EIP-3009 authorizations with one key.
type Wallet struct {
	key  *secp256k1.PrivateKey
	Addr evm.Address
	// As, when set, is a contract wallet (EIP-1271) this key owns: payments
	// come from that address, signed by this key, as a smart-wallet user's
	// would. For tests and the demo client.
	As *evm.Address
}

// Payer is the address payments come from: As when set, else the key's own.
func (w *Wallet) Payer() evm.Address {
	if w.As != nil {
		return *w.As
	}
	return w.Addr
}

// NewWallet parses a 32-byte hex private key, with or without 0x.
func NewWallet(hexKey string) (*Wallet, error) {
	b, err := hex.DecodeString(strings.TrimPrefix(hexKey, "0x"))
	if err != nil || len(b) != 32 {
		return nil, errors.New("private key must be 32 hex bytes")
	}
	key := secp256k1.PrivKeyFromBytes(b)
	return &Wallet{key: key, Addr: evm.PubkeyAddress(key.PubKey())}, nil
}

// Pay signs a payment for the first offer in the challenge this wallet can
// satisfy (scheme exact, EIP-3009, an eip155 network).
func (w *Wallet) Pay(pr *x402.PaymentRequired, now time.Time) (*x402.PaymentPayload, error) {
	for _, req := range pr.Accepts {
		p, err := w.PayOffer(req, pr.Resource, now)
		if err == nil {
			// Clients echo the challenge's extensions (specification v2
			// section 5.1); the bazaar extension, for one, is read from
			// the payment at settlement.
			if len(pr.Extensions) > 0 {
				if p.Extensions, err = json.Marshal(pr.Extensions); err != nil {
					return nil, err
				}
			}
			return p, nil
		}
	}
	return nil, errors.New("no payable offer in challenge")
}

// PayOffer signs a payment for one specific offer.
func (w *Wallet) PayOffer(req x402.Requirements, res x402.ResourceInfo, now time.Time) (*x402.PaymentPayload, error) {
	if req.Scheme != x402.SchemeExact {
		return nil, fmt.Errorf("unsupported scheme %q", req.Scheme)
	}
	if m, ok := req.Extra["assetTransferMethod"]; ok && m != "eip3009" {
		return nil, errors.New("unsupported asset transfer method")
	}
	chain, ok := strings.CutPrefix(req.Network, "eip155:")
	if !ok {
		return nil, fmt.Errorf("unsupported network %q", req.Network)
	}
	chainID, err := evm.ParseUint256(chain)
	if err != nil {
		return nil, fmt.Errorf("network %q: %w", req.Network, err)
	}
	asset, err := evm.ParseAddress(req.Asset)
	if err != nil {
		return nil, fmt.Errorf("asset: %w", err)
	}
	payTo, err := evm.ParseAddress(req.PayTo)
	if err != nil {
		return nil, fmt.Errorf("payTo: %w", err)
	}
	amount, err := evm.ParseUint256(req.Amount)
	if err != nil {
		return nil, fmt.Errorf("amount: %w", err)
	}
	name, _ := req.Extra["name"].(string)
	version, _ := req.Extra["version"].(string)
	if name == "" || version == "" {
		return nil, errors.New("offer lacks extra.name / extra.version")
	}

	auth := evm.Authorization{
		From:  w.Payer(),
		To:    payTo,
		Value: amount,
		// Backdated to tolerate clock skew between client and gateway.
		ValidAfter:  big.NewInt(now.Add(-10 * time.Minute).Unix()),
		ValidBefore: big.NewInt(now.Add(time.Duration(req.MaxTimeoutSeconds) * time.Second).Unix()),
	}
	if _, err := rand.Read(auth.Nonce[:]); err != nil {
		return nil, err
	}
	return w.Sign(req, res, auth, evm.Domain{Name: name, Version: version, ChainID: chainID, VerifyingContract: asset})
}

// Sign wraps an explicit authorization into a payment payload.
func (w *Wallet) Sign(req x402.Requirements, res x402.ResourceInfo, auth evm.Authorization, d evm.Domain) (*x402.PaymentPayload, error) {
	sig := evm.Sign(auth.Digest(d), w.key)

	var ex x402.ExactEVMPayload
	ex.Signature = "0x" + hex.EncodeToString(sig)
	ex.Authorization.From = auth.From.String()
	ex.Authorization.To = auth.To.String()
	ex.Authorization.Value = auth.Value.String()
	ex.Authorization.ValidAfter = strconv.FormatInt(auth.ValidAfter.Int64(), 10)
	ex.Authorization.ValidBefore = strconv.FormatInt(auth.ValidBefore.Int64(), 10)
	ex.Authorization.Nonce = "0x" + hex.EncodeToString(auth.Nonce[:])
	raw, err := json.Marshal(ex)
	if err != nil {
		return nil, err
	}
	return &x402.PaymentPayload{X402Version: x402.Version, Resource: &res, Accepted: req, Payload: raw}, nil
}
