// Package verify checks an x402 payment payload.
//
// It covers everything that can be decided from the payload and the route's
// terms: structure, terms, validity window and signature. An ordinary (EOA)
// signature is checked locally with no network call. Only when the signature
// does not recover to the payer, or is not 65 bytes, is the payer asked as a
// contract wallet (EIP-1271), through Options.Contract. Whether the payer can
// actually cover the amount, and whether the nonce is unused, is the
// payer-state engine's job.
package verify

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"time"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/x402"
)

// Offer is one entry of a route's accepts list, resolved to typed values.
type Offer struct {
	// Wire is exactly what the 402 advertises for this offer.
	Wire x402.Requirements

	Amount *big.Int
	PayTo  evm.Address
	Domain evm.Domain // token contract's EIP-712 domain; VerifyingContract is the asset
	// MaxUnsettled caps a payer's unsettled exposure on this asset (atomic
	// units); nil means no cap.
	MaxUnsettled *big.Int
	// Batch is set on batch-settlement offers.
	Batch *BatchTerms
}

// BatchTerms are a batch-settlement offer's channel terms.
type BatchTerms struct {
	ReceiverAuthorizer evm.Address
	WithdrawDelay      uint64
	MinDeposit         *big.Int
}

// Payment is a payload that passed local verification.
type Payment struct {
	Offer       *Offer
	Payer       evm.Address
	Auth        evm.Authorization
	Signature   []byte
	ValidBefore time.Time
	// Contract is true when the payer is a contract wallet that accepted the
	// signature (EIP-1271), rather than a key that made it.
	Contract bool
}

// Error explains a rejected payload.
type Error struct {
	Reason  string // x402 error code
	Message string // operator-facing detail; never echo client bytes here
	// Malformed means the payload could not be understood at all (HTTP 400)
	// as opposed to understood and refused (HTTP 402).
	Malformed bool
	// Unavailable means the signature could not be checked (the chain did
	// not answer): fail closed, HTTP 503, not the payer's fault.
	Unavailable bool
}

func (e *Error) Error() string { return e.Reason + ": " + e.Message }

func malformed(reason, msg string) *Error {
	return &Error{Reason: reason, Message: msg, Malformed: true}
}
func refused(reason, msg string) *Error { return &Error{Reason: reason, Message: msg} }

// Options tunes verification.
type Options struct {
	// MinRemaining is how much life the authorization must have left. It has
	// to cover the backend call plus settlement, or the seller serves a
	// request it can no longer charge for.
	MinRemaining time.Duration
	// Contract, when set, checks signatures that are not the payer's own key
	// against the payer as a contract wallet. Nil: EOA signatures only.
	Contract ContractChecker
}

// ContractChecker asks a contract wallet whether it accepts a signature
// (EIP-1271 isValidSignature). valid is false for an account that is not a
// contract or that refuses; err is for a chain that could not be asked.
type ContractChecker interface {
	IsValidSignature(ctx context.Context, network string, account evm.Address, digest [32]byte, sig []byte) (valid bool, err error)
}

// MaxSignatureBytes bounds a contract-wallet signature: a Coinbase Smart
// Wallet passkey signature is about 640 bytes, about 1.1 KB wrapped for an
// undeployed wallet (ERC-6492); room is left for wallets with more owners.
const MaxSignatureBytes = 4096

// CounterfactualChecker is a ContractChecker that can also check a wallet
// that is not deployed yet (ERC-6492).
type CounterfactualChecker interface {
	VerifyCounterfactual(ctx context.Context, c Counterfactual) (valid bool, err error)
}

// Counterfactual is an authorization signed by a wallet that may not exist
// on chain yet, with what it takes to deploy it.
type Counterfactual struct {
	Network     string
	Token       evm.Address
	Auth        evm.Authorization
	Digest      [32]byte
	Factory     evm.Address
	FactoryData []byte
	Signature   []byte // the wallet's own signature, unwrapped
}

// Verify checks payload against the offers of the matched route.
func Verify(ctx context.Context, p *x402.PaymentPayload, offers []Offer, now time.Time, opt Options) (*Payment, *Error) {
	if p.X402Version != x402.Version {
		return nil, malformed(x402.ErrInvalidVersion, "only x402Version 2 is supported")
	}

	if p.Accepted.Scheme != x402.SchemeExact {
		return nil, refused(x402.ErrInvalidScheme, "not an exact payment")
	}
	offer, verr := selectOffer(&p.Accepted, offers)
	if verr != nil {
		return nil, verr
	}

	var ex x402.ExactEVMPayload
	if len(p.Payload) == 0 || json.Unmarshal(p.Payload, &ex) != nil {
		return nil, malformed(x402.ErrInvalidPayload, "payload is not an exact/eip3009 object")
	}

	var (
		auth evm.Authorization
		err  error
	)
	if auth.From, err = evm.ParseAddress(ex.Authorization.From); err != nil {
		return nil, malformed(x402.ErrInvalidPayload, "authorization.from is not an address")
	}
	if auth.To, err = evm.ParseAddress(ex.Authorization.To); err != nil {
		return nil, malformed(x402.ErrInvalidPayload, "authorization.to is not an address")
	}
	if auth.Value, err = evm.ParseUint256(ex.Authorization.Value); err != nil {
		return nil, malformed(x402.ErrInvalidPayload, "authorization.value is not a uint256")
	}
	if auth.ValidAfter, err = evm.ParseUint256(ex.Authorization.ValidAfter); err != nil {
		return nil, malformed(x402.ErrInvalidPayload, "authorization.validAfter is not a uint256")
	}
	if auth.ValidBefore, err = evm.ParseUint256(ex.Authorization.ValidBefore); err != nil {
		return nil, malformed(x402.ErrInvalidPayload, "authorization.validBefore is not a uint256")
	}
	if auth.Nonce, err = evm.ParseBytes32(ex.Authorization.Nonce); err != nil {
		return nil, malformed(x402.ErrInvalidPayload, "authorization.nonce is not 32 bytes")
	}
	// Ordinary keys sign 65 bytes; contract wallets may send more.
	sig, err := evm.ParseHex(ex.Signature, -1)
	if err != nil || len(sig) == 0 || len(sig) > MaxSignatureBytes {
		return nil, refused(x402.ErrSignature, "signature is not hex of at most 4096 bytes")
	}

	if auth.To != offer.PayTo {
		return nil, refused(x402.ErrRecipientMismatch, "authorization.to is not the route's payTo")
	}
	if auth.Value.Cmp(offer.Amount) != 0 {
		return nil, refused(x402.ErrValueMismatch, "authorization.value is not the route's amount")
	}

	nowUnix := big.NewInt(now.Unix())
	if auth.ValidAfter.Cmp(nowUnix) > 0 {
		return nil, refused(x402.ErrValidAfter, "authorization is not valid yet")
	}
	deadline := big.NewInt(now.Add(opt.MinRemaining).Unix())
	if auth.ValidBefore.Cmp(deadline) <= 0 {
		return nil, refused(x402.ErrValidBefore, "authorization expired or expires too soon to settle")
	}

	digest := auth.Digest(offer.Domain)
	contract := false
	// ERC-6492 first, as the standard requires: a wallet that may not be
	// deployed yet, with the call that deploys it.
	if evm.IsERC6492(sig) {
		cf, ok := opt.Contract.(CounterfactualChecker)
		if !ok {
			return nil, refused(x402.ErrSignature, "ERC-6492 signatures (wallets not deployed yet) are not supported here")
		}
		factory, data, inner, err := evm.ParseERC6492(sig)
		if err != nil {
			return nil, refused(x402.ErrSignature, "malformed ERC-6492 signature")
		}
		valid, err := cf.VerifyCounterfactual(ctx, Counterfactual{Network: offer.Wire.Network, Token: offer.Domain.VerifyingContract,
			Auth: auth, Digest: digest, Factory: factory, FactoryData: data, Signature: inner})
		if err != nil {
			return nil, &Error{Reason: x402.ErrUnexpectedVerify, Message: "smart wallet signature could not be checked: " + err.Error(), Unavailable: true}
		}
		if !valid {
			return nil, refused(x402.ErrSignature, "ERC-6492 signature not accepted (factory not allowed, or deploying the wallet and transferring would fail)")
		}
		contract = true
		goto signed
	}
	if len(sig) == 65 {
		if signer, err := evm.RecoverSigner(digest, sig); err == nil && signer == auth.From {
			goto signed // the payer's own key: no network call
		}
	}
	// Not the payer's key: the payer may be a contract wallet.
	if opt.Contract == nil {
		return nil, refused(x402.ErrSignature, "signature does not recover to authorization.from")
	}
	if ok, err := opt.Contract.IsValidSignature(ctx, offer.Wire.Network, auth.From, digest, sig); err != nil {
		return nil, &Error{Reason: x402.ErrUnexpectedVerify, Message: "contract wallet signature could not be checked: " + err.Error(), Unavailable: true}
	} else if !ok {
		return nil, refused(x402.ErrSignature, "signature is neither the payer's key nor accepted by the payer as a contract wallet")
	}
	contract = true
signed:

	// Clamp far-future expiries so time.Unix cannot overflow.
	vb := time.Unix(1<<40, 0)
	if auth.ValidBefore.IsInt64() && auth.ValidBefore.Int64() < 1<<40 {
		vb = time.Unix(auth.ValidBefore.Int64(), 0)
	}
	return &Payment{Offer: offer, Payer: auth.From, Auth: auth, Signature: sig, ValidBefore: vb, Contract: contract}, nil
}

// selectOffer finds the route offer the client says it accepted. The client's
// copy is only used to pick; every later check uses the server's own terms.
func selectOffer(acc *x402.Requirements, offers []Offer) (*Offer, *Error) {
	if acc.Scheme != x402.SchemeExact && acc.Scheme != x402.SchemeBatch {
		return nil, refused(x402.ErrInvalidScheme, "scheme is not exact or batch-settlement")
	}
	if m, ok := acc.Extra["assetTransferMethod"]; ok && m != "eip3009" {
		return nil, refused(x402.ErrInvalidScheme, "only assetTransferMethod eip3009 is supported")
	}
	networkSeen := false
	for i := range offers {
		o := &offers[i]
		if acc.Network != o.Wire.Network || acc.Scheme != o.Wire.Scheme {
			continue
		}
		networkSeen = true
		if strings.EqualFold(acc.Asset, o.Wire.Asset) &&
			strings.EqualFold(acc.PayTo, o.Wire.PayTo) &&
			acc.Amount == o.Wire.Amount {
			return o, nil
		}
	}
	if !networkSeen {
		return nil, refused(x402.ErrInvalidNetwork, "network is not accepted on this route")
	}
	return nil, refused(x402.ErrInvalidRequirements, "accepted terms do not match any offer on this route")
}
