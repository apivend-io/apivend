package verify

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"time"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/x402"
)

// maxUint128 bounds voucher amounts (uint128 on chain).
var maxUint128 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

// Voucher is a batch-settlement voucher that passed local verification:
// its channel config matches the offer, hashes to the channel id, and the
// voucher is signed by the channel's payer authorizer (or accepted by the
// payer as a contract wallet). Whether the channel holds enough and the
// cumulative amount is in step is the payer state's job.
type Voucher struct {
	Offer        *Offer
	Config       evm.ChannelConfig
	ChannelID    [32]byte
	MaxClaimable *big.Int
	Signature    []byte
	// Contract: the voucher was accepted by the payer as a contract wallet.
	Contract bool
	// Deposit is set on deposit payloads: the payer's signed transfer into
	// the channel, to execute before the request is served.
	Deposit *Deposit
	// Refund is set on refund payloads: the voucher is zero-charge (its
	// ceiling is the charged total) and RefundAmount, when set, asks for a
	// partial refund; nil means everything left.
	Refund       bool
	RefundAmount *big.Int
}

// Deposit is a verified ERC-3009 receiveWithAuthorization from the payer to
// the deposit collector, for this channel.
type Deposit struct {
	Amount      *big.Int
	Auth        evm.Authorization
	ValidBefore time.Time
}

// depositWire is BatchEVMPayload.Deposit.
type depositWire struct {
	Amount        string `json:"amount"`
	Authorization struct {
		ERC3009 *struct {
			ValidAfter  string `json:"validAfter"`
			ValidBefore string `json:"validBefore"`
			Salt        string `json:"salt"`
			Signature   string `json:"signature"`
		} `json:"erc3009Authorization"`
		Permit2 json.RawMessage `json:"permit2Authorization"`
	} `json:"authorization"`
}

// VerifyBatch checks a batch-settlement payload against the route's offers
// (scheme_batch_settlement_evm.md, verification rules 1-6). Only voucher
// payloads are served for now; deposits and refunds are refused.
func VerifyBatch(ctx context.Context, p *x402.PaymentPayload, offers []Offer, now time.Time, opt Options) (*Voucher, *Error) {
	if p.X402Version != x402.Version {
		return nil, malformed(x402.ErrInvalidVersion, "only x402Version 2 is supported")
	}
	offer, verr := selectOffer(&p.Accepted, offers)
	if verr != nil {
		return nil, verr
	}
	if offer.Batch == nil {
		return nil, refused(x402.ErrInvalidScheme, "offer is not batch-settlement")
	}
	var bp x402.BatchEVMPayload
	if len(p.Payload) == 0 || json.Unmarshal(p.Payload, &bp) != nil {
		return nil, malformed(x402.ErrBatchVoucherPayload, "payload is not a batch-settlement object")
	}
	switch bp.Type {
	case "voucher", "deposit", "refund":
	default:
		return nil, malformed(x402.ErrBatchPayloadType, "payload type is not deposit, voucher or refund")
	}

	cc := bp.ChannelConfig
	var (
		cfg evm.ChannelConfig
		err error
	)
	for _, f := range []struct {
		dst *evm.Address
		src string
	}{{&cfg.Payer, cc.Payer}, {&cfg.PayerAuthorizer, cc.PayerAuthorizer}, {&cfg.Receiver, cc.Receiver},
		{&cfg.ReceiverAuthorizer, cc.ReceiverAuthorizer}, {&cfg.Token, cc.Token}} {
		if *f.dst, err = evm.ParseAddress(f.src); err != nil {
			return nil, malformed(x402.ErrBatchVoucherPayload, "channelConfig has an address that is not one")
		}
	}
	if cfg.Salt, err = evm.ParseBytes32(cc.Salt); err != nil {
		return nil, malformed(x402.ErrBatchVoucherPayload, "channelConfig.salt is not 32 bytes")
	}
	cfg.WithdrawDelay = cc.WithdrawDelay
	if cfg.Payer == (evm.Address{}) {
		return nil, malformed(x402.ErrBatchVoucherPayload, "channelConfig.payer is the zero address")
	}

	// Rules 2-5: the channel pays this route's seller, in this token, under
	// our receiver authorizer and withdrawal delay.
	terms := offer.Batch
	switch {
	case cfg.Token != offer.Domain.VerifyingContract:
		return nil, refused(x402.ErrBatchTokenMismatch, "channel token is not the route's asset")
	case cfg.Receiver != offer.PayTo:
		return nil, refused(x402.ErrBatchReceiverMismatch, "channel receiver is not the route's payTo")
	case cfg.Payer == offer.PayTo:
		return nil, refused(x402.ErrSelfSend, "the channel's payer is the route's payTo: a payment to yourself cannot settle; pay from another wallet")
	case cfg.ReceiverAuthorizer != terms.ReceiverAuthorizer:
		return nil, refused(x402.ErrBatchAuthorizerMismatch, "channel receiver authorizer is not ours")
	case cfg.WithdrawDelay != terms.WithdrawDelay:
		return nil, refused(x402.ErrBatchWithdrawDelay, "channel withdraw delay is not the route's")
	}

	// Rule 1: the config hashes to the claimed channel id on this chain.
	chain := offer.Domain.ChainID
	id := cfg.ChannelID(chain)
	claimed, err := evm.ParseBytes32(bp.Voucher.ChannelID)
	if err != nil {
		return nil, malformed(x402.ErrBatchVoucherPayload, "voucher.channelId is not 32 bytes")
	}
	if claimed != id {
		return nil, refused(x402.ErrBatchChannelIDMismatch, "channel config does not hash to voucher.channelId")
	}

	maxClaimable, err := evm.ParseUint256(bp.Voucher.MaxClaimableAmount)
	if err != nil || maxClaimable.Cmp(maxUint128) > 0 || (maxClaimable.Sign() == 0 && bp.Type != "refund") {
		return nil, malformed(x402.ErrBatchVoucherPayload, "voucher.maxClaimableAmount is not a positive uint128")
	}
	sig, err := evm.ParseHex(bp.Voucher.Signature, -1)
	if err != nil || len(sig) == 0 || len(sig) > MaxSignatureBytes {
		return nil, refused(x402.ErrBatchVoucherSignature, "voucher signature is not hex of at most 4096 bytes")
	}

	// Rule 6: a non-zero payer authorizer is an EOA (ECDSA only); a zero one
	// means the payer signs, as a key or as a contract wallet (EIP-1271).
	v := &Voucher{Offer: offer, Config: cfg, ChannelID: id, MaxClaimable: maxClaimable, Signature: sig}
	if bp.Type == "refund" {
		v.Refund = true
		if bp.Amount != "" {
			a, err := evm.ParseUint256(bp.Amount)
			if err != nil || a.Sign() == 0 || a.Cmp(maxUint128) > 0 {
				return nil, refused(x402.ErrBatchRefundAmount, "refund amount is not a positive uint128")
			}
			v.RefundAmount = a
		}
	}
	if bp.Type == "deposit" {
		d, verr := verifyDeposit(ctx, bp.Deposit, offer, cfg, id, now, opt)
		if verr != nil {
			return nil, verr
		}
		v.Deposit = d
	}
	digest := evm.VoucherDigest(chain, id, maxClaimable)
	if cfg.PayerAuthorizer != (evm.Address{}) {
		if len(sig) != 65 {
			return nil, refused(x402.ErrBatchVoucherSignature, "voucher signature is not 65 bytes")
		}
		if signer, err := evm.RecoverSigner(digest, sig); err != nil || signer != cfg.PayerAuthorizer {
			return nil, refused(x402.ErrBatchVoucherSignature, "voucher signature does not recover to the payer authorizer")
		}
		return v, nil
	}
	if len(sig) == 65 {
		if signer, err := evm.RecoverSigner(digest, sig); err == nil && signer == cfg.Payer {
			return v, nil
		}
	}
	if opt.Contract == nil || evm.IsERC6492(sig) {
		return nil, refused(x402.ErrBatchVoucherSignature, "voucher signature does not recover to the payer")
	}
	ok, err := opt.Contract.IsValidSignature(ctx, offer.Wire.Network, cfg.Payer, digest, sig)
	if err != nil {
		return nil, &Error{Reason: x402.ErrUnexpectedVerify, Message: "contract wallet voucher could not be checked: " + err.Error(), Unavailable: true}
	}
	if !ok {
		return nil, refused(x402.ErrBatchVoucherSignature, "voucher signature is neither the payer's key nor accepted by the payer as a contract wallet")
	}
	v.Contract = true
	return v, nil
}

// verifyDeposit checks the deposit's ERC-3009 authorization: from the payer
// to the deposit collector, the nonce bound to this channel, valid long
// enough to execute, signed by the payer (or accepted by it as a contract
// wallet). Whether the payer holds the amount is the payer state's job.
func verifyDeposit(ctx context.Context, raw json.RawMessage, offer *Offer, cfg evm.ChannelConfig, id [32]byte, now time.Time, opt Options) (*Deposit, *Error) {
	var dw depositWire
	if len(raw) == 0 || json.Unmarshal(raw, &dw) != nil {
		return nil, malformed(x402.ErrBatchDepositPayload, "deposit is not an object")
	}
	e := dw.Authorization.ERC3009
	if e == nil {
		if len(dw.Authorization.Permit2) > 0 {
			return nil, refused(x402.ErrBatchDepositPayload, "only erc3009Authorization deposits are accepted")
		}
		return nil, malformed(x402.ErrBatchERC3009Required, "deposit has no erc3009Authorization")
	}
	amount, err := evm.ParseUint256(dw.Amount)
	if err != nil || amount.Sign() == 0 || amount.Cmp(maxUint128) > 0 {
		return nil, malformed(x402.ErrBatchDepositPayload, "deposit.amount is not a positive uint128")
	}
	auth := evm.Authorization{From: cfg.Payer, To: evm.ERC3009DepositCollector, Value: amount}
	if auth.ValidAfter, err = evm.ParseUint256(e.ValidAfter); err != nil {
		return nil, malformed(x402.ErrBatchDepositPayload, "validAfter is not a uint256")
	}
	if auth.ValidBefore, err = evm.ParseUint256(e.ValidBefore); err != nil {
		return nil, malformed(x402.ErrBatchDepositPayload, "validBefore is not a uint256")
	}
	salt, err := parseSalt(e.Salt)
	if err != nil {
		return nil, malformed(x402.ErrBatchDepositPayload, "salt is not a uint256")
	}
	sig, err := evm.ParseHex(e.Signature, -1)
	if err != nil || len(sig) == 0 || len(sig) > MaxSignatureBytes {
		return nil, refused(x402.ErrBatchReceiveSignature, "deposit signature is not hex of at most 4096 bytes")
	}
	if auth.ValidAfter.Cmp(big.NewInt(now.Unix())) > 0 {
		return nil, refused(x402.ErrBatchDepositValidAfter, "deposit authorization is not valid yet")
	}
	if auth.ValidBefore.Cmp(big.NewInt(now.Add(opt.MinRemaining).Unix())) <= 0 {
		return nil, refused(x402.ErrBatchDepositValidBefore, "deposit authorization expired or expires too soon to execute")
	}
	auth.Nonce = evm.DepositNonce(id, salt)
	digest := auth.ReceiveDigest(offer.Domain)
	if len(sig) == 65 {
		if signer, err := evm.RecoverSigner(digest, sig); err == nil && signer == cfg.Payer {
			goto signed
		}
	}
	if opt.Contract == nil || evm.IsERC6492(sig) {
		return nil, refused(x402.ErrBatchReceiveSignature, "deposit signature does not recover to the payer")
	}
	if ok, err := opt.Contract.IsValidSignature(ctx, offer.Wire.Network, cfg.Payer, digest, sig); err != nil {
		return nil, &Error{Reason: x402.ErrUnexpectedVerify, Message: "contract wallet deposit could not be checked: " + err.Error(), Unavailable: true}
	} else if !ok {
		return nil, refused(x402.ErrBatchReceiveSignature, "deposit signature is neither the payer's key nor accepted by the payer as a contract wallet")
	}
signed:
	vb := auth.ValidBefore
	if !vb.IsInt64() {
		vb = big.NewInt(1 << 62)
	}
	return &Deposit{Amount: amount, Auth: auth, ValidBefore: time.Unix(vb.Int64(), 0)}, nil
}

// parseSalt reads the deposit salt: 0x-hex (as the SDKs send it) or decimal,
// at most 32 bytes.
func parseSalt(s string) (*big.Int, error) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		n, ok := new(big.Int).SetString(s[2:], 16)
		if !ok || n.BitLen() > 256 || len(s) > 66 {
			return nil, errors.New("salt is not 32-byte hex")
		}
		return n, nil
	}
	return evm.ParseUint256(s)
}
