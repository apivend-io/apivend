package verify_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/verify"
	"apivend.io/apivend/internal/x402"
)

const receiverAuth = "0x01d4000000000000000000000000000000006aE6"

func batchOffers(t *testing.T) []verify.Offer {
	t.Helper()
	table, err := pricing.Build(pricing.Config{
		Batch: &pricing.BatchConfig{ReceiverAuthorizer: receiverAuth, WithdrawDelay: 86400},
		Routes: []pricing.RouteConfig{{
			Name:    "search",
			Match:   pricing.MatchConfig{PathPrefix: "/search"},
			Price:   pricing.PriceConfig{Amount: "0.001", Currency: "USD"},
			Accepts: []pricing.AcceptConfig{{Network: "base-sepolia", Asset: "USDC", PayTo: payTo, Schemes: []string{"exact", "batch-settlement"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return table.Match("", "/search", "").Offers
}

func key(t *testing.T, k string) (*secp256k1.PrivateKey, evm.Address) {
	b, err := hex.DecodeString(strings.TrimPrefix(k, "0x"))
	if err != nil {
		t.Fatal(err)
	}
	priv := secp256k1.PrivKeyFromBytes(b)
	return priv, evm.PubkeyAddress(priv.PubKey())
}

// voucherPayload builds a voucher payload for maxClaimable, signed by
// signer, after mutate adjusts the channel config.
func voucherPayload(t *testing.T, o verify.Offer, signer string, maxClaimable int64, mutate func(*x402.BatchChannelConfig)) *x402.PaymentPayload {
	t.Helper()
	priv, addr := key(t, signer)
	cc := x402.BatchChannelConfig{
		Payer: addr.String(), PayerAuthorizer: addr.String(), Receiver: o.Wire.PayTo, ReceiverAuthorizer: receiverAuth,
		Token: o.Wire.Asset, WithdrawDelay: 86400, Salt: "0x" + strings.Repeat("00", 31) + "01",
	}
	if mutate != nil {
		mutate(&cc)
	}
	cfg := evm.ChannelConfig{WithdrawDelay: cc.WithdrawDelay}
	cfg.Payer, _ = evm.ParseAddress(cc.Payer)
	cfg.PayerAuthorizer, _ = evm.ParseAddress(cc.PayerAuthorizer)
	cfg.Receiver, _ = evm.ParseAddress(cc.Receiver)
	cfg.ReceiverAuthorizer, _ = evm.ParseAddress(cc.ReceiverAuthorizer)
	cfg.Token, _ = evm.ParseAddress(cc.Token)
	cfg.Salt, _ = evm.ParseBytes32(cc.Salt)
	id := cfg.ChannelID(o.Domain.ChainID)
	sig := evm.Sign(evm.VoucherDigest(o.Domain.ChainID, id, big.NewInt(maxClaimable)), priv)
	raw, _ := json.Marshal(x402.BatchEVMPayload{Type: "voucher", ChannelConfig: cc, Voucher: x402.BatchVoucher{
		ChannelID: "0x" + hex.EncodeToString(id[:]), MaxClaimableAmount: big.NewInt(maxClaimable).String(), Signature: "0x" + hex.EncodeToString(sig),
	}})
	return &x402.PaymentPayload{X402Version: 2, Accepted: o.Wire, Payload: raw}
}

func TestBatchOfferOnTheWire(t *testing.T) {
	offers := batchOffers(t)
	if len(offers) != 2 || offers[0].Wire.Scheme != "exact" || offers[1].Wire.Scheme != "batch-settlement" {
		t.Fatalf("offers %+v", offers)
	}
	ex := offers[1].Wire.Extra
	if ex["receiverAuthorizer"] != "0x01D4000000000000000000000000000000006Ae6" && !strings.EqualFold(ex["receiverAuthorizer"].(string), receiverAuth) {
		t.Errorf("receiverAuthorizer %v", ex["receiverAuthorizer"])
	}
	if ex["withdrawDelay"] != uint64(86400) || ex["minDeposit"] != "100000" || ex["name"] != "USDC" {
		t.Errorf("extra %v", ex)
	}
	// Batch offers need the gateway's terms.
	_, err := pricing.Build(pricing.Config{Routes: []pricing.RouteConfig{{
		Name: "x", Price: pricing.PriceConfig{Amount: "0.001", Currency: "USD"},
		Accepts: []pricing.AcceptConfig{{Network: "base-sepolia", Asset: "USDC", PayTo: payTo, Schemes: []string{"batch-settlement"}}},
	}}})
	if err == nil {
		t.Error("batch offer without receiver authorizer accepted")
	}
}

func TestVerifyVoucher(t *testing.T) {
	o := batchOffers(t)[1]
	v, verr := verify.VerifyBatch(context.Background(), voucherPayload(t, o, payerKey, 3000, nil), batchOffers(t), now, verify.Options{})
	if verr != nil {
		t.Fatal(verr)
	}
	if v.MaxClaimable.Int64() != 3000 || v.Config.Receiver != o.PayTo || v.Contract {
		t.Fatalf("%+v", v)
	}
	// An exact payload is not a batch one, and the other way round.
	if _, verr := verify.Verify(context.Background(), voucherPayload(t, o, payerKey, 3000, nil), batchOffers(t), now, verify.Options{}); verr == nil {
		t.Error("exact verifier accepted a voucher")
	}

	other := func(c *x402.BatchChannelConfig) { c.Receiver = "0x0000000000000000000000000000000000000bad" }
	cases := map[string]struct {
		p    *x402.PaymentPayload
		want string
	}{
		"receiver":     {voucherPayload(t, o, payerKey, 3000, other), x402.ErrBatchReceiverMismatch},
		"self send":    {voucherPayload(t, o, payerKey, 3000, func(c *x402.BatchChannelConfig) { c.Payer = payTo }), x402.ErrSelfSend},
		"authorizer":   {voucherPayload(t, o, payerKey, 3000, func(c *x402.BatchChannelConfig) { c.ReceiverAuthorizer = payTo }), x402.ErrBatchAuthorizerMismatch},
		"token":        {voucherPayload(t, o, payerKey, 3000, func(c *x402.BatchChannelConfig) { c.Token = payTo }), x402.ErrBatchTokenMismatch},
		"delay":        {voucherPayload(t, o, payerKey, 3000, func(c *x402.BatchChannelConfig) { c.WithdrawDelay = 900 }), x402.ErrBatchWithdrawDelay},
		"wrong signer": {voucherPayload(t, o, otherKey, 3000, func(c *x402.BatchChannelConfig) { _, a := key(t, payerKey); c.PayerAuthorizer = a.String() }), x402.ErrBatchVoucherSignature},
	}
	for name, c := range cases {
		_, verr := verify.VerifyBatch(context.Background(), c.p, batchOffers(t), now, verify.Options{})
		if verr == nil || verr.Reason != c.want {
			t.Errorf("%s: got %v, want %s", name, verr, c.want)
		}
	}

	// A channel id that is not the config's hash.
	p := voucherPayload(t, o, payerKey, 3000, nil)
	var bp x402.BatchEVMPayload
	json.Unmarshal(p.Payload, &bp)
	bp.Voucher.ChannelID = "0x" + strings.Repeat("11", 32)
	p.Payload, _ = json.Marshal(bp)
	if _, verr := verify.VerifyBatch(context.Background(), p, batchOffers(t), now, verify.Options{}); verr == nil || verr.Reason != x402.ErrBatchChannelIDMismatch {
		t.Errorf("channel id: %v", verr)
	}
	// A refund payload (full refund: no amount).
	bp = x402.BatchEVMPayload{}
	json.Unmarshal(voucherPayload(t, o, payerKey, 3000, nil).Payload, &bp)
	bp.Type = "refund"
	p.Payload, _ = json.Marshal(bp)
	if v, verr := verify.VerifyBatch(context.Background(), p, batchOffers(t), now, verify.Options{}); verr != nil || !v.Refund || v.RefundAmount != nil {
		t.Errorf("refund: %v %+v", verr, v)
	}
}

// depositPayload turns a voucher payload into a deposit of amount, the
// receiveWithAuthorization signed by signer.
func depositPayload(t *testing.T, o verify.Offer, signer string, amount int64, validBefore int64) *x402.PaymentPayload {
	t.Helper()
	p := voucherPayload(t, o, payerKey, 1000, nil)
	var bp x402.BatchEVMPayload
	json.Unmarshal(p.Payload, &bp)
	id, _ := evm.ParseBytes32(bp.Voucher.ChannelID)
	_, payer := key(t, payerKey)
	priv, _ := key(t, signer)
	salt := big.NewInt(42)
	auth := evm.Authorization{From: payer, To: evm.ERC3009DepositCollector, Value: big.NewInt(amount),
		ValidAfter: big.NewInt(0), ValidBefore: big.NewInt(validBefore), Nonce: evm.DepositNonce(id, salt)}
	sig := evm.Sign(auth.ReceiveDigest(o.Domain), priv)
	bp.Type = "deposit"
	bp.Deposit, _ = json.Marshal(map[string]any{"amount": big.NewInt(amount).String(), "authorization": map[string]any{
		"erc3009Authorization": map[string]string{"validAfter": "0", "validBefore": big.NewInt(validBefore).String(),
			"salt": "0x2a", "signature": "0x" + hex.EncodeToString(sig)}}})
	p.Payload, _ = json.Marshal(bp)
	return p
}

func TestVerifyDeposit(t *testing.T) {
	o := batchOffers(t)[1]
	opt := verify.Options{MinRemaining: 10 * time.Second}
	v, verr := verify.VerifyBatch(context.Background(), depositPayload(t, o, payerKey, 100000, now.Unix()+3600), batchOffers(t), now, opt)
	if verr != nil {
		t.Fatal(verr)
	}
	if v.Deposit == nil || v.Deposit.Amount.Int64() != 100000 || v.Deposit.Auth.To != evm.ERC3009DepositCollector || v.MaxClaimable.Int64() != 1000 {
		t.Fatalf("%+v", v.Deposit)
	}
	cases := map[string]struct {
		p    *x402.PaymentPayload
		want string
	}{
		"other signer": {depositPayload(t, o, otherKey, 100000, now.Unix()+3600), x402.ErrBatchReceiveSignature},
		"expiring":     {depositPayload(t, o, payerKey, 100000, now.Unix()+5), x402.ErrBatchDepositValidBefore},
	}
	for name, c := range cases {
		if _, verr := verify.VerifyBatch(context.Background(), c.p, batchOffers(t), now, opt); verr == nil || verr.Reason != c.want {
			t.Errorf("%s: %v, want %s", name, verr, c.want)
		}
	}
	// The signature binds the amount: claiming a bigger deposit fails.
	p := depositPayload(t, o, payerKey, 100000, now.Unix()+3600)
	var bp x402.BatchEVMPayload
	json.Unmarshal(p.Payload, &bp)
	bp.Deposit = []byte(strings.Replace(string(bp.Deposit), `"amount":"100000"`, `"amount":"900000"`, 1))
	p.Payload, _ = json.Marshal(bp)
	if _, verr := verify.VerifyBatch(context.Background(), p, batchOffers(t), now, opt); verr == nil || verr.Reason != x402.ErrBatchReceiveSignature {
		t.Errorf("inflated amount: %v", verr)
	}
}

type voucherChecker struct {
	wallet evm.Address
	valid  bool
	err    error
}

func (c voucherChecker) IsValidSignature(_ context.Context, _ string, account evm.Address, _ [32]byte, _ []byte) (bool, error) {
	return c.valid && account == c.wallet, c.err
}

func TestVerifyVoucherFromContractWallet(t *testing.T) {
	o := batchOffers(t)[1]
	wallet := "0x00000000000000000000000000000000000c0de1"
	asWallet := func(c *x402.BatchChannelConfig) {
		c.Payer, c.PayerAuthorizer = wallet, "0x0000000000000000000000000000000000000000"
	}
	w, _ := evm.ParseAddress(wallet)
	p := voucherPayload(t, o, otherKey, 3000, asWallet)
	if _, verr := verify.VerifyBatch(context.Background(), p, batchOffers(t), now, verify.Options{}); verr == nil {
		t.Error("contract voucher accepted without a checker")
	}
	v, verr := verify.VerifyBatch(context.Background(), p, batchOffers(t), now, verify.Options{Contract: voucherChecker{wallet: w, valid: true}})
	if verr != nil || !v.Contract {
		t.Fatalf("%v %+v", verr, v)
	}
	if _, verr := verify.VerifyBatch(context.Background(), p, batchOffers(t), now, verify.Options{Contract: voucherChecker{wallet: w, err: errors.New("rpc down")}}); verr == nil || !verr.Unavailable {
		t.Errorf("rpc down: %v", verr)
	}
}
