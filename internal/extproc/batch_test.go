package extproc_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/extproc"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/redistest"
	"apivend.io/apivend/internal/settle"
	"apivend.io/apivend/internal/x402"
)

const receiverAuth = "0x01d4000000000000000000000000000000006aE6"

func batchTable(t *testing.T) *pricing.Table {
	t.Helper()
	table, err := pricing.Build(pricing.Config{
		Batch: &pricing.BatchConfig{ReceiverAuthorizer: receiverAuth, WithdrawDelay: 86400},
		Routes: []pricing.RouteConfig{{
			Name: "cheap", Match: pricing.MatchConfig{PathPrefix: "/cheap"},
			Price:   pricing.PriceConfig{Amount: "0.001", Currency: "USD"},
			Accepts: []pricing.AcceptConfig{{Network: "base-sepolia", Asset: "USDC", PayTo: payTo, Schemes: []string{"exact", "batch-settlement"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

// channelClient is a payer with one channel, signing vouchers as the SDK
// clients do: charged so far plus the price.
type channelClient struct {
	t      *testing.T
	key    *secp256k1.PrivateKey
	cc     x402.BatchChannelConfig
	id     [32]byte
	chain  *big.Int
	offer  x402.Requirements
	signed int64
}

func newChannelClient(t *testing.T, pr x402.PaymentRequired) *channelClient {
	return newChannelClientWithSalt(t, pr, "0x"+strings.Repeat("00", 31)+"05")
}

func newChannelClientWithSalt(t *testing.T, pr x402.PaymentRequired, salt string) *channelClient {
	var offer x402.Requirements
	for _, a := range pr.Accepts {
		if a.Scheme == x402.SchemeBatch {
			offer = a
		}
	}
	if offer.Scheme == "" {
		t.Fatal("402 does not offer batch-settlement")
	}
	b, _ := hex.DecodeString(strings.TrimPrefix(payerKey, "0x"))
	key := secp256k1.PrivKeyFromBytes(b)
	addr := evm.PubkeyAddress(key.PubKey())
	c := &channelClient{t: t, key: key, chain: big.NewInt(84532), offer: offer, cc: x402.BatchChannelConfig{
		Payer: addr.String(), PayerAuthorizer: addr.String(), Receiver: offer.PayTo,
		ReceiverAuthorizer: offer.Extra["receiverAuthorizer"].(string), Token: offer.Asset,
		WithdrawDelay: uint64(offer.Extra["withdrawDelay"].(float64)), Salt: salt,
	}}
	cfg := evm.ChannelConfig{WithdrawDelay: c.cc.WithdrawDelay}
	cfg.Payer, cfg.PayerAuthorizer = addr, addr
	cfg.Receiver, _ = evm.ParseAddress(c.cc.Receiver)
	cfg.ReceiverAuthorizer, _ = evm.ParseAddress(c.cc.ReceiverAuthorizer)
	cfg.Token, _ = evm.ParseAddress(c.cc.Token)
	cfg.Salt, _ = evm.ParseBytes32(c.cc.Salt)
	c.id = cfg.ChannelID(c.chain)
	return c
}

func (c *channelClient) voucher(max int64) string {
	sig := evm.Sign(evm.VoucherDigest(c.chain, c.id, big.NewInt(max)), c.key)
	raw, _ := json.Marshal(x402.BatchEVMPayload{Type: "voucher", ChannelConfig: c.cc, Voucher: x402.BatchVoucher{
		ChannelID: "0x" + hex.EncodeToString(c.id[:]), MaxClaimableAmount: big.NewInt(max).String(), Signature: "0x" + hex.EncodeToString(sig),
	}})
	h, err := x402.EncodeHeader(x402.PaymentPayload{X402Version: 2, Accepted: c.offer, Payload: raw})
	if err != nil {
		c.t.Fatal(err)
	}
	return h
}

func TestBatchVouchers(t *testing.T) {
	rdb, _ := redistest.Start(t)
	escrow := evm.Channel{Balance: big.NewInt(100000), TotalClaimed: big.NewInt(0), RefundNonce: big.NewInt(0)}
	channels := payerstate.NewChannels(rdb, func(context.Context, string, [32]byte) (evm.Channel, error) { return escrow, nil })
	c := serveTable(t, batchTable(t), payerstate.NewMemory(), extproc.Options{Channels: channels})

	pr := challenge(t, open(t, c).request("/cheap"), 402)
	if len(pr.Accepts) != 2 {
		t.Fatalf("accepts %+v", pr.Accepts)
	}
	client := newChannelClient(t, pr)

	// Three paid requests: 1000, 2000, 3000 cumulative.
	for i := int64(1); i <= 3; i++ {
		e := open(t, c)
		resp := e.request("/cheap", "payment-signature", client.voucher(i*1000))
		set := mutation(resp.GetRequestHeaders().GetResponse().GetHeaderMutation().GetSetHeaders())
		if set[extproc.HeaderPayer] == "" {
			t.Fatalf("request %d not forwarded: %v", i, resp)
		}
		resp = e.response("200")
		var sr x402.SettlementResponse
		hdr := mutation(resp.GetResponseHeaders().GetResponse().GetHeaderMutation().GetSetHeaders())[x402.HeaderPaymentResponse]
		if err := x402.DecodeHeader(hdr, &sr); err != nil {
			t.Fatal(err)
		}
		st := sr.Extra["channelState"].(map[string]any)
		if !sr.Success || sr.Transaction != "" || sr.Amount == nil || *sr.Amount != "" ||
			sr.Extra["chargedAmount"] != "1000" || st["chargedCumulativeAmount"] != big.NewInt(i*1000).String() {
			t.Fatalf("request %d: %+v", i, sr)
		}
	}

	// A backend failure charges nothing: the same voucher pays next time.
	e := open(t, c)
	e.request("/cheap", "payment-signature", client.voucher(4000))
	e.response("500")
	e = open(t, c)
	e.request("/cheap", "payment-signature", client.voucher(4000))
	e.response("200")

	// A replayed (old) voucher gets the corrective 402 with our state.
	pr = challenge(t, open(t, c).request("/cheap", "payment-signature", client.voucher(2000)), 402)
	if pr.Error != x402.ErrBatchCumulativeMismatch {
		t.Fatalf("replay: %s", pr.Error)
	}
	var fixed map[string]any
	for _, a := range pr.Accepts {
		if a.Scheme == x402.SchemeBatch {
			fixed = a.Extra
		}
	}
	cs, _ := fixed["channelState"].(map[string]any)
	vs, _ := fixed["voucherState"].(map[string]any)
	if cs["chargedCumulativeAmount"] != "4000" || vs["signedMaxClaimable"] != "4000" || vs["signature"] == "" {
		t.Fatalf("corrective extra %v", fixed)
	}

	// Beyond the escrow: refused.
	pr = challenge(t, open(t, c).request("/cheap", "payment-signature", client.voucher(101000)), 402)
	if pr.Error != x402.ErrBatchCumulativeMismatch && pr.Error != x402.ErrBatchExceedsBalance {
		t.Errorf("over escrow: %s", pr.Error)
	}

	// Unclaimed charges are queued for the claim job.
	if n, _ := rdb.SCard(context.Background(), payerstate.UnclaimedKey).Result(); n != 1 {
		t.Errorf("unclaimed channels %d", n)
	}
}

func TestBatchWithoutChannelsIsRefused(t *testing.T) {
	c := serveTable(t, batchTable(t), payerstate.NewMemory(), extproc.Options{})
	pr := challenge(t, open(t, c).request("/cheap"), 402)
	client := newChannelClient(t, pr)
	pr = challenge(t, open(t, c).request("/cheap", "payment-signature", client.voucher(1000)), 402)
	if pr.Error != x402.ErrInvalidScheme {
		t.Errorf("error %s", pr.Error)
	}
}

// depositHeader is the client's first payment on a new channel: a deposit
// of amount plus the voucher for the first request.
func (c *channelClient) depositHeader(amount int64) string {
	c.t.Helper()
	var p x402.PaymentPayload
	if err := x402.DecodeHeader(c.voucher(1000), &p); err != nil {
		c.t.Fatal(err)
	}
	var bp x402.BatchEVMPayload
	json.Unmarshal(p.Payload, &bp)
	usdc, _ := evm.ParseAddress(c.offer.Asset)
	auth := evm.Authorization{From: evm.PubkeyAddress(c.key.PubKey()), To: evm.ERC3009DepositCollector, Value: big.NewInt(amount),
		ValidAfter: big.NewInt(0), ValidBefore: big.NewInt(time.Now().Unix() + 3600), Nonce: evm.DepositNonce(c.id, big.NewInt(9))}
	sig := evm.Sign(auth.ReceiveDigest(evm.Domain{Name: "USDC", Version: "2", ChainID: c.chain, VerifyingContract: usdc}), c.key)
	bp.Type = "deposit"
	bp.Deposit, _ = json.Marshal(map[string]any{"amount": big.NewInt(amount).String(), "authorization": map[string]any{
		"erc3009Authorization": map[string]string{"validAfter": "0", "validBefore": auth.ValidBefore.String(), "salt": "0x09", "signature": "0x" + hex.EncodeToString(sig)}}})
	p.Payload, _ = json.Marshal(bp)
	h, _ := x402.EncodeHeader(p)
	return h
}

func TestBatchDeposit(t *testing.T) {
	rdb, _ := redistest.Start(t)
	state := payerstate.NewRedis(rdb) // no balance source: nonce checks only
	var deposited atomic.Bool
	channels := payerstate.NewChannels(rdb, func(context.Context, string, [32]byte) (evm.Channel, error) {
		bal := int64(0)
		if deposited.Load() {
			bal = 100000
		}
		return evm.Channel{Balance: big.NewInt(bal), TotalClaimed: big.NewInt(0), RefundNonce: big.NewInt(0)}, nil
	})
	c := serveTable(t, batchTable(t), state, extproc.Options{Channels: channels})

	// The settler: executes deposit jobs, the first one fails.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs := make(chan settle.Job, 4)
	go func() {
		n := 0
		for ctx.Err() == nil {
			cl, err := state.Claim(ctx, time.Second)
			if err != nil || cl == nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			var job settle.Job
			json.Unmarshal(cl.Body, &job)
			jobs <- job
			if n++; n == 1 {
				state.Failed(ctx, cl.ID, "insufficient_funds", 0)
				state.PublishResult(ctx, cl.ID, payerstate.Result{Status: "unpaid", Reason: "insufficient_funds"})
				continue
			}
			deposited.Store(true)
			state.Settled(ctx, cl.ID, 1)
			state.PublishResult(ctx, cl.ID, payerstate.Result{Status: "settled", Transaction: "0xdeadbeef"})
		}
	}()

	pr := challenge(t, open(t, c).request("/cheap"), 402)
	client := newChannelClient(t, pr)

	// A failed deposit: 402 with the facilitator's reason, nothing served.
	pr = challenge(t, open(t, c).request("/cheap", "payment-signature", client.depositHeader(100000)), 402)
	if pr.Error != "insufficient_funds" {
		t.Fatalf("failed deposit: %s", pr.Error)
	}
	job := <-jobs
	if job.Kind != settle.KindBatchDeposit || job.Amount != "100000" || job.Requirements.Scheme != x402.SchemeBatch {
		t.Fatalf("deposit job %+v", job)
	}

	// The deposit lands: the request is served and the response reports it.
	client.cc.Salt = "0x" + strings.Repeat("00", 31) + "06" // a fresh channel
	client = newChannelClientWithSalt(t, pr, client.cc.Salt)
	e := open(t, c)
	resp := e.request("/cheap", "payment-signature", client.depositHeader(100000))
	if mutation(resp.GetRequestHeaders().GetResponse().GetHeaderMutation().GetSetHeaders())[extproc.HeaderPayer] == "" {
		t.Fatalf("not forwarded after deposit: %v", resp)
	}
	<-jobs
	resp = e.response("200")
	var sr x402.SettlementResponse
	x402.DecodeHeader(mutation(resp.GetResponseHeaders().GetResponse().GetHeaderMutation().GetSetHeaders())[x402.HeaderPaymentResponse], &sr)
	if sr.Transaction != "0xdeadbeef" || sr.Amount == nil || *sr.Amount != "100000" || sr.Extra["chargedAmount"] != "1000" {
		t.Fatalf("deposit response %+v", sr)
	}
	// Then vouchers, no deposit.
	e = open(t, c)
	e.request("/cheap", "payment-signature", client.voucher(2000))
	resp = e.response("200")
	x402.DecodeHeader(mutation(resp.GetResponseHeaders().GetResponse().GetHeaderMutation().GetSetHeaders())[x402.HeaderPaymentResponse], &sr)
	if sr.Transaction != "" || *sr.Amount != "" {
		t.Fatalf("voucher after deposit %+v", sr)
	}
}

func (c *channelClient) refundHeader(charged int64, amount string) string {
	c.t.Helper()
	var p x402.PaymentPayload
	x402.DecodeHeader(c.voucher(charged), &p)
	var bp x402.BatchEVMPayload
	json.Unmarshal(p.Payload, &bp)
	bp.Type, bp.Amount = "refund", amount
	p.Payload, _ = json.Marshal(bp)
	h, _ := x402.EncodeHeader(p)
	return h
}

func TestBatchRefund(t *testing.T) {
	rdb, _ := redistest.Start(t)
	state := payerstate.NewRedis(rdb)
	var refunded atomic.Bool
	channels := payerstate.NewChannels(rdb, func(context.Context, string, [32]byte) (evm.Channel, error) {
		if refunded.Load() {
			return evm.Channel{Balance: big.NewInt(2000), TotalClaimed: big.NewInt(2000), RefundNonce: big.NewInt(1)}, nil
		}
		return evm.Channel{Balance: big.NewInt(100000), TotalClaimed: big.NewInt(0), RefundNonce: big.NewInt(0)}, nil
	})
	c := serveTable(t, batchTable(t), state, extproc.Options{Channels: channels})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs := make(chan settle.Job, 4)
	go func() {
		for ctx.Err() == nil {
			cl, err := state.Claim(ctx, time.Second)
			if err != nil || cl == nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			var job settle.Job
			json.Unmarshal(cl.Body, &job)
			jobs <- job
			refunded.Store(true)
			state.Settled(ctx, cl.ID, 1)
			state.PublishResult(ctx, cl.ID, payerstate.Result{Status: "settled", Transaction: "0xrefund"})
		}
	}()

	pr := challenge(t, open(t, c).request("/cheap"), 402)
	client := newChannelClient(t, pr)
	for i := int64(1); i <= 2; i++ {
		e := open(t, c)
		e.request("/cheap", "payment-signature", client.voucher(i*1000))
		e.response("200")
	}
	// Too much asked: refused, channel stays open.
	pr = challenge(t, open(t, c).request("/cheap", "payment-signature", client.refundHeader(2000, "99000")), 402)
	if pr.Error != x402.ErrBatchRefundExceeds {
		t.Fatalf("over-refund: %s", pr.Error)
	}
	e := open(t, c)
	if resp := e.request("/cheap", "payment-signature", client.voucher(3000)); resp.GetImmediateResponse() != nil {
		t.Fatal("channel not reopened after a refused refund")
	}
	e.response("200")

	// Full refund: answered directly (the API is not called) with the refund.
	resp := open(t, c).request("/cheap", "payment-signature", client.refundHeader(3000, ""))
	im := resp.GetImmediateResponse()
	if im == nil || im.Status.Code != 200 {
		t.Fatalf("refund answer %v", resp)
	}
	var sr x402.SettlementResponse
	x402.DecodeHeader(mutation(im.Headers.SetHeaders)[x402.HeaderPaymentResponse], &sr)
	if !sr.Success || sr.Transaction != "0xrefund" || sr.Amount == nil || *sr.Amount != "97000" {
		t.Fatalf("refund response %+v", sr)
	}
	job := <-jobs
	if job.Kind != settle.KindBatchRefund || job.RefundAmount != "97000" {
		t.Fatalf("refund job %+v", job)
	}
}
