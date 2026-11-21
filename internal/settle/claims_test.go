package settle

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/receipts"
	"apivend.io/apivend/internal/redistest"
	"apivend.io/apivend/internal/x402"
)

// claimBackend records batch work and answers with outcome.
type claimBackend struct {
	name    string
	mu      sync.Mutex
	jobs    []Job
	outcome Outcome
}

func (b *claimBackend) Name() string { return b.name }
func (b *claimBackend) Settle(_ context.Context, j *Job) Result {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.jobs = append(b.jobs, *j)
	if b.outcome != Settled {
		return Result{Outcome: b.outcome, Reason: "nope"}
	}
	return Result{Outcome: Settled, Transaction: "0xc1a1"}
}

func TestClaimRound(t *testing.T) {
	rdb, _ := redistest.Start(t)
	ctx := context.Background()
	key, _ := secp256k1.GeneratePrivateKey()
	me := evm.PubkeyAddress(key.PubKey())
	usdc, _ := evm.ParseAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e")
	seller, _ := evm.ParseAddress("0x707522232dF81025b7A1893c104aCB0973863c7A")

	var mu sync.Mutex
	claimed := map[[32]byte]*big.Int{}
	channels := payerstate.NewChannels(rdb, func(_ context.Context, _ string, id [32]byte) (evm.Channel, error) {
		mu.Lock()
		defer mu.Unlock()
		c := claimed[id]
		if c == nil {
			c = big.NewInt(0)
		}
		return evm.Channel{Balance: big.NewInt(100000), TotalClaimed: new(big.Int).Set(c), RefundNonce: big.NewInt(0)}, nil
	})

	// Two payers' channels, charged 3000 and 1000 by the filter.
	charge := func(salt byte, times int) [32]byte {
		cfg := evm.ChannelConfig{Payer: evm.Address{salt}, PayerAuthorizer: evm.Address{salt}, Receiver: seller,
			ReceiverAuthorizer: me, Token: usdc, WithdrawDelay: 86400, Salt: [32]byte{31: salt}}
		id := cfg.ChannelID(big.NewInt(84532))
		for i := 1; i <= times; i++ {
			h, err := channels.Reserve(ctx, payerstate.VoucherRequest{Network: "eip155:84532", ChannelID: id, Config: cfg,
				MaxClaimable: big.NewInt(int64(i) * 1000), Price: big.NewInt(1000), Signature: []byte{salt, byte(i)},
				Provider: "testnet", Route: "t-trace/route-1"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := channels.Commit(ctx, h); err != nil {
				t.Fatal(err)
			}
		}
		return id
	}
	a, b := charge(1, 3), charge(2, 1)

	backend := &claimBackend{name: "facilitator", outcome: Unavailable}
	second := &claimBackend{name: "fallback", outcome: Settled}
	store := &receipts.Memory{}
	s := &Settler{Providers: map[string][]Backend{"testnet": {backend, second}}, Receipts: store}
	c := &Claimer{Settler: s, Channels: channels, Key: key}

	if err := c.Round(ctx); err != nil {
		t.Fatal(err)
	}
	// The first backend was down; the fallback claimed and paid out.
	if len(backend.jobs) == 0 || len(second.jobs) != 2 {
		t.Fatalf("jobs: first %d, fallback %d", len(backend.jobs), len(second.jobs))
	}
	claim, payout := second.jobs[0], second.jobs[1]
	if claim.Kind != KindBatchClaim || payout.Kind != KindBatchPayout || claim.Requirements.Scheme != x402.SchemeBatch ||
		!strings.EqualFold(claim.Requirements.PayTo, seller.String()) || claim.Requirements.Amount != "0" {
		t.Fatalf("claim %+v payout %+v", claim, payout)
	}
	var body struct {
		Type   string `json:"type"`
		Claims []struct {
			Voucher struct {
				Channel            map[string]any `json:"channel"`
				MaxClaimableAmount string         `json:"maxClaimableAmount"`
			} `json:"voucher"`
			Signature    string `json:"signature"`
			TotalClaimed string `json:"totalClaimed"`
		} `json:"claims"`
		ClaimAuthorizerSignature string `json:"claimAuthorizerSignature"`
	}
	json.Unmarshal(claim.Payload.Payload, &body)
	if body.Type != "claim" || len(body.Claims) != 2 {
		t.Fatalf("claim body %+v", body)
	}
	// Our signature covers exactly these rows.
	var entries []evm.ClaimEntry
	for _, cl := range body.Claims {
		cfg := evm.ChannelConfig{WithdrawDelay: 86400}
		cfg.Payer, _ = evm.ParseAddress(cl.Voucher.Channel["payer"].(string))
		cfg.PayerAuthorizer, _ = evm.ParseAddress(cl.Voucher.Channel["payerAuthorizer"].(string))
		cfg.Receiver, _ = evm.ParseAddress(cl.Voucher.Channel["receiver"].(string))
		cfg.ReceiverAuthorizer, _ = evm.ParseAddress(cl.Voucher.Channel["receiverAuthorizer"].(string))
		cfg.Token, _ = evm.ParseAddress(cl.Voucher.Channel["token"].(string))
		cfg.Salt, _ = evm.ParseBytes32(cl.Voucher.Channel["salt"].(string))
		max, _ := new(big.Int).SetString(cl.Voucher.MaxClaimableAmount, 10)
		total, _ := new(big.Int).SetString(cl.TotalClaimed, 10)
		entries = append(entries, evm.ClaimEntry{ChannelID: cfg.ChannelID(big.NewInt(84532)), MaxClaimable: max, TotalClaimed: total})
	}
	sig, _ := hex.DecodeString(strings.TrimPrefix(body.ClaimAuthorizerSignature, "0x"))
	if signer, err := evm.RecoverSigner(evm.ClaimBatchDigest(big.NewInt(84532), entries), sig); err != nil || signer != me {
		t.Fatalf("claim signature recovers to %v (%v), want %v", signer, err, me)
	}
	got := map[[32]byte]string{}
	for _, e := range entries {
		got[e.ChannelID] = e.TotalClaimed.String()
	}
	if got[a] != "3000" || got[b] != "1000" {
		t.Fatalf("claimed totals %v", got)
	}
	// Receipts: one per channel, the claimed delta.
	rows := store.Rows()
	if len(rows) != 2 || rows[0].Status != StatusBatchClaimed || rows[0].TxHash != "0xc1a1" {
		t.Fatalf("receipts %+v", rows)
	}
	// And one per charged request (4), with its cumulative position.
	c.RecordCharges(ctx)
	charged := map[string]bool{}
	for _, r := range store.Rows() {
		if r.Status == StatusBatchCharged {
			if r.Amount != "1000" || r.Route != "t-trace/route-1" {
				t.Errorf("charge receipt %+v", r)
			}
			charged[r.ReservationID] = true
		}
	}
	if len(charged) != 4 {
		t.Fatalf("%d charge receipts, want 4", len(charged))
	}
	if n, _ := rdb.LLen(ctx, payerstate.ChargesKey).Result(); n != 0 {
		t.Errorf("%d charges left undrained", n)
	}

	// Once the chain shows the claim, nothing is left to claim.
	mu.Lock()
	claimed[a], claimed[b] = big.NewInt(3000), big.NewInt(1000)
	mu.Unlock()
	before := len(second.jobs)
	if err := c.Round(ctx); err != nil {
		t.Fatal(err)
	}
	if len(second.jobs) != before {
		t.Errorf("claimed again: %d jobs", len(second.jobs)-before)
	}
	if n, _ := rdb.SCard(ctx, payerstate.UnclaimedKey).Result(); n != 0 {
		t.Errorf("%d channels still queued", n)
	}
}

func TestClaimWaitsForThreshold(t *testing.T) {
	rdb, _ := redistest.Start(t)
	ctx := context.Background()
	key, _ := secp256k1.GeneratePrivateKey()
	me := evm.PubkeyAddress(key.PubKey())
	withdrawing := uint64(0)
	channels := payerstate.NewChannels(rdb, func(context.Context, string, [32]byte) (evm.Channel, error) {
		return evm.Channel{Balance: big.NewInt(100000), TotalClaimed: big.NewInt(0), RefundNonce: big.NewInt(0), WithdrawRequestedAt: withdrawing}, nil
	})
	cfg := evm.ChannelConfig{Payer: evm.Address{9}, Receiver: evm.Address{8}, ReceiverAuthorizer: me, Token: evm.Address{7}, WithdrawDelay: 86400}
	id := cfg.ChannelID(big.NewInt(84532))
	h, _ := channels.Reserve(ctx, payerstate.VoucherRequest{Network: "eip155:84532", ChannelID: id, Config: cfg,
		MaxClaimable: big.NewInt(1000), Price: big.NewInt(1000), Signature: []byte{1}, Provider: "p"})
	channels.Commit(ctx, h)

	backend := &claimBackend{name: "f", outcome: Settled}
	c := &Claimer{Settler: &Settler{Providers: map[string][]Backend{"p": {backend}}, Receipts: &receipts.Memory{}},
		Channels: channels, Key: key, MinAmount: big.NewInt(50000)}
	c.Round(ctx)
	if len(backend.jobs) != 0 {
		t.Fatal("claimed below the threshold")
	}
	// A pending withdrawal makes it urgent.
	withdrawing = 1795000000
	c.Round(ctx)
	if len(backend.jobs) == 0 || backend.jobs[0].Kind != KindBatchClaim {
		t.Fatalf("withdrawal did not trigger a claim: %+v", backend.jobs)
	}
}

func TestPrepareRefund(t *testing.T) {
	rdb, _ := redistest.Start(t)
	key, _ := secp256k1.GeneratePrivateKey()
	me := evm.PubkeyAddress(key.PubKey())
	channels := payerstate.NewChannels(rdb, func(context.Context, string, [32]byte) (evm.Channel, error) {
		return evm.Channel{Balance: big.NewInt(100000), TotalClaimed: big.NewInt(1000), RefundNonce: big.NewInt(3)}, nil
	})
	c := &Claimer{Settler: &Settler{}, Channels: channels, Key: key}
	cc := x402.BatchChannelConfig{Payer: "0x0000000000000000000000000000000000000009", PayerAuthorizer: "0x0000000000000000000000000000000000000009",
		Receiver: "0x0000000000000000000000000000000000000008", ReceiverAuthorizer: me.String(), Token: "0x036CbD53842c5426634e7929541eC2318f3dCF7e",
		WithdrawDelay: 86400, Salt: "0x" + strings.Repeat("00", 32)}
	raw, _ := json.Marshal(x402.BatchEVMPayload{Type: "refund", ChannelConfig: cc, Voucher: x402.BatchVoucher{ChannelID: "0x00", MaxClaimableAmount: "3000", Signature: "0xabcd"}})
	job := &Job{Kind: KindBatchRefund, RefundAmount: "97000", Payload: x402.PaymentPayload{X402Version: 2, Payload: raw},
		Requirements: x402.Requirements{Scheme: x402.SchemeBatch, Network: "eip155:84532"}}
	if err := c.PrepareRefund(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	var out struct {
		Amount, RefundNonce, RefundAuthorizerSignature, ClaimAuthorizerSignature string
		Claims                                                                   []struct{ TotalClaimed, Signature string }
	}
	json.Unmarshal(job.Payload.Payload, &out)
	if out.Amount != "97000" || out.RefundNonce != "3" || len(out.Claims) != 1 || out.Claims[0].TotalClaimed != "3000" || out.Claims[0].Signature != "0xabcd" {
		t.Fatalf("%+v", out)
	}
	cfg, _ := wireConfig(cc)
	id := cfg.ChannelID(big.NewInt(84532))
	rsig, _ := hex.DecodeString(strings.TrimPrefix(out.RefundAuthorizerSignature, "0x"))
	if s, err := evm.RecoverSigner(evm.RefundDigest(big.NewInt(84532), id, big.NewInt(3), big.NewInt(97000)), rsig); err != nil || s != me {
		t.Errorf("refund signature: %v %v", s, err)
	}
	csig, _ := hex.DecodeString(strings.TrimPrefix(out.ClaimAuthorizerSignature, "0x"))
	if s, err := evm.RecoverSigner(evm.ClaimBatchDigest(big.NewInt(84532), []evm.ClaimEntry{{ChannelID: id, MaxClaimable: big.NewInt(3000), TotalClaimed: big.NewInt(3000)}}), csig); err != nil || s != me {
		t.Errorf("claim signature: %v %v", s, err)
	}
}
