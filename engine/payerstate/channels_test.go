package payerstate

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/redistest"
)

type fakeChannel struct {
	mu    sync.Mutex
	ch    evm.Channel
	reads atomic.Int32
	err   error
}

func (f *fakeChannel) read(_ context.Context, network string, _ [32]byte) (evm.Channel, error) {
	f.reads.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return evm.Channel{}, f.err
	}
	c := f.ch
	return evm.Channel{Balance: new(big.Int).Set(c.Balance), TotalClaimed: new(big.Int).Set(c.TotalClaimed), RefundNonce: big.NewInt(0), WithdrawRequestedAt: c.WithdrawRequestedAt}, nil
}

func voucher(max, price int64) VoucherRequest {
	return VoucherRequest{Network: "eip155:84532", ChannelID: [32]byte{7}, MaxClaimable: big.NewInt(max), Price: big.NewInt(price), Signature: []byte{1, 2, 3}}
}

func TestChannelVouchers(t *testing.T) {
	rdb, _ := redistest.Start(t)
	ctx := context.Background()
	chain := &fakeChannel{ch: evm.Channel{Balance: big.NewInt(10000), TotalClaimed: big.NewInt(0)}}
	c := NewChannels(rdb, chain.read)

	// First voucher: read from the chain, charged starts at the claimed total.
	h, err := c.Reserve(ctx, voucher(1000, 1000))
	if err != nil {
		t.Fatal(err)
	}
	if h.Charged.Int64() != 0 || chain.reads.Load() != 1 {
		t.Fatalf("charged %v reads %d", h.Charged, chain.reads.Load())
	}
	// A second request on the same channel waits for the first (spec: one
	// at a time per channel).
	if _, err := c.Reserve(ctx, voucher(1000, 1000)); !isDenied(err, ReasonChannelBusy) {
		t.Fatalf("concurrent: %v", err)
	}
	n, err := c.Commit(ctx, h)
	if err != nil || n.Int64() != 1000 {
		t.Fatalf("commit %v %v", n, err)
	}
	if ok, _ := rdb.SIsMember(ctx, UnclaimedKey, "eip155:84532|0x07"+strings.Repeat("00", 31)).Result(); !ok {
		t.Error("committed channel not queued for claiming")
	}

	// The next voucher must be charged + price; a stale one gets the state
	// to resynchronise from.
	_, err = c.Reserve(ctx, voucher(1000, 1000))
	var mm *MismatchError
	if !errors.As(err, &mm) || mm.Charged.Int64() != 1000 || mm.Signed.Int64() != 1000 || len(mm.Signature) != 3 {
		t.Fatalf("stale voucher: %v %+v", err, mm)
	}
	h, err = c.Reserve(ctx, voucher(2000, 1000))
	if err != nil {
		t.Fatal(err)
	}
	// A failed request is not charged: the same voucher works again.
	if err := c.Release(ctx, h); err != nil {
		t.Fatal(err)
	}
	h, err = c.Reserve(ctx, voucher(2000, 1000))
	if err != nil {
		t.Fatal(err)
	}
	c.Commit(ctx, h)
	if chain.reads.Load() != 1 {
		t.Errorf("chain read %d times within freshness", chain.reads.Load())
	}

	// Never beyond the escrow.
	if _, err := c.Reserve(ctx, voucher(11000, 9000)); !isDenied(err, ReasonExceedsBalance) {
		t.Errorf("over balance: %v", err)
	}
	// A commit after the hold expired charges nothing.
	c.Lock = time.Millisecond
	h, _ = c.Reserve(ctx, voucher(3000, 1000))
	time.Sleep(5 * time.Millisecond)
	h2, err := c.Reserve(ctx, voucher(3000, 1000)) // takes over the stale hold
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(ctx, h); !errors.Is(err, ErrExpired) {
		t.Errorf("stale hold committed: %v", err)
	}
	if n, err := c.Commit(ctx, h2); err != nil || n.Int64() != 3000 {
		t.Errorf("commit %v %v", n, err)
	}
}

func TestChannelStateFromChain(t *testing.T) {
	rdb, _ := redistest.Start(t)
	ctx := context.Background()
	// The gateway lost its state, and 5000 was already claimed on chain:
	// the charged total starts there, never from the client's voucher.
	chain := &fakeChannel{ch: evm.Channel{Balance: big.NewInt(10000), TotalClaimed: big.NewInt(5000)}}
	c := NewChannels(rdb, chain.read)
	if _, err := c.Reserve(ctx, voucher(1000, 1000)); err == nil {
		t.Fatal("voucher below the claimed total accepted")
	}
	h, err := c.Reserve(ctx, voucher(6000, 1000))
	if err != nil || h.Charged.Int64() != 5000 {
		t.Fatalf("%v %v", h.Charged, err)
	}
	c.Release(ctx, h)

	// No channel (zero balance) and an unreadable chain.
	empty := NewChannels(rdb, (&fakeChannel{ch: evm.Channel{Balance: big.NewInt(0), TotalClaimed: big.NewInt(0)}}).read)
	v := voucher(1000, 1000)
	v.ChannelID = [32]byte{8}
	if _, err := empty.Reserve(ctx, v); !isDenied(err, ReasonChannelNotFound) {
		t.Errorf("empty channel: %v", err)
	}
	down := NewChannels(rdb, (&fakeChannel{err: errors.New("rpc down")}).read)
	v.ChannelID = [32]byte{9}
	if _, err := down.Reserve(ctx, v); !errors.Is(err, ErrUnavailable) {
		t.Errorf("rpc down: %v", err)
	}
}

func isDenied(err error, reason string) bool {
	var d *DeniedError
	return errors.As(err, &d) && d.Reason == reason
}

func TestChannelRefundAndWithdrawalClose(t *testing.T) {
	rdb, _ := redistest.Start(t)
	ctx := context.Background()
	chain := &fakeChannel{ch: evm.Channel{Balance: big.NewInt(10000), TotalClaimed: big.NewInt(0)}}
	c := NewChannels(rdb, chain.read)
	h, _ := c.Reserve(ctx, voucher(1000, 1000))
	c.Commit(ctx, h)

	// A refund voucher must be zero-charge: its ceiling is the charged total.
	if _, err := c.BeginRefund(ctx, voucher(2000, 0), time.Minute); err == nil {
		t.Fatal("refund with a charging voucher accepted")
	}
	rh, err := c.BeginRefund(ctx, voucher(1000, 0), time.Minute)
	if err != nil || rh.Charged.Int64() != 1000 || rh.Refundable.Int64() != 9000 {
		t.Fatalf("begin refund %+v %v", rh, err)
	}
	// Closed: no new vouchers, no second refund.
	if _, err := c.Reserve(ctx, voucher(2000, 1000)); !isDenied(err, ReasonChannelClosing) {
		t.Fatalf("voucher during refund: %v", err)
	}
	if _, err := c.BeginRefund(ctx, voucher(1000, 0), time.Minute); !isDenied(err, ReasonChannelBusy) {
		t.Fatalf("second refund: %v", err)
	}
	// Refund landed in full: claimed 1000, balance 1000 left = claimed.
	chain.mu.Lock()
	chain.ch = evm.Channel{Balance: big.NewInt(1000), TotalClaimed: big.NewInt(1000)}
	chain.mu.Unlock()
	if _, err := c.EndRefund(ctx, rh, voucher(1000, 0)); err != nil {
		t.Fatal(err)
	}
	if n, _ := rdb.Exists(ctx, channelKey("eip155:84532", [32]byte{7})).Result(); n != 0 {
		t.Error("fully refunded channel not forgotten")
	}

	// A timed withdrawal seen on chain closes the channel too.
	v := voucher(1000, 1000)
	v.ChannelID = [32]byte{11}
	chain.mu.Lock()
	chain.ch = evm.Channel{Balance: big.NewInt(10000), TotalClaimed: big.NewInt(0), WithdrawRequestedAt: 1795000000}
	chain.mu.Unlock()
	if _, err := c.Reserve(ctx, v); !isDenied(err, ReasonChannelClosing) {
		t.Errorf("voucher during withdrawal: %v", err)
	}
}
