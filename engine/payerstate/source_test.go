package payerstate

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/statesource"
	"apivend.io/apivend/internal/redistest"
)

// twoSources is an engine with a preferred source ("hosted") and a fallback
// ("rpc"), each over its own fake chain so the test can break one.
func twoSources(t *testing.T) (*Redis, *chain.Fake, *chain.Fake) {
	rdb, _ := redistest.Start(t)
	primary, fallback := chain.NewFake(), chain.NewFake()
	opt := statesource.RPCOptions{PollInterval: 20 * time.Millisecond}
	s := NewRedis(rdb,
		statesource.NewRPC("hosted", map[string]statesource.ChainClient{net84532: primary}, opt),
		statesource.NewRPC("rpc", map[string]statesource.ChainClient{net84532: fallback}, opt))
	s.ReadTimeout = time.Second
	return s, primary, fallback
}

func liveReq(nonce string, amount int64) Request {
	return Request{Chain: net84532, Asset: usdc, Payer: alice, Nonce: nonce, Amount: big.NewInt(amount),
		Expiry: time.Now().Add(time.Minute), Policy: Policy{Freshness: 300 * time.Millisecond}}
}

func TestDecisionRecordsSource(t *testing.T) {
	s, primary, fallback := twoSources(t)
	primary.SetBalance(aliceAddr, 10_000)
	fallback.SetBalance(aliceAddr, 10_000)

	res, err := s.ReserveDetailed(context.Background(), liveReq("0xa", 1_000))
	if err != nil || res.Source != "hosted" || res.BlockHeight != 101 {
		t.Fatalf("reservation = %+v, %v", res, err)
	}
}

// The preferred source dies mid-traffic. Requests continue on the fallback;
// none is served on the strength of the dead source's data.
func TestFailoverToSecondSourceNeverFailsOpen(t *testing.T) {
	s, primary, fallback := twoSources(t)
	ctx := context.Background()
	primary.SetBalance(aliceAddr, 10_000)
	fallback.SetBalance(aliceAddr, 10_000)
	if res, _ := s.ReserveDetailed(ctx, liveReq("0xa", 1_000)); res.Source != "hosted" {
		t.Fatalf("setup: source = %q", res.Source)
	}

	primary.SetErr(errors.New("hosted service unreachable"))
	// The payer moves most of the money away; only the fallback can see it.
	// 1 500 left, of which 1 000 is already held by the first request.
	fallback.SetBalance(aliceAddr, 1_500)
	time.Sleep(350 * time.Millisecond) // past freshness: cached state is no longer usable

	_, err := s.ReserveDetailed(ctx, liveReq("0xb", 1_000))
	var denied *DeniedError
	if !errors.As(err, &denied) || denied.Reason != ReasonInsufficientBalance {
		t.Fatalf("served on stale data after failover? err = %v", err)
	}
	res, err := s.ReserveDetailed(ctx, liveReq("0xc", 400))
	if err != nil || res.Source != "rpc" {
		t.Fatalf("fallback not used: %+v, %v", res, err)
	}

	// Every source down: refused, not served.
	fallback.SetErr(errors.New("rpc down"))
	time.Sleep(350 * time.Millisecond)
	if _, err := s.ReserveDetailed(ctx, liveReq("0xd", 1)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("all sources down: err = %v, want unavailable", err)
	}
}

func TestWatchKeepsPayerFreshOffTheRequestPath(t *testing.T) {
	s, primary, _ := twoSources(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	primary.SetBalance(aliceAddr, 10_000)

	s.ReserveDetailed(ctx, liveReq("0x1", 100)) // first sight: one synchronous read
	time.Sleep(200 * time.Millisecond)          // let the watch take over
	reads := primary.ReadCount()

	// Well past freshness. Without the watch each of these would need a read.
	deadline := time.Now().Add(1200 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		if _, err := s.ReserveDetailed(ctx, liveReq("0x2"+string(rune('a'+i%26))+string(rune('a'+i/26)), 1)); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The watch polls every 20ms, so reads keep happening, but a balance
	// change must now arrive without any request triggering it:
	_ = reads
	primary.SetBalance(aliceAddr, 0)
	time.Sleep(200 * time.Millisecond)
	_, err := s.ReserveDetailed(ctx, liveReq("0x3", 1))
	var denied *DeniedError
	if !errors.As(err, &denied) || denied.Reason != ReasonInsufficientBalance {
		t.Fatalf("on-chain balance change did not reach the gateway: %v", err)
	}
}

// An authorization used or cancelled outside this gateway is refused once the
// watch has reported it.
func TestExternallyUsedOrCancelledAuthorizationIsRefused(t *testing.T) {
	s, primary, _ := twoSources(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	primary.SetBalance(aliceAddr, 10_000)
	s.ReserveDetailed(ctx, liveReq("0x01"+zeros62, 100)) // makes alice a watched payer
	time.Sleep(100 * time.Millisecond)

	primary.Transfer(aliceAddr, [32]byte{0xaa}, big.NewInt(1), "0xtx") // spent at another merchant
	primary.Cancel(aliceAddr, [32]byte{0xbb})                          // cancelled by the payer

	for _, nonce := range []string{"0xaa" + zeros62, "0xbb" + zeros62} {
		deadline := time.Now().Add(2 * time.Second)
		for {
			_, err := s.ReserveDetailed(ctx, liveReq(nonce, 100))
			var denied *DeniedError
			if errors.As(err, &denied) && denied.Reason == ReasonNonceUsed {
				break
			}
			if err == nil {
				// Accepted before the event arrived; undo and keep waiting.
				s.Release(ctx, makeID(net84532, usdc, alice, nonce))
			}
			if time.Now().After(deadline) {
				t.Fatalf("nonce %s still accepted: %v", nonce[:6], err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

const zeros62 = "00000000000000000000000000000000000000000000000000000000000000"

// slowSource answers nothing until its caller gives up, like a hosted service
// that is reachable but stuck on its own cold chain read.
type slowSource struct{ name string }

func (s slowSource) Name() string { return s.name }
func (s slowSource) Snapshot(ctx context.Context, _, _, _ string) (statesource.PayerSnapshot, error) {
	<-ctx.Done()
	return statesource.PayerSnapshot{}, ctx.Err()
}
func (s slowSource) Watch(ctx context.Context, _ statesource.Subscription) (<-chan statesource.StateUpdate, error) {
	return nil, errors.New("not watching")
}
func (s slowSource) AuthorizationState(ctx context.Context, _, _, _, _ string) (statesource.AuthState, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// A preferred source that is slow, not down, must not use up the whole read
// budget: the fallback still gets its share and the payer is served from it.
// Seen on the dev cluster: the hosted service spent the filter's 120 ms on a
// cold chain read and the RPC fallback started with nothing left (503).
func TestSlowPreferredSourceLeavesTimeForFallback(t *testing.T) {
	rdb, _ := redistest.Start(t)
	fallback := chain.NewFake()
	fallback.SetBalance(aliceAddr, 10_000)
	s := NewRedis(rdb, slowSource{"hosted"},
		statesource.NewRPC("rpc", map[string]statesource.ChainClient{net84532: fallback}, statesource.RPCOptions{}))
	s.ReadTimeout = 200 * time.Millisecond

	start := time.Now()
	res, err := s.ReserveDetailed(context.Background(), liveReq("0xa", 1_000))
	if err != nil || res.Source != "rpc" {
		t.Fatalf("reservation = %+v, %v; want served from the fallback", res, err)
	}
	if took := time.Since(start); took > 300*time.Millisecond {
		t.Fatalf("took %v; the read budget is 200ms", took)
	}
}

// Hedging costs nothing when the preferred source is quick: the fallback is
// never asked.
func TestQuickPreferredSourceLeavesFallbackIdle(t *testing.T) {
	s, primary, fallback := twoSources(t)
	primary.SetBalance(aliceAddr, 10_000)
	fallback.SetBalance(aliceAddr, 10_000)
	before := fallback.ReadCount()

	res, err := s.ReserveDetailed(context.Background(), liveReq("0xa", 1_000))
	if err != nil || res.Source != "hosted" {
		t.Fatalf("reservation = %+v, %v", res, err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := fallback.ReadCount() - before; n != 0 {
		t.Fatalf("fallback read %d times for a payer the preferred source answered quickly", n)
	}
}
