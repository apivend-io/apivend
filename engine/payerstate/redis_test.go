package payerstate

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/statesource"
	"apivend.io/apivend/internal/redistest"
)

const (
	net84532 = "eip155:84532"
	usdc     = "0x036cbd53842c5426634e7929541ec2318f3dcf7e"
	alice    = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"
)

var aliceAddr, _ = evm.ParseAddress(alice)

type fixture struct {
	t     *testing.T
	state *Redis
	chain *chain.Fake
	clock *time.Time
}

func newFixture(t *testing.T) *fixture {
	rdb, _ := redistest.Start(t)
	fake := chain.NewFake()
	now := time.Unix(1_800_000_000, 0)
	s := NewRedis(rdb, statesource.NewRPC("rpc", map[string]statesource.ChainClient{net84532: fake},
		statesource.RPCOptions{Now: func() time.Time { return now }}))
	s.now = func() time.Time { return now }
	s.ReadTimeout = time.Second
	return &fixture{t: t, state: s, chain: fake, clock: &now}
}

func (f *fixture) advance(d time.Duration) { *f.clock = f.clock.Add(d) }

func (f *fixture) req(nonce string, amount int64) Request {
	return Request{
		Chain: net84532, Asset: usdc, Payer: alice, Nonce: nonce,
		Amount: big.NewInt(amount),
		Expiry: f.clock.Add(60 * time.Second),
		Policy: Policy{Freshness: 5 * time.Second},
	}
}

func (f *fixture) reserve(nonce string, amount int64) ReservationID {
	f.t.Helper()
	id, err := f.state.Reserve(context.Background(), f.req(nonce, amount))
	if err != nil {
		f.t.Fatalf("Reserve(%s, %d): %v", nonce, amount, err)
	}
	return id
}

func (f *fixture) denied(req Request, reason string) {
	f.t.Helper()
	_, err := f.state.Reserve(context.Background(), req)
	var d *DeniedError
	if !errors.As(err, &d) || d.Reason != reason {
		f.t.Fatalf("Reserve = %v, want denied %s", err, reason)
	}
}

func (f *fixture) exposure() int64 {
	f.t.Helper()
	v, err := f.state.Exposure(context.Background(), net84532, usdc, alice)
	if err != nil {
		f.t.Fatal(err)
	}
	return v
}

func job(f *fixture) Job { return Job{Body: []byte(`{}`), Deadline: f.clock.Add(60 * time.Second)} }

func TestBalanceIsNetOfHolds(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 25_000)

	a := f.reserve("0xa", 10_000)
	f.reserve("0xb", 10_000)
	// 5 000 left: the third does not fit even though the chain says 25 000.
	f.denied(f.req("0xc", 10_000), ReasonInsufficientBalance)
	if got := f.exposure(); got != 20_000 {
		t.Fatalf("exposure = %d", got)
	}

	// Backend failed for the first: its funds are spendable again.
	if err := f.state.Release(ctx, a); err != nil {
		t.Fatal(err)
	}
	f.reserve("0xc", 10_000)
	if n := f.chain.ReadCount(); n != 1 {
		t.Fatalf("chain read %d times; want 1 (cache)", n)
	}
}

func TestNonceReplay(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 100_000)

	id := f.reserve("0xa", 10_000)
	f.denied(f.req("0xa", 10_000), ReasonNonceUsed) // in flight

	// Released: the client may retry with the same authorization.
	f.state.Release(ctx, id)
	id = f.reserve("0xa", 10_000)

	// Committed: used for good, and it cannot be released any more.
	if err := f.state.Commit(ctx, id, job(f)); err != nil {
		t.Fatal(err)
	}
	f.denied(f.req("0xa", 10_000), ReasonNonceUsed)
	if err := f.state.Release(ctx, id); err == nil {
		t.Fatal("released a committed reservation")
	}
	if got := f.exposure(); got != 10_000 {
		t.Fatalf("exposure = %d", got)
	}
	// Still used after settlement.
	f.state.Settled(ctx, id, 0)
	f.denied(f.req("0xa", 10_000), ReasonNonceUsed)
}

// The chain reports an authorization used (spent at another merchant) while
// this gateway holds it; the request then fails and the hold is released. The
// release must not erase what the chain said: the nonce stays used.
func TestReleaseKeepsAuthorizationUsedOnChain(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 100_000)

	id := f.reserve("0xa", 10_000)
	if err := f.state.MarkAuthorizationUsed(ctx, net84532, usdc, alice, "0xa"); err != nil {
		t.Fatal(err)
	}
	if err := f.state.Release(ctx, id); err != nil {
		t.Fatal(err)
	}
	f.denied(f.req("0xa", 10_000), ReasonNonceUsed)
	if got := f.exposure(); got != 0 {
		t.Fatalf("exposure = %d after release, want 0", got)
	}
}

func TestExposureCap(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 1_000_000)
	capped := func(nonce string) Request {
		r := f.req(nonce, 10_000)
		r.Policy.MaxUnsettled = big.NewInt(25_000)
		return r
	}
	var ids []ReservationID
	for _, n := range []string{"0xa", "0xb"} {
		id, err := f.state.Reserve(ctx, capped(n))
		if err != nil {
			t.Fatal(err)
		}
		f.state.Commit(ctx, id, job(f))
		ids = append(ids, id)
	}
	// Plenty of balance, but 20 000 is already served and unsettled.
	f.denied(capped("0xc"), ReasonExposureLimit)
	// Settlement frees room.
	if err := f.state.Settled(ctx, ids[0], 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.state.Reserve(ctx, capped("0xc")); err != nil {
		t.Fatal(err)
	}
}

func TestStaleBalanceIsReread(t *testing.T) {
	f := newFixture(t)
	f.chain.SetBalance(aliceAddr, 10_000)
	f.reserve("0xa", 5_000)

	// The payer moves the money elsewhere. Within the freshness window the
	// cache still says yes (this is the bounded risk the exposure cap covers).
	f.chain.SetBalance(aliceAddr, 0)
	f.advance(4 * time.Second)
	f.reserve("0xb", 5_000)

	// Past the window the chain is consulted again.
	f.advance(2 * time.Second)
	f.denied(f.req("0xc", 1), ReasonInsufficientBalance)
	if n := f.chain.ReadCount(); n != 2 {
		t.Fatalf("chain read %d times; want 2", n)
	}
}

func TestChainUnavailable(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetErr(errors.New("rpc down"))

	_, err := f.state.Reserve(ctx, f.req("0xa", 10_000))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want unavailable", err)
	}
	if got := f.exposure(); got != 0 {
		t.Fatalf("exposure = %d after refused reservation", got)
	}

	// Routes that prefer availability still get nonce and exposure checks.
	r := f.req("0xa", 10_000)
	r.Policy.AllowOnUnavailable = true
	r.Policy.MaxUnsettled = big.NewInt(15_000)
	if _, err := f.state.Reserve(ctx, r); err != nil {
		t.Fatal(err)
	}
	f.denied(r, ReasonNonceUsed)
	r.Nonce = "0xb"
	f.denied(r, ReasonExposureLimit)
}

func TestNoReaderSkipsBalance(t *testing.T) {
	f := newFixture(t)
	f.state.sources = nil
	f.reserve("0xa", 10_000)
	f.denied(f.req("0xa", 10_000), ReasonNonceUsed)
}

func TestAbandonedHoldLapses(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 10_000)
	id := f.reserve("0xa", 10_000)
	// The filter crashed: no commit, no release.
	f.advance(61 * time.Second)

	if err := f.state.Commit(ctx, id, job(f)); !errors.Is(err, ErrExpired) {
		t.Fatalf("late commit = %v, want ErrExpired", err)
	}
	// The funds are free again for the next request.
	f.reserve("0xb", 10_000)
	if got := f.exposure(); got != 10_000 {
		t.Fatalf("exposure = %d", got)
	}
	if depth, _, _ := f.state.QueueStats(ctx); depth != 0 {
		t.Fatalf("expired reservation was queued for settlement")
	}
}

func TestSettlementDebitsCachedBalance(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 10_000) // read at block 101
	id := f.reserve("0xa", 10_000)
	f.state.Commit(ctx, id, job(f))

	// Settled in block 105. The hold is gone but the cached balance (block
	// 101) predates the transfer; it must not look spendable again.
	if err := f.state.Settled(ctx, id, 105); err != nil {
		t.Fatal(err)
	}
	if got := f.exposure(); got != 0 {
		t.Fatalf("exposure = %d", got)
	}
	f.denied(f.req("0xb", 1), ReasonInsufficientBalance)
}

func TestFailedSettlementBlocksPayer(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 100_000)
	id := f.reserve("0xa", 10_000)
	f.state.Commit(ctx, id, job(f))

	if err := f.state.Failed(ctx, id, "insufficient_funds", time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := f.exposure(); got != 0 {
		t.Fatalf("exposure = %d", got)
	}
	f.denied(f.req("0xb", 10_000), ReasonPayerBlocked)
	f.state.Unblock(ctx, net84532, usdc, alice)
	f.reserve("0xb", 10_000)
}

func TestQueueOrderLeaseAndRetry(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 100_000)

	commit := func(nonce string, validFor time.Duration) ReservationID {
		r := f.req(nonce, 1_000)
		r.Expiry = f.clock.Add(validFor)
		id, err := f.state.Reserve(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.state.Commit(ctx, id, Job{Body: []byte(nonce), Deadline: r.Expiry}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	late := commit("0x1a7e", 300*time.Second)
	soon := commit("0x500", 30*time.Second)
	// Committing twice must not enqueue twice.
	f.state.Commit(ctx, soon, Job{Body: []byte("dup"), Deadline: f.clock.Add(time.Second)})
	if depth, earliest, _ := f.state.QueueStats(ctx); depth != 2 || !earliest.Equal(f.clock.Add(30*time.Second)) {
		t.Fatalf("queue depth %d, earliest %v", depth, earliest)
	}

	// Earliest deadline first, regardless of arrival order.
	c1, err := f.state.Claim(ctx, 10*time.Second)
	if err != nil || c1 == nil || c1.ID != soon || string(c1.Body) != "0x500" || c1.Attempt != 1 {
		t.Fatalf("first claim = %+v, %v", c1, err)
	}
	c2, _ := f.state.Claim(ctx, 10*time.Second)
	if c2 == nil || c2.ID != late {
		t.Fatalf("second claim = %+v", c2)
	}
	if c3, _ := f.state.Claim(ctx, 10*time.Second); c3 != nil {
		t.Fatalf("leased job claimed twice: %+v", c3)
	}

	// The settler holding c1 dies. After the lease, the job comes back.
	f.advance(11 * time.Second)
	again, _ := f.state.Claim(ctx, 10*time.Second)
	if again == nil || again.ID != soon || again.Attempt != 2 {
		t.Fatalf("lease did not expire: %+v", again)
	}

	// Explicit retry with backoff.
	f.state.Retry(ctx, soon, 5*time.Second)
	f.state.Settled(ctx, late, 0)
	if c, _ := f.state.Claim(ctx, 10*time.Second); c != nil {
		t.Fatalf("claimed during backoff: %+v", c)
	}
	f.advance(6 * time.Second)
	if c, _ := f.state.Claim(ctx, 10*time.Second); c == nil || c.ID != soon || c.Attempt != 3 {
		t.Fatalf("after backoff: %+v", c)
	}
	f.state.Settled(ctx, soon, 0)
	if depth, _, _ := f.state.QueueStats(ctx); depth != 0 {
		t.Fatalf("queue depth %d after settling everything", depth)
	}
}

// Two gateway replicas share one Redis. Many requests race for a balance that
// covers only some of them; exactly that many may win, and each nonce once.
func TestConcurrentReplicasCannotDoubleSpend(t *testing.T) {
	rdb, _ := redistest.Start(t)
	fake := chain.NewFake()
	fake.SetBalance(aliceAddr, 100_000) // covers 10 payments of 10 000
	replicas := []*Redis{
		NewRedis(rdb, statesource.NewRPC("rpc", map[string]statesource.ChainClient{net84532: fake}, statesource.RPCOptions{})),
		NewRedis(rdb, statesource.NewRPC("rpc", map[string]statesource.ChainClient{net84532: fake}, statesource.RPCOptions{})),
	}
	for _, r := range replicas {
		r.ReadTimeout = 2 * time.Second
	}

	const distinct, dupes = 40, 3
	var won, insufficient, replayed, other atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < distinct; i++ {
		for d := 0; d < dupes; d++ {
			wg.Add(1)
			go func(i, d int) {
				defer wg.Done()
				_, err := replicas[(i+d)%2].Reserve(context.Background(), Request{
					Chain: net84532, Asset: usdc, Payer: alice,
					Nonce:  fmt.Sprintf("0x%04x", i),
					Amount: big.NewInt(10_000),
					Expiry: time.Now().Add(time.Minute),
					Policy: Policy{Freshness: time.Minute},
				})
				var denied *DeniedError
				switch {
				case err == nil:
					won.Add(1)
				case errors.As(err, &denied) && denied.Reason == ReasonInsufficientBalance:
					insufficient.Add(1)
				case errors.As(err, &denied) && denied.Reason == ReasonNonceUsed:
					replayed.Add(1)
				default:
					other.Add(1)
					t.Errorf("unexpected: %v", err)
				}
			}(i, d)
		}
	}
	wg.Wait()

	if won.Load() != 10 {
		t.Fatalf("%d reservations succeeded against a balance that covers 10", won.Load())
	}
	held, _ := replicas[0].Exposure(context.Background(), net84532, usdc, alice)
	if held != 100_000 {
		t.Fatalf("held = %d, want 100000", held)
	}
	t.Logf("won=%d insufficient=%d replayed=%d", won.Load(), insufficient.Load(), replayed.Load())
}

// The per-route cap counts every payer's served-but-unsettled amount on the
// route: fresh payer addresses do not get around it.
func TestRouteExposureCap(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 1_000_000)
	other := evm.Address{0xb0, 0xb0}
	f.chain.SetBalance(other, 1_000_000)
	on := func(payer, nonce, route string) Request {
		r := f.req(nonce, 10_000)
		r.Payer, r.Route = payer, route
		r.Policy.MaxUnsettledPerRoute = big.NewInt(25_000)
		return r
	}
	serve := func(r Request) ReservationID {
		t.Helper()
		id, err := f.state.Reserve(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		j := job(f)
		j.Route = r.Route
		if err := f.state.Commit(ctx, id, j); err != nil {
			t.Fatal(err)
		}
		return id
	}
	a := serve(on(alice, "0xa", "search"))
	serve(on(strings.ToLower(other.Hex()), "0xb", "search"))
	// 20 000 served and unsettled on "search", by two payers: a third
	// payment of 10 000 would pass 25 000, whoever pays.
	f.denied(on(strings.ToLower(other.Hex()), "0xc", "search"), ReasonRouteExposureLimit)
	f.denied(on(alice, "0xd", "search"), ReasonRouteExposureLimit)
	// Another route has its own budget; a request without a route or a
	// cap is not limited by it.
	serve(on(alice, "0xe", "maps"))
	r := on(alice, "0xf", "search")
	r.Policy.MaxUnsettledPerRoute = nil
	if _, err := f.state.Reserve(ctx, r); err != nil {
		t.Fatalf("no cap: %v", err)
	}
	// A refusal holds nothing, and settlement frees room.
	if err := f.state.Settled(ctx, a, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.state.Reserve(ctx, on(alice, "0xd", "search")); err != nil {
		t.Fatalf("after settlement: %v", err)
	}
}

func TestExposureByRoute(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.chain.SetBalance(aliceAddr, 100_000)
	commit := func(nonce, route string, amount int64) ReservationID {
		id := f.reserve(nonce, amount)
		j := job(f)
		j.Route = route
		if err := f.state.Commit(ctx, id, j); err != nil {
			t.Fatal(err)
		}
		f.state.Commit(ctx, id, j) // repeat must not double count
		return id
	}
	a := commit("0xa", "search", 10_000)
	b := commit("0xb", "search", 5_000)
	c := commit("0xc", "maps", 1_000)
	f.reserve("0xd", 7_000) // in flight only: not served, not counted

	want := func(search, maps int64) {
		t.Helper()
		got, err := f.state.ExposureByRoute(ctx)
		if err != nil || got["search"] != search || got["maps"] != maps {
			t.Fatalf("exposure by route = %v, %v; want search=%d maps=%d", got, err, search, maps)
		}
	}
	want(15_000, 1_000)
	f.state.Settled(ctx, a, 0)
	f.state.Settled(ctx, a, 0) // repeat
	want(5_000, 1_000)
	f.state.Failed(ctx, b, "insufficient_funds", 0)
	f.state.Settled(ctx, c, 0)
	want(0, 0)
}

func TestResultWait(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	id := makeID(net84532, usdc, alice, "0xa")

	start := time.Now()
	if res, err := f.state.WaitResult(ctx, id, time.Second); res != nil || err != nil {
		t.Fatalf("WaitResult with nothing published = %v, %v", res, err)
	}
	if time.Since(start) < 900*time.Millisecond {
		t.Fatal("WaitResult returned before its timeout")
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		f.state.PublishResult(ctx, id, Result{Status: "settled", Transaction: "0xabc"})
	}()
	res, err := f.state.WaitResult(ctx, id, 5*time.Second)
	if err != nil || res == nil || res.Status != "settled" || res.Transaction != "0xabc" {
		t.Fatalf("WaitResult = %+v, %v", res, err)
	}
}
