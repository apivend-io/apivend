package statesource

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
)

const (
	net84532 = "eip155:84532"
	usdc     = "0x036cbd53842c5426634e7929541ec2318f3dcf7e"
	alice    = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"
)

var aliceAddr, _ = evm.ParseAddress(alice)

func next(t *testing.T, ch <-chan StateUpdate, want UpdateKind) StateUpdate {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case u, ok := <-ch:
			if !ok {
				t.Fatalf("stream closed while waiting for update kind %d", want)
			}
			if u.Kind == want {
				return u
			}
		case <-deadline:
			t.Fatalf("no update of kind %d", want)
		}
	}
}

func TestSnapshotCarriesBlock(t *testing.T) {
	fake := chain.NewFake()
	fake.SetBalance(aliceAddr, 5_000)
	src := NewRPC("rpc", map[string]ChainClient{net84532: fake}, RPCOptions{})

	s, err := src.Snapshot(context.Background(), net84532, usdc, alice)
	if err != nil || s.Balance.Int64() != 5_000 || s.BlockHeight != 101 || s.BlockHash == "" || s.ObservedAt.IsZero() {
		t.Fatalf("snapshot = %+v, %v", s, err)
	}
	if _, err := src.Snapshot(context.Background(), "eip155:1", usdc, alice); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unserved chain: %v", err)
	}
}

// A deposit that is only a block old must not be spendable yet; a withdrawal
// counts immediately.
func TestRecentCreditsArePending(t *testing.T) {
	fake := chain.NewFake()
	fake.SetBalance(aliceAddr, 1_000)
	fake.Mine(10)
	src := NewRPC("rpc", map[string]ChainClient{net84532: fake}, RPCOptions{Confirmations: 3})
	bal := func() int64 {
		s, err := src.Snapshot(context.Background(), net84532, usdc, alice)
		if err != nil {
			t.Fatal(err)
		}
		return s.Balance.Int64()
	}

	fake.SetBalance(aliceAddr, 9_000) // credit, just now
	if got := bal(); got != 1_000 {
		t.Fatalf("fresh credit already counted: %d", got)
	}
	fake.Mine(3)
	if got := bal(); got != 9_000 {
		t.Fatalf("confirmed credit not counted: %d", got)
	}
	fake.SetBalance(aliceAddr, 200) // debit, just now
	if got := bal(); got != 200 {
		t.Fatalf("debit not counted at once: %d", got)
	}
}

func TestWatchStreamsBalancesAndAuthEvents(t *testing.T) {
	fake := chain.NewFake()
	fake.SetBalance(aliceAddr, 5_000)
	src := NewRPC("rpc", map[string]ChainClient{net84532: fake}, RPCOptions{PollInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payers := make(chan string, 1)
	ch, err := src.Watch(ctx, Subscription{Chain: net84532, Asset: usdc, Payers: payers})
	if err != nil {
		t.Fatal(err)
	}
	payers <- alice

	u := next(t, ch, UpdateBalance)
	if u.Payer != alice || u.Balance.Int64() != 5_000 || u.Chain != net84532 || u.Asset != usdc || u.BlockHash == "" {
		t.Fatalf("update = %+v", u)
	}

	// Spent elsewhere: the nonce is reported used and the balance drops.
	nonce := [32]byte{7}
	if err := fake.Transfer(aliceAddr, nonce, big.NewInt(1_500), "0xtx"); err != nil {
		t.Fatal(err)
	}
	a := next(t, ch, UpdateAuthUsed)
	if a.Payer != alice || a.Nonce != "0x07"+zeros(62) {
		t.Fatalf("auth update = %+v", a)
	}
	for {
		if u := next(t, ch, UpdateBalance); u.Balance.Int64() == 3_500 {
			break
		}
	}

	fake.Cancel(aliceAddr, [32]byte{9})
	if c := next(t, ch, UpdateAuthCanceled); c.Nonce != "0x09"+zeros(62) {
		t.Fatalf("cancel update = %+v", c)
	}
}

// After a gap longer than the provider's log range (an outage, a slow tick),
// every event in the gap is still reported: the gap is read in ranges the
// provider accepts, over several ticks, and none is skipped.
func TestWatchReadsWholeGapInProviderRanges(t *testing.T) {
	fake := chain.NewFake()
	fake.SetBalance(aliceAddr, 5_000)
	fake.MaxLogRange = 50
	src := NewRPC("rpc", map[string]ChainClient{net84532: fake},
		RPCOptions{PollInterval: 20 * time.Millisecond, MaxLogRange: 50})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payers := make(chan string, 1)
	ch, err := src.Watch(ctx, Subscription{Chain: net84532, Asset: usdc, Payers: payers})
	if err != nil {
		t.Fatal(err)
	}
	payers <- alice
	next(t, ch, UpdateBalance)

	// 1,200 blocks between two cancellations: 24 provider ranges, more than
	// one tick's worth of queries.
	fake.Cancel(aliceAddr, [32]byte{1})
	fake.Mine(1_200)
	fake.Cancel(aliceAddr, [32]byte{2})

	if c := next(t, ch, UpdateAuthCanceled); c.Nonce != "0x01"+zeros(62) {
		t.Fatalf("first event in the gap: %+v", c)
	}
	if c := next(t, ch, UpdateAuthCanceled); c.Nonce != "0x02"+zeros(62) {
		t.Fatalf("last event in the gap: %+v", c)
	}
}

// A source that cannot read the chain must end the stream rather than leave
// the engine believing old data is being kept fresh.
func TestWatchEndsWhenChainUnreadable(t *testing.T) {
	fake := chain.NewFake()
	src := NewRPC("rpc", map[string]ChainClient{net84532: fake}, RPCOptions{PollInterval: 10 * time.Millisecond})
	payers := make(chan string, 1)
	ch, _ := src.Watch(context.Background(), Subscription{Chain: net84532, Asset: usdc, Payers: payers})
	payers <- alice
	next(t, ch, UpdateBalance)

	fake.SetErr(errors.New("rpc down"))
	deadline := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("stream stayed open with the chain unreadable")
		}
	}
}

func zeros(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '0'
	}
	return string(b)
}

// An authorization used elsewhere between the gateway first seeing a payer
// and the watch's first poll is still reported: the first poll looks back
// instead of starting at the head. (Under load the first poll can come late;
// this showed up as a flaky payerstate test.)
func TestWatchReportsEventsBeforeItsFirstPoll(t *testing.T) {
	fake := chain.NewFake()
	fake.SetBalance(aliceAddr, 5_000)
	src := NewRPC("rpc", map[string]ChainClient{net84532: fake}, RPCOptions{PollInterval: 200 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payers := make(chan string, 1)
	ch, err := src.Watch(ctx, Subscription{Chain: net84532, Asset: usdc, Payers: payers})
	if err != nil {
		t.Fatal(err)
	}
	payers <- alice
	// Spent elsewhere before the first poll (200 ms away).
	fake.Cancel(aliceAddr, [32]byte{5})
	fake.Mine(3)

	if c := next(t, ch, UpdateAuthCanceled); c.Nonce != "0x05"+zeros(62) {
		t.Fatalf("event before the first poll: %+v", c)
	}
}
