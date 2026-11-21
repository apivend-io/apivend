package statesource

import (
	"context"
	"encoding/hex"
	"math/big"
	"sort"
	"time"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
)

// ChainClient is the node access the RPC source needs. *chain.RPC and
// *chain.Fake implement it.
type ChainClient interface {
	Head(ctx context.Context) (number uint64, hash string, err error)
	BalancesAt(ctx context.Context, token evm.Address, owners []evm.Address, block uint64, blockHash string) ([]*big.Int, error)
	AuthEvents(ctx context.Context, token evm.Address, authorizers []evm.Address, from, to uint64) ([]chain.AuthEvent, error)
	AuthorizationState(ctx context.Context, token, authorizer evm.Address, nonce [32]byte) (bool, error)
	AuthorizationStateAt(ctx context.Context, token, authorizer evm.Address, nonce [32]byte, blockHash string) (bool, error)
}

// RPCOptions tunes the RPC source.
type RPCOptions struct {
	// PollInterval is how often Watch looks at the chain. Default 2s, about
	// one Base block.
	PollInterval time.Duration
	// Confirmations holds recent credits back: a balance increase counts only
	// once it is this many blocks old, while a decrease counts at once. It
	// protects against reorged deposits. Default 0 (off).
	Confirmations uint64
	// IdleAfter stops watching a payer that has not been re-sent on the
	// subscription for this long. Default 10m.
	IdleAfter time.Duration
	// MaxLogRange caps the block range of one event query. Default 200, the
	// limit of the public Base endpoints; a longer gap is read in several
	// queries, never skipped.
	MaxLogRange uint64
	Now         func() time.Time
}

// maxLogQueriesPerTick bounds the event queries one Watch tick makes while
// catching up after a gap, so a long outage is worked off over several ticks
// instead of one tick that never finishes within its timeout.
const maxLogQueriesPerTick = 10

// RPC reads chain state directly from nodes, polling for changes.
//
// It does not use websocket log subscriptions.
type RPC struct {
	name   string
	chains map[string]ChainClient // by CAIP-2 network
	opt    RPCOptions
}

func NewRPC(name string, chains map[string]ChainClient, opt RPCOptions) *RPC {
	if opt.PollInterval <= 0 {
		opt.PollInterval = 2 * time.Second
	}
	if opt.IdleAfter <= 0 {
		opt.IdleAfter = 10 * time.Minute
	}
	if opt.MaxLogRange == 0 {
		opt.MaxLogRange = 200
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	return &RPC{name: name, chains: chains, opt: opt}
}

func (r *RPC) Name() string { return r.name }

// KeepWarm keeps a connection open to every endpoint of every chain whose
// client supports it (chain.RPC does), calling each every interval until ctx
// ends. See chain.RPC.KeepWarm.
func (r *RPC) KeepWarm(ctx context.Context, every time.Duration) {
	for _, c := range r.chains {
		if w, ok := c.(interface {
			KeepWarm(context.Context, time.Duration)
		}); ok {
			go w.KeepWarm(ctx, every)
		}
	}
}

// Chains lists the networks this source serves.
func (r *RPC) Chains() []string {
	out := make([]string, 0, len(r.chains))
	for c := range r.chains {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func (r *RPC) Snapshot(ctx context.Context, chainID, asset, payer string) (PayerSnapshot, error) {
	c, ok := r.chains[chainID]
	if !ok {
		return PayerSnapshot{}, ErrUnsupported
	}
	token, err := evm.ParseAddress(asset)
	if err != nil {
		return PayerSnapshot{}, err
	}
	owner, err := evm.ParseAddress(payer)
	if err != nil {
		return PayerSnapshot{}, err
	}
	height, hash, err := c.Head(ctx)
	if err != nil {
		return PayerSnapshot{}, err
	}
	bals, err := r.balances(ctx, c, token, []evm.Address{owner}, height, hash)
	if err != nil {
		return PayerSnapshot{}, err
	}
	return PayerSnapshot{Balance: bals[0], BlockHeight: height, BlockHash: hash, ObservedAt: r.opt.Now()}, nil
}

// balances reads every owner's balance at one block, by hash, so all answers
// come from the same fully processed block. With Confirmations set it also
// reads the balance that many blocks earlier and reports the smaller.
func (r *RPC) balances(ctx context.Context, c ChainClient, token evm.Address, owners []evm.Address, height uint64, hash string) ([]*big.Int, error) {
	now, err := c.BalancesAt(ctx, token, owners, height, hash)
	if err != nil {
		return nil, err
	}
	if r.opt.Confirmations == 0 || height < r.opt.Confirmations {
		return now, nil
	}
	settled, err := c.BalancesAt(ctx, token, owners, height-r.opt.Confirmations, "")
	if err != nil {
		return nil, err
	}
	for i := range now {
		if settled[i].Cmp(now[i]) < 0 {
			now[i] = settled[i]
		}
	}
	return now, nil
}

func (r *RPC) AuthorizationState(ctx context.Context, chainID, asset, payer, nonce string) (AuthState, error) {
	c, ok := r.chains[chainID]
	if !ok {
		return AuthUnused, ErrUnsupported
	}
	token, err := evm.ParseAddress(asset)
	if err != nil {
		return AuthUnused, err
	}
	owner, err := evm.ParseAddress(payer)
	if err != nil {
		return AuthUnused, err
	}
	n, err := evm.ParseBytes32(nonce)
	if err != nil {
		return AuthUnused, err
	}
	used, err := c.AuthorizationState(ctx, token, owner, n)
	if err != nil || !used {
		return AuthUnused, err
	}
	return AuthUsed, nil
}

// AuthorizationStateObserved is AuthorizationState together with the block
// the answer is based on. The hosted service uses it so that every answer it
// gives can be spot-checked.
func (r *RPC) AuthorizationStateObserved(ctx context.Context, chainID, asset, payer, nonce string) (AuthState, PayerSnapshot, error) {
	c, ok := r.chains[chainID]
	if !ok {
		return AuthUnused, PayerSnapshot{}, ErrUnsupported
	}
	token, err := evm.ParseAddress(asset)
	if err != nil {
		return AuthUnused, PayerSnapshot{}, err
	}
	owner, err := evm.ParseAddress(payer)
	if err != nil {
		return AuthUnused, PayerSnapshot{}, err
	}
	n, err := evm.ParseBytes32(nonce)
	if err != nil {
		return AuthUnused, PayerSnapshot{}, err
	}
	height, hash, err := c.Head(ctx)
	if err != nil {
		return AuthUnused, PayerSnapshot{}, err
	}
	used, err := c.AuthorizationStateAt(ctx, token, owner, n, hash)
	if err != nil {
		return AuthUnused, PayerSnapshot{}, err
	}
	state := AuthUnused
	if used {
		state = AuthUsed
	}
	return state, PayerSnapshot{BlockHeight: height, BlockHash: hash, ObservedAt: r.opt.Now()}, nil
}

func (r *RPC) Watch(ctx context.Context, sub Subscription) (<-chan StateUpdate, error) {
	c, ok := r.chains[sub.Chain]
	if !ok {
		return nil, ErrUnsupported
	}
	token, err := evm.ParseAddress(sub.Asset)
	if err != nil {
		return nil, err
	}
	out := make(chan StateUpdate, 256)
	go r.watch(ctx, c, token, sub, out)
	return out, nil
}

func (r *RPC) watch(ctx context.Context, c ChainClient, token evm.Address, sub Subscription, out chan<- StateUpdate) {
	defer close(out)
	seen := map[evm.Address]time.Time{} // payer -> last reminded
	var lastBlock uint64
	failures := 0
	tick := time.NewTicker(r.opt.PollInterval)
	defer tick.Stop()

	emit := func(u StateUpdate) bool {
		u.Chain, u.Asset = sub.Chain, sub.Asset
		select {
		case out <- u:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-sub.Payers:
			if !ok {
				return
			}
			if a, err := evm.ParseAddress(p); err == nil {
				seen[a] = r.opt.Now()
			}
			continue
		case <-tick.C:
		}

		now := r.opt.Now()
		owners := make([]evm.Address, 0, len(seen))
		for a, at := range seen {
			if now.Sub(at) > r.opt.IdleAfter {
				delete(seen, a)
				continue
			}
			owners = append(owners, a)
		}
		if len(owners) == 0 {
			continue
		}

		pctx, cancel := context.WithTimeout(ctx, 4*r.opt.PollInterval)
		height, hash, err := c.Head(pctx)
		var bals []*big.Int
		var events []chain.AuthEvent
		if err == nil {
			bals, err = r.balances(pctx, c, token, owners, height, hash)
		}
		// Read every block since the last tick, in ranges the provider
		// accepts. A long gap (after an outage) is read over several ticks,
		// never skipped: a missed AuthorizationUsed would leave a nonce spent
		// elsewhere looking unused.
		scanned := lastBlock
		if err == nil && lastBlock == 0 {
			// First poll: look back one provider range rather than starting
			// at the head, so an authorization used between the gateway first
			// seeing the payer and this poll is still reported. Reporting an
			// older use is harmless: a used nonce stays used.
			if height > r.opt.MaxLogRange {
				scanned = height - r.opt.MaxLogRange
			}
		}
		for chunk := 0; err == nil && chunk < maxLogQueriesPerTick && scanned < height; chunk++ {
			from := scanned + 1
			to := min(height, from+r.opt.MaxLogRange-1)
			var evs []chain.AuthEvent
			if evs, err = c.AuthEvents(pctx, token, owners, from, to); err == nil {
				events = append(events, evs...)
				scanned = to
			}
		}
		cancel()
		if err != nil {
			// A few misses are tolerated; then the stream ends so the engine
			// can move to another source instead of trusting stale data.
			if failures++; failures >= 3 {
				return
			}
			continue
		}
		failures = 0
		lastBlock = scanned

		for _, e := range events {
			kind := UpdateAuthUsed
			if e.Canceled {
				kind = UpdateAuthCanceled
			}
			if !emit(StateUpdate{Kind: kind, Payer: e.Authorizer.Hex(), Nonce: "0x" + hex.EncodeToString(e.Nonce[:]),
				BlockHeight: e.Block, BlockHash: e.BlockHash, ObservedAt: now}) {
				return
			}
		}
		// Every tick re-states every balance, changed or not: that is what
		// keeps the engine's copy fresh.
		for i, a := range owners {
			if !emit(StateUpdate{Kind: UpdateBalance, Payer: a.Hex(), Balance: bals[i],
				BlockHeight: height, BlockHash: hash, ObservedAt: now}) {
				return
			}
		}
	}
}
