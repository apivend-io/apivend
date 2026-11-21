package payerstate

import (
	"context"
	"sync"
	"time"

	"apivend.io/apivend/engine/statesource"
)

// Keeping payers fresh off the request path. For every (chain, asset) with
// traffic the engine holds one Watch stream on the first healthy source and
// applies what it reports: balances into the cache, used or cancelled
// authorizations into the nonce set. Only payers this gateway has seen
// recently are watched.

const (
	// watchPayerFor is how long after its last payment a payer stays watched.
	watchPayerFor = 10 * time.Minute
	// watchRemind is how often active payers are re-sent on the subscription,
	// and how often a fallback stream checks whether a preferred source is back.
	watchRemind = 30 * time.Second
)

type watcher struct {
	chain, asset string
	mu           sync.Mutex
	seen         map[string]time.Time
	added        chan string
}

// Run starts background watching and blocks until ctx ends. Without it the
// engine still works, reading state on demand for unknown or stale payers.
func (r *Redis) Run(ctx context.Context) {
	r.mu.Lock()
	r.runCtx = ctx
	for _, w := range r.watchers {
		go r.watchLoop(ctx, w)
	}
	r.mu.Unlock()
	<-ctx.Done()
}

// watch notes that a payer is active and makes sure its asset is watched.
func (r *Redis) watch(chain, asset, payer string) {
	if len(r.sources) == 0 {
		return
	}
	key := chain + "|" + asset
	r.mu.Lock()
	w := r.watchers[key]
	if w == nil {
		w = &watcher{chain: chain, asset: asset, seen: map[string]time.Time{}, added: make(chan string, 1024)}
		r.watchers[key] = w
		if r.runCtx != nil {
			go r.watchLoop(r.runCtx, w)
		}
	}
	r.mu.Unlock()

	w.mu.Lock()
	_, known := w.seen[payer]
	w.seen[payer] = time.Now()
	w.mu.Unlock()
	if !known {
		select {
		case w.added <- payer:
		default: // picked up by the next reminder
		}
	}
}

func (w *watcher) active() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.seen))
	for p, at := range w.seen {
		if time.Since(at) > watchPayerFor {
			delete(w.seen, p)
			continue
		}
		out = append(out, p)
	}
	return out
}

func (r *Redis) watchLoop(ctx context.Context, w *watcher) {
	log := r.logger().With("network", w.chain, "asset", w.asset)
	for ctx.Err() == nil {
		served := false
		for i, src := range r.sources {
			if !r.healthy(src.Name()) {
				continue
			}
			sctx, cancel := context.WithCancel(ctx)
			payers := make(chan string, 1024)
			updates, err := src.Watch(sctx, statesource.Subscription{Chain: w.chain, Asset: w.asset, Payers: payers})
			if err != nil {
				cancel()
				if err != statesource.ErrUnsupported {
					log.Warn("state watch failed to start; trying the next source", "source", src.Name(), "err", err)
					r.markUnhealthy(src.Name(), watchRemind)
				}
				continue
			}
			served = true
			log.Info("watching payer state", "source", src.Name())
			ended := r.consume(sctx, w, src, i, updates, payers)
			cancel()
			if ctx.Err() != nil {
				return
			}
			if ended {
				log.Warn("state watch ended; failing over", "source", src.Name())
				r.markUnhealthy(src.Name(), watchRemind)
			}
			break // start again from the most preferred source
		}
		if !served {
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

// consume applies one stream's updates. It reports true when the stream
// ended on its own, false when it was left to return to a preferred source.
func (r *Redis) consume(ctx context.Context, w *watcher, src statesource.Source, index int, updates <-chan statesource.StateUpdate, payers chan<- string) bool {
	send := func(p string) {
		select {
		case payers <- p:
		default:
		}
	}
	for _, p := range w.active() {
		send(p)
	}
	remind := time.NewTicker(watchRemind)
	defer remind.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case p := <-w.added:
			send(p)
		case <-remind.C:
			for _, p := range w.active() {
				send(p)
			}
			for _, better := range r.sources[:index] {
				if r.healthy(better.Name()) {
					return false
				}
			}
		case u, ok := <-updates:
			if !ok {
				return true
			}
			actx, cancel := context.WithTimeout(ctx, 2*time.Second)
			var err error
			switch u.Kind {
			case statesource.UpdateBalance:
				err = r.SetSnapshot(actx, u.Chain, u.Asset, u.Payer, src.Name(), statesource.PayerSnapshot{
					Balance: u.Balance, BlockHeight: u.BlockHeight, BlockHash: u.BlockHash, ObservedAt: u.ObservedAt})
			case statesource.UpdateAuthUsed, statesource.UpdateAuthCanceled:
				err = r.MarkAuthorizationUsed(actx, u.Chain, u.Asset, u.Payer, u.Nonce)
			}
			cancel()
			if err != nil {
				r.logger().Warn("could not apply state update", "source", src.Name(), "err", err)
			}
		}
	}
}
