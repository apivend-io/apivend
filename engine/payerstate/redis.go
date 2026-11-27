package payerstate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"apivend.io/apivend/engine/statesource"
)

// Data model. Per payer and asset, under the prefix x402:{chain|asset|payer}:
//
//	bal      hash    balance, block, at (ms)   cached on-chain balance
//	holds    zset    nonce -> lapse time (ms)  in-flight holds; committed = +inf
//	amounts  hash    nonce -> amount           size of each hold
//	held     string  sum of amounts            the payer's unsettled exposure
//	nonces   zset    nonce -> validBefore (ms) authorizations seen
//	blocked  string  reason                    set after a failed settlement
//
// Global: x402:settle:queue (zset job -> deadline), :jobs, :busy, :attempts.
//
// Available funds are balance - held. Every transition is one Lua script, so
// concurrent replicas cannot both spend the same balance or the same nonce.
//
// Amounts are compared as Lua numbers, which are exact up to 2^53. Cached
// balances are clamped to that (about 9 billion USDC: "at least this much")
// and larger payment amounts are rejected.
//
// The scripts touch per-payer and global keys together, so this needs a
// single Redis (or a primary with replicas), not Redis Cluster.

const maxExact = 1<<53 - 1

// DefaultFreshness applies when a route does not set one.
const DefaultFreshness = 5 * time.Second

var bigMaxExact = big.NewInt(maxExact)

const (
	keyQueue    = "x402:settle:queue"
	keyJobs     = "x402:settle:jobs"
	keyBusy     = "x402:settle:busy"
	keyAttempts = "x402:settle:attempts"
	keyTx       = "x402:settle:tx"
	keyRoutes   = "x402:settle:routes"   // job -> route
	keyExposure = "x402:settle:exposure" // route -> committed, unsettled amount
)

var reserveScript = redis.NewScript(`
-- KEYS: bal holds amounts held nonces blocked exposure
-- ARGV: now amount expiry maxUnsettled freshness nonce skipBalance route maxRoute
local now = tonumber(ARGV[1])
if redis.call('EXISTS', KEYS[6]) == 1 then return 'payer_blocked' end

-- Holds whose authorization expired were never committed or released (the
-- filter died mid-request). Free them.
for _, n in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)) do
  local a = redis.call('HGET', KEYS[3], n)
  if a then redis.call('DECRBY', KEYS[4], a) end
  redis.call('HDEL', KEYS[3], n)
  redis.call('ZREM', KEYS[2], n)
end
redis.call('ZREMRANGEBYSCORE', KEYS[5], '-inf', now)

if redis.call('ZSCORE', KEYS[5], ARGV[6]) then return 'payment_already_used' end

local held = tonumber(redis.call('GET', KEYS[4]) or '0')
local amount = tonumber(ARGV[2])
if ARGV[4] ~= '' and held + amount > tonumber(ARGV[4]) then return 'exposure_limit' end
-- The route's served-but-unsettled amount, across all payers: payer
-- addresses are free, so the per-payer cap alone does not bound a route.
if ARGV[9] ~= '' then
  local exposed = tonumber(redis.call('HGET', KEYS[7], ARGV[8]) or '0')
  if exposed + amount > tonumber(ARGV[9]) then return 'route_exposure_limit' end
end

if ARGV[7] ~= '1' then
  local b = redis.call('HMGET', KEYS[1], 'balance', 'at')
  if not b[1] then return 'unknown' end
  if now - tonumber(b[2]) > tonumber(ARGV[5]) then return 'stale' end
  if tonumber(b[1]) - held < amount then return 'insufficient_balance' end
end

redis.call('ZADD', KEYS[2], ARGV[3], ARGV[6])
redis.call('HSET', KEYS[3], ARGV[6], ARGV[2])
redis.call('INCRBY', KEYS[4], ARGV[2])
redis.call('ZADD', KEYS[5], ARGV[3], ARGV[6])
-- Report which state the decision rested on.
local basis = redis.call('HMGET', KEYS[1], 'source', 'block')
if ARGV[7] == '1' or not basis[1] then return 'ok||0' end
return 'ok|' .. basis[1] .. '|' .. (basis[2] or '0')
`)

var releaseScript = redis.NewScript(`
-- KEYS: holds amounts held nonces   ARGV: nonce
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score then return 0 end
if score == 'inf' then return -1 end
local a = redis.call('HGET', KEYS[2], ARGV[1])
if a then redis.call('DECRBY', KEYS[3], a) end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
-- The nonce entry is this hold's, unless the chain has since reported the
-- authorization used or cancelled (+inf): that must survive the release.
if redis.call('ZSCORE', KEYS[4], ARGV[1]) ~= 'inf' then
  redis.call('ZREM', KEYS[4], ARGV[1])
end
return 1
`)

var commitScript = redis.NewScript(`
-- KEYS: holds queue jobs amounts routes exposure
-- ARGV: nonce now jobID body deadline route
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score then return 0 end
if score == 'inf' then return 1 end
if tonumber(score) <= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], '+inf', ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[3])
redis.call('HSET', KEYS[3], ARGV[3], ARGV[4])
local a = redis.call('HGET', KEYS[4], ARGV[1])
if a then
  redis.call('HSET', KEYS[5], ARGV[3], ARGV[6])
  redis.call('HINCRBY', KEYS[6], ARGV[6], a)
end
return 1
`)

var setBalanceScript = redis.NewScript(`
-- KEYS: bal   ARGV: balance block at source hash
local cur = redis.call('HGET', KEYS[1], 'block')
if cur and tonumber(cur) > tonumber(ARGV[2]) then return 0 end
redis.call('HSET', KEYS[1], 'balance', ARGV[1], 'block', ARGV[2], 'at', ARGV[3], 'source', ARGV[4], 'hash', ARGV[5])
redis.call('EXPIRE', KEYS[1], 86400)
return 1
`)

// markAuthScript records a nonce the chain reports as used or cancelled, so a
// payment carrying it is refused. The entry never lapses.
var markAuthScript = redis.NewScript(`
-- KEYS: nonces   ARGV: nonce
redis.call('ZADD', KEYS[1], '+inf', ARGV[1])
return 1
`)

// Redis is the shared PayerState.
type Redis struct {
	rdb redis.UniversalClient
	// sources supply balances and authorization events, in order of
	// preference. A chain no source serves gets nonce and exposure checks
	// only.
	sources []statesource.Source
	// ReadTimeout bounds the synchronous balance read for an unknown or stale
	// payer. It has to fit inside Envoy's ext_proc message timeout.
	ReadTimeout time.Duration
	// OnStateRead, when set, observes every synchronous snapshot read.
	OnStateRead func(source, network string, err error, d time.Duration)
	// Log receives source failover and watch events. Optional.
	Log *slog.Logger
	sf  singleflight.Group
	now func() time.Time

	mu        sync.Mutex
	unhealthy map[string]time.Time // source name -> skip until
	unserved  map[string]bool      // chains no source serves
	watchers  map[string]*watcher  // chain|asset
	runCtx    context.Context
}

// NewRedis returns a PayerState on rdb, reading chain state from sources in
// order of preference.
func NewRedis(rdb redis.UniversalClient, sources ...statesource.Source) *Redis {
	return &Redis{rdb: rdb, sources: sources, ReadTimeout: 300 * time.Millisecond, now: time.Now,
		unhealthy: map[string]time.Time{}, unserved: map[string]bool{}, watchers: map[string]*watcher{}}
}

func payerKeys(chainID, asset, payer string) (bal, holds, amounts, held, nonces, blocked string) {
	p := "x402:{" + chainID + "|" + asset + "|" + payer + "}:"
	return p + "bal", p + "holds", p + "amounts", p + "held", p + "nonces", p + "blocked"
}

func ms(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

// Reservation is a placed hold and the chain state it was decided on.
type Reservation struct {
	ID ReservationID
	// Source names the state source whose balance backed the decision; empty
	// when no balance check was made.
	Source      string
	BlockHeight uint64
}

func (r *Redis) Reserve(ctx context.Context, req Request) (ReservationID, error) {
	res, err := r.ReserveDetailed(ctx, req)
	return res.ID, err
}

// ReserveDetailed is Reserve, also reporting the state behind the decision.
func (r *Redis) ReserveDetailed(ctx context.Context, req Request) (Reservation, error) {
	if req.Amount == nil || req.Amount.Sign() < 0 || (req.Amount.Sign() == 0 && !req.QueueOnly) || req.Amount.Cmp(bigMaxExact) > 0 {
		return Reservation{}, errors.New("payerstate: amount out of range")
	}
	maxUnsettled := ""
	if m := req.Policy.MaxUnsettled; m != nil {
		if m.Cmp(bigMaxExact) > 0 {
			m = bigMaxExact
		}
		maxUnsettled = m.String()
	}
	maxRoute := ""
	if m := req.Policy.MaxUnsettledPerRoute; m != nil && req.Route != "" {
		if m.Cmp(bigMaxExact) > 0 {
			m = bigMaxExact
		}
		maxRoute = m.String()
	}
	freshness := req.Policy.Freshness
	if freshness <= 0 {
		freshness = DefaultFreshness
	}
	r.mu.Lock()
	skipBalance := len(r.sources) == 0 || r.unserved[req.Chain] || req.QueueOnly
	r.mu.Unlock()
	bal, holds, amounts, held, nonces, blocked := payerKeys(req.Chain, req.Asset, req.Payer)

	for attempt := 0; attempt < 3; attempt++ {
		skip := "0"
		if skipBalance {
			skip = "1"
		}
		res, err := reserveScript.Run(ctx, r.rdb,
			[]string{bal, holds, amounts, held, nonces, blocked, keyExposure},
			ms(r.now()), req.Amount.String(), ms(req.Expiry), maxUnsettled,
			freshness.Milliseconds(), req.Nonce, skip, req.Route, maxRoute).Text()
		if err != nil {
			return Reservation{}, fmt.Errorf("%w: store: %v", ErrUnavailable, err)
		}
		if basis, ok := strings.CutPrefix(res, "ok|"); ok {
			out := Reservation{ID: makeID(req.Chain, req.Asset, req.Payer, req.Nonce)}
			if source, block, ok := strings.Cut(basis, "|"); ok {
				out.Source = source
				out.BlockHeight, _ = strconv.ParseUint(block, 10, 64)
			}
			r.watch(req.Chain, req.Asset, req.Payer)
			return out, nil
		}
		switch res {
		case "unknown", "stale":
			err := r.refresh(ctx, req)
			switch {
			case err == nil:
			case errors.Is(err, statesource.ErrUnsupported):
				// No source serves this chain: nonce and exposure only.
				r.mu.Lock()
				r.unserved[req.Chain] = true
				r.mu.Unlock()
				skipBalance = true
			case req.Policy.AllowOnUnavailable:
				skipBalance = true
			default:
				return Reservation{}, fmt.Errorf("%w: chain state: %v", ErrUnavailable, err)
			}
		default:
			return Reservation{}, &DeniedError{Reason: res}
		}
	}
	return Reservation{}, fmt.Errorf("%w: balance would not stay fresh", ErrUnavailable)
}

// unhealthyFor is how long a source is skipped after failing.
const unhealthyFor = 5 * time.Second

func (r *Redis) healthy(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !time.Now().Before(r.unhealthy[name])
}

func (r *Redis) markUnhealthy(name string, d time.Duration) {
	r.mu.Lock()
	r.unhealthy[name] = time.Now().Add(d)
	r.mu.Unlock()
}

func (r *Redis) logger() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.New(slog.DiscardHandler)
}

// refresh reads the payer's state and caches it. Concurrent requests for the
// same payer share one read.
//
// Sources are asked in order, healthy ones first and those in cooldown after
// (a degraded source is better than refusing the request), and the reads are
// hedged: the next source starts when the previous one fails, or when it has
// not answered within a third of ReadTimeout. The first good answer wins and
// the others are cancelled. A slow preferred source therefore never starves
// its fallback (on the dev cluster a hosted service busy with its own cold
// chain read did exactly that), while a quick one keeps the fallback idle.
func (r *Redis) refresh(ctx context.Context, req Request) error {
	key := req.Chain + "|" + req.Asset + "|" + req.Payer
	_, err, _ := r.sf.Do(key, func() (any, error) {
		// Detached from the triggering request so one cancelled caller does
		// not fail the others sharing this read.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.ReadTimeout)
		defer cancel()
		order := make([]statesource.Source, 0, len(r.sources))
		for pass := 0; pass < 2; pass++ {
			for _, src := range r.sources {
				if (pass == 0) == r.healthy(src.Name()) {
					order = append(order, src)
				}
			}
		}
		if len(order) == 0 {
			return nil, statesource.ErrUnsupported
		}

		type answer struct {
			src  statesource.Source
			snap statesource.PayerSnapshot
			err  error
			took time.Duration
		}
		// Buffered for every source, so losers finish without a reader.
		answers := make(chan answer, len(order))
		sctx, cancelSources := context.WithCancel(rctx)
		defer cancelSources()
		started, inflight := 0, 0
		startNext := func() {
			src := order[started]
			started++
			inflight++
			go func() {
				t := time.Now()
				snap, err := src.Snapshot(sctx, req.Chain, req.Asset, req.Payer)
				answers <- answer{src, snap, err, time.Since(t)}
			}()
		}
		hedge := time.NewTimer(r.ReadTimeout / 3)
		defer hedge.Stop()

		var lastErr error = statesource.ErrUnsupported
		startNext()
		for inflight > 0 {
			select {
			case <-hedge.C:
				if started < len(order) {
					startNext()
					hedge.Reset(r.ReadTimeout / 3)
				}
				continue
			case a := <-answers:
				inflight--
				if errors.Is(a.err, statesource.ErrUnsupported) {
					// Not this source's chain: no time spent, move on.
				} else {
					if r.OnStateRead != nil {
						r.OnStateRead(a.src.Name(), req.Chain, a.err, a.took)
					}
					if a.err == nil {
						cancelSources()
						return nil, r.SetSnapshot(rctx, req.Chain, req.Asset, req.Payer, a.src.Name(), a.snap)
					}
					r.logger().Warn("state source failed; trying the next", "source", a.src.Name(), "network", req.Chain, "err", a.err)
					r.markUnhealthy(a.src.Name(), unhealthyFor)
					lastErr = a.err
				}
				// Nothing else is running: start the next source now rather
				// than waiting for the hedge timer.
				if inflight == 0 && started < len(order) {
					startNext()
				}
			}
		}
		return nil, lastErr
	})
	return err
}

// SetSnapshot caches a payer's on-chain state as reported by a source. A
// write older than what is cached is ignored.
func (r *Redis) SetSnapshot(ctx context.Context, chainID, asset, payer, source string, snap statesource.PayerSnapshot) error {
	balance := snap.Balance
	if balance == nil {
		return errors.New("payerstate: snapshot without balance")
	}
	if balance.Cmp(bigMaxExact) > 0 {
		balance = bigMaxExact
	}
	// Freshness is judged from when the source observed the state, not from
	// when it reached us; a source's clock running ahead gains it nothing.
	at := r.now()
	if !snap.ObservedAt.IsZero() && snap.ObservedAt.Before(at) {
		at = snap.ObservedAt
	}
	bal, _, _, _, _, _ := payerKeys(chainID, asset, payer)
	return setBalanceScript.Run(ctx, r.rdb, []string{bal}, balance.String(), snap.BlockHeight, ms(at), source, snap.BlockHash).Err()
}

// MarkAuthorizationUsed records that the chain reports a nonce used or
// cancelled; a payment carrying it is then refused.
func (r *Redis) MarkAuthorizationUsed(ctx context.Context, chainID, asset, payer, nonce string) error {
	_, _, _, _, nonces, _ := payerKeys(chainID, asset, payer)
	return markAuthScript.Run(ctx, r.rdb, []string{nonces}, nonce).Err()
}

// NonceUsed reports whether an authorization is held, charged or seen used
// on chain, and not yet expired. A released one is not used.
func (r *Redis) NonceUsed(ctx context.Context, chainID, asset, payer, nonce string) (bool, error) {
	_, _, _, _, nonces, _ := payerKeys(chainID, asset, payer)
	score, err := r.rdb.ZScore(ctx, nonces, nonce).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return math.IsInf(score, 1) || int64(score) > r.now().UnixMilli(), nil
}

func (r *Redis) Release(ctx context.Context, id ReservationID) error {
	chainID, asset, payer, nonce, err := id.Parts()
	if err != nil {
		return err
	}
	_, holds, amounts, held, nonces, _ := payerKeys(chainID, asset, payer)
	n, err := releaseScript.Run(ctx, r.rdb, []string{holds, amounts, held, nonces}, nonce).Int()
	if err != nil {
		return err
	}
	if n == -1 {
		return errors.New("payerstate: reservation already committed")
	}
	return nil
}

func (r *Redis) Commit(ctx context.Context, id ReservationID, job Job) error {
	chainID, asset, payer, nonce, err := id.Parts()
	if err != nil {
		return err
	}
	_, holds, amounts, _, _, _ := payerKeys(chainID, asset, payer)
	n, err := commitScript.Run(ctx, r.rdb, []string{holds, keyQueue, keyJobs, amounts, keyRoutes, keyExposure},
		nonce, ms(r.now()), string(id), job.Body, ms(job.Deadline), job.Route).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrExpired
	}
	return nil
}

// Exposure returns the payer's held amount (in flight plus unsettled).
func (r *Redis) Exposure(ctx context.Context, chainID, asset, payer string) (int64, error) {
	_, _, _, held, _, _ := payerKeys(chainID, asset, payer)
	v, err := r.rdb.Get(ctx, held).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return v, err
}
