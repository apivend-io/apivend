package payerstate

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// The settlement queue. Jobs are ordered by authorization deadline, not
// arrival: the one closest to expiring is settled first. A claimed job is
// leased, not removed; if the settler dies the lease runs out and another
// settler picks the job up. Only Settled or Failed remove it.

var claimScript = redis.NewScript(`
-- KEYS: queue jobs busy attempts   ARGV: now lease scan
local now = tonumber(ARGV[1])
for _, id in ipairs(redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[3]) - 1)) do
  local until_ = redis.call('HGET', KEYS[3], id)
  if not until_ or tonumber(until_) <= now then
    redis.call('HSET', KEYS[3], id, now + tonumber(ARGV[2]))
    local n = redis.call('HINCRBY', KEYS[4], id, 1)
    return {id, redis.call('HGET', KEYS[2], id) or '', n, redis.call('ZSCORE', KEYS[1], id)}
  end
end
return false
`)

var settledScript = redis.NewScript(`
-- KEYS: holds amounts held bal queue jobs busy attempts tx routes exposure
-- ARGV: nonce jobID txBlock
local a = redis.call('HGET', KEYS[2], ARGV[1])
if a then
  redis.call('DECRBY', KEYS[3], a)
  local route = redis.call('HGET', KEYS[10], ARGV[2])
  -- a zero hold (queue-only work) is skipped: Lua's -0 reaches Redis as "-0"
  if route and tonumber(a) ~= 0 then redis.call('HINCRBY', KEYS[11], route, -tonumber(a)) end
  redis.call('HDEL', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[1], ARGV[1])
  -- The cached balance predates the transfer: debit it, so the freed hold
  -- does not make the same money look spendable twice. A balance read at or
  -- after the transfer's block already reflects it.
  local b = redis.call('HMGET', KEYS[4], 'balance', 'block')
  if b[1] and tonumber(ARGV[3]) > 0 and tonumber(b[2]) < tonumber(ARGV[3]) then
    local nb = tonumber(b[1]) - tonumber(a)
    if nb < 0 then nb = 0 end
    redis.call('HSET', KEYS[4], 'balance', string.format('%.0f', nb))
  elseif b[1] and tonumber(ARGV[3]) == 0 then
    -- Block unknown: force the next request to re-read the chain.
    redis.call('HSET', KEYS[4], 'at', 0)
  end
end
redis.call('ZREM', KEYS[5], ARGV[2])
redis.call('HDEL', KEYS[6], ARGV[2])
redis.call('HDEL', KEYS[7], ARGV[2])
redis.call('HDEL', KEYS[8], ARGV[2])
redis.call('HDEL', KEYS[9], ARGV[2])
redis.call('HDEL', KEYS[10], ARGV[2])
return a and 1 or 0
`)

var failedScript = redis.NewScript(`
-- KEYS: holds amounts held blocked queue jobs busy attempts bal tx routes exposure
-- ARGV: nonce jobID reason blockTTLms
local a = redis.call('HGET', KEYS[2], ARGV[1])
if a then
  redis.call('DECRBY', KEYS[3], a)
  local route = redis.call('HGET', KEYS[11], ARGV[2])
  if route and tonumber(a) ~= 0 then redis.call('HINCRBY', KEYS[12], route, -tonumber(a)) end
  redis.call('HDEL', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[1], ARGV[1])
end
if tonumber(ARGV[4]) > 0 then
  redis.call('SET', KEYS[4], ARGV[3], 'PX', ARGV[4])
elseif tonumber(ARGV[4]) < 0 then
  redis.call('SET', KEYS[4], ARGV[3])
end
redis.call('HSET', KEYS[9], 'at', 0)
redis.call('ZREM', KEYS[5], ARGV[2])
redis.call('HDEL', KEYS[6], ARGV[2])
redis.call('HDEL', KEYS[7], ARGV[2])
redis.call('HDEL', KEYS[8], ARGV[2])
redis.call('HDEL', KEYS[10], ARGV[2])
redis.call('HDEL', KEYS[11], ARGV[2])
return 1
`)

// Claimed is a leased settlement job.
type Claimed struct {
	ID       ReservationID
	Body     []byte
	Attempt  int
	Deadline time.Time
}

// Claim leases the claimable job with the earliest deadline, or returns nil.
func (r *Redis) Claim(ctx context.Context, lease time.Duration) (*Claimed, error) {
	res, err := claimScript.Run(ctx, r.rdb, []string{keyQueue, keyJobs, keyBusy, keyAttempts},
		ms(r.now()), lease.Milliseconds(), 256).Slice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c := &Claimed{}
	id, _ := res[0].(string)
	body, _ := res[1].(string)
	attempt, _ := res[2].(int64)
	c.ID, c.Body, c.Attempt = ReservationID(id), []byte(body), int(attempt)
	if s, ok := res[3].(string); ok {
		if f, err := parseFloat(s); err == nil {
			c.Deadline = time.UnixMilli(int64(f))
		}
	}
	return c, nil
}

// Retry makes a claimed job claimable again after delay.
func (r *Redis) Retry(ctx context.Context, id ReservationID, delay time.Duration) error {
	return r.rdb.HSet(ctx, keyBusy, string(id), r.now().Add(delay).UnixMilli()).Err()
}

// Settled removes the job and frees the payer's hold: the transfer landed.
// txBlock is the block it landed in, or 0 when unknown.
func (r *Redis) Settled(ctx context.Context, id ReservationID, txBlock uint64) error {
	chainID, asset, payer, nonce, err := id.Parts()
	if err != nil {
		return err
	}
	bal, holds, amounts, held, _, _ := payerKeys(chainID, asset, payer)
	return settledScript.Run(ctx, r.rdb,
		[]string{holds, amounts, held, bal, keyQueue, keyJobs, keyBusy, keyAttempts, keyTx, keyRoutes, keyExposure},
		nonce, string(id), txBlock).Err()
}

// Failed removes the job and frees the hold without payment, and blocks the
// payer for blockFor (negative: until unblocked by hand; zero: not at all).
func (r *Redis) Failed(ctx context.Context, id ReservationID, reason string, blockFor time.Duration) error {
	chainID, asset, payer, nonce, err := id.Parts()
	if err != nil {
		return err
	}
	bal, holds, amounts, held, _, blocked := payerKeys(chainID, asset, payer)
	ttl := blockFor.Milliseconds()
	if blockFor < 0 {
		ttl = -1
	}
	return failedScript.Run(ctx, r.rdb,
		[]string{holds, amounts, held, blocked, keyQueue, keyJobs, keyBusy, keyAttempts, bal, keyTx, keyRoutes, keyExposure},
		nonce, string(id), reason, ttl).Err()
}

// Unblock lifts a payer block set by Failed.
func (r *Redis) Unblock(ctx context.Context, chainID, asset, payer string) error {
	_, _, _, _, _, blocked := payerKeys(chainID, asset, payer)
	return r.rdb.Del(ctx, blocked).Err()
}

// QueueStats reports the queue depth and the earliest deadline in it.
func (r *Redis) QueueStats(ctx context.Context) (depth int64, earliest time.Time, err error) {
	depth, err = r.rdb.ZCard(ctx, keyQueue).Result()
	if err != nil || depth == 0 {
		return depth, time.Time{}, err
	}
	z, err := r.rdb.ZRangeWithScores(ctx, keyQueue, 0, 0).Result()
	if err != nil || len(z) == 0 {
		return depth, time.Time{}, err
	}
	return depth, time.UnixMilli(int64(z[0].Score)), nil
}

func parseFloat(s string) (float64, error) { return strconv.ParseFloat(s, 64) }

// SetPendingTx remembers a transaction that was broadcast for a job but not
// yet confirmed, so a later attempt can report it instead of resubmitting.
func (r *Redis) SetPendingTx(ctx context.Context, id ReservationID, txHash string) error {
	return r.rdb.HSet(ctx, keyTx, string(id), txHash).Err()
}

// PendingTx returns the transaction recorded by SetPendingTx, or "".
func (r *Redis) PendingTx(ctx context.Context, id ReservationID) (string, error) {
	tx, err := r.rdb.HGet(ctx, keyTx, string(id)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return tx, err
}

// ExposureByRoute returns the served-but-unsettled amount per route.
func (r *Redis) ExposureByRoute(ctx context.Context) (map[string]int64, error) {
	m, err := r.rdb.HGetAll(ctx, keyExposure).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(m))
	for route, v := range m {
		out[route], _ = strconv.ParseInt(v, 10, 64)
	}
	return out, nil
}

// Result is the final outcome of a settlement, published for a filter that is
// holding a response until the payment lands (sync mode).
type Result struct {
	Status      string `json:"status"` // "settled" or "unpaid"
	Transaction string `json:"transaction,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

func resultKey(id ReservationID) string { return "x402:settle:result:" + string(id) }

// PublishResult makes a job's outcome available to WaitResult for a while.
func (r *Redis) PublishResult(ctx context.Context, id ReservationID, res Result) error {
	body, err := json.Marshal(res)
	if err != nil {
		return err
	}
	pipe := r.rdb.TxPipeline()
	pipe.LPush(ctx, resultKey(id), body)
	pipe.Expire(ctx, resultKey(id), 5*time.Minute)
	_, err = pipe.Exec(ctx)
	return err
}

// WaitResult blocks until the job's outcome is published or timeout (rounded
// to whole seconds, at least one) passes.
// It returns nil, nil on timeout.
func (r *Redis) WaitResult(ctx context.Context, id ReservationID, timeout time.Duration) (*Result, error) {
	// The blocking pop has whole-second resolution.
	if timeout = timeout.Round(time.Second); timeout < time.Second {
		timeout = time.Second
	}
	vals, err := r.rdb.BRPop(ctx, timeout, resultKey(id)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var res Result
	if len(vals) != 2 || json.Unmarshal([]byte(vals[1]), &res) != nil {
		return nil, errors.New("payerstate: malformed settlement result")
	}
	return &res, nil
}
