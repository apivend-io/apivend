package payerstate

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"apivend.io/apivend/engine/evm"
)

// Channels is the gateway's state for batch-settlement channels
// (self-managed vouchers, docs/BATCH-SETTLEMENT.md): per channel, the
// charged total, the latest voucher, and the balance, claimed total and
// withdrawal mirrored from the chain. Every transition is one Lua script.
//
// The channel's escrow backs every voucher, so there is no exposure cap:
// a voucher is accepted only up to the balance the contract holds. The
// balance only falls by a timed withdrawal (after at least 15 minutes) or a
// refund (signed by us), so a mirror a minute old is safe.
type Channels struct {
	rdb redis.UniversalClient
	// Read returns a channel's on-chain state; required.
	Read ChannelReader
	// Freshness is how old the mirrored on-chain state may be. Default 60s.
	Freshness time.Duration
	// Lock bounds one request's hold on a channel (the spec's per-channel
	// serialization). Default 30s; a crashed filter's hold expires with it.
	Lock time.Duration
	now  func() time.Time
}

// ChannelReader reads a channel's on-chain state on a network (CAIP-2).
type ChannelReader func(ctx context.Context, network string, id [32]byte) (evm.Channel, error)

// NewChannels returns channel state on rdb.
func NewChannels(rdb redis.UniversalClient, read ChannelReader) *Channels {
	return &Channels{rdb: rdb, Read: read, Freshness: time.Minute, Lock: 30 * time.Second, now: time.Now}
}

// UnclaimedKey is a set of "network|channelId" with charges not yet
// claimed on chain; the settler's claim job reads it.
const UnclaimedKey = "x402:ch:unclaimed"

func channelKey(network string, id [32]byte) string {
	return "x402:ch:{" + network + "}:0x" + hex.EncodeToString(id[:])
}

// VoucherRequest is a verified voucher for one request.
type VoucherRequest struct {
	Network      string
	ChannelID    [32]byte
	Config       evm.ChannelConfig
	MaxClaimable *big.Int
	Price        *big.Int
	Signature    []byte
	// Provider and Route are kept on the channel for the claim job: where
	// to submit claims, and what route its receipts name.
	Provider, Route string
	RequestID       string
}

// VoucherHold is a channel held for one request.
type VoucherHold struct {
	key, token string
	member     string
	charge     Charge
	// The channel as decided on: what was charged before this request, and
	// the mirrored on-chain state.
	Charged *big.Int
	Chain   evm.Channel
}

// MismatchError: the voucher's cumulative amount is not the charged total
// plus the price. The client resynchronises from the corrective 402.
type MismatchError struct {
	Charged   *big.Int // what this channel has been charged
	Signed    *big.Int // the latest voucher's ceiling; nil when none
	Signature []byte   // that voucher's signature
	Chain     evm.Channel
}

func (e *MismatchError) Error() string {
	return "voucher cumulative amount does not match the charged total " + e.Charged.String()
}

// Reasons a voucher is refused, beyond MismatchError.
const (
	ReasonChannelBusy = "channel_busy"
	// ReasonChannelClosing: a refund or a timed withdrawal is under way; the
	// channel takes no new vouchers.
	ReasonChannelClosing   = "channel_closing"
	ReasonRefundNoBalance  = "refund_no_balance"
	ReasonRefundExceeds    = "refund_exceeds_balance"
	ReasonExceedsBalance   = "exceeds_balance"
	ReasonBelowClaimed     = "below_claimed"
	ReasonChannelNotFound  = "channel_not_found"
	ReasonAmountOutOfRange = "amount_out_of_range"
)

// Lua numbers are doubles: amounts stay below 2^53 (9e9 USDC), checked in Go.
var reserveVoucherScript = redis.NewScript(`
local h = KEYS[1]
local now = tonumber(ARGV[3])
if redis.call('EXISTS', h) == 0 then return {'sync'} end
if tonumber(redis.call('HGET', h, 'synced') or '0') < tonumber(ARGV[6]) then return {'sync'} end
if redis.call('HGET', h, 'closing') == '1' or tonumber(redis.call('HGET', h, 'withdrawAt') or '0') > 0 then return {'closing'} end
local busy = redis.call('HGET', h, 'busy')
if busy and tonumber(redis.call('HGET', h, 'busyUntil') or '0') > now then return {'busy'} end
local f = redis.call('HMGET', h, 'charged', 'balance', 'claimed', 'signed', 'sig')
local charged, balance, claimed = tonumber(f[1]), tonumber(f[2]), tonumber(f[3])
local max, price = tonumber(ARGV[1]), tonumber(ARGV[2])
if balance <= 0 then return {'notfound'} end
if max ~= charged + price then return {'mismatch', f[1], f[4] or '', f[5] or ''} end
if max > balance then return {'exceeds'} end
if max <= claimed or max < claimed + price then return {'below'} end
redis.call('HSET', h, 'busy', ARGV[5], 'busyUntil', ARGV[4], 'pmax', ARGV[1], 'psig', ARGV[7])
return {'ok', f[1]}
`)

// syncChannelScript mirrors the chain; a new record starts charged at the
// claimed total (spec: recovery after state loss), and the charged total
// never falls below what was claimed.
var syncChannelScript = redis.NewScript(`
local h = KEYS[1]
local claimed = tonumber(ARGV[2])
local charged = tonumber(redis.call('HGET', h, 'charged') or '-1')
if charged < claimed then charged = claimed end
redis.call('HSET', h, 'balance', ARGV[1], 'claimed', ARGV[2], 'withdrawAt', ARGV[3], 'refundNonce', ARGV[4],
  'synced', ARGV[5], 'charged', string.format('%.0f', charged))
if redis.call('HEXISTS', h, 'cfg') == 0 then redis.call('HSET', h, 'cfg', ARGV[6], 'provider', ARGV[7], 'route', ARGV[8]) end
return 1
`)

// commitVoucherScript charges the held request and, in the same step,
// appends it to the charges list the settler turns into receipts.
var commitVoucherScript = redis.NewScript(`
local h = KEYS[1]
if redis.call('HGET', h, 'busy') ~= ARGV[1] then return false end
local pmax = redis.call('HGET', h, 'pmax')
redis.call('HSET', h, 'charged', pmax, 'signed', pmax, 'sig', redis.call('HGET', h, 'psig'))
redis.call('HDEL', h, 'busy', 'busyUntil', 'pmax', 'psig')
redis.call('SADD', KEYS[2], ARGV[2])
redis.call('RPUSH', KEYS[3], (string.gsub(ARGV[3], '"CUMULATIVE"', '"' .. pmax .. '"')))
return pmax
`)

// ChargesKey lists charged requests (JSON Charge) for the settler's receipts.
const ChargesKey = "x402:ch:charges"

// Charge is one request paid by voucher.
type Charge struct {
	Network    string    `json:"network"`
	ChannelID  string    `json:"channelId"`
	Payer      string    `json:"payer"`
	Token      string    `json:"token"`
	Route      string    `json:"route"`
	RequestID  string    `json:"requestId,omitempty"`
	Amount     string    `json:"amount"`
	Cumulative string    `json:"cumulative"`
	At         time.Time `json:"at"`
}

// DrainCharges pops up to n charges, oldest first.
func (c *Channels) DrainCharges(ctx context.Context, n int64) ([]Charge, error) {
	raw, err := c.rdb.LPopCount(ctx, ChargesKey, int(n)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]Charge, 0, len(raw))
	for _, r := range raw {
		var ch Charge
		if json.Unmarshal([]byte(r), &ch) == nil {
			out = append(out, ch)
		}
	}
	return out, nil
}

// Requeue puts charges back at the front (receipts could not be written).
func (c *Channels) Requeue(ctx context.Context, charges []Charge) error {
	for i := len(charges) - 1; i >= 0; i-- {
		b, _ := json.Marshal(charges[i])
		if err := c.rdb.LPush(ctx, ChargesKey, b).Err(); err != nil {
			return err
		}
	}
	return nil
}

var releaseVoucherScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'busy') == ARGV[1] then
  redis.call('HDEL', KEYS[1], 'busy', 'busyUntil', 'pmax', 'psig')
end
return 1
`)

// Reserve holds the channel for one request if the voucher is in step,
// within the escrow, and no other request holds the channel.
func (c *Channels) Reserve(ctx context.Context, v VoucherRequest) (VoucherHold, error) {
	if !exact(v.MaxClaimable) || !exact(v.Price) {
		return VoucherHold{}, &DeniedError{Reason: ReasonAmountOutOfRange}
	}
	key := channelKey(v.Network, v.ChannelID)
	token := strconv.FormatInt(c.now().UnixNano(), 36) + hex.EncodeToString(v.Signature[:min(8, len(v.Signature))])
	for attempt := 0; attempt < 2; attempt++ {
		now := c.now()
		res, err := reserveVoucherScript.Run(ctx, c.rdb, []string{key},
			v.MaxClaimable.String(), v.Price.String(), now.UnixMilli(), now.Add(c.Lock).UnixMilli(), token,
			now.Add(-c.Freshness).UnixMilli(), hex.EncodeToString(v.Signature)).StringSlice()
		if err != nil {
			return VoucherHold{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		switch res[0] {
		case "ok":
			hold := VoucherHold{key: key, token: token, member: v.Network + "|0x" + hex.EncodeToString(v.ChannelID[:])}
			hold.charge = Charge{Network: v.Network, ChannelID: "0x" + hex.EncodeToString(v.ChannelID[:]), Payer: v.Config.Payer.Hex(),
				Token: v.Config.Token.Hex(), Route: v.Route, RequestID: v.RequestID, Amount: v.Price.String(), Cumulative: "CUMULATIVE"}
			hold.Charged, _ = new(big.Int).SetString(res[1], 10)
			hold.Chain, _ = c.mirrored(ctx, key)
			return hold, nil
		case "sync":
			if attempt == 1 {
				return VoucherHold{}, fmt.Errorf("%w: channel state did not stay fresh", ErrUnavailable)
			}
			if err := c.sync(ctx, key, v); err != nil {
				return VoucherHold{}, err
			}
		case "busy":
			return VoucherHold{}, &DeniedError{Reason: ReasonChannelBusy}
		case "closing":
			return VoucherHold{}, &DeniedError{Reason: ReasonChannelClosing}
		case "notfound":
			return VoucherHold{}, &DeniedError{Reason: ReasonChannelNotFound}
		case "exceeds":
			return VoucherHold{}, &DeniedError{Reason: ReasonExceedsBalance}
		case "below":
			return VoucherHold{}, &DeniedError{Reason: ReasonBelowClaimed}
		case "mismatch":
			m := &MismatchError{}
			m.Charged, _ = new(big.Int).SetString(res[1], 10)
			if res[2] != "" {
				m.Signed, _ = new(big.Int).SetString(res[2], 10)
				m.Signature, _ = hex.DecodeString(res[3])
			}
			m.Chain, _ = c.mirrored(ctx, key)
			return VoucherHold{}, m
		default:
			return VoucherHold{}, fmt.Errorf("%w: unexpected script answer %q", ErrUnavailable, res[0])
		}
	}
	return VoucherHold{}, ErrUnavailable
}

// AfterDeposit mirrors the channel from the chain once a deposit of at
// least minBalance has landed, retrying briefly: the RPC node read may lag
// the facilitator's by a block.
func (c *Channels) AfterDeposit(ctx context.Context, v VoucherRequest, minBalance *big.Int) error {
	key := channelKey(v.Network, v.ChannelID)
	var last error
	for i := 0; i < 6; i++ {
		ch, err := c.Read(ctx, v.Network, v.ChannelID)
		if err == nil && ch.Balance.Cmp(minBalance) >= 0 {
			return c.mirror(ctx, key, v, ch)
		}
		last = err
		if last == nil {
			last = errors.New("deposit not visible on chain yet")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ErrUnavailable, ctx.Err())
		case <-time.After(400 * time.Millisecond):
		}
	}
	return fmt.Errorf("%w: reading channel after deposit: %v", ErrUnavailable, last)
}

// sync reads the channel from the chain and mirrors it.
func (c *Channels) sync(ctx context.Context, key string, v VoucherRequest) error {
	ch, err := c.Read(ctx, v.Network, v.ChannelID)
	if err != nil {
		return fmt.Errorf("%w: reading channel: %v", ErrUnavailable, err)
	}
	return c.mirror(ctx, key, v, ch)
}

func (c *Channels) mirror(ctx context.Context, key string, v VoucherRequest, ch evm.Channel) error {
	cfg, _ := json.Marshal(configJSON(v.Config))
	return syncChannelScript.Run(ctx, c.rdb, []string{key},
		clamp(ch.Balance).String(), clamp(ch.TotalClaimed).String(), ch.WithdrawRequestedAt, ch.RefundNonce.String(),
		c.now().UnixMilli(), string(cfg), v.Provider, v.Route).Err()
}

// Commit charges the held request: the voucher becomes the channel's
// latest, and the channel is queued for the claim job. It returns the new
// charged total.
func (c *Channels) Commit(ctx context.Context, h VoucherHold) (*big.Int, error) {
	ch := h.charge
	ch.At = c.now()
	rec, _ := json.Marshal(ch)
	res, err := commitVoucherScript.Run(ctx, c.rdb, []string{h.key, UnclaimedKey, ChargesKey}, h.token, h.member, string(rec)).Text()
	if errors.Is(err, redis.Nil) {
		return nil, ErrExpired
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	n, ok := new(big.Int).SetString(res, 10)
	if !ok {
		return nil, fmt.Errorf("%w: bad charged total %q", ErrUnavailable, res)
	}
	return n, nil
}

// Release drops the hold without charging.
func (c *Channels) Release(ctx context.Context, h VoucherHold) error {
	return releaseVoucherScript.Run(ctx, c.rdb, []string{h.key}, h.token).Err()
}

func (c *Channels) mirrored(ctx context.Context, key string) (evm.Channel, error) {
	f, err := c.rdb.HMGet(ctx, key, "balance", "claimed", "withdrawAt", "refundNonce").Result()
	if err != nil {
		return evm.Channel{}, err
	}
	num := func(v any) *big.Int {
		n, _ := new(big.Int).SetString(fmt.Sprint(v), 10)
		if n == nil {
			n = new(big.Int)
		}
		return n
	}
	return evm.Channel{Balance: num(f[0]), TotalClaimed: num(f[1]), WithdrawRequestedAt: num(f[2]).Uint64(), RefundNonce: num(f[3])}, nil
}

func exact(v *big.Int) bool { return v != nil && v.Sign() >= 0 && v.Cmp(bigMaxExact) <= 0 }

func clamp(v *big.Int) *big.Int {
	if v == nil || v.Sign() < 0 {
		return new(big.Int)
	}
	if v.Cmp(bigMaxExact) > 0 {
		return new(big.Int).Set(bigMaxExact)
	}
	return v
}

func configJSON(c evm.ChannelConfig) map[string]any {
	return map[string]any{
		"payer": c.Payer.String(), "payerAuthorizer": c.PayerAuthorizer.String(), "receiver": c.Receiver.String(),
		"receiverAuthorizer": c.ReceiverAuthorizer.String(), "token": c.Token.String(),
		"withdrawDelay": c.WithdrawDelay, "salt": "0x" + hex.EncodeToString(c.Salt[:]),
	}
}

// Caller makes a JSON-RPC call (the chain client).
type Caller interface {
	Call(ctx context.Context, method string, params ...any) (json.RawMessage, error)
}

// ChainChannelReader reads channels through each network's RPC.
func ChainChannelReader(rpcs map[string]Caller) ChannelReader {
	return func(ctx context.Context, network string, id [32]byte) (evm.Channel, error) {
		rpc, ok := rpcs[network]
		if !ok {
			return evm.Channel{}, fmt.Errorf("no RPC for %s", network)
		}
		call := func(data []byte) ([]byte, error) {
			raw, err := rpc.Call(ctx, "eth_call", map[string]string{
				"to": evm.BatchSettlement.Hex(), "data": "0x" + hex.EncodeToString(data),
			}, "latest")
			if err != nil {
				return nil, err
			}
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, err
			}
			return hex.DecodeString(strings.TrimPrefix(s, "0x"))
		}
		ch, err := call(evm.ChannelsCall(id))
		if err != nil {
			return evm.Channel{}, err
		}
		wd, err := call(evm.PendingWithdrawalsCall(id))
		if err != nil {
			return evm.Channel{}, err
		}
		n, err := call(evm.RefundNonceCall(id))
		if err != nil {
			return evm.Channel{}, err
		}
		return evm.ParseChannel(ch, wd, n)
	}
}

// Claimable is a channel with charges not yet claimed on chain.
type Claimable struct {
	Network   string
	ChannelID [32]byte
	Config    evm.ChannelConfig
	// Charged is the total charged; Signature is the payer's voucher for
	// exactly that amount (the latest voucher's ceiling is the charged total).
	Charged   *big.Int
	Signature []byte
	Chain     evm.Channel // as mirrored
	Provider  string
	Route     string
}

// Unclaimed lists the channels queued for claiming, each refreshed from the
// chain first (an out-of-band claim, or a withdrawal that makes claiming
// urgent). Channels whose charges are all claimed leave the queue.
func (c *Channels) Unclaimed(ctx context.Context) ([]Claimable, error) {
	members, err := c.rdb.SMembers(ctx, UnclaimedKey).Result()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	var out []Claimable
	for _, m := range members {
		network, idHex, ok := strings.Cut(m, "|")
		id, err := evm.ParseBytes32(idHex)
		if !ok || err != nil {
			c.rdb.SRem(ctx, UnclaimedKey, m)
			continue
		}
		key := channelKey(network, id)
		f, err := c.rdb.HMGet(ctx, key, "cfg", "charged", "signed", "sig", "provider", "route").Result()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		if f[0] == nil {
			c.rdb.SRem(ctx, UnclaimedKey, m)
			continue
		}
		cfg, err := parseConfigJSON(fmt.Sprint(f[0]))
		if err != nil {
			continue
		}
		ch, err := c.Read(ctx, network, id)
		if err != nil {
			return nil, fmt.Errorf("%w: reading channel: %v", ErrUnavailable, err)
		}
		v := VoucherRequest{Network: network, ChannelID: id, Config: cfg}
		if err := c.mirror(ctx, key, v, ch); err != nil {
			return nil, err
		}
		charged, _ := new(big.Int).SetString(fmt.Sprint(f[1]), 10)
		signed, _ := new(big.Int).SetString(fmt.Sprint(f[2]), 10)
		sig, _ := hex.DecodeString(fmt.Sprint(f[3]))
		if charged == nil || signed == nil || charged.Cmp(signed) != 0 || len(sig) == 0 {
			continue // a request in flight, or nothing signed yet
		}
		if charged.Cmp(ch.TotalClaimed) <= 0 {
			c.rdb.SRem(ctx, UnclaimedKey, m)
			continue
		}
		out = append(out, Claimable{Network: network, ChannelID: id, Config: cfg, Charged: charged, Signature: sig,
			Chain: ch, Provider: str(f[4]), Route: str(f[5])})
	}
	return out, nil
}

var markClaimedScript = redis.NewScript(`
local h = KEYS[1]
local total = tonumber(ARGV[1])
if tonumber(redis.call('HGET', h, 'claimed') or '0') < total then
  redis.call('HSET', h, 'claimed', ARGV[1])
end
if tonumber(redis.call('HGET', h, 'charged') or '0') <= total then
  redis.call('SREM', KEYS[2], ARGV[2])
end
return 1
`)

// MarkClaimed records a confirmed claim up to total; the channel leaves the
// queue unless more was charged meanwhile.
func (c *Channels) MarkClaimed(ctx context.Context, network string, id [32]byte, total *big.Int) error {
	return markClaimedScript.Run(ctx, c.rdb, []string{channelKey(network, id), UnclaimedKey},
		clamp(total).String(), network+"|0x"+hex.EncodeToString(id[:])).Err()
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func parseConfigJSON(s string) (evm.ChannelConfig, error) {
	var w struct {
		Payer, PayerAuthorizer, Receiver, ReceiverAuthorizer, Token, Salt string
		WithdrawDelay                                                     uint64
	}
	var cfg evm.ChannelConfig
	if err := json.Unmarshal([]byte(s), &w); err != nil {
		return cfg, err
	}
	var err error
	for _, f := range []struct {
		dst *evm.Address
		src string
	}{{&cfg.Payer, w.Payer}, {&cfg.PayerAuthorizer, w.PayerAuthorizer}, {&cfg.Receiver, w.Receiver},
		{&cfg.ReceiverAuthorizer, w.ReceiverAuthorizer}, {&cfg.Token, w.Token}} {
		if *f.dst, err = evm.ParseAddress(f.src); err != nil {
			return cfg, err
		}
	}
	cfg.Salt, err = evm.ParseBytes32(w.Salt)
	cfg.WithdrawDelay = w.WithdrawDelay
	return cfg, err
}

// beginRefundScript closes the channel for a refund: the refund voucher must
// be zero-charge (its ceiling is the charged total) and no request may hold
// the channel. It returns the charged total and what can be refunded:
// everything deposited that is not charged.
var beginRefundScript = redis.NewScript(`
local h = KEYS[1]
local now = tonumber(ARGV[2])
if redis.call('EXISTS', h) == 0 then return {'sync'} end
if tonumber(redis.call('HGET', h, 'synced') or '0') < tonumber(ARGV[4]) then return {'sync'} end
if redis.call('HGET', h, 'closing') == '1' and tonumber(redis.call('HGET', h, 'closingUntil') or '0') > now then return {'busy'} end
local busy = redis.call('HGET', h, 'busy')
if busy and tonumber(redis.call('HGET', h, 'busyUntil') or '0') > now then return {'busy'} end
local f = redis.call('HMGET', h, 'charged', 'balance', 'signed', 'sig')
local charged, balance = tonumber(f[1]), tonumber(f[2])
if tonumber(ARGV[1]) ~= charged then return {'mismatch', f[1], f[3] or '', f[4] or ''} end
local refundable = balance - charged
if refundable <= 0 then return {'nobalance'} end
redis.call('HSET', h, 'closing', '1', 'closingUntil', ARGV[3])
return {'ok', f[1], string.format('%.0f', refundable)}
`)

var endRefundScript = redis.NewScript(`
redis.call('HDEL', KEYS[1], 'closing', 'closingUntil')
return 1
`)

// RefundHold is a channel closed for a refund.
type RefundHold struct {
	key        string
	Charged    *big.Int
	Refundable *big.Int
	Chain      evm.Channel
}

// BeginRefund closes the channel to new vouchers for a refund of up to
// Refundable (balance minus charged), held for at most lock.
func (c *Channels) BeginRefund(ctx context.Context, v VoucherRequest, lock time.Duration) (RefundHold, error) {
	if !exact(v.MaxClaimable) {
		return RefundHold{}, &DeniedError{Reason: ReasonAmountOutOfRange}
	}
	key := channelKey(v.Network, v.ChannelID)
	for attempt := 0; attempt < 2; attempt++ {
		now := c.now()
		res, err := beginRefundScript.Run(ctx, c.rdb, []string{key}, v.MaxClaimable.String(), now.UnixMilli(),
			now.Add(lock).UnixMilli(), now.Add(-c.Freshness).UnixMilli()).StringSlice()
		if err != nil {
			return RefundHold{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		switch res[0] {
		case "ok":
			h := RefundHold{key: key}
			h.Charged, _ = new(big.Int).SetString(res[1], 10)
			h.Refundable, _ = new(big.Int).SetString(res[2], 10)
			h.Chain, _ = c.mirrored(ctx, key)
			return h, nil
		case "sync":
			if attempt == 1 {
				return RefundHold{}, fmt.Errorf("%w: channel state did not stay fresh", ErrUnavailable)
			}
			if err := c.sync(ctx, key, v); err != nil {
				return RefundHold{}, err
			}
		case "busy":
			return RefundHold{}, &DeniedError{Reason: ReasonChannelBusy}
		case "nobalance":
			return RefundHold{}, &DeniedError{Reason: ReasonRefundNoBalance}
		case "mismatch":
			m := &MismatchError{}
			m.Charged, _ = new(big.Int).SetString(res[1], 10)
			if res[2] != "" {
				m.Signed, _ = new(big.Int).SetString(res[2], 10)
				m.Signature, _ = hex.DecodeString(res[3])
			}
			m.Chain, _ = c.mirrored(ctx, key)
			return RefundHold{}, m
		default:
			return RefundHold{}, fmt.Errorf("%w: unexpected script answer %q", ErrUnavailable, res[0])
		}
	}
	return RefundHold{}, ErrUnavailable
}

// EndRefund reopens the channel after a refund, mirroring the chain first:
// a channel with nothing left (fully refunded and claimed) is forgotten.
func (c *Channels) EndRefund(ctx context.Context, h RefundHold, v VoucherRequest) (evm.Channel, error) {
	ch, err := c.Read(ctx, v.Network, v.ChannelID)
	if err == nil {
		err = c.mirror(ctx, h.key, v, ch)
	}
	if err == nil && ch.Balance.Cmp(ch.TotalClaimed) <= 0 {
		c.rdb.Del(ctx, h.key)
		c.rdb.SRem(ctx, UnclaimedKey, v.Network+"|0x"+hex.EncodeToString(v.ChannelID[:]))
		return ch, nil
	}
	if e := endRefundScript.Run(ctx, c.rdb, []string{h.key}).Err(); e != nil && err == nil {
		err = e
	}
	return ch, err
}
