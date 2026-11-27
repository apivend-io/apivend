package extproc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/redis/go-redis/v9"

	"apivend.io/apivend/internal/x402"
)

// Redelivery: a paid GET answer is kept for a short window. When the same
// payer sends the same request again within it, with a new valid payment,
// the kept answer is returned and the new payment is not charged. A buyer
// whose connection dropped after the charge is the case it is for: x402
// clients do not resend a payment, they sign a new one, so a retry would
// otherwise pay twice.
//
// Only GET (a repeated POST may be a second action the buyer means), only
// exact payments, only answers the filter held whole (the answer check's
// buffered body), and only once their payment has settled (sync routes):
// an answer served before settlement may yet go unpaid. The new payment must verify, so only the payer can fetch
// what the payer paid for.

// DefaultRedeliverWindow is how long a paid answer is kept for a repeat.
// x402 payments are valid for 60 s by default; a client retries well within.
const DefaultRedeliverWindow = 60 * time.Second

// MaxRedeliverWindow caps the window: the kept answers are sellers' data and
// a repeat this much later is a new question, not a retry.
const MaxRedeliverWindow = time.Hour

// DefaultRedeliverBudget caps the bytes of answers kept per window, so kept
// answers cannot fill the Redis that holds reservations (a full Redis
// refuses every paid request). Past it, answers are not kept.
const DefaultRedeliverBudget = 32 << 20

// headerRedelivered marks a kept answer returned again.
const headerRedelivered = "x-x402-redelivered"

// Delivered is a paid answer as kept for a repeat.
type Delivered struct {
	Status          int         `json:"s"`
	Headers         [][2]string `json:"h,omitempty"`
	Body            []byte      `json:"b,omitempty"`
	PaymentResponse string      `json:"p"`
	// Nonce of the payment the answer was charged to: resent, it unlocks the
	// answer again.
	Nonce string `json:"n,omitempty"`
}

// nonceChecker is implemented by payer states that can say whether an
// authorization is already held or charged.
type nonceChecker interface {
	NonceUsed(ctx context.Context, chainID, asset, payer, nonce string) (bool, error)
}

// Redeliveries keeps paid answers for a repeat. Get returns nil, nil when
// nothing is kept. An error from either is never a reason to refuse or to
// serve free: the request is charged as usual.
//
// Claim, InFlight and Unclaim mark a paid request as in flight, so that a
// repeat arriving before the first answer is kept waits for it instead of
// being charged as well (a client that timed out and retried).
type Redeliveries interface {
	Get(ctx context.Context, key string) (*Delivered, error)
	Put(ctx context.Context, key string, d *Delivered, ttl time.Duration) error
	// Claim sets the mark under token unless one is set; Unclaim removes
	// it only while it still holds token, so a call whose mark lapsed
	// never removes a later call's.
	Claim(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	InFlight(ctx context.Context, key string) (bool, error)
	Unclaim(ctx context.Context, key, token string) error
}

// flightTTL bounds an in-flight mark whose filter died before removing it.
// It outlasts a call: the upstream's time plus the settlement wait.
const flightTTL = 60 * time.Second

// flightPoll is how soon a waiting repeat first looks for the answer; the
// interval doubles up to flightPollMax.
const (
	flightPoll    = 50 * time.Millisecond
	flightPollMax = 400 * time.Millisecond
)

// Repeats waiting at once, per filter process and per request. A repeat
// past either is charged as usual instead of waiting.
const (
	maxWaiters       = 256
	maxWaitersPerKey = 8
)

type waiters struct {
	mu    sync.Mutex
	total int
	per   map[string]int
}

func (w *waiters) enter(key string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.total >= maxWaiters || w.per[key] >= maxWaitersPerKey {
		return false
	}
	if w.per == nil {
		w.per = map[string]int{}
	}
	w.total++
	w.per[key]++
	return true
}

func (w *waiters) leave(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.total--
	if w.per[key]--; w.per[key] <= 0 {
		delete(w.per, key)
	}
}

func newFlightToken() string {
	var b [16]byte
	rand.Read(b[:])
	return hexEncode(b[:])
}

func flightKey(key string) string { return key + ":flight" }

// keptHeaders are the answer's headers returned with a kept answer. Others
// (cookies, the upstream's own headers) are not kept.
var keptHeaders = []string{"content-type", "content-encoding", "content-language", "access-control-allow-origin", "vary",
	"cache-control", "x-content-type-options"}

// keyHeaders are the request headers that commonly pick a different
// answer to the same URL: who the caller is to the API (one wallet may pay
// for many users), and which form of the answer it accepts. They are part
// of the key, hashed, so a repeat must send the same ones.
var keyHeaders = []string{"authorization", "proxy-authorization", "cookie", "x-api-key",
	"accept", "accept-language", "accept-encoding", "origin"}

// redeliveryKey names the kept answer to a request: who paid, on which
// network and asset, for which route, method, host, path and query, and
// with which keyHeaders.
func redeliveryKey(network, asset, payer, route, method, authority, path string, hdr map[string]string) string {
	h := sha256.New()
	parts := []string{network, strings.ToLower(asset), strings.ToLower(payer), route, method, strings.ToLower(authority), path}
	for _, k := range keyHeaders {
		v := hdr[k]
		if k == "cookie" {
			// As the API gets them: without Cloudflare Access's session
			// cookies, which are new on every request and never forwarded.
			v = upstreamCookies(v)
		}
		parts = append(parts, v)
	}
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return "x402:{redeliver}:" + hexEncode(h.Sum(nil))
}

// redeliverable reports whether a GET may have its answer kept or be
// answered from one: not a range request (its answer is a part), and
// without a body (the key does not cover one). A body is one with a
// Content-Length: Transfer-Encoding alone says little, since a proxy may
// forward a bodiless GET chunked.
func redeliverable(hdr map[string]string) bool {
	if _, ok := hdr["range"]; ok {
		return false
	}
	if cl, ok := hdr["content-length"]; ok && strings.TrimSpace(cl) != "0" {
		return false
	}
	return true
}

// varyCovered reports whether every request header the answer says it
// varies with is in the key.
func varyCovered(vary string) bool {
	for _, f := range strings.Split(vary, ",") {
		f = strings.ToLower(strings.TrimSpace(f))
		if f == "" {
			continue
		}
		if !slices.Contains(keyHeaders, f) {
			return false
		}
	}
	return true
}

func keepHeaders(hdr map[string]string) [][2]string {
	var out [][2]string
	for _, k := range keptHeaders {
		if v, ok := hdr[k]; ok {
			out = append(out, [2]string{k, v})
		}
	}
	return out
}

// redeliverResponse is the kept answer, returned again.
func redeliverResponse(d *Delivered) *extprocv3.ProcessingResponse {
	set := make([]*corev3.HeaderValueOption, 0, len(d.Headers)+4)
	cache := false
	for _, h := range d.Headers {
		set = append(set, setHeader(h[0], h[1]))
		cache = cache || h[0] == "cache-control"
	}
	if !cache {
		set = append(set, setHeader("cache-control", "no-store"))
	}
	set = append(set,
		setHeader(x402.HeaderPaymentResponse, d.PaymentResponse),
		setHeader("access-control-expose-headers", "PAYMENT-RESPONSE"),
		setHeader(headerRedelivered, "true"),
	)
	return immediate(typev3.StatusCode(d.Status), set, d.Body, "x402_redelivered")
}

// RedisRedeliveries keeps answers in the gateway's Redis, at most Budget
// bytes (default DefaultRedeliverBudget) per window.
type RedisRedeliveries struct {
	RDB    redis.UniversalClient
	Budget int64
}

// keepScript counts the answer against the window's budget and keeps it if
// it fits. The budget key lives two windows, so kept bytes stay under about
// twice the budget.
var keepScript = redis.NewScript(`
local used = redis.call('INCRBY', KEYS[2], ARGV[3])
redis.call('PEXPIRE', KEYS[2], 2 * tonumber(ARGV[2]))
if used > tonumber(ARGV[4]) then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 1`)

// ErrRedeliverBudget: the window's budget is spent; the answer is not kept.
var ErrRedeliverBudget = errors.New("redelivery budget spent for this window")

func (r RedisRedeliveries) Get(ctx context.Context, key string) (*Delivered, error) {
	b, err := r.RDB.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var d Delivered
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (r RedisRedeliveries) Put(ctx context.Context, key string, d *Delivered, ttl time.Duration) error {
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	budget := r.Budget
	if budget <= 0 {
		budget = DefaultRedeliverBudget
	}
	bucket := "x402:{redeliver}:budget:" + strconv.FormatInt(time.Now().UnixMilli()/ttl.Milliseconds(), 10)
	ok, err := keepScript.Run(ctx, r.RDB, []string{key, bucket}, b, ttl.Milliseconds(), len(b), budget).Int()
	if err != nil {
		return err
	}
	if ok == 0 {
		return ErrRedeliverBudget
	}
	return nil
}

func (r RedisRedeliveries) Claim(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	return r.RDB.SetNX(ctx, flightKey(key), token, ttl).Result()
}

func (r RedisRedeliveries) InFlight(ctx context.Context, key string) (bool, error) {
	n, err := r.RDB.Exists(ctx, flightKey(key)).Result()
	return n > 0, err
}

var unclaimScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end
return 0`)

func (r RedisRedeliveries) Unclaim(ctx context.Context, key, token string) error {
	return unclaimScript.Run(ctx, r.RDB, []string{flightKey(key)}, token).Err()
}

// MemoryRedeliveries keeps answers in this process (one replica).
type MemoryRedeliveries struct {
	mu     sync.Mutex
	m      map[string]memoryDelivered
	flight map[string]memoryFlight
	used   int64
	Now    func() time.Time
	Budget int64 // bytes kept at once; default DefaultRedeliverBudget
}

type memoryDelivered struct {
	d       Delivered
	expires time.Time
}

func (m *MemoryRedeliveries) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *MemoryRedeliveries) Get(_ context.Context, key string) (*Delivered, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.m[key]
	if !ok {
		return nil, nil
	}
	if !m.now().Before(e.expires) {
		m.used -= int64(len(e.d.Body))
		delete(m.m, key)
		return nil, nil
	}
	d := e.d
	return &d, nil
}

func (m *MemoryRedeliveries) Put(_ context.Context, key string, d *Delivered, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m == nil {
		m.m = map[string]memoryDelivered{}
	}
	now := m.now()
	for k, e := range m.m { // drop what has expired
		if !now.Before(e.expires) {
			m.used -= int64(len(e.d.Body))
			delete(m.m, k)
		}
	}
	budget := m.Budget
	if budget <= 0 {
		budget = DefaultRedeliverBudget
	}
	if old, ok := m.m[key]; ok {
		m.used -= int64(len(old.d.Body))
	}
	if m.used+int64(len(d.Body)) > budget {
		delete(m.m, key)
		return ErrRedeliverBudget
	}
	m.used += int64(len(d.Body))
	m.m[key] = memoryDelivered{d: *d, expires: now.Add(ttl)}
	return nil
}

type memoryFlight struct {
	token   string
	expires time.Time
}

func (m *MemoryRedeliveries) Claim(_ context.Context, key, token string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.flight == nil {
		m.flight = map[string]memoryFlight{}
	}
	now := m.now()
	if f, ok := m.flight[key]; ok && now.Before(f.expires) {
		return false, nil
	}
	m.flight[key] = memoryFlight{token, now.Add(ttl)}
	return true, nil
}

func (m *MemoryRedeliveries) InFlight(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.flight[key]
	return ok && m.now().Before(f.expires), nil
}

func (m *MemoryRedeliveries) Unclaim(_ context.Context, key, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f, ok := m.flight[key]; ok && f.token == token {
		delete(m.flight, key)
	}
	return nil
}

// upstreamCookies is a Cookie header without Cloudflare Access's cookies.
func upstreamCookies(v string) string {
	var keep []string
	for _, c := range strings.Split(v, ";") {
		c = strings.TrimSpace(c)
		if name, _, _ := strings.Cut(c, "="); c != "" && !accessCookie(strings.TrimSpace(name)) {
			keep = append(keep, c)
		}
	}
	return strings.Join(keep, "; ")
}
