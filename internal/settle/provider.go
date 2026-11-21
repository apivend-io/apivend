package settle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/yaml"

	"apivend.io/apivend/internal/x402"
)

// Outcome classifies one settlement attempt.
type Outcome int

const (
	// Settled: the transfer landed.
	Settled Outcome = iota
	// Pending: a transaction was broadcast but not confirmed. Do not
	// resubmit; check the chain.
	Pending
	// Refused: the backend understood the payment and will not settle it.
	Refused
	// Unavailable: the backend could not be reached or failed; another
	// backend, or a later attempt, may work.
	Unavailable
)

// Result is what a backend reports.
type Result struct {
	Outcome     Outcome
	Transaction string
	Reason      string // x402 error code, or a short description
}

// Backend settles one payment.
type Backend interface {
	Name() string
	Settle(ctx context.Context, job *Job) Result
}

// Capable is a Backend that can say which payments it handles. known is
// false when it cannot tell (its list could not be fetched); the settler then
// tries it anyway rather than lose a working backend.
type Capable interface {
	Supports(ctx context.Context, x402Version int, scheme, network string) (ok, known bool)
}

// ProvidersConfig is the on-disk form of the SettlementProvider objects.
type ProvidersConfig struct {
	Providers []ProviderConfig `json:"providers"`
}

type ProviderConfig struct {
	Name     string          `json:"name"`
	Backends []BackendConfig `json:"backends"`
}

type BackendConfig struct {
	// Type is "facilitator" or "direct" (the settler submits the transfer
	// itself, gas paid by the key in Token/CredentialsFile).
	Type string `json:"type"`
	URL  string `json:"url,omitempty"`
	// CredentialsFile holds a bearer token for the facilitator, typically a
	// mounted Secret. Optional.
	CredentialsFile string `json:"credentialsFile,omitempty"`
	// Token is the bearer token inline. The controller uses it when it
	// renders providers into a Secret; prefer CredentialsFile by hand.
	Token string `json:"token,omitempty"`
	// Auth is how the credentials are used: "bearer" (default) sends them
	// as a static bearer token; "cdp" treats them as a Coinbase Developer
	// Platform Secret API key file and sends a fresh JWT on every request.
	Auth string `json:"auth,omitempty"`
	// Priority orders backends; lower is tried first.
	Priority int `json:"priority,omitempty"`
	// MaxFee (type direct) is the most one settlement may spend on gas, in
	// ETH, e.g. "0.00002" (the default).
	MaxFee string `json:"maxFee,omitempty"`
}

// Option configures BuildProviders.
type Option func(*buildOptions)

type buildOptions struct{ chains map[string]Chain }

// WithChains gives direct backends the RPC of each network (CAIP-2), from
// the settler's -rpc flags. Without it a direct backend supports no network.
func WithChains(chains map[string]Chain) Option {
	return func(o *buildOptions) { o.chains = chains }
}

// LoadProviders reads a providers file into backends by provider name.
func LoadProviders(path string, httpTimeout time.Duration, opts ...Option) (map[string][]Backend, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p, err := ParseProviders(raw, httpTimeout, opts...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return p, nil
}

// ParseProviders reads a providers document (the file LoadProviders reads,
// or the controller's Secret key).
func ParseProviders(raw []byte, httpTimeout time.Duration, opts ...Option) (map[string][]Backend, error) {
	var cfg ProvidersConfig
	if err := yaml.UnmarshalStrict(raw, &cfg); err != nil {
		return nil, err
	}
	return BuildProviders(cfg, httpTimeout, opts...)
}

// BuildProviders validates cfg and constructs its backends.
func BuildProviders(cfg ProvidersConfig, httpTimeout time.Duration, opts ...Option) (map[string][]Backend, error) {
	var o buildOptions
	for _, f := range opts {
		f(&o)
	}
	out := map[string][]Backend{}
	for i, pc := range cfg.Providers {
		if pc.Name == "" {
			return nil, fmt.Errorf("providers[%d]: name is required", i)
		}
		if _, dup := out[pc.Name]; dup {
			return nil, fmt.Errorf("providers[%d]: duplicate name %q", i, pc.Name)
		}
		if len(pc.Backends) == 0 {
			return nil, fmt.Errorf("provider %q: at least one backend is required", pc.Name)
		}
		bcs := append([]BackendConfig(nil), pc.Backends...)
		sort.SliceStable(bcs, func(a, b int) bool { return bcs[a].Priority < bcs[b].Priority })
		for j, bc := range bcs {
			switch bc.Type {
			case "facilitator":
				f, err := newFacilitator(bc, httpTimeout)
				if err != nil {
					return nil, fmt.Errorf("provider %q backend %d: %w", pc.Name, j, err)
				}
				out[pc.Name] = append(out[pc.Name], f)
			case "direct":
				d, err := newDirect(bc, o.chains)
				if err != nil {
					return nil, fmt.Errorf("provider %q backend %d: %w", pc.Name, j, err)
				}
				out[pc.Name] = append(out[pc.Name], d)
			default:
				return nil, fmt.Errorf("provider %q: unknown backend type %q", pc.Name, bc.Type)
			}
		}
	}
	return out, nil
}

// Facilitator settles through an x402 facilitator's POST /settle.
type Facilitator struct {
	name   string
	url    string
	host   string // for the CDP JWT: host without scheme
	path   string // for the CDP JWT: path of the settle endpoint
	token  string
	cdp    *cdpKey
	client *http.Client
	now    func() time.Time

	// What GET /supported last said, by kindKey; refreshed every
	// supportedTTL, or after supportedRetry when a fetch failed.
	supMu    sync.Mutex
	sup      map[string]bool
	supNext  time.Time
	supFetch bool
}

const (
	supportedTTL     = 10 * time.Minute
	supportedRetry   = time.Minute
	supportedTimeout = 3 * time.Second
)

func kindKey(version int, scheme, network string) string {
	return fmt.Sprintf("%d/%s/%s", version, scheme, network)
}

// Supports answers from the facilitator's GET /supported list (x402
// specification v2 section 7.3), fetched at most every supportedTTL. With
// no list yet (never fetched, or the facilitator does not publish one) the
// answer is not known.
func (f *Facilitator) Supports(ctx context.Context, version int, scheme, network string) (ok, known bool) {
	f.supMu.Lock()
	refresh := !f.supFetch && !f.now().Before(f.supNext)
	if refresh {
		f.supFetch = true
	}
	f.supMu.Unlock()
	if refresh {
		kinds, err := f.fetchSupported(ctx)
		f.supMu.Lock()
		f.supFetch = false
		if err == nil {
			f.sup, f.supNext = kinds, f.now().Add(supportedTTL)
		} else {
			f.supNext = f.now().Add(supportedRetry) // keep the last good list
		}
		f.supMu.Unlock()
	}
	f.supMu.Lock()
	defer f.supMu.Unlock()
	if f.sup == nil {
		return false, false
	}
	return f.sup[kindKey(version, scheme, network)], true
}

func (f *Facilitator) fetchSupported(ctx context.Context) (map[string]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, supportedTimeout)
	defer cancel()
	u := strings.TrimSuffix(f.url, "/settle") + "/supported"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	switch {
	case f.cdp != nil:
		jwt, err := f.cdp.jwt(http.MethodGet, f.host, strings.TrimSuffix(f.path, "/settle")+"/supported", f.now())
		if err != nil {
			return nil, err
		}
		req.Header.Set("authorization", "Bearer "+jwt)
	case f.token != "":
		req.Header.Set("authorization", "Bearer "+f.token)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("supported: http %d", resp.StatusCode)
	}
	var body struct {
		Kinds []struct {
			X402Version int    `json:"x402Version"`
			Scheme      string `json:"scheme"`
			Network     string `json:"network"`
		} `json:"kinds"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return nil, fmt.Errorf("supported: %w", err)
	}
	out := map[string]bool{}
	for _, k := range body.Kinds {
		out[kindKey(k.X402Version, k.Scheme, k.Network)] = true
	}
	return out, nil
}

func newFacilitator(bc BackendConfig, timeout time.Duration) (*Facilitator, error) {
	u, err := url.Parse(bc.URL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, errors.New("facilitator url must be an http(s) URL")
	}
	f := &Facilitator{
		name:   u.Host,
		url:    strings.TrimRight(bc.URL, "/") + "/settle",
		host:   u.Host,
		path:   strings.TrimRight(u.EscapedPath(), "/") + "/settle",
		client: &http.Client{Timeout: timeout},
		now:    time.Now,
	}
	f.token = bc.Token
	if bc.CredentialsFile != "" {
		if bc.Token != "" {
			return nil, errors.New("set credentialsFile or token, not both")
		}
		b, err := os.ReadFile(bc.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("credentialsFile: %w", err)
		}
		f.token = strings.TrimSpace(string(b))
	}
	switch bc.Auth {
	case "", "bearer":
	case "cdp":
		if f.token == "" {
			return nil, errors.New("auth cdp needs credentials: the CDP API key file")
		}
		if u.Scheme != "https" {
			return nil, errors.New("auth cdp needs an https url")
		}
		k, err := parseCDPKey(f.token)
		if err != nil {
			return nil, err
		}
		f.cdp, f.token = k, ""
	default:
		return nil, fmt.Errorf("unknown auth %q (bearer or cdp)", bc.Auth)
	}
	return f, nil
}

func (f *Facilitator) Name() string { return f.name }

type settleRequest struct {
	X402Version         int                  `json:"x402Version"`
	PaymentPayload      *x402.PaymentPayload `json:"paymentPayload"`
	PaymentRequirements *x402.Requirements   `json:"paymentRequirements"`
}

// errSettlementPending is the non-terminal code a facilitator returns when it
// broadcast a transaction but could not confirm it.
const errSettlementPending = "settlement_pending"

func (f *Facilitator) Settle(ctx context.Context, job *Job) Result {
	body, err := json.Marshal(settleRequest{X402Version: x402.Version, PaymentPayload: &job.Payload, PaymentRequirements: &job.Requirements})
	if err != nil {
		return Result{Outcome: Refused, Reason: "unencodable_job"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, bytes.NewReader(body))
	if err != nil {
		return Result{Outcome: Unavailable, Reason: "bad_request"}
	}
	req.Header.Set("content-type", "application/json")
	switch {
	case f.cdp != nil:
		jwt, err := f.cdp.jwt(http.MethodPost, f.host, f.path, f.now())
		if err != nil {
			return Result{Outcome: Unavailable, Reason: "cdp_jwt"}
		}
		req.Header.Set("authorization", "Bearer "+jwt)
	case f.token != "":
		req.Header.Set("authorization", "Bearer "+f.token)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return Result{Outcome: Unavailable, Reason: "unreachable"}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	// Facilitators answer refusals with a SettleResponse body on various
	// status codes, so the body is consulted before the status.
	var sr x402.SettlementResponse
	parsed := json.Unmarshal(raw, &sr) == nil
	switch {
	case parsed && sr.Success && sr.Transaction != "":
		return Result{Outcome: Settled, Transaction: sr.Transaction}
	case parsed && sr.ErrorReason == errSettlementPending && sr.Transaction != "":
		return Result{Outcome: Pending, Transaction: sr.Transaction, Reason: errSettlementPending}
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return Result{Outcome: Unavailable, Reason: fmt.Sprintf("http_%d", resp.StatusCode)}
	case parsed && sr.ErrorReason != "" && accountReason(sr.ErrorReason):
		// The facilitator's account with us (PayAI stops settling once its
		// free credits are used up), not the payment: try the next backend.
		return Result{Outcome: Unavailable, Reason: "facilitator_account_" + sanitizeReason(sr.ErrorReason)}
	case parsed && sr.ErrorReason != "":
		return Result{Outcome: Refused, Reason: sanitizeReason(sr.ErrorReason), Transaction: sr.Transaction}
	case resp.StatusCode == http.StatusPaymentRequired:
		// A 402 without an errorReason: CDP's payment_method_required once
		// the free tier is used up and no billing is set up. Our account,
		// not the payer (a payer refusal on 402 carries an errorReason and
		// is handled above). Retried until fixed or the authorization expires.
		return Result{Outcome: Unavailable, Reason: "facilitator_billing"}
	default:
		return Result{Outcome: Unavailable, Reason: fmt.Sprintf("http_%d_unparseable", resp.StatusCode)}
	}
}

// sanitizeReason keeps a remote error code safe to log and store.
func sanitizeReason(s string) string {
	if len(s) > 80 {
		s = s[:80]
	}
	b := []byte(s)
	for i, c := range b {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.'
		if !ok {
			b[i] = '_'
		}
	}
	return string(b)
}

// accountReason reports an error reason about the facilitator's account
// (credits, quota, billing, rate limits) rather than the payment. No
// facilitator documents these codes, so this matches on the words they use.
func accountReason(reason string) bool {
	r := strings.ToLower(reason)
	if strings.Contains(r, "insufficient_balance") || strings.Contains(r, "insufficient_funds") {
		return false // the payer's balance
	}
	for _, w := range []string{"credit", "quota", "billing", "payment_method", "rate_limit", "plan_limit", "subscription"} {
		if strings.Contains(r, w) {
			return true
		}
	}
	return false
}
