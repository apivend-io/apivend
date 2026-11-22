// Package pricing turns route configuration into the price table the filter
// consults on every request.
//
// The file format mirrors the PaymentPolicy spec from the design doc so that
// the controller can later generate it from CRDs. The controller must validate
// policies with Load/Build from this package: one parser, so it can never
// accept what the filter would refuse.
package pricing

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/verify"
	"apivend.io/apivend/internal/x402"
)

// Config is the on-disk price table.
type Config struct {
	Routes []RouteConfig `json:"routes"`
	// Batch holds the gateway's batch-settlement channel terms; required
	// when any route accepts scheme batch-settlement.
	Batch *BatchConfig `json:"batch,omitempty"`
	// Refuse lists Envoy route name prefixes (httproute/<ns>/<name>/) whose
	// PaymentPolicy exists but was rejected: the filter answers 503 there,
	// never serves them free.
	Refuse []string `json:"refuse,omitempty"`
}

// BatchConfig: the receiver authorizer is our claim-signing key's address
// (the settler holds the key); withdrawDelay in seconds (15 min to 30 days).
type BatchConfig struct {
	ReceiverAuthorizer string `json:"receiverAuthorizer"`
	WithdrawDelay      uint64 `json:"withdrawDelay"`
}

type RouteConfig struct {
	Name        string         `json:"name"`
	Match       MatchConfig    `json:"match"`
	Description string         `json:"description,omitempty"`
	MimeType    string         `json:"mimeType,omitempty"`
	Price       PriceConfig    `json:"price"`
	Accepts     []AcceptConfig `json:"accepts"`
	// MaxTimeoutSeconds is advertised to clients as the payment's lifetime.
	MaxTimeoutSeconds int              `json:"maxTimeoutSeconds,omitempty"`
	Settlement        SettlementConfig `json:"settlement,omitempty"`
	Risk              RiskConfig       `json:"risk,omitempty"`
	// Discovery describes the route for x402 catalogues.
	Discovery *DiscoveryConfig `json:"discovery,omitempty"`
}

// DiscoveryConfig describes a route for x402 catalogues: resource metadata
// and the bazaar extension ({"info": ..., "schema": ...}). Publish puts them
// in the route's 402 and on the payments it settles, which is what lists the
// route (CDP's Bazaar indexes it after the first settled payment). Without
// Publish the description is kept for the gateway's own use only.
type DiscoveryConfig struct {
	Publish     bool            `json:"publish,omitempty"`
	ServiceName string          `json:"serviceName,omitempty"`
	Tags        []string        `json:"tags,omitempty"`
	IconURL     string          `json:"iconUrl,omitempty"`
	Bazaar      json.RawMessage `json:"bazaar"`
}

type SettlementConfig struct {
	// Mode is "async" (default: respond, then settle) or "sync" (hold the
	// response until the payment has settled).
	Mode string `json:"mode,omitempty"`
	// SyncTimeout bounds the wait in sync mode, e.g. "10s" (the default).
	// The gateway's ext_proc message timeout must be at least this long.
	SyncTimeout string       `json:"syncTimeout,omitempty"`
	ProviderRef *ProviderRef `json:"providerRef,omitempty"`
	// MaxUnsettledPerPayer caps, in the price currency, what one payer may
	// have in flight plus served-but-not-yet-settled. Default 1.00.
	MaxUnsettledPerPayer string `json:"maxUnsettledPerPayer,omitempty"`
	// MaxUnsettledPerRoute caps, in the price currency, what the route may
	// have served-but-not-yet-settled across all payers. Empty: no cap.
	MaxUnsettledPerRoute string `json:"maxUnsettledPerRoute,omitempty"`
}

type ProviderRef struct {
	Name string `json:"name"`
}

type RiskConfig struct {
	// Freshness is the maximum age of a cached payer balance, e.g. "5s".
	Freshness string `json:"freshness,omitempty"`
	// Simulate is reserved; setting it true is rejected for now.
	Simulate bool `json:"simulate,omitempty"`
	// OnStateUnavailable is "deny" (default) or "allow".
	OnStateUnavailable string `json:"onStateUnavailable,omitempty"`
}

type MatchConfig struct {
	// Hosts restricts the route to these exact hostnames. Empty means any.
	Hosts []string `json:"hosts,omitempty"`
	// PathPrefix matches whole path segments, as Gateway API PathPrefix does.
	PathPrefix string `json:"pathPrefix,omitempty"`
	// EnvoyRoutePrefix matches the name of the Envoy route that selected the
	// request (the xds.route_name attribute), by prefix. Envoy Gateway names
	// the routes of an HTTPRoute "httproute/<namespace>/<name>/...", so
	// "httproute/prod/search/" prices exactly that HTTPRoute, whatever its
	// hostnames and matches are. Requires the gateway to send the attribute.
	EnvoyRoutePrefix string `json:"envoyRoutePrefix,omitempty"`
}

type PriceConfig struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type AcceptConfig struct {
	Network string `json:"network"`
	Asset   string `json:"asset"`
	PayTo   string `json:"payTo"`
	// Schemes offered for this asset: exact (default) and/or batch-settlement.
	Schemes []string `json:"schemes,omitempty"`
}

const (
	defaultMaxTimeoutSeconds = 60
	defaultMaxUnsettled      = "1.00"
	defaultProvider          = "default"
)

// DefaultSyncTimeout is how long a sync route waits for settlement.
const DefaultSyncTimeout = 10 * time.Second

// Route is a priced route ready for the request path.
type Route struct {
	Name        string
	Description string
	MimeType    string
	// Discovery, when set, describes the route for catalogues; it is put on
	// the wire only when Discovery.Publish is true.
	Discovery *Discovery
	Offers    []verify.Offer
	// Freshness bounds the age of the payer balance a decision may use.
	Freshness time.Duration
	// AllowOnUnavailable serves requests when payer state cannot be reached.
	AllowOnUnavailable bool
	// Provider names the SettlementProvider that settles this route.
	Provider string
	// Sync holds the response until settlement, for at most SyncTimeout.
	Sync        bool
	SyncTimeout time.Duration

	hosts       map[string]struct{}
	prefix      string
	hasPrefix   bool
	routePrefix string
}

// Discovery is a route's validated catalogue description.
type Discovery struct {
	Publish     bool
	ServiceName string
	Tags        []string
	IconURL     string
	// Bazaar is the extension value, compact JSON.
	Bazaar json.RawMessage
}

// Table is an immutable set of priced routes.
type Table struct {
	routes []*Route // longest prefix first
	refuse []string
}

// Refused reports whether envoyRoute belongs to a route whose policy was
// rejected (Config.Refuse).
func (t *Table) Refused(envoyRoute string) bool {
	for _, p := range t.refuse {
		if envoyRoute != "" && strings.HasPrefix(envoyRoute, p) {
			return true
		}
	}
	return false
}

// Load reads and validates a price table file.
func Load(path string) (*Table, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	t, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return t, nil
}

// Parse validates a price table given as YAML (the content of the file Load
// reads, or of the controller's ConfigMap key).
func Parse(raw []byte) (*Table, error) {
	var cfg Config
	if err := yaml.UnmarshalStrict(raw, &cfg); err != nil {
		return nil, err
	}
	return Build(cfg)
}

// Empty is a table with no priced routes.
func Empty() *Table { return &Table{} }

// Build validates cfg and resolves it into a Table.
func Build(cfg Config) (*Table, error) {
	t := &Table{}
	for i, p := range cfg.Refuse {
		if parts := strings.Split(p, "/"); len(parts) != 4 || parts[0] != "httproute" || parts[1] == "" || parts[2] == "" || parts[3] != "" {
			return nil, fmt.Errorf("refuse[%d] %q: an Envoy route name prefix httproute/<namespace>/<name>/", i, p)
		}
		t.refuse = append(t.refuse, p)
	}
	names := map[string]struct{}{}
	for i, rc := range cfg.Routes {
		r, err := buildRoute(rc, cfg.Batch)
		if err != nil {
			return nil, fmt.Errorf("routes[%d] (%q): %w", i, rc.Name, err)
		}
		if _, dup := names[r.Name]; dup {
			return nil, fmt.Errorf("routes[%d]: duplicate name %q", i, r.Name)
		}
		names[r.Name] = struct{}{}
		t.routes = append(t.routes, r)
	}
	// Most specific first: longest path prefix, then longest route-name prefix.
	sort.SliceStable(t.routes, func(i, j int) bool {
		a, b := t.routes[i], t.routes[j]
		if len(a.prefix) != len(b.prefix) {
			return len(a.prefix) > len(b.prefix)
		}
		return len(a.routePrefix) > len(b.routePrefix)
	})
	return t, nil
}

func buildRoute(rc RouteConfig, batch *BatchConfig) (*Route, error) {
	if rc.Name == "" {
		return nil, errors.New("name is required")
	}
	if rc.Match.PathPrefix == "" && rc.Match.EnvoyRoutePrefix == "" {
		return nil, errors.New("match needs pathPrefix or envoyRoutePrefix")
	}
	if rc.Match.PathPrefix != "" && !strings.HasPrefix(rc.Match.PathPrefix, "/") {
		return nil, errors.New("match.pathPrefix must start with /")
	}
	if !strings.EqualFold(rc.Price.Currency, "USD") {
		return nil, fmt.Errorf("price.currency %q is not supported (only USD)", rc.Price.Currency)
	}
	if len(rc.Accepts) == 0 {
		return nil, errors.New("accepts must list at least one network")
	}
	timeout := rc.MaxTimeoutSeconds
	if timeout == 0 {
		timeout = defaultMaxTimeoutSeconds
	}
	if timeout < 0 {
		return nil, errors.New("maxTimeoutSeconds must be positive")
	}

	r := &Route{
		Name:        rc.Name,
		Description: rc.Description,
		MimeType:    rc.MimeType,
		Freshness:   payerstate.DefaultFreshness,
		Provider:    defaultProvider,
		prefix:      strings.TrimRight(rc.Match.PathPrefix, "/"),
		hasPrefix:   rc.Match.PathPrefix != "",
		routePrefix: rc.Match.EnvoyRoutePrefix,
	}
	switch rc.Settlement.Mode {
	case "", "async":
	case "sync":
		r.Sync, r.SyncTimeout = true, DefaultSyncTimeout
		if rc.Settlement.SyncTimeout != "" {
			d, err := time.ParseDuration(rc.Settlement.SyncTimeout)
			if err != nil || d <= 0 || d > time.Minute {
				return nil, fmt.Errorf("settlement.syncTimeout %q is not a duration between 0 and 1m", rc.Settlement.SyncTimeout)
			}
			r.SyncTimeout = d
		}
	default:
		return nil, fmt.Errorf("settlement.mode %q is not async or sync", rc.Settlement.Mode)
	}
	if rc.Settlement.ProviderRef != nil {
		if rc.Settlement.ProviderRef.Name == "" {
			return nil, errors.New("settlement.providerRef.name is empty")
		}
		r.Provider = rc.Settlement.ProviderRef.Name
	}
	if rc.Risk.Simulate {
		return nil, errors.New("risk.simulate is not implemented yet")
	}
	if rc.Discovery != nil {
		d, err := buildDiscovery(rc.Description, rc.Discovery)
		if err != nil {
			return nil, fmt.Errorf("discovery: %w", err)
		}
		r.Discovery = d
	}
	if rc.Risk.Freshness != "" {
		d, err := time.ParseDuration(rc.Risk.Freshness)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("risk.freshness %q is not a positive duration", rc.Risk.Freshness)
		}
		r.Freshness = d
	}
	switch rc.Risk.OnStateUnavailable {
	case "", "deny":
	case "allow":
		r.AllowOnUnavailable = true
	default:
		return nil, fmt.Errorf("risk.onStateUnavailable %q is not deny or allow", rc.Risk.OnStateUnavailable)
	}
	maxUnsettled := rc.Settlement.MaxUnsettledPerPayer
	if maxUnsettled == "" {
		maxUnsettled = defaultMaxUnsettled
	}
	if len(rc.Match.Hosts) > 0 {
		r.hosts = map[string]struct{}{}
		for _, h := range rc.Match.Hosts {
			r.hosts[strings.ToLower(h)] = struct{}{}
		}
	}

	var batchOffers []verify.Offer
	for i, ac := range rc.Accepts {
		asset, err := lookupAsset(ac.Network, ac.Asset)
		if err != nil {
			return nil, fmt.Errorf("accepts[%d]: %w", i, err)
		}
		if !asset.usdPegged {
			return nil, fmt.Errorf("accepts[%d]: %s is not USD-pegged; cannot price in USD", i, ac.Asset)
		}
		payTo, err := evm.ParseAddress(ac.PayTo)
		if err != nil {
			return nil, fmt.Errorf("accepts[%d]: payTo: %w", i, err)
		}
		if payTo == (evm.Address{}) {
			return nil, fmt.Errorf("accepts[%d]: payTo is the zero address", i)
		}
		amount, err := toAtomic(rc.Price.Amount, asset.decimals)
		if err != nil {
			return nil, fmt.Errorf("price.amount: %w", err)
		}
		limit, err := toAtomic(maxUnsettled, asset.decimals)
		if err != nil {
			return nil, fmt.Errorf("settlement.maxUnsettledPerPayer: %w", err)
		}
		if limit.Cmp(amount) < 0 {
			return nil, fmt.Errorf("settlement.maxUnsettledPerPayer %s is below the price %s: no request could ever be served",
				maxUnsettled, rc.Price.Amount)
		}
		var routeLimit *big.Int
		if m := rc.Settlement.MaxUnsettledPerRoute; m != "" {
			if routeLimit, err = toAtomic(m, asset.decimals); err != nil {
				return nil, fmt.Errorf("settlement.maxUnsettledPerRoute: %w", err)
			}
			if routeLimit.Cmp(amount) < 0 {
				return nil, fmt.Errorf("settlement.maxUnsettledPerRoute %s is below the price %s: no request could ever be served",
					m, rc.Price.Amount)
			}
		}
		schemes := ac.Schemes
		if len(schemes) == 0 {
			schemes = []string{x402.SchemeExact}
		}
		for _, scheme := range schemes {
			if scheme != x402.SchemeExact && scheme != x402.SchemeBatch {
				return nil, fmt.Errorf("accepts[%d]: scheme %q: exact or batch-settlement", i, scheme)
			}
		}
		if slices.Contains(schemes, x402.SchemeBatch) {
			o, err := batchOffer(asset, payTo, amount, limit, timeout, batch)
			if err != nil {
				return nil, fmt.Errorf("accepts[%d]: %w", i, err)
			}
			batchOffers = append(batchOffers, o) // listed after the exact offers
		}
		if !slices.Contains(schemes, x402.SchemeExact) {
			continue
		}
		r.Offers = append(r.Offers, verify.Offer{
			MaxUnsettled:         limit,
			MaxUnsettledPerRoute: routeLimit,
			Wire: x402.Requirements{
				Scheme:            x402.SchemeExact,
				Network:           asset.caip2,
				Amount:            amount.String(),
				Asset:             asset.contract.String(),
				PayTo:             payTo.String(),
				MaxTimeoutSeconds: timeout,
				Extra:             map[string]any{"name": asset.domainName, "version": asset.domainVersion},
			},
			Amount: amount,
			PayTo:  payTo,
			Domain: evm.Domain{
				Name:              asset.domainName,
				Version:           asset.domainVersion,
				ChainID:           big.NewInt(asset.chainID),
				VerifyingContract: asset.contract,
			},
		})
	}
	r.Offers = append(r.Offers, batchOffers...)
	return r, nil
}

// batchDepositPrices is the deposit the 402 suggests, in requests.
const batchDepositPrices = 100

func batchOffer(asset asset, payTo evm.Address, amount, limit *big.Int, timeout int, batch *BatchConfig) (verify.Offer, error) {
	if batch == nil {
		return verify.Offer{}, errors.New("batch-settlement needs the gateway's batch terms (receiver authorizer)")
	}
	auth, err := evm.ParseAddress(batch.ReceiverAuthorizer)
	if err != nil || auth == (evm.Address{}) {
		return verify.Offer{}, fmt.Errorf("batch.receiverAuthorizer %q: not an address", batch.ReceiverAuthorizer)
	}
	if batch.WithdrawDelay < evm.MinWithdrawDelay || batch.WithdrawDelay > evm.MaxWithdrawDelay {
		return verify.Offer{}, fmt.Errorf("batch.withdrawDelay %d: 900 to 2592000 seconds", batch.WithdrawDelay)
	}
	minDeposit := new(big.Int).Mul(amount, big.NewInt(batchDepositPrices))
	return verify.Offer{
		MaxUnsettled: limit,
		Wire: x402.Requirements{
			Scheme:            x402.SchemeBatch,
			Network:           asset.caip2,
			Amount:            amount.String(),
			Asset:             asset.contract.String(),
			PayTo:             payTo.String(),
			MaxTimeoutSeconds: timeout,
			Extra: map[string]any{
				"name": asset.domainName, "version": asset.domainVersion,
				"receiverAuthorizer": auth.String(), "withdrawDelay": batch.WithdrawDelay,
				"minDeposit": minDeposit.String(),
			},
		},
		Amount: amount,
		PayTo:  payTo,
		Domain: evm.Domain{Name: asset.domainName, Version: asset.domainVersion, ChainID: big.NewInt(asset.chainID), VerifyingContract: asset.contract},
		Batch:  &verify.BatchTerms{ReceiverAuthorizer: auth, WithdrawDelay: batch.WithdrawDelay, MinDeposit: minDeposit},
	}, nil
}

// Match returns the priced route for a request, or nil if the request is free.
// host may carry a port; path may carry a query string; envoyRoute is the
// xds.route_name attribute, or "" when the gateway does not send it.
func (t *Table) Match(host, path, envoyRoute string) *Route {
	if i := strings.LastIndexByte(host, ':'); i >= 0 && !strings.HasSuffix(host, "]") {
		host = host[:i]
	}
	host = strings.ToLower(host)
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	for _, r := range t.routes {
		if r.routePrefix != "" && !strings.HasPrefix(envoyRoute, r.routePrefix) {
			continue
		}
		if r.hosts != nil {
			if _, ok := r.hosts[host]; !ok {
				continue
			}
		}
		if !r.hasPrefix || path == r.prefix || strings.HasPrefix(path, r.prefix+"/") {
			return r
		}
	}
	return nil
}

// Len reports how many priced routes the table holds.
func (t *Table) Len() int { return len(t.routes) }

// Route returns the route with this name (the PaymentPolicy's
// "namespace/name"), or nil.
func (t *Table) Route(name string) *Route {
	for _, r := range t.routes {
		if r.Name == name {
			return r
		}
	}
	return nil
}

// toAtomic converts a decimal string such as "0.01" to atomic token units.
// It is exact: more fractional digits than the token has are an error.
func toAtomic(s string, decimals int) (*big.Int, error) {
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" && frac == "" {
		return nil, errors.New("empty amount")
	}
	if len(frac) > decimals {
		return nil, fmt.Errorf("%q has more than %d decimal places", s, decimals)
	}
	digits := whole + frac + strings.Repeat("0", decimals-len(frac))
	v, err := evm.ParseUint256(digits)
	if err != nil {
		return nil, fmt.Errorf("%q is not a decimal amount", s)
	}
	if v.Sign() == 0 {
		return nil, errors.New("amount must be greater than zero")
	}
	return v, nil
}

// Networks lists the CAIP-2 networks the table accepts payment on.
func (t *Table) Networks() []string {
	seen := map[string]struct{}{}
	var out []string
	for _, r := range t.routes {
		for i := range r.Offers {
			n := r.Offers[i].Wire.Network
			if _, ok := seen[n]; !ok {
				seen[n] = struct{}{}
				out = append(out, n)
			}
		}
	}
	sort.Strings(out)
	return out
}

// Limits from CDP's listing requirements and the bazaar extension
// specification: catalogues reject longer descriptions, more tags or longer
// service names (a too-long description fails the payment itself at CDP).
const (
	maxDiscoveryDescription = 500
	maxServiceName          = 32
	maxTags                 = 5
	maxBazaarBytes          = 16 << 10
)

func buildDiscovery(description string, dc *DiscoveryConfig) (*Discovery, error) {
	if description == "" {
		return nil, errors.New("a route described for catalogues needs a description")
	}
	if len(description) > maxDiscoveryDescription {
		return nil, fmt.Errorf("description is %d characters; catalogues accept at most %d", len(description), maxDiscoveryDescription)
	}
	if len(dc.ServiceName) > maxServiceName {
		return nil, fmt.Errorf("serviceName is longer than %d characters", maxServiceName)
	}
	for _, c := range dc.ServiceName {
		if c < 0x20 || c > 0x7e {
			return nil, errors.New("serviceName must be printable ASCII")
		}
	}
	if len(dc.Tags) > maxTags {
		return nil, fmt.Errorf("at most %d tags", maxTags)
	}
	if dc.IconURL != "" && !strings.HasPrefix(dc.IconURL, "https://") {
		return nil, errors.New("iconUrl must be an https URL")
	}
	if len(dc.Bazaar) > maxBazaarBytes {
		return nil, fmt.Errorf("bazaar is larger than %d bytes", maxBazaarBytes)
	}
	var ext struct {
		Info *struct {
			Input map[string]any `json:"input"`
		} `json:"info"`
		Schema map[string]any `json:"schema"`
	}
	if err := json.Unmarshal(dc.Bazaar, &ext); err != nil {
		return nil, fmt.Errorf("bazaar is not a JSON object: %w", err)
	}
	if ext.Info == nil || ext.Info.Input == nil || ext.Schema == nil {
		return nil, errors.New("bazaar needs info.input and schema")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, dc.Bazaar); err != nil {
		return nil, err
	}
	return &Discovery{Publish: dc.Publish, ServiceName: dc.ServiceName, Tags: dc.Tags, IconURL: dc.IconURL, Bazaar: compact.Bytes()}, nil
}
