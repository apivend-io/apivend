// Package demo is the public demo site: a page showing recent payments,
// statistics over the whole payment record (our own test wallets counted
// apart), the live chain head and the prices of the demo routes.
//
// Its numbers come from the payment record (receipts in PostgreSQL), never
// from the gateway's monitoring metrics: those belong to whatever
// observability stack runs alongside, and restart with the pods. Prices come
// from the controller's price table. It holds no keys and makes no payments.
package demo

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/x402"
)

//go:embed page.html
var pageHTML []byte

// Brand assets and the fonts (SIL Open Font License, see FONTS-OFL.txt),
// served from the binary: the page makes no third-party requests.
//
//go:embed assets
var assets embed.FS

// Route is a demo route visitors can try.
type Route struct {
	Name string `json:"name"`
	// URL is the public URL of the priced route.
	URL  string `json:"url"`
	Note string `json:"note,omitempty"`
	// Key is the HTTPRoute as "namespace/name", as receipts record it, so the
	// page can name feed rows. Optional.
	Key string `json:"key,omitempty"`
}

// Server serves the demo page and its JSON.
type Server struct {
	// DB reads receipts. Give it a read-only role. Optional: without it the
	// feed is empty.
	DB     *sql.DB
	Routes []Route
	// Prices, when set, returns the current price table (the controller's
	// x402-routes ConfigMap): the menu then shows each route's terms from it
	// without sending the route an unpaid request. Without it the routes are
	// checked over HTTP (see ProbeVia).
	Prices func() *pricing.Table
	// Explorers maps a CAIP-2 network to a transaction URL prefix.
	Explorers map[string]string
	Client    *http.Client
	// Heads reads the latest block per network (CAIP-2), for the page's live
	// block counter. Optional; one read serves every visitor.
	Heads map[string]HeadReader

	// OwnPayers are the operator's own test wallets (lowercase addresses).
	// The page labels their receipts and counts payers other than them, so
	// a visitor sees openly who has paid so far.
	OwnPayers []string

	// ProbeVia, when set, is the base URL the route check is sent to instead
	// of the public URL (e.g. the gateway's in-cluster Service), with the
	// public host as Host and X-Forwarded-Proto https. Behind an access gate
	// the public URL answers with a login page, not with the route's terms.
	ProbeVia string

	mu      sync.Mutex
	cached  map[string]cacheEntry
	cacheOK time.Duration
	// One refresh at a time per key: when an entry expires under many
	// viewers, the database or the RPC sees one call, not one per viewer.
	refreshing map[string]*sync.Mutex
}

// HeadReader is the part of the chain client the block counter needs.
type HeadReader interface {
	Head(ctx context.Context) (uint64, string, error)
}

type cacheEntry struct {
	at   time.Time
	body []byte
}

// DefaultExplorers covers the networks the gateway knows.
var DefaultExplorers = map[string]string{
	"eip155:8453":  "https://basescan.org/tx/",
	"eip155:84532": "https://sepolia.basescan.org/tx/",
}

func (s *Server) Handler() http.Handler {
	if s.Client == nil {
		s.Client = &http.Client{Timeout: 5 * time.Second}
	}
	// A redirect is an answer about the route (a login page, a moved route),
	// never something to follow and report as the route's status.
	s.Client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if s.Explorers == nil {
		s.Explorers = DefaultExplorers
	}
	s.cached = map[string]cacheEntry{}
	s.cacheOK = 3 * time.Second

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// 'self' scripts: the coin slot (three.js) is served from /assets.
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'unsafe-inline'; script-src 'self' 'unsafe-inline'")
		w.Write(pageHTML)
	})
	static := http.FileServerFS(assets)
	mux.HandleFunc("GET /assets/{file}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=86400")
		static.ServeHTTP(w, r)
	})
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=86400")
		http.ServeFileFS(w, r, assets, "assets/favicon.ico")
	})
	mux.HandleFunc("GET /api/feed", s.cache("feed", s.cacheOK, s.feed))
	mux.HandleFunc("GET /api/stats", s.cache("stats", s.cacheOK, s.stats))
	mux.HandleFunc("GET /api/routes", s.cache("routes", s.cacheOK, s.routes))
	mux.HandleFunc("GET /api/totals", s.cache("totals", s.cacheOK, s.totals))
	// Base makes a block every 2 s; a second of sharing keeps the counter live.
	mux.HandleFunc("GET /api/chain", s.cache("chain", time.Second, s.chainHeads))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	return mux
}

// cache shares one computed answer between visitors for a few seconds, so
// page views do not multiply into database and scrape load.
func (s *Server) cache(key string, ttl time.Duration, f func(context.Context) (any, error)) http.HandlerFunc {
	s.mu.Lock()
	if s.refreshing == nil {
		s.refreshing = map[string]*sync.Mutex{}
	}
	lock := &sync.Mutex{}
	s.refreshing[key] = lock
	s.mu.Unlock()
	get := func() (cacheEntry, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		e, ok := s.cached[key]
		return e, ok && time.Since(e.at) <= ttl
	}
	maxAge := fmt.Sprintf("public, max-age=%d", max(1, int(ttl/time.Second)))
	return func(w http.ResponseWriter, r *http.Request) {
		e, fresh := get()
		if !fresh {
			lock.Lock()
			if e, fresh = get(); !fresh { // another request may have refreshed it meanwhile
				v, err := f(r.Context())
				if err != nil {
					lock.Unlock()
					http.Error(w, `{"error":"temporarily unavailable"}`, http.StatusServiceUnavailable)
					return
				}
				body, _ := json.Marshal(v)
				e = cacheEntry{at: time.Now(), body: body}
				s.mu.Lock()
				s.cached[key] = e
				s.mu.Unlock()
			}
			lock.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", maxAge)
		w.Write(e.body)
	}
}

// Head is the latest block of one network.
type Head struct {
	Network string `json:"network"`
	Block   uint64 `json:"block"`
}

// chainHeads reads the latest block of each configured network. A network
// whose RPC fails is left out rather than failing the whole answer.
func (s *Server) chainHeads(ctx context.Context) (any, error) {
	out := []Head{}
	for network, h := range s.Heads {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		n, _, err := h.Head(ctx)
		cancel()
		if err == nil && n > 0 {
			out = append(out, Head{Network: network, Block: n})
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Network < out[b].Network })
	return out, nil
}

// Totals counts every settled payment so far, per network: test USDC is
// never added to real money.
type Totals struct {
	Network string `json:"network"`
	Count   int64  `json:"count"`
	Amount  string `json:"amount"` // atomic units
}

func (s *Server) totals(ctx context.Context) (any, error) {
	out := []Totals{}
	if s.DB == nil {
		return out, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
SELECT chain, count(*), coalesce(sum(amount), 0)::text
FROM x402_receipts WHERE (status = 'settled' AND tx_hash <> '') OR status = 'batch_charged'
GROUP BY chain ORDER BY chain`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var t Totals
		if err := rows.Scan(&t.Network, &t.Count, &t.Amount); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Payment is one row of the live feed.
type Payment struct {
	At      time.Time `json:"at"`
	Route   string    `json:"route"`
	Amount  string    `json:"amount"` // atomic units
	Network string    `json:"network"`
	Payer   string    `json:"payer"`
	TxHash  string    `json:"txHash"`
	TxURL   string    `json:"txUrl,omitempty"`
	Block   int64     `json:"block,omitempty"`
	// Own is true for the operator's own test wallets (see OwnPayers).
	Own bool `json:"own,omitempty"`
}

// feed lists recent settled payments. Only payments with a transaction hash
// are shown: every row must resolve on the explorer.
func (s *Server) feed(ctx context.Context) (any, error) {
	out := []Payment{}
	if s.DB == nil {
		return out, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
SELECT recorded_at, route, amount::text, chain, payer, tx_hash, block
FROM x402_receipts WHERE status = 'settled' AND tx_hash <> ''
ORDER BY id DESC LIMIT 25`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var p Payment
		if err := rows.Scan(&p.At, &p.Route, &p.Amount, &p.Network, &p.Payer, &p.TxHash, &p.Block); err != nil {
			return nil, err
		}
		if prefix := s.Explorers[p.Network]; prefix != "" {
			p.TxURL = prefix + p.TxHash
		}
		p.Own = s.own(p.Payer)
		out = append(out, p)
	}
	return out, rows.Err()
}

// RouteTerms is a demo route with its current terms: from the price table,
// or decoded from what the route answers to an unpaid request.
type RouteTerms struct {
	Route
	Status int                   `json:"status,omitempty"` // HTTP checks only
	Priced bool                  `json:"priced"`
	Terms  *x402.PaymentRequired `json:"terms,omitempty"`
}

// Stats are the whole payment record so far, over all routes.
type Stats struct {
	Answered int64 `json:"answered"` // paid requests the API answered
	Settled  int64 `json:"settled"`  // settled on chain, with a transaction
	Unpaid   int64 `json:"unpaid"`   // answered, but the payment could not be settled
	Payers   int64 `json:"payers"`   // distinct payer addresses among the answered
	// OtherPayers excludes the operator's own test wallets; OwnPayers is
	// how many of those have paid.
	OtherPayers int64 `json:"otherPayers"`
	OwnPayers   int64 `json:"ownPayers"`
	// Since is the first answered paid request; zero when there is none.
	Since *time.Time `json:"since,omitempty"`
	// SettleSeconds is the median time from the API's answer to the
	// settlement being recorded; 0 when nothing settled.
	SettleSeconds float64 `json:"settleSeconds"`
}

func (s *Server) stats(ctx context.Context) (any, error) {
	var st Stats
	if s.DB == nil {
		return st, nil
	}
	// One row per reservation and stage; served_at is copied onto the later
	// stages. A sync route settles before it answers: counted as 0 s.
	err := s.DB.QueryRowContext(ctx, `
SELECT count(*) FILTER (WHERE status IN ('served', 'batch_charged')),
       count(*) FILTER (WHERE status = 'settled' AND tx_hash <> ''),
       count(*) FILTER (WHERE status = 'unpaid'),
       count(DISTINCT payer) FILTER (WHERE status IN ('served', 'batch_charged')),
       count(DISTINCT payer) FILTER (WHERE status IN ('served', 'batch_charged') AND NOT (lower(payer) = ANY($1))),
       coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY greatest(0, extract(epoch FROM recorded_at - served_at)))
                FILTER (WHERE status = 'settled' AND tx_hash <> '' AND served_at IS NOT NULL), 0),
       min(recorded_at) FILTER (WHERE status = 'served')
FROM x402_receipts`, pgTextArray(s.OwnPayers)).
		Scan(&st.Answered, &st.Settled, &st.Unpaid, &st.Payers, &st.OtherPayers, &st.SettleSeconds, &st.Since)
	st.OwnPayers = st.Payers - st.OtherPayers
	return st, err
}

// routes asks each configured demo route, unpaid, and decodes the challenge.
// Only the configured URLs are ever fetched.
func (s *Server) routes(ctx context.Context) (any, error) {
	if s.Prices != nil {
		return s.routesFromTable(), nil
	}
	out := make([]RouteTerms, len(s.Routes))
	var wg sync.WaitGroup
	for i, r := range s.Routes {
		out[i].Route = r
		wg.Add(1)
		go func(i int, url string) {
			defer wg.Done()
			req, err := s.probeRequest(ctx, url)
			if err != nil {
				return
			}
			resp, err := s.Client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			out[i].Status = resp.StatusCode
			var pr x402.PaymentRequired
			if resp.StatusCode == http.StatusPaymentRequired && x402.DecodeHeader(resp.Header.Get("PAYMENT-REQUIRED"), &pr) == nil {
				out[i].Priced, out[i].Terms = true, &pr
			}
		}(i, r.URL)
	}
	wg.Wait()
	sort.SliceStable(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out, nil
}

// probeRequest builds the unpaid request for a route check: to the public
// URL, or through ProbeVia with the public host and scheme carried in headers.
func (s *Server) probeRequest(ctx context.Context, public string) (*http.Request, error) {
	if s.ProbeVia == "" {
		return http.NewRequestWithContext(ctx, http.MethodGet, public, nil)
	}
	u, err := url.Parse(public)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(s.ProbeVia, "/")+u.RequestURI(), nil)
	if err != nil {
		return nil, err
	}
	req.Host = u.Host
	req.Header.Set("X-Forwarded-Proto", u.Scheme)
	return req, nil
}

// routesFromTable gives each route's terms as its 402 would advertise them
// (each offer's wire form), read from the price table. A route missing from
// the table is free right now: its PaymentPolicy is gone or not accepted.
func (s *Server) routesFromTable() []RouteTerms {
	t := s.Prices()
	out := make([]RouteTerms, len(s.Routes))
	for i, r := range s.Routes {
		out[i].Route = r
		pr := (*pricing.Route)(nil)
		if t != nil && r.Key != "" {
			pr = t.Route(r.Key)
		}
		if pr == nil {
			continue
		}
		terms := &x402.PaymentRequired{X402Version: x402.Version, Resource: x402.ResourceInfo{URL: resourceOf(r.URL), Description: pr.Description}}
		for _, o := range pr.Offers {
			terms.Accepts = append(terms.Accepts, o.Wire)
		}
		out[i].Priced, out[i].Terms = len(terms.Accepts) > 0, terms
	}
	sort.SliceStable(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out
}

// resourceOf is the URL without its query, as the gateway names a resource.
func resourceOf(u string) string {
	if i := strings.IndexByte(u, '?'); i >= 0 {
		return u[:i]
	}
	return u
}

func (s *Server) own(payer string) bool {
	for _, o := range s.OwnPayers {
		if strings.EqualFold(o, payer) {
			return true
		}
	}
	return false
}

// pgTextArray renders addresses as a PostgreSQL text[] literal for = ANY($1).
// Addresses are 0x and hex only; anything else is left out.
func pgTextArray(addrs []string) string {
	var ok []string
	for _, a := range addrs {
		a = strings.ToLower(a)
		if len(a) == 42 && strings.HasPrefix(a, "0x") && strings.Trim(a[2:], "0123456789abcdef") == "" {
			ok = append(ok, a)
		}
	}
	return "{" + strings.Join(ok, ",") + "}"
}
