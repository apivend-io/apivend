package demo_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"apivend.io/apivend/internal/demo"
	"apivend.io/apivend/internal/pgtest"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/receipts"
	"apivend.io/apivend/internal/x402"
)

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s: %d %s", url, resp.StatusCode, b)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func TestDemoAPIs(t *testing.T) {
	text := func(body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, body) }))
	}
	priced := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h, _ := x402.EncodeHeader(x402.PaymentRequired{X402Version: 2, Accepts: []x402.Requirements{{Scheme: "exact", Network: "eip155:84532", Amount: "1000"}}})
		w.Header().Set("PAYMENT-REQUIRED", h)
		w.WriteHeader(http.StatusPaymentRequired)
	}))
	defer priced.Close()
	free := text("hello")
	defer free.Close()

	dsn := pgtest.Start(t)
	var store *receipts.Postgres
	var err error
	for i := 0; i < 50; i++ {
		if store, err = receipts.OpenPostgres(context.Background(), dsn); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Three answered requests from two payers: a settled with a transaction
	// two seconds after the answer, b settled without one (must not be shown:
	// it cannot be checked on an explorer), c unpaid.
	served := time.Now().Add(-2 * time.Second)
	base := receipts.Receipt{Route: "demo/search", Payer: "0x11d0dcb977c973731215d7005665159d21faf0c5", Amount: "1000", Asset: "0xasset", Chain: "eip155:84532", Nonce: "0x1", ServedAt: served}
	for _, r := range []struct{ id, status, tx, payer string }{
		{"a", "served", "", "0x11d0dcb977c973731215d7005665159d21faf0c5"}, {"a", "settled", "0xabc", "0x11d0dcb977c973731215d7005665159d21faf0c5"},
		{"b", "served", "", "0x11d0dcb977c973731215d7005665159d21faf0c5"}, {"b", "settled", "", "0x11d0dcb977c973731215d7005665159d21faf0c5"},
		{"c", "served", "", "0x744d86e964064e45db518bbc6c1ab91ce92241c4"}, {"c", "unpaid", "", "0x744d86e964064e45db518bbc6c1ab91ce92241c4"},
		// A seller's endpoint on the same gateway: not one of the demo's
		// routes, so it is on no tape, total or count.
		{"d", "served", "", "0x9965507d1a55bcc2695c58ba16fb37d819b0a4dc"}, {"d", "settled", "0xdef", "0x9965507d1a55bcc2695c58ba16fb37d819b0a4dc"},
	} {
		rc := base
		rc.ReservationID, rc.Status, rc.TxHash, rc.Payer = r.id, r.status, r.tx, r.payer
		if r.id == "d" {
			rc.Route = "t-geo/route-1"
		}
		if err := store.Record(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}

	srv := httptest.NewServer((&demo.Server{
		DB:        store.DB(),
		Routes:    []demo.Route{{Name: "search", URL: priced.URL, Key: "demo/search"}, {Name: "hello", URL: free.URL, Note: "policy deleted"}},
		OwnPayers: []string{"0x11d0dcb977c973731215d7005665159d21faf0c5", "not-an-address'); DROP TABLE x402_receipts; --"},
	}).Handler())
	defer srv.Close()

	// Statistics from the whole payment record; our own test wallet is
	// counted apart (and a junk entry in the list is ignored, not run).
	var st demo.Stats
	getJSON(t, srv.URL+"/api/stats", &st)
	if st.Answered != 3 || st.Settled != 1 || st.Unpaid != 1 || st.Payers != 2 || st.OtherPayers != 1 || st.OwnPayers != 1 || st.Since == nil {
		t.Fatalf("stats = %+v", st)
	}
	if st.SettleSeconds < 1.5 || st.SettleSeconds > 10 {
		t.Fatalf("median answer-to-settlement = %v s, want about 2", st.SettleSeconds)
	}

	var feed []demo.Payment
	getJSON(t, srv.URL+"/api/feed", &feed)
	if len(feed) != 1 || feed[0].TxHash != "0xabc" || feed[0].TxURL != "https://sepolia.basescan.org/tx/0xabc" || feed[0].Amount != "1000" || !feed[0].Own {
		t.Fatalf("feed = %+v", feed)
	}

	// One payer's own receipts; a malformed address is refused.
	getJSON(t, srv.URL+"/api/feed?payer=0x11D0DCB977C973731215D7005665159D21FAF0C5", &feed)
	if len(feed) != 1 || feed[0].TxHash != "0xabc" {
		t.Fatalf("feed for a payer = %+v", feed)
	}
	getJSON(t, srv.URL+"/api/feed?payer=0x744d86e964064e45db518bbc6c1ab91ce92241c4", &feed)
	if len(feed) != 0 {
		t.Fatalf("feed for a payer with nothing settled = %+v", feed)
	}
	if resp, err := http.Get(srv.URL + "/api/feed?payer=nobody'--"); err != nil || resp.StatusCode != 400 {
		t.Fatalf("bad payer: %v %v", resp, err)
	}

	// The menu is in the HTML for readers without JavaScript, and llms.txt
	// names the sandbox, its price and where receipts are.
	page := getText(t, srv.URL+"/")
	if !strings.Contains(page, ">search</h3>") || !strings.Contains(page, `href="`+priced.URL+`"`) || strings.Contains(page, "Loading the menu") {
		t.Fatalf("server-rendered menu missing:\n%s", page[:min(len(page), 400)])
	}
	// The payment record is in the HTML too: the settled receipt with its
	// transaction (not the one without, nor the seller's), the totals and
	// the counters.
	if !strings.Contains(page, `<span class="what">search</span><span class="amt">$0.001</span>`) ||
		!strings.Contains(page, `href="https://sepolia.basescan.org/tx/0xabc"`) || strings.Contains(page, "0xdef") ||
		!strings.Contains(page, `<dt>1 payment in the sandbox</dt><dd>$0.001 test</dd>`) ||
		!strings.Contains(page, `<span>Paid requests answered</span><b>3</b>`) || !strings.Contains(page, `<span>Payers other than us</span><b>1</b>`) ||
		strings.Contains(page, `id="totals" hidden`) || !strings.Contains(page, `href="https://docs.apivend.dev"`) {
		t.Fatalf("server-rendered payment record missing:\n%s", page)
	}
	// Without JavaScript the commands and snippets name a real route, and
	// the page points agents at llms.txt.
	if strings.Contains(page, "{URL}") || strings.Contains(page, "curl -i …") || !strings.Contains(page, `curl -i &#34;`+priced.URL+`&#34;`) || !strings.Contains(page, `href="/llms.txt"`) {
		t.Fatalf("commands not filled in, or no llms.txt link")
	}
	llms := getText(t, srv.URL+"/llms.txt")
	for _, want := range []string{"## Sandbox", "[search](" + priced.URL + "): $0.001 in USDC on Base Sepolia", "/api/feed?payer=", "eip155:84532 only"} {
		if !strings.Contains(llms, want) {
			t.Fatalf("llms.txt lacks %q:\n%s", want, llms)
		}
	}

	// Only settled payments with a transaction count, per network.
	var totals []demo.Totals
	getJSON(t, srv.URL+"/api/totals", &totals)
	if len(totals) != 1 || totals[0].Network != "eip155:84532" || totals[0].Count != 1 || totals[0].Amount != "1000" {
		t.Fatalf("totals = %+v", totals)
	}

	var routes []demo.RouteTerms
	getJSON(t, srv.URL+"/api/routes", &routes)
	if len(routes) != 2 || routes[0].Name != "hello" || routes[0].Priced || routes[0].Status != 200 ||
		!routes[1].Priced || routes[1].Terms.Accepts[0].Amount != "1000" {
		t.Fatalf("routes = %+v", routes)
	}

	resp, err := http.Get(srv.URL + "/")
	if err != nil || resp.StatusCode != 200 || resp.Header.Get("Content-Security-Policy") == "" {
		t.Fatalf("page: %v %v", resp, err)
	}
}

func TestBalanceBackend(t *testing.T) {
	state := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.Header.Get("Authorization") != "Bearer k" || q.Get("chain") != "eip155:84532" ||
			q.Get("asset") != "0x036cbd53842c5426634e7929541ec2318f3dcf7e" || q.Get("payer") != "0x70997970c51812dc3a010c7d01b50e0d17dc79c8" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		io.WriteString(w, `{"balance":"5000","observation":{"blockHeight":"101","blockHash":"0xabc","observedAt":"2026-11-17T00:00:00Z","provider":"p"}}`)
	}))
	defer state.Close()
	srv := httptest.NewServer((&demo.Backend{StateURL: state.URL, StateKey: "k"}).Handler())
	defer srv.Close()

	var got map[string]string
	getJSON(t, srv.URL+"/v1/balance?address=0x70997970C51812dc3A010C7d01b50e0d17dc79C8", &got)
	if got["balanceAtomic"] != "5000" || got["blockHeight"] != "101" || got["blockHash"] != "0xabc" || got["network"] != "base-sepolia" {
		t.Fatalf("balance = %v", got)
	}
	if resp, _ := http.Get(srv.URL + "/v1/balance?address=nope"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad address: %d", resp.StatusCode)
	}
	// Source down: a 5xx, so the gateway does not charge for it.
	state.Close()
	if resp, _ := http.Get(srv.URL + "/v1/balance?address=0x70997970C51812dc3A010C7d01b50e0d17dc79C8"); resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("source down: %d", resp.StatusCode)
	}
}

// Behind an access gate the public URL answers with a login redirect, so the
// check goes through the gateway's in-cluster address with the public host;
// and a redirect is reported, never followed into a 200 that reads as "free".
func TestRoutesProbeVia(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Host == "api.example.dev" && r.URL.RequestURI() == "/v1/name-check?name=a-b" && r.Header.Get("X-Forwarded-Proto") == "https":
			pr := x402.PaymentRequired{X402Version: 2, Accepts: []x402.Requirements{{Scheme: "exact", Network: "eip155:8453", Amount: "1000"}}}
			h, _ := x402.EncodeHeader(pr)
			w.Header().Set("PAYMENT-REQUIRED", h)
			w.WriteHeader(http.StatusPaymentRequired)
		case r.Host == "gated.example.dev":
			http.Redirect(w, r, "/login", http.StatusFound)
		case r.URL.Path == "/login":
			io.WriteString(w, "please log in")
		default:
			http.Error(w, "unexpected "+r.Host+r.URL.RequestURI(), http.StatusTeapot)
		}
	}))
	defer gateway.Close()
	srv := httptest.NewServer((&demo.Server{ProbeVia: gateway.URL, Routes: []demo.Route{
		{Name: "a", URL: "https://api.example.dev/v1/name-check?name=a-b", Key: "api/namecheck"},
		{Name: "b", URL: "https://gated.example.dev/"},
	}}).Handler())
	defer srv.Close()

	var routes []demo.RouteTerms
	getJSON(t, srv.URL+"/api/routes", &routes)
	if len(routes) != 2 || !routes[0].Priced || routes[0].Terms.Accepts[0].Amount != "1000" || routes[0].Key != "api/namecheck" {
		t.Fatalf("priced route through the gateway: %+v", routes)
	}
	if routes[1].Priced || routes[1].Status != http.StatusFound {
		t.Fatalf("gated route must report the redirect, got %+v", routes[1])
	}
}

// The page's fonts and icons come from the binary, not from a third party.
func TestAssetsServed(t *testing.T) {
	srv := httptest.NewServer((&demo.Server{}).Handler())
	defer srv.Close()
	for path, ctype := range map[string]string{
		"/assets/space-grotesk.woff2":        "font/woff2",
		"/assets/jetbrains-mono.woff2":       "font/woff2",
		"/assets/apivend-lockup-on-dark.svg": "image/svg+xml",
		"/assets/favicon.svg":                "image/svg+xml",
		"/favicon.ico":                       "",
		"/assets/FONTS-OFL.txt":              "text/plain; charset=utf-8",
		"/assets/slot.js":                    "text/javascript; charset=utf-8",
		"/assets/three.module.min.js":        "text/javascript; charset=utf-8",
		"/assets/RoomEnvironment.js":         "text/javascript; charset=utf-8",
		"/assets/apivend-mark-on-dark.svg":   "image/svg+xml",
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil || resp.StatusCode != 200 || (ctype != "" && resp.Header.Get("Content-Type") != ctype) {
			t.Errorf("%s: %v %v %q", path, err, resp.StatusCode, resp.Header.Get("Content-Type"))
		}
		resp.Body.Close()
	}
	if resp, _ := http.Get(srv.URL + "/assets/../demo.go"); resp.StatusCode == 200 {
		t.Error("path escaped the assets directory")
	}
}

type countingHead struct {
	mu    sync.Mutex
	calls int
	block uint64
	err   error
}

func (h *countingHead) Head(context.Context) (uint64, string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	time.Sleep(20 * time.Millisecond) // a slow RPC, so viewers pile up on it
	return h.block, "0xhash", h.err
}

// The block counter: many viewers at once cost one RPC read, and a network
// whose RPC fails is left out instead of failing the answer.
func TestChainHeads(t *testing.T) {
	base := &countingHead{block: 59219891}
	broken := &countingHead{err: errors.New("rpc down")}
	srv := httptest.NewServer((&demo.Server{Heads: map[string]demo.HeadReader{"eip155:8453": base, "eip155:84532": broken}}).Handler())
	defer srv.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var heads []demo.Head
			getJSON(t, srv.URL+"/api/chain", &heads)
			if len(heads) != 1 || heads[0].Network != "eip155:8453" || heads[0].Block != 59219891 {
				t.Errorf("heads = %+v", heads)
			}
		}()
	}
	wg.Wait()
	base.mu.Lock()
	defer base.mu.Unlock()
	if base.calls != 1 {
		t.Fatalf("20 concurrent viewers made %d RPC reads, want 1", base.calls)
	}
}

// With the price table, the menu shows each route's terms from it and sends
// the route nothing; a route without a policy shows as free.
func TestRoutesFromPriceTable(t *testing.T) {
	table, err := pricing.Parse([]byte(`routes:
- name: api/namecheck
  match: { envoyRoutePrefix: httproute/api/namecheck/ }
  price: { amount: "0.001", currency: USD }
  accepts: [{ network: base, asset: USDC, payTo: "0xFA85448D290bCD2D2C224565D3c02695C250688C" }]
`))
	if err != nil {
		t.Fatal(err)
	}
	hit := false
	never := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit = true }))
	defer never.Close()
	srv := httptest.NewServer((&demo.Server{
		Prices: func() *pricing.Table { return table },
		Routes: []demo.Route{
			{Name: "a", Key: "api/namecheck", URL: never.URL + "/v1/name-check?name=x"},
			{Name: "b", Key: "demo/gone", URL: never.URL + "/v1/balance"},
		},
	}).Handler())
	defer srv.Close()

	var routes []demo.RouteTerms
	getJSON(t, srv.URL+"/api/routes", &routes)
	if hit {
		t.Fatal("the menu sent a route a request")
	}
	if len(routes) != 2 || !routes[0].Priced || routes[1].Priced {
		t.Fatalf("routes = %+v", routes)
	}
	a := routes[0].Terms.Accepts[0]
	if a.Network != "eip155:8453" || a.Amount != "1000" || a.Asset != "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913" ||
		routes[0].Terms.Resource.URL != never.URL+"/v1/name-check" {
		t.Fatalf("terms = %+v", routes[0].Terms)
	}
}

func getText(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("GET %s: %v %v", url, resp, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}
