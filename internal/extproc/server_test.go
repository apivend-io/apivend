package extproc_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/types/known/structpb"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/engine/statesource"
	"apivend.io/apivend/internal/extproc"
	"apivend.io/apivend/internal/payer"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/redistest"
	"apivend.io/apivend/internal/settle"
	"apivend.io/apivend/internal/x402"
)

const (
	payerKey = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
	payTo    = "0x209693Bc6afc0C5328bA36FaF03C514EF312287C"
)

// spyState wraps Memory and records what happened to reservations.
type spyState struct {
	*payerstate.Memory
	events     chan string
	reserveErr error
	commitErr  error
	lastJob    payerstate.Job
	lastReq    payerstate.Request
}

func (s *spyState) Reserve(ctx context.Context, r payerstate.Request) (payerstate.ReservationID, error) {
	s.lastReq = r
	if s.reserveErr != nil {
		return "", s.reserveErr
	}
	return s.Memory.Reserve(ctx, r)
}
func (s *spyState) Release(ctx context.Context, id payerstate.ReservationID) error {
	err := s.Memory.Release(ctx, id)
	s.events <- "release"
	return err
}
func (s *spyState) Commit(ctx context.Context, id payerstate.ReservationID, job payerstate.Job) error {
	if s.commitErr != nil {
		return s.commitErr
	}
	s.lastJob = job
	err := s.Memory.Commit(ctx, id, job)
	s.events <- "commit"
	return err
}

func (s *spyState) expect(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-s.events:
		if got != want {
			t.Fatalf("state event = %s, want %s", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("no state event; want %s", want)
	}
}

func (s *spyState) expectNone(t *testing.T) {
	t.Helper()
	select {
	case got := <-s.events:
		t.Fatalf("unexpected state event %s", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func testTable(t *testing.T) *pricing.Table {
	t.Helper()
	route := func(name, prefix string) pricing.RouteConfig {
		return pricing.RouteConfig{
			Name:        name,
			Description: "Search API",
			Match:       pricing.MatchConfig{PathPrefix: prefix},
			Price:       pricing.PriceConfig{Amount: "0.01", Currency: "USD"},
			Accepts:     []pricing.AcceptConfig{{Network: "base-sepolia", Asset: "USDC", PayTo: payTo}},
			Settlement:  pricing.SettlementConfig{MaxUnsettledPerPayer: "0.03"},
		}
	}
	lenient := route("lenient", "/lenient")
	lenient.Risk.OnStateUnavailable = "allow"
	sync := route("sync", "/sync")
	sync.Settlement.Mode, sync.Settlement.SyncTimeout = "sync", "300ms"
	listed := route("listed", "/listed")
	listed.Discovery = &pricing.DiscoveryConfig{Publish: true, ServiceName: "Search", Tags: []string{"search"},
		Bazaar: json.RawMessage(testBazaar)}
	needs := route("needs", "/needs")
	needs.RequiredQuery = []string{"address"}
	quiet := route("quiet", "/quiet")
	quiet.Discovery = &pricing.DiscoveryConfig{ServiceName: "Quiet", Bazaar: json.RawMessage(testBazaar)}
	table, err := pricing.Build(pricing.Config{Routes: []pricing.RouteConfig{route("search", "/search"), lenient, sync, listed, quiet, needs}})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func setup(t *testing.T) (extprocv3.ExternalProcessorClient, *spyState) {
	t.Helper()
	state := &spyState{Memory: payerstate.NewMemory(), events: make(chan string, 16)}
	return serve(t, state), state
}

func serve(t *testing.T, state payerstate.PayerState) extprocv3.ExternalProcessorClient {
	return serveWith(t, state, extproc.Options{})
}

func serveWith(t *testing.T, state payerstate.PayerState, o extproc.Options) extprocv3.ExternalProcessorClient {
	t.Helper()
	return serveTable(t, testTable(t), state, o)
}

func serveTable(t *testing.T, table *pricing.Table, state payerstate.PayerState, o extproc.Options) extprocv3.ExternalProcessorClient {
	t.Helper()
	o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := extproc.NewServer(table, state, o)

	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(g, srv)
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return extprocv3.NewExternalProcessorClient(conn)
}

// exchange plays Envoy for one HTTP request.
type exchange struct {
	t        *testing.T
	stream   extprocv3.ExternalProcessor_ProcessClient
	cancel   context.CancelFunc
	extended time.Duration // last message-timeout override the filter asked for
}

func open(t *testing.T, c extprocv3.ExternalProcessorClient) *exchange {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := c.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return &exchange{t: t, stream: stream, cancel: cancel}
}

func headers(kv ...string) *extprocv3.HttpHeaders {
	m := &corev3.HeaderMap{}
	for i := 0; i < len(kv); i += 2 {
		// Envoy populates raw_value, not value.
		m.Headers = append(m.Headers, &corev3.HeaderValue{Key: kv[i], RawValue: []byte(kv[i+1])})
	}
	return &extprocv3.HttpHeaders{Headers: m}
}

func (e *exchange) request(path string, extra ...string) *extprocv3.ProcessingResponse {
	e.t.Helper()
	kv := append([]string{":method", "GET", ":scheme", "https", ":authority", "api.example.com", ":path", path}, extra...)
	return e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: headers(kv...)}})
}

func (e *exchange) response(status string) *extprocv3.ProcessingResponse {
	e.t.Helper()
	return e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{ResponseHeaders: headers(":status", status)}})
}

func (e *exchange) roundTrip(req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
	e.t.Helper()
	if err := e.stream.Send(req); err != nil {
		e.t.Fatal(err)
	}
	for {
		resp, err := e.stream.Recv()
		if err != nil {
			e.t.Fatal(err)
		}
		// A request for more time is not the answer; Envoy keeps waiting.
		if resp.Response == nil && resp.OverrideMessageTimeout != nil {
			e.extended = resp.OverrideMessageTimeout.AsDuration()
			continue
		}
		return resp
	}
}

func mutation(set []*corev3.HeaderValueOption) map[string]string {
	out := map[string]string{}
	for _, h := range set {
		out[h.Header.Key] = string(h.Header.RawValue)
	}
	return out
}

// challenge asserts an immediate response and decodes its PAYMENT-REQUIRED.
func challenge(t *testing.T, resp *extprocv3.ProcessingResponse, status int) x402.PaymentRequired {
	t.Helper()
	im := resp.GetImmediateResponse()
	if im == nil {
		t.Fatalf("want immediate response, got %T", resp.Response)
	}
	if int(im.Status.Code) != status {
		t.Fatalf("status = %d, want %d", im.Status.Code, status)
	}
	var pr x402.PaymentRequired
	if err := x402.DecodeHeader(mutation(im.Headers.SetHeaders)[x402.HeaderPaymentRequired], &pr); err != nil {
		t.Fatalf("PAYMENT-REQUIRED: %v", err)
	}
	var body x402.PaymentRequired
	if err := json.Unmarshal(im.Body, &body); err != nil || body.X402Version != 2 {
		t.Fatalf("body is not the challenge: %v", err)
	}
	return pr
}

func pay(t *testing.T, pr x402.PaymentRequired) string {
	t.Helper()
	w, err := payer.NewWallet(payerKey)
	if err != nil {
		t.Fatal(err)
	}
	p, err := w.Pay(&pr, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h, err := x402.EncodeHeader(p)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestFreeRoutePassesThrough(t *testing.T) {
	c, state := setup(t)
	e := open(t, c)
	resp := e.request("/healthz")
	// Passed on unchanged, except that a client-made-up payer is dropped.
	rh := resp.GetRequestHeaders()
	if rh == nil || rh.Response == nil || len(rh.Response.HeaderMutation.SetHeaders) != 0 ||
		len(rh.Response.HeaderMutation.RemoveHeaders) != 1 || rh.Response.HeaderMutation.RemoveHeaders[0] != extproc.HeaderPayer {
		t.Fatalf("free route: %v", resp)
	}
	if rh := e.response("200").GetResponseHeaders(); rh == nil || rh.Response != nil {
		t.Fatal("free route response was touched")
	}
	state.expectNone(t)
}

func TestPreflightIsAnsweredNotForwarded(t *testing.T) {
	c, state := setup(t)
	e := open(t, c)
	resp := e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestHeaders{
		RequestHeaders: headers(":method", "OPTIONS", ":authority", "api.example.com", ":path", "/search",
			"origin", "https://app.example", "access-control-request-method", "POST",
			"access-control-request-headers", "payment-signature, content-type, bad header\r\nx: y")}})
	im := resp.GetImmediateResponse()
	if im == nil || im.Status.Code != typev3.StatusCode_NoContent {
		t.Fatalf("preflight got %v", resp.Response)
	}
	got := map[string]string{}
	for _, h := range im.Headers.SetHeaders {
		got[h.Header.Key] = string(h.Header.RawValue)
	}
	if got["access-control-allow-methods"] != "POST" || got["access-control-allow-headers"] != "payment-signature, content-type" ||
		!strings.Contains(got["access-control-expose-headers"], "payment-required") {
		t.Fatalf("preflight headers %v", got)
	}
	state.expectNone(t)
}

// An OPTIONS that is not a CORS preflight is priced like any request: an
// upstream that ignores the method must not serve it for free.
func TestPlainOptionsIsPriced(t *testing.T) {
	c, _ := setup(t)
	e := open(t, c)
	resp := e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestHeaders{
		RequestHeaders: headers(":method", "OPTIONS", ":authority", "api.example.com", ":path", "/search")}})
	if im := resp.GetImmediateResponse(); im == nil || im.Status.Code != typev3.StatusCode_PaymentRequired {
		t.Fatalf("plain OPTIONS got %v", resp.Response)
	}
}

func TestChallenge(t *testing.T) {
	c, _ := setup(t)
	pr := challenge(t, open(t, c).request("/search?q=x"), 402)

	if pr.X402Version != 2 || pr.Resource.URL != "https://api.example.com/search" || pr.Resource.Description != "Search API" {
		t.Fatalf("challenge = %+v", pr)
	}
	if len(pr.Accepts) != 1 {
		t.Fatalf("accepts = %+v", pr.Accepts)
	}
	a := pr.Accepts[0]
	if a.Scheme != "exact" || a.Network != "eip155:84532" || a.Amount != "10000" ||
		a.Asset != "0x036CbD53842c5426634e7929541eC2318f3dCF7e" || a.PayTo != payTo ||
		a.MaxTimeoutSeconds != 60 || a.Extra["name"] != "USDC" || a.Extra["version"] != "2" {
		t.Fatalf("accepts[0] = %+v", a)
	}
}

func TestPaidRequestCommitsOn2xx(t *testing.T) {
	c, state := setup(t)
	pr := challenge(t, open(t, c).request("/search"), 402)

	e := open(t, c)
	resp := e.request("/search", "payment-signature", pay(t, pr), "x-x402-payer", "0xforged", "x-request-id", "req-1")
	rh := resp.GetRequestHeaders()
	if rh == nil {
		t.Fatalf("paid request refused: %v", resp)
	}
	w, _ := payer.NewWallet(payerKey)
	hm := rh.Response.HeaderMutation
	if got := mutation(hm.SetHeaders)[extproc.HeaderPayer]; got != w.Addr.String() {
		t.Fatalf("payer header = %q", got)
	}
	if len(hm.RemoveHeaders) != 1 || hm.RemoveHeaders[0] != "payment-signature" {
		t.Fatalf("signed payment leaks to backend: %v", hm.RemoveHeaders)
	}
	state.expectNone(t)

	out := mutation(e.response("200").GetResponseHeaders().Response.HeaderMutation.SetHeaders)
	state.expect(t, "commit")
	var sr x402.SettlementResponse
	if err := x402.DecodeHeader(out[x402.HeaderPaymentResponse], &sr); err != nil {
		t.Fatal(err)
	}
	if !sr.Success || sr.Network != "eip155:84532" || sr.Payer != w.Addr.String() || sr.Transaction != "" {
		t.Fatalf("PAYMENT-RESPONSE = %+v", sr)
	}

	// The settlement job carries what the settler and the receipt need.
	var job settle.Job
	if err := json.Unmarshal(state.lastJob.Body, &job); err != nil {
		t.Fatal(err)
	}
	if job.Route != "search" || job.RequestID != "req-1" || job.Provider != "default" ||
		job.Payer != w.Addr.Hex() || job.Amount != "10000" || job.Requirements.PayTo != payTo ||
		job.Payload.Accepted.Network != "eip155:84532" || len(job.Payload.Payload) == 0 {
		t.Fatalf("job = %+v", job)
	}
	if state.lastJob.Deadline.Before(time.Now()) {
		t.Fatalf("job deadline %v is not the authorization expiry", state.lastJob.Deadline)
	}

	// Closing the stream after a commit must not release it.
	e.stream.CloseSend()
	state.expectNone(t)
}

func TestReplayIsRefused(t *testing.T) {
	c, state := setup(t)
	sig := pay(t, challenge(t, open(t, c).request("/search"), 402))

	first := open(t, c)
	if first.request("/search", "payment-signature", sig).GetRequestHeaders() == nil {
		t.Fatal("first use refused")
	}
	// Same payment while the first is still in flight.
	pr := challenge(t, open(t, c).request("/search", "payment-signature", sig), 402)
	if pr.Error != payerstate.ReasonNonceUsed {
		t.Fatalf("error = %q", pr.Error)
	}
	first.response("200")
	state.expect(t, "commit")
	// And after it was charged.
	pr = challenge(t, open(t, c).request("/search", "payment-signature", sig), 402)
	if pr.Error != payerstate.ReasonNonceUsed {
		t.Fatalf("error = %q", pr.Error)
	}
}

func TestBackendFailureReleases(t *testing.T) {
	c, state := setup(t)
	sig := pay(t, challenge(t, open(t, c).request("/search"), 402))

	for _, status := range []string{"500", "404", "302"} {
		e := open(t, c)
		if e.request("/search", "payment-signature", sig).GetRequestHeaders() == nil {
			t.Fatalf("payment refused before backend %s; previous failure was charged", status)
		}
		rh := e.response(status).GetResponseHeaders()
		if rh == nil || rh.Response != nil {
			t.Fatalf("backend %s: response was decorated: %v", status, rh)
		}
		state.expect(t, "release")
	}
}

func TestAbortedRequestReleases(t *testing.T) {
	c, state := setup(t)
	sig := pay(t, challenge(t, open(t, c).request("/search"), 402))

	e := open(t, c)
	if e.request("/search", "payment-signature", sig).GetRequestHeaders() == nil {
		t.Fatal("payment refused")
	}
	e.cancel() // client went away / upstream timed out before any response
	state.expect(t, "release")

	// The same authorization is usable again.
	if open(t, c).request("/search", "payment-signature", sig).GetRequestHeaders() == nil {
		t.Fatal("released payment not reusable")
	}
}

func TestBadPayments(t *testing.T) {
	c, state := setup(t)
	pr := challenge(t, open(t, c).request("/search"), 402)

	t.Run("not base64", func(t *testing.T) {
		got := challenge(t, open(t, c).request("/search", "payment-signature", "%%%"), 400)
		if got.Error != x402.ErrInvalidPayload {
			t.Fatalf("error = %q", got.Error)
		}
	})
	t.Run("not json", func(t *testing.T) {
		challenge(t, open(t, c).request("/search", "payment-signature", base64.StdEncoding.EncodeToString([]byte("<xml/>"))), 400)
	})
	t.Run("oversized", func(t *testing.T) {
		challenge(t, open(t, c).request("/search", "payment-signature", strings.Repeat("A", x402.MaxHeaderBytes+4)), 400)
	})
	t.Run("tampered amount", func(t *testing.T) {
		w, _ := payer.NewWallet(payerKey)
		p, _ := w.Pay(&pr, time.Now())
		var ex x402.ExactEVMPayload
		json.Unmarshal(p.Payload, &ex)
		ex.Authorization.Value = "1"
		p.Payload, _ = json.Marshal(ex)
		h, _ := x402.EncodeHeader(p)
		got := challenge(t, open(t, c).request("/search", "payment-signature", h), 402)
		if got.Error != x402.ErrValueMismatch {
			t.Fatalf("error = %q", got.Error)
		}
	})
	state.expectNone(t)
}

func TestStateUnavailableFailsClosed(t *testing.T) {
	c, state := setup(t)
	sig := pay(t, challenge(t, open(t, c).request("/search"), 402))
	state.reserveErr = errors.New("redis: connection refused")

	im := open(t, c).request("/search", "payment-signature", sig).GetImmediateResponse()
	if im == nil || im.Status.Code != 503 {
		t.Fatalf("want 503, got %v", im)
	}
}

// A route at its unsettled cap is busy, not the payer's fault: 503 with
// Retry-After, not a 402 asking to pay again. The reservation names the
// route, so the store can count it.
func TestRouteExposureLimitIs503(t *testing.T) {
	c, state := setup(t)
	sig := pay(t, challenge(t, open(t, c).request("/search"), 402))
	state.reserveErr = &payerstate.DeniedError{Reason: payerstate.ReasonRouteExposureLimit}
	im := open(t, c).request("/search", "payment-signature", sig).GetImmediateResponse()
	if im == nil || im.Status.Code != 503 || mutation(im.Headers.SetHeaders)["retry-after"] != "30" {
		t.Fatalf("want 503 with retry-after, got %v", im)
	}
	if state.lastReq.Route == "" {
		t.Fatal("reservation without its route")
	}
	state.expectNone(t)
}

func TestRouteCanPreferAvailability(t *testing.T) {
	c, state := setup(t)
	sig := pay(t, challenge(t, open(t, c).request("/lenient"), 402))
	state.reserveErr = payerstate.ErrUnavailable

	e := open(t, c)
	if e.request("/lenient", "payment-signature", sig).GetRequestHeaders() == nil {
		t.Fatal("onStateUnavailable: allow did not serve")
	}
	// No reservation exists, so nothing is committed and no settlement is claimed.
	if rh := e.response("200").GetResponseHeaders(); rh == nil || rh.Response != nil {
		t.Fatalf("unpaid response claims a payment: %v", rh)
	}
	state.expectNone(t)
}

// The filter on the real payer state: Redis reservations and a (fake) chain.
func TestWithRedisPayerState(t *testing.T) {
	rdb, _ := redistest.Start(t)
	fake := chain.NewFake()
	state := payerstate.NewRedis(rdb, statesource.NewRPC("rpc", map[string]statesource.ChainClient{"eip155:84532": fake}, statesource.RPCOptions{}))
	state.ReadTimeout = time.Second
	c := serve(t, state)
	w, _ := payer.NewWallet(payerKey)
	pr := challenge(t, open(t, c).request("/search"), 402)
	paid := func() *extprocv3.ProcessingResponse {
		return open(t, c).request("/search", "payment-signature", pay(t, pr))
	}

	// Valid signature, empty wallet.
	if got := challenge(t, paid(), 402); got.Error != x402.ErrInsufficientFunds {
		t.Fatalf("empty wallet: error = %q", got.Error)
	}

	// Funded: served, committed, queued for settlement.
	fake.SetBalance(w.Addr, 1_000_000)
	rdb.FlushAll(context.Background())
	for i := 0; i < 3; i++ {
		e := open(t, c)
		if e.request("/search", "payment-signature", pay(t, pr)).GetRequestHeaders() == nil {
			t.Fatalf("payment %d refused", i)
		}
		if e.response("200").GetResponseHeaders().Response == nil {
			t.Fatalf("payment %d: no PAYMENT-RESPONSE", i)
		}
	}
	if depth, _, err := state.QueueStats(context.Background()); err != nil || depth != 3 {
		t.Fatalf("queue depth = %d, %v", depth, err)
	}
	// Three served and unsettled at 0.01 each hits the route's 0.03 cap.
	if got := challenge(t, paid(), 402); got.Error != payerstate.ReasonExposureLimit {
		t.Fatalf("over cap: error = %q", got.Error)
	}
}

// syncState is a payer state whose settlement outcome the test dictates.
type syncState struct {
	*payerstate.Memory
	result *payerstate.Result // nil: never settles within the wait
	waited time.Duration
}

func (s *syncState) WaitResult(ctx context.Context, _ payerstate.ReservationID, timeout time.Duration) (*payerstate.Result, error) {
	s.waited = timeout
	return s.result, nil
}

func TestSyncSettlement(t *testing.T) {
	w, _ := payer.NewWallet(payerKey)
	run := func(t *testing.T, result *payerstate.Result) (*extprocv3.ProcessingResponse, *exchange, *syncState) {
		state := &syncState{Memory: payerstate.NewMemory(), result: result}
		c := serve(t, state)
		sig := pay(t, challenge(t, open(t, c).request("/sync"), 402))
		e := open(t, c)
		if e.request("/sync", "payment-signature", sig).GetRequestHeaders() == nil {
			t.Fatal("payment refused")
		}
		return e.response("200"), e, state
	}
	decode := func(t *testing.T, set []*corev3.HeaderValueOption) x402.SettlementResponse {
		var sr x402.SettlementResponse
		if err := x402.DecodeHeader(mutation(set)[x402.HeaderPaymentResponse], &sr); err != nil {
			t.Fatal(err)
		}
		return sr
	}

	t.Run("settled: real transaction in the header", func(t *testing.T) {
		resp, e, state := run(t, &payerstate.Result{Status: "settled", Transaction: "0xabc"})
		sr := decode(t, resp.GetResponseHeaders().Response.HeaderMutation.SetHeaders)
		if !sr.Success || sr.Transaction != "0xabc" || sr.Payer != w.Addr.String() {
			t.Fatalf("PAYMENT-RESPONSE = %+v", sr)
		}
		if state.waited != 300*time.Millisecond {
			t.Fatalf("waited %v, want the route's syncTimeout", state.waited)
		}
		if e.extended <= state.waited {
			t.Fatalf("asked Envoy for %v, which does not cover the %v wait", e.extended, state.waited)
		}
	})

	t.Run("unpaid: response withheld", func(t *testing.T) {
		resp, _, _ := run(t, &payerstate.Result{Status: "unpaid", Reason: "insufficient_funds"})
		im := resp.GetImmediateResponse()
		if im == nil || im.Status.Code != 402 {
			t.Fatalf("want the backend response replaced by a 402, got %v", resp)
		}
		sr := decode(t, im.Headers.SetHeaders)
		if sr.Success || sr.ErrorReason != "insufficient_funds" || sr.Transaction != "" || sr.Network != "eip155:84532" {
			t.Fatalf("PAYMENT-RESPONSE = %+v", sr)
		}
	})

	t.Run("not settled in time: served, async header", func(t *testing.T) {
		resp, _, _ := run(t, nil)
		sr := decode(t, resp.GetResponseHeaders().Response.HeaderMutation.SetHeaders)
		if !sr.Success || sr.Transaction != "" {
			t.Fatalf("PAYMENT-RESPONSE = %+v", sr)
		}
	})
}

const testBazaar = `{"info":{"input":{"type":"http","method":"GET","queryParams":{"q":"x"}}},"schema":{"type":"object","required":["input"]}}`

// payWithExtensions pays the challenge as a client that adds its own
// extensions to the payment.
func payWithExtensions(t *testing.T, pr x402.PaymentRequired, ext string) string {
	t.Helper()
	w, err := payer.NewWallet(payerKey)
	if err != nil {
		t.Fatal(err)
	}
	p, err := w.Pay(&pr, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p.Extensions = json.RawMessage(ext)
	p.Resource = &x402.ResourceInfo{URL: "https://elsewhere.example/", Description: "not this route"}
	h, err := x402.EncodeHeader(p)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func settledJob(t *testing.T, c extprocv3.ExternalProcessorClient, state *spyState, path, sig string) settle.Job {
	t.Helper()
	e := open(t, c)
	if e.request(path, "payment-signature", sig).GetRequestHeaders() == nil {
		t.Fatal("paid request refused")
	}
	e.response("200")
	state.expect(t, "commit")
	var job settle.Job
	if err := json.Unmarshal(state.lastJob.Body, &job); err != nil {
		t.Fatal(err)
	}
	return job
}

func extensions(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	m := map[string]json.RawMessage{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

func TestPublishedRouteCarriesDiscovery(t *testing.T) {
	c, state := setup(t)
	pr := challenge(t, open(t, c).request("/listed?q=x"), 402)
	if got := string(pr.Extensions[x402.ExtensionBazaar]); got != testBazaar {
		t.Fatalf("402 bazaar = %s", got)
	}
	if pr.Resource.ServiceName != "Search" || len(pr.Resource.Tags) != 1 {
		t.Fatalf("resource = %+v", pr.Resource)
	}

	// An honest client echoes the extension; the settled payment carries the
	// route's own, and the route's resource.
	job := settledJob(t, c, state, "/listed", pay(t, pr))
	if got := string(extensions(t, job.Payload.Extensions)[x402.ExtensionBazaar]); got != testBazaar {
		t.Fatalf("settled bazaar = %s", got)
	}

	// A client that rewrites the description, adds its own extension and
	// names another resource: the bazaar and resource are replaced, its other
	// extension is kept.
	pr = challenge(t, open(t, c).request("/listed?q=x"), 402)
	job = settledJob(t, c, state, "/listed", payWithExtensions(t, pr, `{"bazaar":{"info":{"input":{"type":"http","method":"GET"}},"schema":{}},"other":{"a":1}}`))
	ext := extensions(t, job.Payload.Extensions)
	if string(ext[x402.ExtensionBazaar]) != testBazaar || string(ext["other"]) != `{"a":1}` {
		t.Fatalf("settled extensions = %s", job.Payload.Extensions)
	}
	if job.Payload.Resource == nil || job.Payload.Resource.URL != "https://api.example.com/listed" || job.Payload.Resource.ServiceName != "Search" {
		t.Fatalf("settled resource = %+v", job.Payload.Resource)
	}
}

func TestUnpublishedRouteIsNeverListed(t *testing.T) {
	c, state := setup(t)
	for _, path := range []string{"/quiet", "/search"} { // described but not published; not described
		pr := challenge(t, open(t, c).request(path), 402)
		if len(pr.Extensions) != 0 || pr.Resource.ServiceName != "" {
			t.Fatalf("%s: 402 publishes discovery: %+v", path, pr)
		}
		job := settledJob(t, c, state, path, payWithExtensions(t, pr, `{"bazaar":{"info":{"input":{"type":"http","method":"GET"}},"schema":{}},"other":{"a":1}}`))
		ext := extensions(t, job.Payload.Extensions)
		if _, listed := ext[x402.ExtensionBazaar]; listed || string(ext["other"]) != `{"a":1}` {
			t.Fatalf("%s: settled extensions = %s", path, job.Payload.Extensions)
		}
	}
	// Extensions that are not an object, or null, are dropped.
	pr := challenge(t, open(t, c).request("/quiet"), 402)
	for _, ext := range []string{`null`, `[1,2]`, `"x"`} {
		job := settledJob(t, c, state, "/quiet", payWithExtensions(t, pr, ext))
		if len(job.Payload.Extensions) != 0 {
			t.Fatalf("extensions %s settled as %s", ext, job.Payload.Extensions)
		}
	}
}

type contractChecker struct {
	valid bool
	err   error
}

func (c contractChecker) IsValidSignature(context.Context, string, evm.Address, [32]byte, []byte) (bool, error) {
	return c.valid, c.err
}

// A contract wallet pays with its owner's signature: served when the wallet
// accepts it, 402 when it does not, 503 (fail closed) when the chain cannot
// be asked.
func TestContractWalletPayments(t *testing.T) {
	wallet, _ := evm.ParseAddress("0xC1f5BF53c9560C232c391EC444990CB25b960bbF")
	owner, _ := payer.NewWallet(payerKey)
	owner.As = &wallet
	for name, tc := range map[string]struct {
		checker contractChecker
		code    typev3.StatusCode
	}{
		"accepted":   {contractChecker{valid: true}, 0},
		"refused":    {contractChecker{}, typev3.StatusCode_PaymentRequired},
		"chain down": {contractChecker{err: errors.New("rpc down")}, typev3.StatusCode_ServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			state := &spyState{Memory: payerstate.NewMemory(), events: make(chan string, 16)}
			c := serveWith(t, state, extproc.Options{Contract: tc.checker})
			pr := challenge(t, open(t, c).request("/search"), 402)
			p, err := owner.Pay(&pr, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			sig, _ := x402.EncodeHeader(p)
			resp := open(t, c).request("/search", "payment-signature", sig)
			if tc.code == 0 {
				rh := resp.GetRequestHeaders()
				if rh == nil {
					t.Fatalf("refused: %v", resp)
				}
				if got := mutation(rh.Response.HeaderMutation.SetHeaders)[extproc.HeaderPayer]; got != wallet.String() {
					t.Fatalf("payer header %q, want the wallet", got)
				}
				return
			}
			ir := resp.GetImmediateResponse()
			if ir == nil || ir.Status.Code != tc.code {
				t.Fatalf("got %v, want %v", resp, tc.code)
			}
			// 503 says when to try again (outside tester's T1).
			if tc.code == typev3.StatusCode_ServiceUnavailable && mutation(ir.Headers.SetHeaders)["retry-after"] == "" {
				t.Fatal("503 without Retry-After")
			}
		})
	}
}

// A response whose payment could not be committed (the authorization
// expired while the backend worked, or Redis failed) is withheld.
func TestUncommittedResponseIsWithheld(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code typev3.StatusCode
	}{{payerstate.ErrExpired, typev3.StatusCode_PaymentRequired}, {errors.New("redis down"), typev3.StatusCode_ServiceUnavailable}} {
		c, state := setup(t)
		pr := challenge(t, open(t, c).request("/search"), 402)
		e := open(t, c)
		if rh := e.request("/search", "payment-signature", pay(t, pr)).GetRequestHeaders(); rh == nil {
			t.Fatal("paid request refused")
		}
		state.commitErr = tc.err
		im := e.response("200").GetImmediateResponse()
		if im == nil || im.Status.Code != tc.code {
			t.Fatalf("%v: response not withheld: %v", tc.err, im)
		}
	}
}

func TestUpgradeRefusedOnPricedRoute(t *testing.T) {
	c, _ := setup(t)
	e := open(t, c)
	resp := e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestHeaders{
		RequestHeaders: headers(":method", "GET", ":authority", "api.example.com", ":path", "/search", "upgrade", "websocket")}})
	if im := resp.GetImmediateResponse(); im == nil || im.Status.Code != typev3.StatusCode_BadRequest {
		t.Fatalf("upgrade got %v", resp.Response)
	}
}

// A route that must always be priced but has no prices loaded is refused,
// not passed on for free.
func TestUnpricedTenantRouteIsRefused(t *testing.T) {
	state := &spyState{Memory: payerstate.NewMemory(), events: make(chan string, 10)}
	c := serveWith(t, state, extproc.Options{RefuseUnpriced: []string{"httproute/t-*/p-*"}})
	ask := func(route string) *extprocv3.ProcessingResponse {
		e := open(t, c)
		attrs, _ := structpb.NewStruct(map[string]any{"xds.route_name": route})
		return e.roundTrip(&extprocv3.ProcessingRequest{
			Attributes: map[string]*structpb.Struct{"envoy.filters.http.ext_proc": attrs},
			Request: &extprocv3.ProcessingRequest_RequestHeaders{
				RequestHeaders: headers(":method", "GET", ":authority", "acme.apivend.dev", ":path", "/nowhere")}})
	}
	if im := ask("httproute/t-acme/p-v1-search-4d58200d/rule/0/match/0/acme_apivend_dev").GetImmediateResponse(); im == nil || im.Status.Code != typev3.StatusCode_ServiceUnavailable {
		t.Fatal("unpriced tenant route was not refused")
	}
	for _, free := range []string{"httproute/api/catalogue/rule/0/match/0/api_apivend_dev", "httproute/t-acme/discovery/rule/0/match/0/acme_apivend_dev", ""} {
		if ask(free).GetRequestHeaders() == nil {
			t.Fatalf("free route %q was refused", free)
		}
	}
}

// A route whose PaymentPolicy was rejected is refused (503), whatever the
// filter's own patterns say: the controller lists it in the table.
func TestRejectedPolicyRouteIsRefused(t *testing.T) {
	table, err := pricing.Build(pricing.Config{Refuse: []string{"httproute/shop/search/"}})
	if err != nil {
		t.Fatal(err)
	}
	state := &spyState{Memory: payerstate.NewMemory(), events: make(chan string, 10)}
	c := serveTable(t, table, state, extproc.Options{})
	ask := func(route string) *extprocv3.ProcessingResponse {
		e := open(t, c)
		attrs, _ := structpb.NewStruct(map[string]any{"xds.route_name": route})
		return e.roundTrip(&extprocv3.ProcessingRequest{
			Attributes: map[string]*structpb.Struct{"envoy.filters.http.ext_proc": attrs},
			Request: &extprocv3.ProcessingRequest_RequestHeaders{
				RequestHeaders: headers(":method", "GET", ":authority", "shop.example", ":path", "/search")}})
	}
	if im := ask("httproute/shop/search/rule/0/match/0/shop_example").GetImmediateResponse(); im == nil || im.Status.Code != typev3.StatusCode_ServiceUnavailable {
		t.Fatal("route of a rejected policy was not refused")
	}
	for _, free := range []string{"httproute/shop/searching/rule/0/match/0/shop_example", "httproute/shop/other/rule/0/match/0/shop_example"} {
		if ask(free).GetRequestHeaders() == nil {
			t.Fatalf("%s was refused", free)
		}
	}
	if _, err := pricing.Build(pricing.Config{Refuse: []string{"httproute/"}}); err == nil {
		t.Fatal("a refuse prefix without namespace and name was accepted")
	}
}

// Outside tester's N2 and N3: headers in the gateway's namespaces, a
// client's X-Forwarded-Host and a Cloudflare Access session never reach
// the upstream as the client sent them; other cookies and the origin
// secret header (overwritten by Envoy) are left alone. Paid and free.
func TestClientHeadersAreScrubbed(t *testing.T) {
	c, state := setup(t)
	spoof := []string{"x-apivend-tenant", "geo", "x-apivend-payer", "0xdead", "x-x402-whatever", "1",
		"x-forwarded-host", "evil.example", "cf-access-jwt-assertion", "eyJ...", "x-apivend-secret", "fake",
		"cookie", "a=1; CF_Authorization=eyJ.jwt; b=2"}
	check := func(name string, resp *extprocv3.ProcessingResponse) {
		t.Helper()
		m := resp.GetRequestHeaders().GetResponse().GetHeaderMutation()
		removed := map[string]bool{}
		for _, h := range m.GetRemoveHeaders() {
			removed[h] = true
		}
		for _, h := range []string{"x-apivend-tenant", "x-apivend-payer", "x-x402-whatever", "x-forwarded-host", "cf-access-jwt-assertion"} {
			if !removed[h] {
				t.Errorf("%s: %s kept", name, h)
			}
		}
		if removed["x-apivend-secret"] {
			t.Errorf("%s: the origin secret header removed (Envoy sets it)", name)
		}
		if got := mutation(m.GetSetHeaders())["cookie"]; got != "a=1; b=2" {
			t.Errorf("%s: cookie = %q", name, got)
		}
	}
	check("free", open(t, c).request("/healthz", spoof...))
	sig := pay(t, challenge(t, open(t, c).request("/search"), 402))
	e := open(t, c)
	check("paid", e.request("/search", append([]string{"payment-signature", sig}, spoof...)...))
	if e.response("200").GetResponseHeaders() == nil {
		t.Fatal("paid request not committed")
	}
	state.expect(t, "commit")
}

// The reviewer's cases: every Cf-Access-* header (service-token
// credentials, the user's email), the other Access cookies, cookie names
// with padding, cookies split over several headers (HTTP/2), a cookie
// header holding only the Access session, and a forged payer on a paid
// request (overwritten).
func TestScrubEdgeCases(t *testing.T) {
	c, state := setup(t)
	get := func(resp *extprocv3.ProcessingResponse) (map[string]bool, map[string]string) {
		m := resp.GetRequestHeaders().GetResponse().GetHeaderMutation()
		removed := map[string]bool{}
		for _, h := range m.GetRemoveHeaders() {
			removed[h] = true
		}
		return removed, mutation(m.GetSetHeaders())
	}
	removed, set := get(open(t, c).request("/healthz",
		"cf-access-client-id", "id.access", "cf-access-client-secret", "s3cret", "cf-access-authenticated-user-email", "a@b.example",
		"cookie", "a=1; CF_AppSession=x", "cookie", "CF_Authorization =jwt; b=2; cf_binding=y"))
	for _, h := range []string{"cf-access-client-id", "cf-access-client-secret", "cf-access-authenticated-user-email"} {
		if !removed[h] {
			t.Errorf("%s kept", h)
		}
	}
	if set["cookie"] != "a=1; b=2" || removed["cookie"] {
		t.Errorf("split cookies: set %q, removed %v", set["cookie"], removed["cookie"])
	}
	removed, set = get(open(t, c).request("/healthz", "cookie", "CF_Authorization=jwt"))
	if !removed["cookie"] || set["cookie"] != "" {
		t.Errorf("access-only cookie: removed %v, set %q", removed["cookie"], set["cookie"])
	}
	sig := pay(t, challenge(t, open(t, c).request("/search"), 402))
	e := open(t, c)
	_, set = get(e.request("/search", "payment-signature", sig, "x-x402-payer", "0x000000000000000000000000000000000000dEaD"))
	if set[extproc.HeaderPayer] == "" || strings.Contains(strings.ToLower(set[extproc.HeaderPayer]), "dead") {
		t.Errorf("payer = %q", set[extproc.HeaderPayer])
	}
	e.response("200")
	state.expect(t, "commit")
}

// A required query parameter missing: without a payment the 402 still
// comes (directories probe the bare URL) and names it; with a payment the
// request is refused 400 before the payment is looked at, nothing reserved.
func TestMissingRequiredParameter(t *testing.T) {
	c, state := setup(t)
	pr := challenge(t, open(t, c).request("/needs"), 402)
	if !strings.Contains(pr.Error, "missing required query parameter: address") {
		t.Fatalf("402 error %q", pr.Error)
	}
	if pr := challenge(t, open(t, c).request("/needs?address=0xabc"), 402); strings.Contains(pr.Error, "missing") {
		t.Fatalf("402 with the parameter: %q", pr.Error)
	}
	for _, path := range []string{"/needs", "/needs?address=", "/needs?other=1"} {
		resp := open(t, c).request(path, "payment-signature", pay(t, pr))
		im := resp.GetImmediateResponse()
		if im == nil || im.Status.Code != typev3.StatusCode_BadRequest || !strings.Contains(string(im.Body), "nothing was charged") {
			t.Fatalf("%s paid: %v", path, resp.Response)
		}
	}
	state.expectNone(t)
	resp := open(t, c).request("/needs?address=0xabc", "payment-signature", pay(t, pr))
	if resp.GetRequestHeaders() == nil {
		t.Fatalf("paid with the parameter: %v", resp.Response)
	}
}
