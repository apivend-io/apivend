package extproc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/extproc"
	"apivend.io/apivend/internal/redistest"
	"apivend.io/apivend/internal/x402"
)

// spyKeep wraps the memory store and reports each answer kept.
type spyKeep struct {
	extproc.MemoryRedeliveries
	kept   chan string
	getErr error
}

func (s *spyKeep) Get(ctx context.Context, key string) (*extproc.Delivered, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.MemoryRedeliveries.Get(ctx, key)
}

func (s *spyKeep) Put(ctx context.Context, key string, d *extproc.Delivered, ttl time.Duration) error {
	err := s.MemoryRedeliveries.Put(ctx, key, d, ttl)
	s.kept <- key
	return err
}

func (s *spyKeep) expectKept(t *testing.T) {
	t.Helper()
	select {
	case <-s.kept:
	case <-time.After(2 * time.Second):
		t.Fatal("answer not kept")
	}
}

func (s *spyKeep) expectNotKept(t *testing.T) {
	t.Helper()
	select {
	case <-s.kept:
		t.Fatal("answer kept")
	case <-time.After(50 * time.Millisecond):
	}
}

func setupRedeliver(t *testing.T) (extprocv3.ExternalProcessorClient, *spyState, *spyKeep, *time.Time) {
	t.Helper()
	clock := time.Now()
	keep := &spyKeep{kept: make(chan string, 4)}
	keep.Now = func() time.Time { return clock }
	state := &spyState{Memory: payerstate.NewMemory(), events: make(chan string, 16)}
	c := serveWith(t, state, extproc.Options{AnswerCheck: extproc.AnswerCheckEnforce, Redeliveries: keep})
	return c, state, keep, &clock
}

// paidGet pays path and answers with a 200 JSON answer and body.
func paidGet(t *testing.T, c extprocv3.ExternalProcessorClient, path, body string) (*extprocv3.ProcessingResponse, map[string]string) {
	t.Helper()
	pr := challenge(t, open(t, c).request(path), 402)
	e := open(t, c)
	if e.request(path, "payment-signature", pay(t, pr)).GetRequestHeaders() == nil {
		t.Fatal("paid request refused")
	}
	asksForBody(t, e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{
		ResponseHeaders: headers(":status", "200", "content-type", "application/json", "set-cookie", "upstream=1")}}))
	resp := e.body(body)
	return resp, mutation(resp.GetResponseBody().GetResponse().GetHeaderMutation().GetSetHeaders())
}

// repeat pays path again with a new payment and returns the filter's answer
// to the request headers.
func repeat(t *testing.T, c extprocv3.ExternalProcessorClient, path string) *extprocv3.ProcessingResponse {
	t.Helper()
	pr := challenge(t, open(t, c).request(path), 402)
	return open(t, c).request(path, "payment-signature", pay(t, pr))
}

func TestRedeliverRepeatGetsKeptAnswerUncharged(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	_, set := paidGet(t, c, "/sync?q=a", `{"results":[1]}`)
	state.expect(t, "commit")
	keep.expectKept(t)

	im := repeat(t, c, "/sync?q=a").GetImmediateResponse()
	if im == nil || im.Status.Code != 200 || string(im.Body) != `{"results":[1]}` {
		t.Fatalf("want the kept answer, got %v", im)
	}
	got := mutation(im.Headers.SetHeaders)
	if got[x402.HeaderPaymentResponse] != set[x402.HeaderPaymentResponse] {
		t.Fatal("redelivered without the original PAYMENT-RESPONSE")
	}
	if got["x-x402-redelivered"] != "true" || got["content-type"] != "application/json" {
		t.Fatalf("headers = %v", got)
	}
	if _, ok := got["set-cookie"]; ok {
		t.Fatal("an upstream header outside the kept list was redelivered")
	}
	state.expectNone(t) // the new payment: never reserved, never charged
}

func TestRedeliverOnlyTheSameRequest(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	paidGet(t, c, "/sync?q=a", `{"results":[1]}`)
	state.expect(t, "commit")
	keep.expectKept(t)
	if repeat(t, c, "/sync?q=b").GetRequestHeaders() == nil {
		t.Fatal("another query got the kept answer")
	}
}

func TestRedeliverNeedsAPayment(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	paidGet(t, c, "/sync?q=a", `{"results":[1]}`)
	state.expect(t, "commit")
	keep.expectKept(t)
	challenge(t, open(t, c).request("/sync?q=a"), 402)
}

func TestRedeliverWindowEnds(t *testing.T) {
	c, state, keep, clock := setupRedeliver(t)
	paidGet(t, c, "/sync?q=a", `{"results":[1]}`)
	state.expect(t, "commit")
	keep.expectKept(t)
	*clock = clock.Add(extproc.DefaultRedeliverWindow)
	if repeat(t, c, "/sync?q=a").GetRequestHeaders() == nil {
		t.Fatal("redelivered after the window")
	}
}

func TestRedeliverKeepsNoRefusedAnswer(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	resp, _ := paidGet(t, c, "/sync?q=a", `{"error":"upstream timed out"}`)
	if resp.GetImmediateResponse().GetStatus().GetCode() != 503 {
		t.Fatal("unusable answer not refused")
	}
	state.expect(t, "release")
	keep.expectNotKept(t)
}

func TestRedeliverOnlyGet(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	pr := challenge(t, open(t, c).request("/sync"), 402)
	e := open(t, c)
	e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: headers(
		":method", "POST", ":scheme", "https", ":authority", "api.example.com", ":path", "/sync", "payment-signature", pay(t, pr))}})
	e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{
		ResponseHeaders: headers(":status", "200", "content-type", "application/json")}})
	e.body(`{"sent":true}`)
	state.expect(t, "commit")
	keep.expectNotKept(t)
}

// A store that cannot answer means no kept answer: charged as usual.
func TestRedeliverStoreDownCharges(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	keep.getErr = errors.New("redis down")
	paidGet(t, c, "/sync?q=a", `{"results":[1]}`)
	state.expect(t, "commit")
}

func TestRedisRedeliveriesBudget(t *testing.T) {
	rdb, _ := redistest.Start(t)
	ctx := context.Background()
	// The budget counts the stored record (about 80 bytes here), not just
	// the body: one fits, two do not.
	r := extproc.RedisRedeliveries{RDB: rdb, Budget: 100}
	d := &extproc.Delivered{Status: 200, Body: []byte("0123456789012345678901234567890123456789"), PaymentResponse: "p"}
	if err := r.Put(ctx, "x402:{redeliver}:a", d, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(ctx, "x402:{redeliver}:a")
	if err != nil || got == nil || len(got.Body) != 40 || got.PaymentResponse != "p" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if err := r.Put(ctx, "x402:{redeliver}:b", d, time.Minute); !errors.Is(err, extproc.ErrRedeliverBudget) {
		t.Fatalf("over budget: %v", err)
	}
	if got, _ := r.Get(ctx, "x402:{redeliver}:b"); got != nil {
		t.Fatal("kept over budget")
	}
	if got, _ := r.Get(ctx, "x402:{redeliver}:none"); got != nil {
		t.Fatal("phantom answer")
	}
}

func TestMemoryRedeliveriesBudget(t *testing.T) {
	clock := time.Now()
	m := &extproc.MemoryRedeliveries{Budget: 10, Now: func() time.Time { return clock }}
	ctx := context.Background()
	d := &extproc.Delivered{Status: 200, Body: []byte("123456"), PaymentResponse: "p"}
	if err := m.Put(ctx, "a", d, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, "b", d, time.Minute); !errors.Is(err, extproc.ErrRedeliverBudget) {
		t.Fatalf("over budget: %v", err)
	}
	clock = clock.Add(time.Minute)
	if err := m.Put(ctx, "b", d, time.Minute); err != nil {
		t.Fatalf("budget not freed after the window: %v", err)
	}
}

// A client that resends the same signed payment (x402 PR #4094) gets the
// kept answer too, not a 402 for a used nonce.
func TestRedeliverSameSignature(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	pr := challenge(t, open(t, c).request("/sync?q=a"), 402)
	sig := pay(t, pr)
	e := open(t, c)
	if e.request("/sync?q=a", "payment-signature", sig).GetRequestHeaders() == nil {
		t.Fatal("paid request refused")
	}
	e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{
		ResponseHeaders: headers(":status", "200", "content-type", "application/json")}})
	e.body(`{"results":[1]}`)
	state.expect(t, "commit")
	keep.expectKept(t)
	im := open(t, c).request("/sync?q=a", "payment-signature", sig).GetImmediateResponse()
	if im == nil || im.Status.Code != 200 || string(im.Body) != `{"results":[1]}` {
		t.Fatalf("same signature resent: %v", im)
	}
	state.expectNone(t)
}

// An answer served before its payment settled (an async route) is not kept:
// it may yet go unpaid.
func TestRedeliverKeepsOnlySettled(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	paidGet(t, c, "/search?q=a", `{"results":[1]}`)
	state.expect(t, "commit")
	keep.expectNotKept(t)
}

// A payment already charged for another request does not unlock a kept
// answer (Grokky, 2026-11-27: a still-valid used signature read the
// wallet's other kept answers). It is refused as used.
func TestRedeliverNotForAPaymentUsedElsewhere(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	paidGet(t, c, "/sync?q=a", `{"results":[1]}`)
	state.expect(t, "commit")
	keep.expectKept(t)

	pr := challenge(t, open(t, c).request("/sync?q=b"), 402)
	sigB := pay(t, pr)
	e := open(t, c)
	if e.request("/sync?q=b", "payment-signature", sigB).GetRequestHeaders() == nil {
		t.Fatal("paid request refused")
	}
	e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{
		ResponseHeaders: headers(":status", "200", "content-type", "application/json")}})
	e.body(`{"results":[2]}`)
	state.expect(t, "commit")
	keep.expectKept(t)

	got := challenge(t, open(t, c).request("/sync?q=a", "payment-signature", sigB), 402)
	if got.Error != payerstate.ReasonNonceUsed {
		t.Fatalf("error = %q, want %q", got.Error, payerstate.ReasonNonceUsed)
	}
}

// The answer's own cache-control and nosniff come back with it.
func TestRedeliverKeepsCacheHeaders(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	pr := challenge(t, open(t, c).request("/sync?q=h"), 402)
	e := open(t, c)
	e.request("/sync?q=h", "payment-signature", pay(t, pr))
	e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{ResponseHeaders: headers(
		":status", "200", "content-type", "application/json", "cache-control", "no-store, no-transform", "x-content-type-options", "nosniff")}})
	e.body(`{"results":[1]}`)
	state.expect(t, "commit")
	keep.expectKept(t)
	got := mutation(repeat(t, c, "/sync?q=h").GetImmediateResponse().GetHeaders().GetSetHeaders())
	if got["cache-control"] != "no-store, no-transform" || got["x-content-type-options"] != "nosniff" {
		t.Fatalf("headers = %v", got)
	}
}

// startPaid sends a paid GET's request headers and leaves the call open (in
// flight at the upstream).
func startPaid(t *testing.T, c extprocv3.ExternalProcessorClient, path string) *exchange {
	t.Helper()
	pr := challenge(t, open(t, c).request(path), 402)
	e := open(t, c)
	if e.request(path, "payment-signature", pay(t, pr)).GetRequestHeaders() == nil {
		t.Fatal("paid request refused")
	}
	return e
}

// repeatAsync sends a repeat with a new payment and returns its answer to the
// request headers on a channel (it may wait for the first call).
func repeatAsync(t *testing.T, c extprocv3.ExternalProcessorClient, path string) <-chan *extprocv3.ProcessingResponse {
	t.Helper()
	pr := challenge(t, open(t, c).request(path), 402)
	sig := pay(t, pr)
	e := open(t, c)
	out := make(chan *extprocv3.ProcessingResponse, 1)
	go func() {
		if err := e.stream.Send(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: headers(
			":method", "GET", ":scheme", "https", ":authority", "api.example.com", ":path", path, "payment-signature", sig)}}); err != nil {
			out <- nil
			return
		}
		r, err := e.stream.Recv()
		if err != nil {
			r = nil
		}
		out <- r
	}()
	return out
}

func waiting(t *testing.T, out <-chan *extprocv3.ProcessingResponse) {
	t.Helper()
	select {
	case r := <-out:
		t.Fatalf("repeat answered while the first call was in flight: %v", r)
	case <-time.After(100 * time.Millisecond):
	}
}

// A repeat sent while the first call is still being answered (a client that
// timed out) waits for it and gets its answer, charged once (Grokky,
// 2026-11-27, finding 1).
func TestRedeliverRepeatWaitsForInFlight(t *testing.T) {
	c, state, _, _ := setupRedeliver(t)
	first := startPaid(t, c, "/sync?q=f")
	out := repeatAsync(t, c, "/sync?q=f")
	waiting(t, out)

	first.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{
		ResponseHeaders: headers(":status", "200", "content-type", "application/json")}})
	first.body(`{"results":[7]}`)
	state.expect(t, "commit")

	select {
	case r := <-out:
		im := r.GetImmediateResponse()
		if im == nil || im.Status.Code != 200 || string(im.Body) != `{"results":[7]}` || mutation(im.Headers.SetHeaders)["x-x402-redelivered"] != "true" {
			t.Fatalf("repeat got %v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("repeat still waiting after the first answer was kept")
	}
	state.expectNone(t)
}

// The first call fails: the waiting repeat goes on as a call of its own.
func TestRedeliverRepeatAfterFailedFirst(t *testing.T) {
	c, state, _, _ := setupRedeliver(t)
	first := startPaid(t, c, "/sync?q=g")
	out := repeatAsync(t, c, "/sync?q=g")
	waiting(t, out)
	first.response("500")
	state.expect(t, "release")
	select {
	case r := <-out:
		if r.GetRequestHeaders() == nil {
			t.Fatalf("repeat not passed on after the first failed: %v", r)
		}
	case <-time.After(200 * time.Millisecond): // under the route's 300ms wait
		t.Fatal("repeat still waiting after the first call failed")
	}
}

// The first call takes longer than the route's settlement wait: the repeat
// stops waiting and is charged as usual.
func TestRedeliverRepeatWaitIsBounded(t *testing.T) {
	c, _, _, _ := setupRedeliver(t)
	startPaid(t, c, "/sync?q=w")
	out := repeatAsync(t, c, "/sync?q=w")
	select {
	case r := <-out:
		if r.GetRequestHeaders() == nil {
			t.Fatalf("repeat not passed on after the wait: %v", r)
		}
	case <-time.After(2 * time.Second): // the test route waits 300ms
		t.Fatal("repeat waited past the route's settlement wait")
	}
}

func TestRedisRedeliveriesInFlight(t *testing.T) {
	rdb, _ := redistest.Start(t)
	ctx := context.Background()
	r := extproc.RedisRedeliveries{RDB: rdb}
	if ok, err := r.Claim(ctx, "x402:{redeliver}:f", "a", time.Minute); err != nil || !ok {
		t.Fatalf("first claim: %v %v", ok, err)
	}
	if ok, _ := r.Claim(ctx, "x402:{redeliver}:f", "b", time.Minute); ok {
		t.Fatal("claimed twice")
	}
	if busy, _ := r.InFlight(ctx, "x402:{redeliver}:f"); !busy {
		t.Fatal("not in flight")
	}
	// Another call's token (a call whose own mark lapsed) leaves it.
	if err := r.Unclaim(ctx, "x402:{redeliver}:f", "b"); err != nil {
		t.Fatal(err)
	}
	if busy, _ := r.InFlight(ctx, "x402:{redeliver}:f"); !busy {
		t.Fatal("unclaimed by another call's token")
	}
	if err := r.Unclaim(ctx, "x402:{redeliver}:f", "a"); err != nil {
		t.Fatal(err)
	}
	if busy, _ := r.InFlight(ctx, "x402:{redeliver}:f"); busy {
		t.Fatal("still in flight after unclaim")
	}
}

// paidGetWith is paidGet with request headers and answer headers of the
// caller's choosing.
func paidGetWith(t *testing.T, c extprocv3.ExternalProcessorClient, path string, req, resp []string, body string) {
	t.Helper()
	pr := challenge(t, open(t, c).request(path, req...), 402)
	e := open(t, c)
	if e.request(path, append([]string{"payment-signature", pay(t, pr)}, req...)...).GetRequestHeaders() == nil {
		t.Fatal("paid request refused")
	}
	r := e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{
		ResponseHeaders: headers(append([]string{"content-type", "application/json"}, resp...)...)}})
	if r.GetResponseHeaders() != nil && r.GetModeOverride() != nil {
		e.body(body)
	}
}

func repeatWith(t *testing.T, c extprocv3.ExternalProcessorClient, path string, req ...string) *extprocv3.ProcessingResponse {
	t.Helper()
	pr := challenge(t, open(t, c).request(path, req...), 402)
	return open(t, c).request(path, append([]string{"payment-signature", pay(t, pr)}, req...)...)
}

func TestRedeliverOnlyTheSameCaller(t *testing.T) {
	// One wallet paying for many users of an API: each user's answer is
	// their own.
	c, state, keep, _ := setupRedeliver(t)
	paidGetWith(t, c, "/sync?q=me", []string{"authorization", "Bearer A"}, []string{":status", "200"}, `{"user":"A"}`)
	state.expect(t, "commit")
	keep.expectKept(t)
	if repeatWith(t, c, "/sync?q=me", "authorization", "Bearer B").GetRequestHeaders() == nil {
		t.Fatal("another caller of the API got the kept answer")
	}
	if repeatWith(t, c, "/sync?q=me").GetRequestHeaders() == nil {
		t.Fatal("a caller without credentials got the kept answer")
	}
	if im := repeatWith(t, c, "/sync?q=me", "authorization", "Bearer A").GetImmediateResponse(); im == nil || string(im.Body) != `{"user":"A"}` {
		t.Fatalf("the same caller's repeat: %v", im)
	}
}

func TestRedeliverOnlyTheSameForm(t *testing.T) {
	c, state, keep, _ := setupRedeliver(t)
	paidGetWith(t, c, "/sync?q=f", []string{"accept-encoding", "gzip"}, []string{":status", "200"}, `{"r":1}`)
	state.expect(t, "commit")
	keep.expectKept(t)
	if repeatWith(t, c, "/sync?q=f").GetRequestHeaders() == nil {
		t.Fatal("a repeat asking for another encoding got the kept answer")
	}
}

func TestRedeliverKeepsOnlyWholePlainAnswers(t *testing.T) {
	for _, tc := range []struct {
		name      string
		req, resp []string
	}{
		{"range request", []string{"range", "bytes=0-9"}, []string{":status", "200"}},
		{"request with a body", []string{"content-length", "12"}, []string{":status", "200"}},
		{"not a 200", nil, []string{":status", "203"}},
		{"varies with an unkeyed header", nil, []string{":status", "200", "vary", "accept, x-tenant"}},
		{"varies with anything", nil, []string{":status", "200", "vary", "*"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, state, keep, _ := setupRedeliver(t)
			paidGetWith(t, c, "/sync?q=p", tc.req, tc.resp, `{"r":1}`)
			state.expect(t, "commit")
			keep.expectNotKept(t)
		})
	}
	// Proxies forward GETs chunked: that alone is no body.
	c, state, keep, _ := setupRedeliver(t)
	paidGetWith(t, c, "/sync?q=v", []string{"transfer-encoding", "chunked"}, []string{":status", "200", "vary", "Accept-Encoding, Origin"}, `{"r":1}`)
	state.expect(t, "commit")
	keep.expectKept(t)
}

func TestMemoryUnclaimOnlyItsOwnMark(t *testing.T) {
	ctx := context.Background()
	m := &extproc.MemoryRedeliveries{}
	if ok, _ := m.Claim(ctx, "k", "a", time.Minute); !ok {
		t.Fatal("first claim")
	}
	m.Unclaim(ctx, "k", "b")
	if busy, _ := m.InFlight(ctx, "k"); !busy {
		t.Fatal("removed by another call's token")
	}
	m.Unclaim(ctx, "k", "a")
	if busy, _ := m.InFlight(ctx, "k"); busy {
		t.Fatal("own token did not remove it")
	}
}

func TestRedeliverIgnoresAccessCookies(t *testing.T) {
	// Cloudflare Access in front of the gateway adds a new session cookie
	// to every request; the API never sees it, and it must not make each
	// request a different one.
	c, state, keep, _ := setupRedeliver(t)
	paidGetWith(t, c, "/sync?q=c", []string{"cookie", "CF_Authorization=jwt1; app=x"}, []string{":status", "200"}, `{"r":1}`)
	state.expect(t, "commit")
	keep.expectKept(t)
	if im := repeatWith(t, c, "/sync?q=c", "cookie", "app=x; CF_Authorization=jwt2").GetImmediateResponse(); im == nil {
		t.Fatal("a new Access cookie made the repeat another request")
	}
	if repeatWith(t, c, "/sync?q=c", "cookie", "CF_Authorization=jwt3; app=y").GetRequestHeaders() == nil {
		t.Fatal("another API cookie got the kept answer")
	}
}
