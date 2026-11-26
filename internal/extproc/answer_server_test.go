package extproc_test

import (
	"encoding/json"
	"testing"

	extprocfilterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/extproc"
	"apivend.io/apivend/internal/metrics"
	"apivend.io/apivend/internal/x402"
)

func setupCheck(t *testing.T, mode extproc.AnswerCheck) (extprocv3.ExternalProcessorClient, *spyState) {
	t.Helper()
	state := &spyState{Memory: payerstate.NewMemory(), events: make(chan string, 16)}
	return serveWith(t, state, extproc.Options{AnswerCheck: mode}), state
}

// paidJSON pays /search and answers its response headers with a 200 JSON
// answer; it returns the exchange and the filter's answer to the headers.
func paidJSON(t *testing.T, c extprocv3.ExternalProcessorClient, kv ...string) (*exchange, *extprocv3.ProcessingResponse) {
	t.Helper()
	pr := challenge(t, open(t, c).request("/search"), 402)
	e := open(t, c)
	if e.request("/search", "payment-signature", pay(t, pr)).GetRequestHeaders() == nil {
		t.Fatal("paid request refused")
	}
	h := append([]string{":status", "200", "content-type", "application/json"}, kv...)
	return e, e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{ResponseHeaders: headers(h...)}})
}

func (e *exchange) body(b string) *extprocv3.ProcessingResponse {
	e.t.Helper()
	return e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseBody{
		ResponseBody: &extprocv3.HttpBody{Body: []byte(b), EndOfStream: true}}})
}

func asksForBody(t *testing.T, resp *extprocv3.ProcessingResponse) {
	t.Helper()
	if resp.GetResponseHeaders() == nil {
		t.Fatalf("want a headers response, got %T", resp.Response)
	}
	mo := resp.GetModeOverride()
	if mo.GetResponseBodyMode() != extprocfilterv3.ProcessingMode_BUFFERED_PARTIAL {
		t.Fatalf("mode override = %v", mo)
	}
	if resp.GetResponseHeaders().GetResponse().GetHeaderMutation() != nil {
		t.Fatal("PAYMENT-RESPONSE set before the answer was checked")
	}
}

func settled(t *testing.T, set map[string]string) {
	t.Helper()
	var sr x402.SettlementResponse
	if err := x402.DecodeHeader(set[x402.HeaderPaymentResponse], &sr); err != nil || !sr.Success {
		t.Fatalf("PAYMENT-RESPONSE: %v %+v", err, sr)
	}
}

func TestAnswerCheckEnforceRefusesUnusable(t *testing.T) {
	c, state := setupCheck(t, extproc.AnswerCheckEnforce)
	e, resp := paidJSON(t, c)
	asksForBody(t, resp)
	state.expectNone(t)

	im := e.body(`{"error":"upstream timed out"}`).GetImmediateResponse()
	if im == nil || im.Status.Code != 503 {
		t.Fatalf("want 503, got %v", im)
	}
	var got struct{ Error, Reason string }
	if json.Unmarshal(im.Body, &got) != nil || got.Error != "unusable_answer" || got.Reason != extproc.VerdictErrorOnly {
		t.Fatalf("body = %s", im.Body)
	}
	state.expect(t, "release")
	e.cancel()
	state.expectNone(t)
}

func TestAnswerCheckEnforceChargesUsable(t *testing.T) {
	c, state := setupCheck(t, extproc.AnswerCheckEnforce)
	e, resp := paidJSON(t, c)
	asksForBody(t, resp)
	br := e.body(`{"results":[]}`).GetResponseBody()
	if br == nil {
		t.Fatal("usable answer not passed on")
	}
	state.expect(t, "commit")
	settled(t, mutation(br.GetResponse().GetHeaderMutation().GetSetHeaders()))
}

func TestAnswerCheckWatchCharges(t *testing.T) {
	c, state := setupCheck(t, extproc.AnswerCheckWatch)
	e, resp := paidJSON(t, c)
	asksForBody(t, resp)
	br := e.body(``).GetResponseBody()
	if br == nil {
		t.Fatal("watch mode replaced the answer")
	}
	state.expect(t, "commit")
	settled(t, mutation(br.GetResponse().GetHeaderMutation().GetSetHeaders()))
}

// A body asked for but never sent (a filter config without
// allow_mode_override) is charged as before, never served free.
func TestAnswerCheckNoBodyCharges(t *testing.T) {
	c, state := setupCheck(t, extproc.AnswerCheckEnforce)
	e, resp := paidJSON(t, c)
	asksForBody(t, resp)
	e.stream.CloseSend()
	state.expect(t, "commit")
	state.expectNone(t)
}

func TestAnswerCheckSkipsNonJSON(t *testing.T) {
	c, state := setupCheck(t, extproc.AnswerCheckEnforce)
	pr := challenge(t, open(t, c).request("/search"), 402)
	e := open(t, c)
	e.request("/search", "payment-signature", pay(t, pr))
	resp := e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{
		ResponseHeaders: headers(":status", "200", "content-type", "image/png")}})
	if resp.GetModeOverride() != nil {
		t.Fatal("asked for a non-JSON body")
	}
	state.expect(t, "commit")
	settled(t, mutation(resp.GetResponseHeaders().GetResponse().GetHeaderMutation().GetSetHeaders()))
}

func TestAnswerCheckOffChargesOnHeaders(t *testing.T) {
	c, state := setupCheck(t, extproc.AnswerCheckOff)
	_, resp := paidJSON(t, c)
	if resp.GetModeOverride() != nil {
		t.Fatal("asked for the body with the check off")
	}
	state.expect(t, "commit")
}

// With the check on, a paid request asks the upstream for an encoding the
// check can read; off, it is passed as it came.
func TestAnswerCheckNarrowsAcceptEncoding(t *testing.T) {
	for mode, want := range map[extproc.AnswerCheck]string{
		extproc.AnswerCheckWatch: "gzip",
		extproc.AnswerCheckOff:   "",
	} {
		c, _ := setupCheck(t, mode)
		pr := challenge(t, open(t, c).request("/search"), 402)
		resp := open(t, c).request("/search", "payment-signature", pay(t, pr), "accept-encoding", "gzip, deflate, br, zstd")
		set := mutation(resp.GetRequestHeaders().GetResponse().GetHeaderMutation().GetSetHeaders())
		if set[extproc.HeaderPayer] == "" {
			t.Fatalf("%s: paid request not passed: %v", mode, resp)
		}
		if got := set["accept-encoding"]; got != want {
			t.Errorf("%s: accept-encoding = %q, want %q", mode, got, want)
		}
	}
}

// A checked answer records how long it was held, both parts.
func TestAnswerCheckRecordsHold(t *testing.T) {
	m := metrics.NewFilter()
	state := &spyState{Memory: payerstate.NewMemory(), events: make(chan string, 16)}
	c := serveWith(t, state, extproc.Options{AnswerCheck: extproc.AnswerCheckWatch, Metrics: m})
	e, resp := paidJSON(t, c)
	asksForBody(t, resp)
	if e.body(`{"results":[]}`).GetResponseBody() == nil {
		t.Fatal("want a body response")
	}
	mfs, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]uint64{}
	for _, mf := range mfs {
		if mf.GetName() != "x402_answer_hold_seconds" {
			continue
		}
		for _, mt := range mf.GetMetric() {
			got[mt.GetLabel()[0].GetValue()] = mt.GetHistogram().GetSampleCount()
		}
	}
	if got["body_wait"] != 1 || got["check"] != 1 {
		t.Fatalf("x402_answer_hold_seconds samples = %v", got)
	}
}

// paidNoBody is paidJSON for an answer that ends with its headers (an empty
// 200): Envoy sends no body message for it.
func paidNoBody(t *testing.T, c extprocv3.ExternalProcessorClient) (*exchange, *extprocv3.ProcessingResponse) {
	t.Helper()
	pr := challenge(t, open(t, c).request("/search"), 402)
	e := open(t, c)
	if e.request("/search", "payment-signature", pay(t, pr)).GetRequestHeaders() == nil {
		t.Fatal("paid request refused")
	}
	h := headers(":status", "200", "content-type", "application/json", "content-length", "0")
	h.EndOfStream = true
	return e, e.roundTrip(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{ResponseHeaders: h}})
}

func TestAnswerCheckEnforceRefusesNoBody(t *testing.T) {
	c, state := setupCheck(t, extproc.AnswerCheckEnforce)
	e, resp := paidNoBody(t, c)
	im := resp.GetImmediateResponse()
	if im == nil || im.Status.Code != 503 {
		t.Fatalf("want 503 on the headers, got %v", resp)
	}
	var got struct{ Error, Reason string }
	if json.Unmarshal(im.Body, &got) != nil || got.Reason != extproc.VerdictEmpty {
		t.Fatalf("body = %s", im.Body)
	}
	state.expect(t, "release")
	e.cancel()
	state.expectNone(t)
}

func TestAnswerCheckWatchChargesNoBodyOnHeaders(t *testing.T) {
	c, state := setupCheck(t, extproc.AnswerCheckWatch)
	_, resp := paidNoBody(t, c)
	if resp.GetResponseHeaders() == nil || resp.GetModeOverride() != nil {
		t.Fatalf("want a plain headers response, got %v", resp)
	}
	state.expect(t, "commit")
	settled(t, mutation(resp.GetResponseHeaders().GetResponse().GetHeaderMutation().GetSetHeaders()))
}
