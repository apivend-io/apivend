package settle

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHealth(t *testing.T) {
	var h health
	now := time.Unix(1_800_000_000, 0)
	if !h.allow("a", now) {
		t.Fatal("new backend not allowed")
	}
	if up, changed := h.result("a", false, now); !up || changed {
		t.Fatal("one failure opened the circuit")
	}
	if up, changed := h.result("a", false, now); up || !changed {
		t.Fatal("two failures did not open it")
	}
	if h.allow("a", now.Add(minCooldown-time.Second)) {
		t.Fatal("allowed during cool-down")
	}
	// After the cool-down exactly one probe at a time.
	later := now.Add(minCooldown)
	if !h.allow("a", later) || h.allow("a", later) {
		t.Fatal("probe not single")
	}
	// A failed probe doubles the cool-down.
	h.result("a", false, later)
	if h.allow("a", later.Add(2*minCooldown-time.Second)) || !h.allow("a", later.Add(2*minCooldown)) {
		t.Fatal("cool-down did not double")
	}
	if up, changed := h.result("a", true, later.Add(2*minCooldown)); !up || !changed {
		t.Fatal("success did not close it")
	}
	if !h.allow("a", later.Add(2*minCooldown)) {
		t.Fatal("closed backend not allowed")
	}
	// The cool-down is capped.
	for i := 0; i < 20; i++ {
		h.result("b", false, now)
	}
	if h.get("b").cooldown != maxCooldown {
		t.Fatalf("cool-down %v", h.get("b").cooldown)
	}
	// Refusals are not failures: result(ok=true) is what the settler sends.
}

func TestSupportedIsCachedAndKeptOnErrors(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/fac/supported" || fail.Load() {
			http.Error(w, "no", http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"kinds": []any{
			map[string]any{"x402Version": 2, "scheme": "exact", "network": "eip155:84532"},
			map[string]any{"x402Version": 1, "scheme": "exact", "network": "base"},
		}})
	}))
	t.Cleanup(srv.Close)
	f, err := newFacilitator(BackendConfig{Type: "facilitator", URL: srv.URL + "/fac"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Unix(1_800_000_000, 0)
	f.now = func() time.Time { return clock }
	ctx := context.Background()

	if ok, known := f.Supports(ctx, 2, "exact", "eip155:84532"); !ok || !known {
		t.Fatal("listed kind not supported")
	}
	if ok, known := f.Supports(ctx, 2, "exact", "eip155:8453"); ok || !known {
		t.Fatal("unlisted kind supported")
	}
	if ok, _ := f.Supports(ctx, 2, "exact", "base"); ok {
		t.Fatal("a v1 kind counted for v2")
	}
	if calls.Load() != 1 {
		t.Fatalf("fetched %d times", calls.Load())
	}
	// After the TTL a failed refresh keeps the last good list...
	fail.Store(true)
	clock = clock.Add(supportedTTL)
	if ok, known := f.Supports(ctx, 2, "exact", "eip155:84532"); !ok || !known {
		t.Fatal("lost the list on a failed refresh")
	}
	// ...and is not retried before supportedRetry.
	f.Supports(ctx, 2, "exact", "eip155:84532")
	if calls.Load() != 2 {
		t.Fatalf("fetched %d times", calls.Load())
	}

	// A facilitator that never answered is "not known": tried anyway.
	g, _ := newFacilitator(BackendConfig{Type: "facilitator", URL: srv.URL + "/other"}, time.Second)
	if _, known := g.Supports(ctx, 2, "exact", "eip155:84532"); known {
		t.Fatal("unanswered list counted as known")
	}
}

func TestAccountReasons(t *testing.T) {
	for reason, want := range map[string]bool{
		"insufficient_credits":                                  true,
		"credits_exhausted":                                     true,
		"quota_exceeded":                                        true,
		"payment_method_required":                               true,
		"invalid_exact_evm_insufficient_balance":                false,
		"insufficient_funds":                                    false,
		"invalid_exact_evm_payload_signature":                   false,
		"invalid_batch_settlement_evm_claim_transaction_failed": false,
	} {
		if accountReason(reason) != want {
			t.Errorf("%s: %v", reason, !want)
		}
	}
}
