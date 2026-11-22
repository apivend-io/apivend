package settle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/engine/statesource"
	"apivend.io/apivend/internal/payer"
	"apivend.io/apivend/internal/receipts"
	"apivend.io/apivend/internal/redistest"
	"apivend.io/apivend/internal/x402"
)

const (
	payerKey = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
	network  = "eip155:84532"
	usdc     = "0x036cbd53842c5426634e7929541ec2318f3dcf7e"
	payTo    = "0x209693Bc6afc0C5328bA36FaF03C514EF312287C"
)

// facilitator is a fake x402 facilitator settling against a fake chain.
type facilitator struct {
	mu    sync.Mutex
	chain *chain.Fake
	// mode: "ok", "down", "refuse", "pending", "settle-then-500"
	mode  string
	calls int // settle calls
	txSeq int
	srv   *httptest.Server
	// supported, when set, is the network list GET /supported answers with;
	// otherwise it answers 404 (a facilitator that publishes no list).
	supported []string
}

func newFakeFacilitator(t *testing.T, c *chain.Fake) *facilitator {
	f := &facilitator{chain: c, mode: "ok"}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *facilitator) set(mode string) { f.mu.Lock(); f.mode = mode; f.mu.Unlock() }
func (f *facilitator) count() int      { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

func (f *facilitator) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodGet && r.URL.Path == "/supported" {
		if f.supported == nil {
			http.NotFound(w, r)
			return
		}
		var kinds []map[string]any
		for _, n := range f.supported {
			kinds = append(kinds, map[string]any{"x402Version": 2, "scheme": "exact", "network": n})
		}
		json.NewEncoder(w).Encode(map[string]any{"kinds": kinds})
		return
	}
	f.calls++
	raw, _ := io.ReadAll(r.Body)
	var req settleRequest
	if r.URL.Path != "/settle" || json.Unmarshal(raw, &req) != nil || req.X402Version != 2 ||
		req.PaymentRequirements == nil || req.PaymentPayload == nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var ex x402.ExactEVMPayload
	json.Unmarshal(req.PaymentPayload.Payload, &ex)
	from, _ := evm.ParseAddress(ex.Authorization.From)
	nonce, _ := evm.ParseBytes32(ex.Authorization.Nonce)
	amount, _ := evm.ParseUint256(ex.Authorization.Value)
	reply := func(code int, sr x402.SettlementResponse) {
		sr.Network = req.PaymentRequirements.Network
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(sr)
	}
	transfer := func() (string, error) {
		f.txSeq++
		tx := fmt.Sprintf("0x%064x", f.txSeq)
		to, _ := evm.ParseAddress(ex.Authorization.To)
		return tx, f.chain.TransferTo(from, to, nonce, amount, tx)
	}

	switch f.mode {
	case "down":
		http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
	case "refuse":
		reply(http.StatusOK, x402.SettlementResponse{ErrorReason: "invalid_network"})
	case "pending":
		tx, err := transfer()
		if err != nil {
			reply(http.StatusOK, x402.SettlementResponse{ErrorReason: "invalid_transaction_state"})
			return
		}
		reply(http.StatusOK, x402.SettlementResponse{ErrorReason: "settlement_pending", Transaction: tx})
	case "settle-then-500":
		transfer() // the transfer lands, the caller never learns
		http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
	default:
		tx, err := transfer()
		if err != nil {
			reason := "invalid_transaction_state"
			if err.Error() == "transfer amount exceeds balance" {
				reason = "insufficient_funds"
			}
			reply(http.StatusPaymentRequired, x402.SettlementResponse{ErrorReason: reason})
			return
		}
		reply(http.StatusOK, x402.SettlementResponse{Success: true, Transaction: tx})
	}
}

type env struct {
	t        *testing.T
	state    *payerstate.Redis
	chain    *chain.Fake
	receipts *receipts.Memory
	settler  *Settler
	wallet   *payer.Wallet
	clock    time.Time
	nonce    byte
}

func newEnv(t *testing.T) *env {
	rdb, _ := redistest.Start(t)
	e := &env{t: t, chain: chain.NewFake(), receipts: &receipts.Memory{}, clock: time.Now()}
	e.wallet, _ = payer.NewWallet(payerKey)
	e.chain.SetBalance(e.wallet.Addr, 1_000_000)
	chains := map[string]chain.Reader{network: e.chain}
	e.state = payerstate.NewRedis(rdb, statesource.NewRPC("rpc", map[string]statesource.ChainClient{network: e.chain}, statesource.RPCOptions{}))
	e.state.ReadTimeout = time.Second
	e.settler = &Settler{
		Queue: e.state, Chains: chains, Receipts: e.receipts,
		Providers: map[string][]Backend{},
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:       func() time.Time { return e.clock },
	}
	return e
}

func (e *env) use(facs ...*facilitator) {
	var bcs []BackendConfig
	for i, f := range facs {
		f.chain = e.chain
		bcs = append(bcs, BackendConfig{Type: "facilitator", URL: f.srv.URL, Priority: i})
	}
	p, err := BuildProviders(ProvidersConfig{Providers: []ProviderConfig{{Name: "default", Backends: bcs}}}, 5*time.Second)
	if err != nil {
		e.t.Fatal(err)
	}
	e.settler.Providers = p
}

// serve plays the filter: verify-equivalent payload, reserve, commit.
func (e *env) serve(amount int64) payerstate.ReservationID {
	e.t.Helper()
	ctx := context.Background()
	e.nonce++
	req := x402.Requirements{Scheme: "exact", Network: network, Amount: fmt.Sprint(amount), Asset: usdc, PayTo: payTo,
		MaxTimeoutSeconds: 60, Extra: map[string]any{"name": "USDC", "version": "2"}}
	asset, _ := evm.ParseAddress(usdc)
	to, _ := evm.ParseAddress(payTo)
	auth := evm.Authorization{From: e.wallet.Addr, To: to, Value: big.NewInt(amount),
		ValidAfter: big.NewInt(0), ValidBefore: big.NewInt(time.Now().Add(60 * time.Second).Unix()), Nonce: [32]byte{e.nonce}}
	payload, err := e.wallet.Sign(req, x402.ResourceInfo{}, auth, evm.Domain{Name: "USDC", Version: "2", ChainID: big.NewInt(84532), VerifyingContract: asset})
	if err != nil {
		e.t.Fatal(err)
	}
	expiry := time.Unix(auth.ValidBefore.Int64(), 0)
	id, err := e.state.Reserve(ctx, payerstate.Request{
		Chain: network, Asset: usdc, Payer: e.wallet.Addr.Hex(), Nonce: fmt.Sprintf("0x%064x", new(big.Int).SetBytes(auth.Nonce[:])),
		Amount: big.NewInt(amount), Expiry: expiry, Policy: payerstate.Policy{Freshness: time.Hour},
	})
	if err != nil {
		e.t.Fatal(err)
	}
	body, _ := json.Marshal(Job{RequestID: "req", Route: "search", Provider: "default", Payer: e.wallet.Addr.Hex(),
		Amount: fmt.Sprint(amount), Payload: *payload, Requirements: req})
	if err := e.state.Commit(ctx, id, payerstate.Job{Body: body, Deadline: expiry}); err != nil {
		e.t.Fatal(err)
	}
	return id
}

// drain processes jobs, skipping backoff by advancing the claim clock, until
// the queue is empty or rounds run out.
func (e *env) drain(rounds int) {
	e.t.Helper()
	for i := 0; i < rounds; i++ {
		worked, err := e.settler.ProcessOne(context.Background())
		if err != nil {
			e.t.Fatal(err)
		}
		if !worked {
			depth, _, _ := e.state.QueueStats(context.Background())
			if depth == 0 {
				return
			}
			time.Sleep(600 * time.Millisecond) // real-time backoff in the store
		}
	}
}

func (e *env) exposure() int64 {
	v, _ := e.state.Exposure(context.Background(), network, usdc, e.wallet.Addr.Hex())
	return v
}

func (e *env) balance() int64 {
	b, _, _ := e.chain.BalanceOf(context.Background(), evm.Address{}, e.wallet.Addr)
	return b.Int64()
}

func (e *env) final(id payerstate.ReservationID) receipts.Receipt {
	return e.receipts.Final(string(id))
}

func TestSettlesAndReleasesExposure(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	e.use(fac)
	id := e.serve(10_000)
	if e.exposure() != 10_000 {
		t.Fatalf("exposure before settle = %d", e.exposure())
	}
	e.drain(3)

	rc := e.final(id)
	if rc.Status != receipts.StatusSettled || rc.TxHash == "" || rc.Block == 0 || rc.Route != "search" ||
		rc.Amount != "10000" || rc.Chain != network || rc.Provider == "" {
		t.Fatalf("receipt = %+v", rc)
	}
	if rows := e.receipts.Rows(); len(rows) != 2 || rows[0].Status != receipts.StatusServed {
		t.Fatalf("rows = %+v", rows)
	}
	if e.exposure() != 0 || e.balance() != 990_000 {
		t.Fatalf("exposure %d, balance %d", e.exposure(), e.balance())
	}
	if depth, _, _ := e.state.QueueStats(context.Background()); depth != 0 {
		t.Fatalf("queue depth %d", depth)
	}
}

func TestFailsOverToSecondBackend(t *testing.T) {
	e := newEnv(t)
	primary, secondary := newFakeFacilitator(t, e.chain), newFakeFacilitator(t, e.chain)
	primary.set("down")
	e.use(primary, secondary)
	id := e.serve(10_000)
	e.drain(2)

	if rc := e.final(id); rc.Status != receipts.StatusSettled {
		t.Fatalf("receipt = %+v", rc)
	}
	if primary.count() != 1 || secondary.count() != 1 {
		t.Fatalf("calls: primary %d, secondary %d", primary.count(), secondary.count())
	}
}

// The transfer lands but the facilitator's answer is lost. The retry must ask
// the chain before resubmitting, and must not misfile the payment as unpaid.
func TestSettledButLookedLikeFailure(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	fac.set("settle-then-500")
	e.use(fac)
	id := e.serve(10_000)
	e.drain(4)

	rc := e.final(id)
	if rc.Status != receipts.StatusSettled || rc.Reason != "confirmed_on_chain" {
		t.Fatalf("receipt = %+v", rc)
	}
	if fac.count() != 1 {
		t.Fatalf("facilitator called %d times; the retry should have checked the chain instead", fac.count())
	}
	if e.balance() != 990_000 || e.exposure() != 0 {
		t.Fatalf("balance %d (charged twice?), exposure %d", e.balance(), e.exposure())
	}
}

func TestPendingIsConfirmedNotResubmitted(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	fac.set("pending")
	e.use(fac)
	id := e.serve(10_000)
	e.drain(6)

	rc := e.final(id)
	if rc.Status != receipts.StatusSettled || rc.TxHash == "" || rc.Block == 0 {
		t.Fatalf("receipt = %+v", rc)
	}
	if fac.count() != 1 {
		t.Fatalf("pending settlement was resubmitted (%d calls)", fac.count())
	}
}

func TestOutageRetriesUntilRecovered(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	fac.set("down")
	e.use(fac)
	id := e.serve(10_000)
	e.drain(2)
	if rc := e.final(id); rc.Status != receipts.StatusServed {
		t.Fatalf("an outage produced a final receipt: %+v", rc)
	}
	if e.exposure() != 10_000 {
		t.Fatalf("exposure dropped during outage: %d", e.exposure())
	}
	fac.set("ok")
	e.drain(8)
	if rc := e.final(id); rc.Status != receipts.StatusSettled {
		t.Fatalf("receipt after recovery = %+v", rc)
	}
}

func TestDrainedWalletIsUnpaidAndBlocked(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	e.use(fac)
	e.settler.MaxAttempts = 2
	id := e.serve(10_000)
	e.chain.SetBalance(e.wallet.Addr, 0) // moved elsewhere after the check
	e.drain(8)

	rc := e.final(id)
	if rc.Status != receipts.StatusUnpaid || rc.Reason != "insufficient_funds" {
		t.Fatalf("receipt = %+v", rc)
	}
	if e.exposure() != 0 {
		t.Fatalf("exposure = %d", e.exposure())
	}
	_, err := e.state.Reserve(context.Background(), payerstate.Request{Chain: network, Asset: usdc,
		Payer: e.wallet.Addr.Hex(), Nonce: "0xff", Amount: big.NewInt(1), Expiry: time.Now().Add(time.Minute)})
	var denied *payerstate.DeniedError
	if !errors.As(err, &denied) || denied.Reason != payerstate.ReasonPayerBlocked {
		t.Fatalf("payer not blocked: %v", err)
	}
}

func TestOurFaultDoesNotBlockPayer(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	fac.set("refuse") // invalid_network: a configuration problem
	e.use(fac)
	e.settler.MaxAttempts = 2
	id := e.serve(10_000)
	e.drain(8)

	if rc := e.final(id); rc.Status != receipts.StatusUnpaid || rc.Reason != "invalid_network" {
		t.Fatalf("receipt = %+v", rc)
	}
	if _, err := e.state.Reserve(context.Background(), payerstate.Request{Chain: network, Asset: usdc,
		Payer: e.wallet.Addr.Hex(), Nonce: "0xff", Amount: big.NewInt(1), Expiry: time.Now().Add(time.Minute),
		Policy: payerstate.Policy{Freshness: time.Hour}}); err != nil {
		t.Fatalf("payer blocked for a failure that was not theirs: %v", err)
	}
}

func TestExpiredAuthorization(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	fac.set("down")
	e.use(fac)
	id := e.serve(10_000)
	e.clock = e.clock.Add(2 * time.Minute) // past validBefore
	e.drain(2)

	if rc := e.final(id); rc.Status != receipts.StatusUnpaid || rc.Reason != "authorization_expired" {
		t.Fatalf("receipt = %+v", rc)
	}
	if fac.count() != 0 {
		t.Fatal("submitted an expired authorization")
	}
}

type flakyReceipts struct {
	receipts.Memory
	failSettled int
}

func (f *flakyReceipts) Record(ctx context.Context, r receipts.Receipt) error {
	if r.Status == receipts.StatusSettled && f.failSettled > 0 {
		f.failSettled--
		return errors.New("postgres: connection refused")
	}
	return f.Memory.Record(ctx, r)
}

// Without a chain reader the settler cannot ask "was this nonce used?". A
// settlement whose receipt write failed must still not be resubmitted.
func TestReceiptOutageDoesNotLoseOrRepeatSettlement(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	e.use(fac)
	flaky := &flakyReceipts{failSettled: 1}
	e.settler.Receipts = flaky
	e.settler.Chains = nil
	id := e.serve(10_000)

	for i := 0; i < 20; i++ {
		e.settler.ProcessOne(context.Background())
		if flaky.Final(string(id)).Status == receipts.StatusSettled {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	rc := flaky.Final(string(id))
	if rc.Status != receipts.StatusSettled || rc.TxHash == "" {
		t.Fatalf("receipt = %+v", rc)
	}
	if fac.count() != 1 || e.balance() != 990_000 {
		t.Fatalf("facilitator calls %d, balance %d", fac.count(), e.balance())
	}
	if e.exposure() != 0 {
		t.Fatalf("exposure = %d", e.exposure())
	}
}

func TestProviderConfigRejects(t *testing.T) {
	bad := []ProvidersConfig{
		{Providers: []ProviderConfig{{Name: "", Backends: []BackendConfig{{Type: "facilitator", URL: "https://f.example"}}}}},
		{Providers: []ProviderConfig{{Name: "p"}}},
		{Providers: []ProviderConfig{{Name: "p", Backends: []BackendConfig{{Type: "direct"}}}}},
		{Providers: []ProviderConfig{{Name: "p", Backends: []BackendConfig{{Type: "facilitator", URL: "ftp://f.example"}}}}},
		{Providers: []ProviderConfig{{Name: "p", Backends: []BackendConfig{{Type: "carrier-pigeon"}}}}},
	}
	for i, cfg := range bad {
		if _, err := BuildProviders(cfg, time.Second); err == nil {
			t.Errorf("config %d accepted", i)
		}
	}
}

// hangingReceipts never returns from recording a settled receipt: a settler
// that got as far as the facilitator and then died.
type hangingReceipts struct {
	receipts.Memory
	hang chan struct{}
}

func (h *hangingReceipts) Record(ctx context.Context, r receipts.Receipt) error {
	if r.Status == receipts.StatusSettled {
		<-h.hang
	}
	return h.Memory.Record(ctx, r)
}

// Addendum B.5: killing the settler mid-queue loses no payment and
// double-charges nobody.
func TestSettlerKilledMidQueue(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	e.use(fac)
	ids := []payerstate.ReservationID{e.serve(10_000), e.serve(10_000), e.serve(10_000)}

	// The first settler takes a job, the transfer lands, and it dies before
	// recording anything.
	dead := &hangingReceipts{hang: make(chan struct{})}
	t.Cleanup(func() { close(dead.hang) })
	doomed := *e.settler
	doomed.Receipts, doomed.Lease = dead, time.Second
	go doomed.ProcessOne(context.Background())
	deadline := time.Now().Add(3 * time.Second)
	for fac.count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("first settler never reached the facilitator")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A replacement settler drains the queue, including the abandoned job
	// once its lease runs out.
	e.settler.Lease = time.Second
	for i := 0; i < 20; i++ {
		e.settler.ProcessOne(context.Background())
		if depth, _, _ := e.state.QueueStats(context.Background()); depth == 0 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	for _, id := range ids {
		if rc := e.final(id); rc.Status != receipts.StatusSettled {
			t.Fatalf("payment lost: %+v", rc)
		}
	}
	if got := e.balance(); got != 1_000_000-3*10_000 {
		t.Fatalf("payer balance %d: charged %d times for 3 payments", got, (1_000_000-got)/10_000)
	}
	if fac.count() != 3 {
		t.Fatalf("facilitator called %d times for 3 payments", fac.count())
	}
	if e.exposure() != 0 {
		t.Fatalf("exposure = %d", e.exposure())
	}
}

// Refusal reasons are sorted into the payer's fault (block them) and anyone
// else's (never block). The x402.org cases are the reasons it returned on Base
// Sepolia on 2026-11-17.
func TestPayerAttributable(t *testing.T) {
	for reason, want := range map[string]bool{
		x402.ErrInsufficientFunds:      true,
		x402.ErrEVMInsufficientBalance: true, // x402.org: wallet emptied
		x402.ErrSignature:              true,
		"invalid_transaction_state":    true,
		x402.ErrEVMTransactionFailed:   false, // x402.org: transient, settled on retry
		x402.ErrInvalidNetwork:         false,
		x402.ErrInvalidRequirements:    false,
		"":                             false,
	} {
		if got := payerAttributable(reason); got != want {
			t.Errorf("payerAttributable(%q) = %v, want %v", reason, got, want)
		}
	}
}

// During an outage the dead backend is skipped after a couple of failures,
// probed again after a cool-down, and used again once it answers.
func TestDeadBackendIsSkippedThenProbed(t *testing.T) {
	e := newEnv(t)
	primary, secondary := newFakeFacilitator(t, e.chain), newFakeFacilitator(t, e.chain)
	primary.set("down")
	e.use(primary, secondary)
	for i := 0; i < 5; i++ {
		e.serve(10_000)
	}
	e.drain(5)
	if primary.count() != tripAfter || secondary.count() != 5 {
		t.Fatalf("calls: primary %d (want %d), secondary %d", primary.count(), tripAfter, secondary.count())
	}

	// After the cool-down one payment probes it (still down)...
	e.clock = e.clock.Add(minCooldown + time.Second)
	e.serve(10_000)
	e.serve(10_000)
	e.drain(2)
	if primary.count() != tripAfter+1 {
		t.Fatalf("probe calls: %d", primary.count())
	}
	// ...and once it is back, a probe after the longer cool-down finds it.
	primary.set("ok")
	e.clock = e.clock.Add(2*minCooldown + time.Second)
	e.serve(10_000)
	e.serve(10_000)
	e.drain(2)
	if primary.count() != tripAfter+3 {
		t.Fatalf("calls after recovery: %d", primary.count())
	}
}

func TestBackendThatDoesNotSupportTheNetworkIsNotAsked(t *testing.T) {
	e := newEnv(t)
	mainnetOnly, other := newFakeFacilitator(t, e.chain), newFakeFacilitator(t, e.chain)
	mainnetOnly.supported = []string{"eip155:8453"}
	mainnetOnly.set("refuse") // would count against the payment if asked
	other.supported = []string{network}
	e.use(mainnetOnly, other)
	id := e.serve(10_000)
	e.drain(2)
	if rc := e.final(id); rc.Status != receipts.StatusSettled || mainnetOnly.count() != 0 {
		t.Fatalf("receipt %+v, mainnet-only facilitator asked %d times", rc, mainnetOnly.count())
	}
}

func TestNoBackendForTheNetworkWaitsInsteadOfFailing(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	fac.supported = []string{"eip155:8453"}
	e.use(fac)
	id := e.serve(10_000)
	e.drain(3)
	if rc := e.final(id); rc.Status != receipts.StatusServed || fac.count() != 0 {
		t.Fatalf("receipt %+v, calls %d", rc, fac.count())
	}
	if e.exposure() != 10_000 {
		t.Fatalf("exposure %d", e.exposure())
	}
	// It stays queued (retried every 15 s, in real time) until the
	// configuration gains a backend for the network or it expires.
}

// A payer who cancels the authorization after presenting it (USDC's
// cancelAuthorization) leaves the nonce "used" on chain too. That is not a
// payment: unpaid, and the payer is blocked.
func TestCancelledAuthorizationIsUnpaidNotSettled(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	fac.set("refuse")
	e.use(fac)
	id := e.serve(10_000)
	e.chain.Cancel(e.wallet.Addr, [32]byte{e.nonce})
	e.drain(8)

	rc := e.final(id)
	if rc.Status != receipts.StatusUnpaid || rc.Reason != "authorization_canceled" {
		t.Fatalf("receipt = %+v", rc)
	}
	_, err := e.state.Reserve(context.Background(), payerstate.Request{Chain: network, Asset: usdc,
		Payer: e.wallet.Addr.Hex(), Nonce: "0xff", Amount: big.NewInt(1), Expiry: time.Now().Add(time.Minute)})
	var denied *payerstate.DeniedError
	if !errors.As(err, &denied) || denied.Reason != payerstate.ReasonPayerBlocked {
		t.Fatalf("payer not blocked: %v", err)
	}
}

// A refusal is final for that attempt: the next backend is not asked to
// overrule it (a facilitator may refuse a payer it screens out).
func TestRefusalDoesNotFailOver(t *testing.T) {
	e := newEnv(t)
	primary, secondary := newFakeFacilitator(t, e.chain), newFakeFacilitator(t, e.chain)
	primary.set("refuse")
	e.use(primary, secondary)
	e.settler.MaxAttempts = 1
	id := e.serve(10_000)
	e.drain(4)
	if rc := e.final(id); rc.Status == receipts.StatusSettled {
		t.Fatalf("settled by the second backend after a refusal: %+v", rc)
	}
	if secondary.count() != 0 {
		t.Fatalf("secondary asked %d times after a refusal", secondary.count())
	}
}

// Grokky's H1 (2026-11-22): the payer signs a second authorization with the
// same nonce, for 1 unit to an address they control, and settles it
// elsewhere before the gateway does. The nonce is "used" and its
// AuthorizationUsed log exists, but the transfer is not this payment:
// unpaid, and the payer is blocked. Also when the amount is short.
func TestPreemptedNonceIsUnpaidAndBlocks(t *testing.T) {
	attacker, _ := evm.ParseAddress("0x00000000000000000000000000000000000000ee")
	us, _ := evm.ParseAddress(payTo)
	for name, preempt := range map[string]func(e *env) error{
		"other recipient": func(e *env) error {
			return e.chain.TransferTo(e.wallet.Addr, attacker, [32]byte{e.nonce}, big.NewInt(1), "0xpre1")
		},
		"short amount to us": func(e *env) error {
			return e.chain.TransferTo(e.wallet.Addr, us, [32]byte{e.nonce}, big.NewInt(1), "0xpre2")
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			fac := newFakeFacilitator(t, e.chain)
			e.use(fac) // a normal facilitator: its settle fails, the nonce is used
			id := e.serve(10_000)
			if err := preempt(e); err != nil {
				t.Fatal(err)
			}
			e.drain(8)
			rc := e.final(id)
			if rc.Status != receipts.StatusUnpaid || rc.Reason != "authorization_preempted" {
				t.Fatalf("receipt = %+v", rc)
			}
			_, err := e.state.Reserve(context.Background(), payerstate.Request{Chain: network, Asset: usdc,
				Payer: e.wallet.Addr.Hex(), Nonce: "0xff", Amount: big.NewInt(1), Expiry: time.Now().Add(time.Minute)})
			var denied *payerstate.DeniedError
			if !errors.As(err, &denied) || denied.Reason != payerstate.ReasonPayerBlocked {
				t.Fatalf("payer not blocked: %v", err)
			}
		})
	}
}

// Our own transfer, found on chain after a lost answer, still counts as
// paid: the AuthorizationUsed log and the Transfer to us of the amount.
func TestOwnTransferFoundOnChainIsSettled(t *testing.T) {
	e := newEnv(t)
	fac := newFakeFacilitator(t, e.chain)
	fac.set("refuse")
	e.use(fac)
	id := e.serve(10_000)
	us, _ := evm.ParseAddress(payTo)
	if err := e.chain.TransferTo(e.wallet.Addr, us, [32]byte{e.nonce}, big.NewInt(10_000), "0xours"); err != nil {
		t.Fatal(err)
	}
	e.drain(8)
	if rc := e.final(id); rc.Status != receipts.StatusSettled {
		t.Fatalf("receipt = %+v", rc)
	}
}
