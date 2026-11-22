package verify_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/payer"
	"apivend.io/apivend/internal/verify"
	"apivend.io/apivend/internal/x402"
)

// A contract wallet's address (no key exists for it); payerKey is its owner.
var walletAddr, _ = evm.ParseAddress("0xC1f5BF53c9560C232c391EC444990CB25b960bbF")

type fakeChecker struct {
	mu      sync.Mutex
	calls   int
	valid   bool
	err     error
	lastSig []byte
}

func (f *fakeChecker) IsValidSignature(_ context.Context, network string, account evm.Address, _ [32]byte, sig []byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastSig = sig
	return f.valid && account == walletAddr && network == "eip155:84532", f.err
}

func fromWallet(t *testing.T) *x402.PaymentPayload {
	return signed(t, payerKey, func(a *evm.Authorization) { a.From = walletAddr })
}

func TestContractWallets(t *testing.T) {
	ctx := context.Background()
	opts := func(c verify.ContractChecker) verify.Options {
		return verify.Options{MinRemaining: 10 * time.Second, Contract: c}
	}

	// An ordinary payer's own key: the chain is never asked.
	fc := &fakeChecker{valid: true}
	if _, verr := verify.Verify(ctx, signed(t, payerKey, nil), offers(t), now, opts(fc)); verr != nil || fc.calls != 0 {
		t.Fatalf("EOA: %v, %d checks", verr, fc.calls)
	}

	// The owner's key signing for the wallet: accepted when the wallet says so.
	got, verr := verify.Verify(ctx, fromWallet(t), offers(t), now, opts(fc))
	if verr != nil || !got.Contract || got.Payer != walletAddr || fc.calls != 1 {
		t.Fatalf("wallet: %+v %v, %d checks", got, verr, fc.calls)
	}

	// The wallet refuses, the chain cannot be asked, no checker at all.
	if _, verr := verify.Verify(ctx, fromWallet(t), offers(t), now, opts(&fakeChecker{})); verr == nil || verr.Reason != x402.ErrSignature || verr.Unavailable {
		t.Fatalf("refused: %+v", verr)
	}
	if _, verr := verify.Verify(ctx, fromWallet(t), offers(t), now, opts(&fakeChecker{err: errors.New("rpc down")})); verr == nil || !verr.Unavailable {
		t.Fatalf("outage: %+v", verr)
	}
	if _, verr := verify.Verify(ctx, fromWallet(t), offers(t), now, opts(nil)); verr == nil || verr.Reason != x402.ErrSignature {
		t.Fatalf("no checker: %+v", verr)
	}

	// Longer signatures go to the wallet as they are.
	long := fromWallet(t)
	editPayload(t, long, func(ex *x402.ExactEVMPayload) { ex.Signature = "0x" + strings.Repeat("ab", 300) })
	fc = &fakeChecker{valid: true}
	if _, verr := verify.Verify(ctx, long, offers(t), now, opts(fc)); verr != nil || len(fc.lastSig) != 300 {
		t.Fatalf("long signature: %v, %d bytes", verr, len(fc.lastSig))
	}

	// ERC-6492 (wallet not deployed yet): refused without asking; too long or
	// odd-length: refused.
	for name, sig := range map[string]string{
		"erc6492":  "0x" + strings.Repeat("ab", 100) + strings.Repeat("6492", 16),
		"too long": "0x" + strings.Repeat("ab", 4097),
		"odd":      "0xabc",
	} {
		p := fromWallet(t)
		editPayload(t, p, func(ex *x402.ExactEVMPayload) { ex.Signature = sig })
		fc = &fakeChecker{valid: true}
		if _, verr := verify.Verify(ctx, p, offers(t), now, opts(fc)); verr == nil || verr.Reason != x402.ErrSignature || fc.calls != 0 {
			t.Fatalf("%s: %+v, %d checks", name, verr, fc.calls)
		}
	}
}

// node answers eth_call for isValidSignature like a chain would.
type node struct {
	mu    sync.Mutex
	calls int
	reply func(to, data string) (result any, rpcErr map[string]any)
}

func (n *node) start(t *testing.T) verify.Caller {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []struct {
			ID     int64             `json:"id"`
			Params []json.RawMessage `json:"params"`
		}
		json.NewDecoder(r.Body).Decode(&reqs)
		var call struct{ To, Data string }
		json.Unmarshal(reqs[0].Params[0], &call)
		n.mu.Lock()
		n.calls++
		res, e := n.reply(call.To, call.Data)
		n.mu.Unlock()
		out := map[string]any{"jsonrpc": "2.0", "id": reqs[0].ID}
		if e != nil {
			out["error"] = e
		} else {
			out["result"] = res
		}
		json.NewEncoder(w).Encode([]any{out})
	}))
	t.Cleanup(srv.Close)
	c, _ := chain.NewRPC([]string{srv.URL}, time.Second)
	return c
}

func TestChainChecker(t *testing.T) {
	ctx := context.Background()
	w, _ := payer.NewWallet(payerKey)
	digest := [32]byte{9, 9, 9}
	sig := make([]byte, 65)
	sig[0] = 0x42

	accept := "0x1626ba7e" + strings.Repeat("0", 56)
	n := &node{reply: func(to, data string) (any, map[string]any) {
		// selector, digest, offset 0x40, length 65, the signature padded to 96
		want := "0x1626ba7e" + hex.EncodeToString(digest[:]) +
			strings.Repeat("0", 62) + "40" + strings.Repeat("0", 62) + "41" + hex.EncodeToString(sig) + strings.Repeat("0", 62)
		if data != want {
			return nil, map[string]any{"code": -32000, "message": "bad calldata"}
		}
		switch strings.ToLower(to) {
		case strings.ToLower(walletAddr.Hex()):
			return accept, nil
		case "0x00000000000000000000000000000000000000aa":
			return nil, map[string]any{"code": 3, "message": "execution reverted"}
		case "0x00000000000000000000000000000000000000bb":
			return "0xffffffff" + strings.Repeat("0", 56), nil
		}
		return "0x", nil // no code
	}}
	clock := time.Unix(1_800_000_000, 0)
	cc := &verify.ChainChecker{Chains: map[string]verify.Caller{"eip155:84532": n.start(t)}, Now: func() time.Time { return clock }}

	if ok, err := cc.IsValidSignature(ctx, "eip155:84532", walletAddr, digest, sig); !ok || err != nil {
		t.Fatalf("wallet: %v %v", ok, err)
	}
	reverts, _ := evm.ParseAddress("0x00000000000000000000000000000000000000aa")
	other, _ := evm.ParseAddress("0x00000000000000000000000000000000000000bb")
	for name, a := range map[string]evm.Address{"reverts": reverts, "wrong magic": other} {
		if ok, err := cc.IsValidSignature(ctx, "eip155:84532", a, digest, sig); ok || err != nil {
			t.Fatalf("%s: %v %v", name, ok, err)
		}
	}
	// An ordinary account (no code) is remembered: the second check makes
	// no call, until the TTL passes.
	before := n.calls
	for i := 0; i < 3; i++ {
		if ok, err := cc.IsValidSignature(ctx, "eip155:84532", w.Addr, digest, sig); ok || err != nil {
			t.Fatalf("EOA: %v %v", ok, err)
		}
	}
	if n.calls != before+1 {
		t.Fatalf("EOA asked %d times", n.calls-before)
	}
	clock = clock.Add(11 * time.Minute)
	cc.IsValidSignature(ctx, "eip155:84532", w.Addr, digest, sig)
	if n.calls != before+2 {
		t.Fatal("EOA not asked again after the TTL")
	}
	// No RPC for the network: refused, not an outage.
	if ok, err := cc.IsValidSignature(ctx, "eip155:8453", walletAddr, digest, sig); ok || err != nil {
		t.Fatalf("no rpc: %v %v", ok, err)
	}
	// An unreachable chain is an error (the filter answers 503).
	dead, _ := chain.NewRPC([]string{"http://127.0.0.1:1"}, 200*time.Millisecond)
	cc2 := &verify.ChainChecker{Chains: map[string]verify.Caller{"eip155:84532": dead}}
	if _, err := cc2.IsValidSignature(ctx, "eip155:84532", walletAddr, digest, sig); err == nil {
		t.Fatal("unreachable chain gave no error")
	}
}

// cfChecker is a ContractChecker that also handles ERC-6492.
type cfChecker struct {
	fakeChecker
	got   *verify.Counterfactual
	valid bool
	err   error
}

func (c *cfChecker) VerifyCounterfactual(_ context.Context, x verify.Counterfactual) (bool, error) {
	c.got = &x
	return c.valid, c.err
}

func wrap6492(factory evm.Address, data, inner []byte) string {
	word := func(n int) []byte { b := make([]byte, 32); b[30], b[31] = byte(n>>8), byte(n); return b }
	pad := func(b []byte) []byte { return append(append([]byte{}, b...), make([]byte, (32-len(b)%32)%32)...) }
	out := append(make([]byte, 12), factory[:]...)
	out = append(out, word(96)...)
	out = append(out, word(96+32+len(pad(data)))...)
	out = append(out, word(len(data))...)
	out = append(out, pad(data)...)
	out = append(out, word(len(inner))...)
	out = append(out, pad(inner)...)
	return "0x" + hex.EncodeToString(append(out, evm.ERC6492Magic...))
}

func TestCounterfactualWallets(t *testing.T) {
	ctx := context.Background()
	opts := func(c verify.ContractChecker) verify.Options {
		return verify.Options{MinRemaining: 10 * time.Second, Contract: c}
	}
	factory := verify.DefaultFactories[0]
	p := fromWallet(t)
	inner := []byte(strings.Repeat("s", 640))
	editPayload(t, p, func(ex *x402.ExactEVMPayload) { ex.Signature = wrap6492(factory, []byte("deploy-call"), inner) })

	cf := &cfChecker{valid: true}
	got, verr := verify.Verify(ctx, p, offers(t), now, opts(cf))
	if verr != nil || !got.Contract || cf.got == nil || cf.got.Factory != factory || string(cf.got.FactoryData) != "deploy-call" ||
		string(cf.got.Signature) != string(inner) || cf.got.Auth.From != walletAddr || cf.got.Network != "eip155:84532" || cf.calls != 0 {
		t.Fatalf("got %+v %v; counterfactual %+v; 1271 calls %d", got, verr, cf.got, cf.calls)
	}
	if _, verr := verify.Verify(ctx, p, offers(t), now, opts(&cfChecker{})); verr == nil || verr.Reason != x402.ErrSignature {
		t.Fatalf("refused: %+v", verr)
	}
	if _, verr := verify.Verify(ctx, p, offers(t), now, opts(&cfChecker{err: errors.New("down")})); verr == nil || !verr.Unavailable {
		t.Fatalf("outage: %+v", verr)
	}
	// A checker without ERC-6492 support refuses without asking.
	fc := &fakeChecker{valid: true}
	if _, verr := verify.Verify(ctx, p, offers(t), now, opts(fc)); verr == nil || fc.calls != 0 {
		t.Fatalf("plain checker: %+v, %d calls", verr, fc.calls)
	}
}

func TestChainCheckerCounterfactual(t *testing.T) {
	ctx := context.Background()
	factory := verify.DefaultFactories[0]
	other := evm.Address{0xee}
	var deployed bool
	var simulated int
	simulateOK := true
	n := &node{reply: func(to, data string) (any, map[string]any) {
		switch {
		case data == "": // eth_getCode carries no data in this fake
			if deployed {
				return "0x6080", nil
			}
			return "0x", nil
		case strings.EqualFold(to, evm.Multicall3.Hex()):
			simulated++
			if !strings.HasPrefix(data, "0x82ad56cb") {
				return nil, map[string]any{"code": -32000, "message": "not aggregate3"}
			}
			if simulateOK {
				return "0x", nil
			}
			return nil, map[string]any{"code": 3, "message": "execution reverted"}
		case strings.HasPrefix(data, "0x1626ba7e"):
			return "0x1626ba7e" + strings.Repeat("0", 56), nil
		}
		return nil, map[string]any{"code": -32601, "message": "?"}
	}}
	rpc := n.start(t)
	cc := &verify.ChainChecker{Chains: map[string]verify.Caller{"eip155:84532": rpc}}
	x := verify.Counterfactual{Network: "eip155:84532", Token: walletAddr, Auth: evm.Authorization{From: walletAddr, To: walletAddr,
		Value: big1(), ValidAfter: big1(), ValidBefore: big1()}, Factory: factory, FactoryData: []byte{1}, Signature: []byte{2}}

	if ok, err := cc.VerifyCounterfactual(ctx, x); !ok || err != nil || simulated != 1 {
		t.Fatalf("undeployed, allowed: %v %v, simulated %d", ok, err, simulated)
	}
	simulateOK = false
	if ok, err := cc.VerifyCounterfactual(ctx, x); ok || err != nil {
		t.Fatalf("simulation reverts: %v %v", ok, err)
	}
	x.Factory = other
	before := simulated
	if ok, err := cc.VerifyCounterfactual(ctx, x); ok || err != nil || simulated != before {
		t.Fatalf("factory not allowed: %v %v (simulated %d more)", ok, err, simulated-before)
	}
	// Already deployed: the wallet is asked (EIP-1271), whatever the factory.
	deployed = true
	if ok, err := cc.VerifyCounterfactual(ctx, x); !ok || err != nil || simulated != before {
		t.Fatalf("deployed: %v %v", ok, err)
	}
	// Chain down: an error, not a refusal.
	dead, _ := chain.NewRPC([]string{"http://127.0.0.1:1"}, 200*time.Millisecond)
	cc2 := &verify.ChainChecker{Chains: map[string]verify.Caller{"eip155:84532": dead}}
	if _, err := cc2.VerifyCounterfactual(ctx, x); err == nil {
		t.Fatal("unreachable chain gave no error")
	}
}

func big1() *big.Int { return big.NewInt(1) }

// Checks spend from one budget of RPC calls: fresh addresses (which the
// no-code memory cannot help with) get ErrBusy once it is spent, and the
// budget refills with time.
func TestChainCheckerBudget(t *testing.T) {
	ctx := context.Background()
	n := &node{reply: func(to, data string) (any, map[string]any) { return "0x", nil }}
	clock := time.Unix(1_800_000_000, 0)
	cc := &verify.ChainChecker{Chains: map[string]verify.Caller{"eip155:84532": n.start(t)}, Now: func() time.Time { return clock },
		PerSecond: 2, Burst: 3}
	fresh := func(i int) evm.Address { var a evm.Address; a[19] = byte(i); a[0] = 0xfe; return a }
	busy := 0
	for i := 0; i < 5; i++ {
		_, err := cc.IsValidSignature(ctx, "eip155:84532", fresh(i), [32]byte{1}, make([]byte, 65))
		if errors.Is(err, verify.ErrBusy) {
			busy++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if busy != 2 || n.calls != 3 {
		t.Fatalf("burst of 3: %d busy, %d calls", busy, n.calls)
	}
	clock = clock.Add(time.Second) // two more
	for i := 10; i < 13; i++ {
		cc.IsValidSignature(ctx, "eip155:84532", fresh(i), [32]byte{1}, make([]byte, 65))
	}
	if n.calls != 5 {
		t.Fatalf("after a second at 2/s: %d calls, want 5", n.calls)
	}
}
