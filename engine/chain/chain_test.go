package chain

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"apivend.io/apivend/engine/evm"
)

var (
	token, _ = evm.ParseAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e")
	owner, _ = evm.ParseAddress("0x857b06519E91e3A54538791bDbb0E22373e36b66")
)

// node answers JSON-RPC batches like an Ethereum node would.
func node(t *testing.T, calls *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var reqs []struct {
			ID     int64             `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(raw, &reqs); err != nil {
			t.Errorf("not a batch: %s", raw)
		}
		type resp struct {
			JSONRPC string `json:"jsonrpc"`
			ID      int64  `json:"id"`
			Result  any    `json:"result"`
		}
		var out []resp
		// Answer in reverse order: clients must match by id.
		for i := len(reqs) - 1; i >= 0; i-- {
			q := reqs[i]
			var result any
			switch q.Method {
			case "eth_blockNumber":
				result = "0x1b4"
			case "eth_call":
				var call struct{ To, Data string }
				json.Unmarshal(q.Params[0], &call)
				*calls = append(*calls, call.To+" "+call.Data)
				switch {
				case strings.HasPrefix(call.Data, "0x70a08231"): // balanceOf(address)
					result = "0x" + strings.Repeat("0", 58) + "0f4240" // 1_000_000
				case strings.HasPrefix(call.Data, "0xe94a0102"): // authorizationState(address,bytes32)
					result = "0x" + strings.Repeat("0", 63) + "1"
				}
			case "eth_getTransactionReceipt":
				result = map[string]string{"blockNumber": "0x10", "status": "0x1"}
			}
			out = append(out, resp{"2.0", q.ID, result})
		}
		json.NewEncoder(w).Encode(out)
	}))
}

func TestRPC(t *testing.T) {
	var calls []string
	srv := node(t, &calls)
	defer srv.Close()
	r, _ := NewRPC([]string{srv.URL}, time.Second)
	ctx := context.Background()

	bal, block, err := r.BalanceOf(ctx, token, owner)
	if err != nil || bal.Cmp(big.NewInt(1_000_000)) != 0 || block != 0x1b4 {
		t.Fatalf("BalanceOf = %v, %d, %v", bal, block, err)
	}
	want := "0x036cbd53842c5426634e7929541ec2318f3dcf7e 0x70a08231000000000000000000000000857b06519e91e3a54538791bdbb0e22373e36b66"
	if calls[0] != want {
		t.Fatalf("balanceOf call = %s", calls[0])
	}

	used, err := r.AuthorizationState(ctx, token, owner, [32]byte{0xaa})
	if err != nil || !used {
		t.Fatalf("AuthorizationState = %v, %v", used, err)
	}
	if !strings.HasSuffix(calls[1], "857b06519e91e3a54538791bdbb0e22373e36b66aa"+strings.Repeat("0", 62)) {
		t.Fatalf("authorizationState call = %s", calls[1])
	}

	blk, ok, found, err := r.TransactionBlock(ctx, "0xabc")
	if err != nil || !found || !ok || blk != 16 {
		t.Fatalf("TransactionBlock = %d %v %v %v", blk, ok, found, err)
	}
}

// KeepWarm holds a connection open to every endpoint, the fallback included,
// so a read on the request path reuses it instead of paying connection setup
// inside its budget; and it stops with its context.
func TestRPCKeepWarm(t *testing.T) {
	type counted struct {
		srv          *httptest.Server
		conns, pings atomic.Int64
	}
	start := func() *counted {
		c := &counted{}
		var calls []string
		inner := node(t, &calls)
		inner.Close() // only its handler is used
		c.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c.pings.Add(1)
			inner.Config.Handler.ServeHTTP(w, r)
		}))
		c.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew {
				c.conns.Add(1)
			}
		}
		c.srv.Start()
		return c
	}
	primary, fallback := start(), start()
	defer primary.srv.Close()
	defer fallback.srv.Close()

	waitPings := func(n int64) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for primary.pings.Load() < n || fallback.pings.Load() < n {
			if time.Now().After(deadline) {
				t.Fatalf("pings: primary %d, fallback %d; want %d each", primary.pings.Load(), fallback.pings.Load(), n)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// The first ping, at once, opens a connection to each endpoint; a read
	// afterwards reuses it. A long interval keeps pings out of the read's way.
	r, _ := NewRPC([]string{primary.srv.URL, fallback.srv.URL}, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.KeepWarm(ctx, time.Hour)
	waitPings(1)
	time.Sleep(50 * time.Millisecond) // the ping's connection returns to the pool
	if primary.conns.Load() != 1 || fallback.conns.Load() != 1 {
		t.Fatalf("connections after the first ping: primary %d, fallback %d; want 1 each", primary.conns.Load(), fallback.conns.Load())
	}
	if _, _, err := r.BalanceOf(context.Background(), token, owner); err != nil {
		t.Fatal(err)
	}
	if got := primary.conns.Load(); got != 1 {
		t.Fatalf("the read opened a new connection (%d in all); want the warm one reused", got)
	}

	// It repeats every interval, and stops with its context.
	r2, _ := NewRPC([]string{primary.srv.URL, fallback.srv.URL}, time.Second)
	ctx2, cancel2 := context.WithCancel(context.Background())
	go r2.KeepWarm(ctx2, 20*time.Millisecond)
	waitPings(primary.pings.Load() + 3)
	cancel2()
	time.Sleep(50 * time.Millisecond)
	stopped := primary.pings.Load()
	time.Sleep(100 * time.Millisecond)
	if primary.pings.Load() != stopped {
		t.Fatal("still pinging after the context ended")
	}
}

func TestRPCFailover(t *testing.T) {
	var calls []string
	good := node(t, &calls)
	defer good.Close()
	hits := 0
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer bad.Close()

	r, _ := NewRPC([]string{bad.URL, good.URL}, time.Second)
	for i := 0; i < 3; i++ {
		if _, _, err := r.BalanceOf(context.Background(), token, owner); err != nil {
			t.Fatal(err)
		}
	}
	if hits != 1 {
		t.Fatalf("bad endpoint hit %d times; want 1 (then stick to the good one)", hits)
	}

	only, _ := NewRPC([]string{bad.URL}, time.Second)
	_, _, err := only.BalanceOf(context.Background(), token, owner)
	if err == nil {
		t.Fatal("no error with every endpoint down")
	}
	if strings.Contains(err.Error(), bad.URL) {
		t.Fatalf("error leaks the endpoint URL: %v", err)
	}
}

func TestRPCRejectsNoCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var reqs []struct{ ID int64 }
		json.Unmarshal(raw, &reqs)
		// "0x" is what a node returns for a call to an address without code.
		io.WriteString(w, `[{"jsonrpc":"2.0","id":`+jsonInt(reqs[0].ID)+`,"result":"0x1"},{"jsonrpc":"2.0","id":`+jsonInt(reqs[1].ID)+`,"result":"0x"}]`)
	}))
	defer srv.Close()
	r, _ := NewRPC([]string{srv.URL}, time.Second)
	if _, _, err := r.BalanceOf(context.Background(), token, owner); err == nil {
		t.Fatal("empty eth_call result read as a balance")
	}
}

func jsonInt(v int64) string { b, _ := json.Marshal(v); return string(b) }

// TransferAfter reads the token's Transfer log right after the given log
// index in a transaction's receipt, and nothing else: a Transfer of another
// token, or a log further away, is not the one.
func TestTransferAfter(t *testing.T) {
	word := func(a evm.Address) string { return "0x" + hex.EncodeToString(evm.AddressWord(a)) }
	to, _ := evm.ParseAddress("0x4C5d78BebbF28853c28eB385b936875e68107C60")
	other, _ := evm.ParseAddress("0x00000000000000000000000000000000000000ee")
	transfer := "0x" + hex.EncodeToString(topicTransfer[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []struct{ ID int64 }
		json.NewDecoder(r.Body).Decode(&reqs)
		logs := []map[string]any{
			{"address": token.Hex(), "topics": []string{"0x" + hex.EncodeToString(topicAuthUsed[:]), word(owner), "0x" + strings.Repeat("11", 32)}, "data": "0x", "logIndex": "0x4"},
			{"address": token.Hex(), "topics": []string{transfer, word(owner), word(to)}, "data": "0x" + strings.Repeat("0", 60) + "2710", "logIndex": "0x5"},
			{"address": other.Hex(), "topics": []string{transfer, word(owner), word(other)}, "data": "0x" + strings.Repeat("0", 63) + "1", "logIndex": "0x6"},
		}
		json.NewEncoder(w).Encode([]map[string]any{{"jsonrpc": "2.0", "id": reqs[0].ID, "result": map[string]any{"logs": logs}}})
	}))
	defer srv.Close()
	r, err := NewRPC([]string{srv.URL}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := r.TransferAfter(context.Background(), "0xabc", token, 4)
	if err != nil || !found || got.From != owner || got.To != to || got.Value.Int64() != 10_000 {
		t.Fatalf("after log 4: %+v %v %v", got, found, err)
	}
	if _, found, _ := r.TransferAfter(context.Background(), "0xabc", token, 5); found {
		t.Fatal("another token's Transfer accepted")
	}
	if _, found, _ := r.TransferAfter(context.Background(), "0xabc", token, 8); found {
		t.Fatal("found a log that is not there")
	}
}
