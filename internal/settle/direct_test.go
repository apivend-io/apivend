package settle

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	"apivend.io/apivend/internal/x402"
)

const gasKey = "0x" + "22222222222222222222222222222222" + "22222222222222222222222222222222"

// node is a fake Base Sepolia JSON-RPC endpoint.
type node struct {
	mu       sync.Mutex
	chainID  string
	estimate func() (string, *rpcErr) // eth_estimateGas
	sent     [][]byte
	nonce    uint64
	receipt  string // status for receipts: "0x1", "0x0", or "" (none yet)
	baseFee  string
	code     string // eth_getCode answer for every address; "0x" by default
	deployer []byte // a sent transaction containing these bytes deploys (code becomes "0x60")
}

type rpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

func newNode(t *testing.T) (*node, Chain) {
	n := &node{chainID: "0x14a34", receipt: "0x1", baseFee: "0x989680", // 0.01 gwei
		estimate: func() (string, *rpcErr) { return "0x15000", nil }}
	srv := httptest.NewServer(http.HandlerFunc(n.serve))
	t.Cleanup(srv.Close)
	c, err := chain.NewRPC([]string{srv.URL}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return n, c
}

func (n *node) serve(w http.ResponseWriter, r *http.Request) {
	var reqs []struct {
		ID     int64             `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	json.NewDecoder(r.Body).Decode(&reqs)
	var out []map[string]any
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, q := range reqs {
		res := map[string]any{"jsonrpc": "2.0", "id": q.ID}
		switch q.Method {
		case "eth_chainId":
			res["result"] = n.chainID
		case "eth_estimateGas":
			if v, e := n.estimate(); e != nil {
				res["error"] = e
			} else {
				res["result"] = v
			}
		case "eth_getCode":
			if n.code == "" {
				res["result"] = "0x"
			} else {
				res["result"] = n.code
			}
		case "eth_maxPriorityFeePerGas":
			res["result"] = "0xf4240"
		case "eth_getBlockByNumber":
			res["result"] = map[string]string{"baseFeePerGas": n.baseFee}
		case "eth_getTransactionCount":
			res["result"] = fmt.Sprintf("0x%x", n.nonce)
		case "eth_sendRawTransaction":
			var s string
			json.Unmarshal(q.Params[0], &s)
			raw, _ := hex.DecodeString(strings.TrimPrefix(s, "0x"))
			n.sent = append(n.sent, raw)
			n.nonce++
			if n.deployer != nil && strings.Contains(string(raw), string(n.deployer)) {
				n.code = "0x60"
			}
			h := evm.Keccak256(raw)
			res["result"] = "0x" + hex.EncodeToString(h[:])
		case "eth_getTransactionReceipt":
			if n.receipt == "" {
				res["result"] = nil
			} else {
				res["result"] = map[string]string{"status": n.receipt, "blockNumber": "0x10"}
			}
		default:
			res["error"] = rpcErr{Code: -32601, Message: "method not found"}
		}
		out = append(out, res)
	}
	json.NewEncoder(w).Encode(out)
}

func signedJob(t *testing.T, nonce byte) *Job {
	t.Helper()
	w, _ := payer.NewWallet(payerKey)
	req := x402.Requirements{Scheme: "exact", Network: network, Amount: "10000", Asset: usdc, PayTo: payTo,
		MaxTimeoutSeconds: 60, Extra: map[string]any{"name": "USDC", "version": "2"}}
	asset, _ := evm.ParseAddress(usdc)
	to, _ := evm.ParseAddress(payTo)
	auth := evm.Authorization{From: w.Addr, To: to, Value: big.NewInt(10000), ValidAfter: big.NewInt(0),
		ValidBefore: big.NewInt(time.Now().Add(time.Minute).Unix()), Nonce: [32]byte{nonce}}
	p, err := w.Sign(req, x402.ResourceInfo{}, auth, evm.Domain{Name: "USDC", Version: "2", ChainID: big.NewInt(84532), VerifyingContract: asset})
	if err != nil {
		t.Fatal(err)
	}
	return &Job{Payload: *p, Requirements: req}
}

func newTestDirect(t *testing.T, c Chain, maxFee string) *Direct {
	t.Helper()
	d, err := newDirect(BackendConfig{Type: "direct", Token: gasKey, MaxFee: maxFee}, map[string]Chain{network: c})
	if err != nil {
		t.Fatal(err)
	}
	d.confirm, d.poll = 300*time.Millisecond, 20*time.Millisecond
	return d
}

func TestDirectSettles(t *testing.T) {
	n, c := newNode(t)
	d := newTestDirect(t, c, "")
	res := d.Settle(context.Background(), signedJob(t, 1))
	if res.Outcome != Settled || len(n.sent) != 1 {
		t.Fatalf("result %+v, sent %d", res, len(n.sent))
	}
	raw := n.sent[0]
	h := evm.Keccak256(raw)
	if res.Transaction != "0x"+hex.EncodeToString(h[:]) {
		t.Fatalf("tx %s is not the hash of what was sent", res.Transaction)
	}
	// The transaction calls transferWithAuthorization on the token.
	sel := evm.Keccak256([]byte("transferWithAuthorization(address,address,uint256,uint256,uint256,bytes32,uint8,bytes32,bytes32)"))
	token, _ := evm.ParseAddress(usdc)
	if raw[0] != 0x02 || !strings.Contains(hex.EncodeToString(raw), hex.EncodeToString(sel[:4])) ||
		!strings.Contains(hex.EncodeToString(raw), hex.EncodeToString(token[:])) {
		t.Fatalf("raw %x", raw)
	}
	if hex.EncodeToString(sel[:4]) != "e3ee160e" {
		t.Fatalf("selector %x (CDP's settlements use e3ee160e)", sel[:4])
	}
}

func TestDirectNoncesNeverCollide(t *testing.T) {
	n, c := newNode(t)
	d := newTestDirect(t, c, "")
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if res := d.Settle(context.Background(), signedJob(t, byte(10+i))); res.Outcome != Settled {
				t.Errorf("settle %d: %+v", i, res)
			}
		}(i)
	}
	wg.Wait()
	if len(n.sent) != 6 || n.nonce != 6 {
		t.Fatalf("sent %d, nonce %d", len(n.sent), n.nonce)
	}
}

func TestDirectRefusalsAndOutages(t *testing.T) {
	revert := func(msg string) func() (string, *rpcErr) {
		return func() (string, *rpcErr) { return "", &rpcErr{Code: 3, Message: "execution reverted: " + msg} }
	}
	for name, tc := range map[string]struct {
		setup   func(*node)
		maxFee  string
		job     func(*Job)
		outcome Outcome
		reason  string
	}{
		"empty wallet":  {setup: func(n *node) { n.estimate = revert("ERC20: transfer amount exceeds balance") }, outcome: Refused, reason: x402.ErrInsufficientFunds},
		"used":          {setup: func(n *node) { n.estimate = revert("FiatTokenV2: authorization is used or canceled") }, outcome: Refused, reason: "authorization_used"},
		"expired":       {setup: func(n *node) { n.estimate = revert("FiatTokenV2: authorization is expired") }, outcome: Refused, reason: "authorization_expired"},
		"bad signature": {setup: func(n *node) { n.estimate = revert("FiatTokenV2: invalid signature") }, outcome: Refused, reason: x402.ErrSignature},
		"other revert":  {setup: func(n *node) { n.estimate = revert("") }, outcome: Refused, reason: "transaction_reverted"},
		"no gas money": {setup: func(n *node) {
			n.estimate = func() (string, *rpcErr) {
				return "", &rpcErr{Code: -32000, Message: "insufficient funds for gas * price + value"}
			}
		}, outcome: Unavailable, reason: "gas_balance_low"},
		"gas too dear":  {maxFee: "0.0000001", outcome: Unavailable, reason: "gas_above_max_fee"},
		"wrong chain":   {setup: func(n *node) { n.chainID = "0x2105" }, outcome: Unavailable, reason: "rpc_wrong_chain"},
		"not mined yet": {setup: func(n *node) { n.receipt = "" }, outcome: Pending, reason: errSettlementPending},
		"reverted":      {setup: func(n *node) { n.receipt = "0x0" }, outcome: Refused, reason: "transaction_reverted"},
		"wrong payee":   {job: func(j *Job) { j.Requirements.PayTo = "0x0000000000000000000000000000000000000001" }, outcome: Refused, reason: x402.ErrInvalidPayload},
		"wrong amount":  {job: func(j *Job) { j.Requirements.Amount = "20000" }, outcome: Refused, reason: x402.ErrInvalidPayload},
		"revert in data": {setup: func(n *node) {
			// Error(string) "ERC20: transfer amount exceeds balance" as revert data.
			msg := "ERC20: transfer amount exceeds balance"
			data := "0x08c379a0" + fmt.Sprintf("%064x", 32) + fmt.Sprintf("%064x", len(msg)) + hex.EncodeToString([]byte(msg)) + strings.Repeat("0", 64-2*len(msg)%64)
			n.estimate = func() (string, *rpcErr) { return "", &rpcErr{Code: 3, Message: "execution reverted", Data: data} }
		}, outcome: Refused, reason: x402.ErrInsufficientFunds},
	} {
		t.Run(name, func(t *testing.T) {
			n, c := newNode(t)
			if tc.setup != nil {
				tc.setup(n)
			}
			d := newTestDirect(t, c, tc.maxFee)
			job := signedJob(t, 1)
			if tc.job != nil {
				tc.job(job)
			}
			res := d.Settle(context.Background(), job)
			if res.Outcome != tc.outcome || res.Reason != tc.reason {
				t.Fatalf("got %+v, want %v %s", res, tc.outcome, tc.reason)
			}
			if tc.outcome == Refused && tc.reason != "transaction_reverted" && len(n.sent) != 0 {
				t.Fatalf("sent a transaction that simulation refused")
			}
			if tc.outcome == Pending && !strings.HasPrefix(res.Transaction, "0x") {
				t.Fatal("pending without a transaction hash")
			}
		})
	}
}

func TestDirectConfig(t *testing.T) {
	_, c := newNode(t)
	d := newTestDirect(t, c, "0.0001")
	if d.maxFee.String() != "100000000000000" {
		t.Fatalf("maxFee %s", d.maxFee)
	}
	if ok, known := d.Supports(context.Background(), 2, "exact", network); !ok || !known {
		t.Fatal("does not support the configured network")
	}
	if ok, known := d.Supports(context.Background(), 2, "exact", "eip155:8453"); ok || !known {
		t.Fatal("supports a network without an RPC")
	}
	if !strings.HasPrefix(d.Name(), "direct:0x") {
		t.Fatalf("name %s", d.Name())
	}
	for _, bc := range []BackendConfig{
		{Type: "direct"},
		{Type: "direct", Token: "0x1234"},
		{Type: "direct", Token: gasKey, MaxFee: "-1"},
		{Type: "direct", Token: gasKey, MaxFee: "lots"},
	} {
		if _, err := BuildProviders(ProvidersConfig{Providers: []ProviderConfig{{Name: "p", Backends: []BackendConfig{bc}}}}, time.Second); err == nil {
			t.Errorf("%+v accepted", bc)
		}
	}
	// Without chains (the controller's validation) a direct backend builds
	// and supports nothing.
	p, err := BuildProviders(ProvidersConfig{Providers: []ProviderConfig{{Name: "p", Backends: []BackendConfig{{Type: "direct", Token: gasKey}}}}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := p["p"][0].(Capable).Supports(context.Background(), 2, "exact", network); ok {
		t.Fatal("supports a network with no chains configured")
	}
}

func TestDirectLongSignatureUsesTheBytesOverload(t *testing.T) {
	job := signedJob(t, 1)
	var p x402.ExactEVMPayload
	json.Unmarshal(job.Payload.Payload, &p)
	p.Signature = "0x" + strings.Repeat("cd", 100)
	job.Payload.Payload, _ = json.Marshal(p)
	data, err := callData(job)
	if err != nil {
		t.Fatal(err)
	}
	sel := evm.Keccak256([]byte("transferWithAuthorization(address,address,uint256,uint256,uint256,bytes32,bytes)"))
	if hex.EncodeToString(data[:4]) != hex.EncodeToString(sel[:4]) || len(data) != 4+7*32+32+128 {
		t.Fatalf("calldata %d bytes, selector %x", len(data), data[:4])
	}
	t.Logf("bytes-calldata %x", data)
	t.Logf("auth %s %s %s %s %s %s", p.Authorization.From, p.Authorization.To, p.Authorization.Value, p.Authorization.ValidAfter, p.Authorization.ValidBefore, p.Authorization.Nonce)
}

func with6492(t *testing.T, job *Job, factory evm.Address, deployCall []byte) []byte {
	t.Helper()
	var p x402.ExactEVMPayload
	json.Unmarshal(job.Payload.Payload, &p)
	inner, _ := hex.DecodeString(strings.TrimPrefix(p.Signature, "0x"))
	word := func(n int) []byte { return new(big.Int).SetInt64(int64(n)).FillBytes(make([]byte, 32)) }
	pad := func(b []byte) []byte { return append(append([]byte{}, b...), make([]byte, (32-len(b)%32)%32)...) }
	w := append(make([]byte, 12), factory[:]...)
	w = append(w, word(96)...)
	w = append(w, word(96+32+len(pad(deployCall)))...)
	w = append(w, word(len(deployCall))...)
	w = append(w, pad(deployCall)...)
	w = append(w, word(len(inner))...)
	w = append(w, pad(inner)...)
	w = append(w, evm.ERC6492Magic...)
	p.Signature = "0x" + hex.EncodeToString(w)
	job.Payload.Payload, _ = json.Marshal(p)
	return inner
}

func TestDirectDeploysAnUndeployedWalletFirst(t *testing.T) {
	deployCall := []byte("create-account-call-data-for-test")
	token, _ := evm.ParseAddress(usdc)

	// Not deployed: two transactions, the factory call, then the transfer
	// with the unwrapped signature.
	n, c := newNode(t)
	n.deployer = deployCall
	d := newTestDirect(t, c, "")
	job := signedJob(t, 1)
	with6492(t, job, DefaultFactories[0], deployCall)
	res := d.Settle(context.Background(), job)
	if res.Outcome != Settled || len(n.sent) != 2 {
		t.Fatalf("result %+v, sent %d", res, len(n.sent))
	}
	if !strings.Contains(string(n.sent[0]), string(DefaultFactories[0][:])) || !strings.Contains(string(n.sent[0]), string(deployCall)) {
		t.Fatal("first transaction is not the factory call")
	}
	if !strings.Contains(string(n.sent[1]), string(token[:])) || strings.Contains(string(n.sent[1]), string(evm.ERC6492Magic)) {
		t.Fatal("second transaction is not the transfer with the unwrapped signature")
	}

	// Already deployed (a wrapped signature anyway): the transfer only.
	n, c = newNode(t)
	n.code = "0x60"
	d = newTestDirect(t, c, "")
	job = signedJob(t, 2)
	with6492(t, job, DefaultFactories[0], deployCall)
	if res := d.Settle(context.Background(), job); res.Outcome != Settled || len(n.sent) != 1 {
		t.Fatalf("deployed: %+v, sent %d", res, len(n.sent))
	}

	// A factory that is not allowed is never called.
	n, c = newNode(t)
	d = newTestDirect(t, c, "")
	job = signedJob(t, 3)
	with6492(t, job, evm.Address{0xee}, deployCall)
	if res := d.Settle(context.Background(), job); res.Outcome != Refused || res.Reason != "eip6492_factory_not_allowed" || len(n.sent) != 0 {
		t.Fatalf("unknown factory: %+v, sent %d", res, len(n.sent))
	}

	// The deployment reverts: refused as a failed deployment, no transfer.
	n, c = newNode(t)
	n.receipt = "0x0"
	d = newTestDirect(t, c, "")
	job = signedJob(t, 4)
	with6492(t, job, DefaultFactories[0], deployCall)
	if res := d.Settle(context.Background(), job); res.Outcome != Refused || res.Reason != "smart_wallet_deployment_failed" || len(n.sent) != 1 {
		t.Fatalf("deploy reverted: %+v, sent %d", res, len(n.sent))
	}
}
