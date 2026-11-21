package verify

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"sync"
	"time"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
)

// Caller is a network's JSON-RPC (chain.RPC).
type Caller interface {
	Call(ctx context.Context, method string, params ...any) (json.RawMessage, error)
}

// ChainChecker is a ContractChecker over each network's RPC: an eth_call to
// the payer's isValidSignature(bytes32,bytes) (EIP-1271).
//
// It only runs for signatures that are not the payer's own key, so ordinary
// payments never wait on it. Accounts found to have no code are remembered
// for NotContractTTL, so junk signatures cannot make the gateway call the
// chain again and again for the same address.
type ChainChecker struct {
	Chains map[string]Caller // by CAIP-2 network
	// Timeout bounds one check; default 1 s.
	Timeout time.Duration
	// NotContractTTL is how long an address without code is remembered;
	// default 10 min.
	NotContractTTL time.Duration
	Now            func() time.Time
	// AllowedFactories may deploy an ERC-6492 wallet; nil means
	// DefaultFactories. Any other factory's calldata is never simulated.
	AllowedFactories []evm.Address

	mu          sync.Mutex
	notContract map[string]time.Time // network|address -> until
}

// DefaultFactories are the Coinbase Smart Wallet factories (v1.1 and v1),
// at the same address on every chain (coinbase/smart-wallet README; code
// checked on Base and Base Sepolia, 2026-11-19).
var DefaultFactories = []evm.Address{
	mustAddress("0xBA5ED110eFDBa3D005bfC882d75358ACBbB85842"),
	mustAddress("0x0BA5ED0c6AA8c49038F819E587E2633c4A9F428a"),
}

func mustAddress(s string) evm.Address {
	a, err := evm.ParseAddress(s)
	if err != nil {
		panic(err)
	}
	return a
}

// isValidSignature(bytes32,bytes) and the value a wallet returns to accept.
var magic1271 = []byte{0x16, 0x26, 0xba, 0x7e}

const maxNotContract = 100_000

func (c *ChainChecker) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// IsValidSignature implements ContractChecker.
func (c *ChainChecker) IsValidSignature(ctx context.Context, network string, account evm.Address, digest [32]byte, sig []byte) (bool, error) {
	rpc := c.Chains[network]
	if rpc == nil {
		return false, nil // no way to ask: refuse as a bad signature, not an outage
	}
	key := network + "|" + account.Hex()
	c.mu.Lock()
	until, known := c.notContract[key]
	c.mu.Unlock()
	if known && c.now().Before(until) {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	return c.ask(ctx, rpc, key, account, digest, sig)
}

func (c *ChainChecker) timeout() time.Duration {
	if c.Timeout == 0 {
		return time.Second
	}
	return c.Timeout
}

// VerifyCounterfactual implements CounterfactualChecker. A wallet that is
// already deployed is asked directly (EIP-1271, with the unwrapped
// signature). One that is not must come from an allowed factory, and is
// checked as the x402 reference facilitator does: one eth_call of a
// Multicall3 batch that deploys the wallet and then runs
// transferWithAuthorization against it. Nothing is sent: the simulation
// only shows that settling would work.
func (c *ChainChecker) VerifyCounterfactual(ctx context.Context, x Counterfactual) (bool, error) {
	rpc := c.Chains[x.Network]
	if rpc == nil {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	raw, err := rpc.Call(ctx, "eth_getCode", x.Auth.From.Hex(), "latest")
	if err != nil {
		return false, err
	}
	var code string
	if json.Unmarshal(raw, &code) != nil {
		return false, errors.New("malformed eth_getCode answer")
	}
	if code != "" && code != "0x" {
		return c.ask(ctx, rpc, x.Network+"|"+x.Auth.From.Hex(), x.Auth.From, x.Digest, x.Signature)
	}
	if !c.allowed(x.Factory) {
		return false, nil
	}
	batch := evm.Aggregate3([]evm.Call{
		{Target: x.Factory, Data: x.FactoryData},
		{Target: x.Token, Data: evm.TransferWithAuthorizationCall(x.Auth, x.Signature)},
	})
	_, err = rpc.Call(ctx, "eth_call", map[string]string{"to": evm.Multicall3.Hex(), "data": "0x" + hex.EncodeToString(batch)}, "latest")
	if err != nil {
		var rpcErr *chain.RPCError
		if errors.As(err, &rpcErr) {
			return false, nil // deploying or transferring would fail
		}
		return false, err
	}
	return true, nil
}

func (c *ChainChecker) allowed(factory evm.Address) bool {
	list := c.AllowedFactories
	if list == nil {
		list = DefaultFactories
	}
	for _, f := range list {
		if f == factory {
			return true
		}
	}
	return false
}

// ask makes the isValidSignature eth_call.
func (c *ChainChecker) ask(ctx context.Context, rpc Caller, key string, account evm.Address, digest [32]byte, sig []byte) (bool, error) {
	data := append(append([]byte{}, magic1271...), digest[:]...)
	data = append(data, word(64)...)
	data = append(data, word(int64(len(sig)))...)
	data = append(data, sig...)
	if pad := len(sig) % 32; pad != 0 {
		data = append(data, make([]byte, 32-pad)...)
	}
	raw, err := rpc.Call(ctx, "eth_call", map[string]string{"to": account.Hex(), "data": "0x" + hex.EncodeToString(data)}, "latest")
	if err != nil {
		var rpcErr *chain.RPCError
		if errors.As(err, &rpcErr) {
			return false, nil // the wallet reverted: it does not accept
		}
		return false, err
	}
	var out string
	if json.Unmarshal(raw, &out) != nil {
		return false, errors.New("malformed eth_call answer")
	}
	b, err := hex.DecodeString(strings.TrimPrefix(out, "0x"))
	if err != nil {
		return false, errors.New("malformed eth_call answer")
	}
	if len(b) == 0 {
		c.remember(key) // no code at this address: an ordinary account
		return false, nil
	}
	return len(b) >= 4 && bytes.Equal(b[:4], magic1271), nil
}

func (c *ChainChecker) remember(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.notContract == nil || len(c.notContract) >= maxNotContract {
		c.notContract = map[string]time.Time{}
	}
	ttl := c.NotContractTTL
	if ttl == 0 {
		ttl = 10 * time.Minute
	}
	c.notContract[key] = c.now().Add(ttl)
}

func word(n int64) []byte { return new(big.Int).SetInt64(n).FillBytes(make([]byte, 32)) }
