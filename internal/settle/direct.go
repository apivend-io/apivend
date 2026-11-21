package settle

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/x402"
)

// Chain is what direct settlement needs from a network's RPC (chain.RPC).
type Chain interface {
	Call(ctx context.Context, method string, params ...any) (json.RawMessage, error)
}

// Direct settles by submitting the payer's EIP-3009 authorization itself:
// USDC transferWithAuthorization in an EIP-1559 transaction signed with a
// gas key. The gas key pays gas only; the authorization moves exactly the
// payer's signed amount to the signed payee, so it cannot be redirected.
//
// A settlement: simulate (eth_estimateGas; a revert becomes a refusal with
// the token's reason), price the gas (refused as Unavailable above maxFee),
// sign and send (one at a time per key, so transaction nonces never
// collide), then wait for the receipt (Settled, Refused on a revert, or
// Pending: the settler then asks the chain before doing anything else).
//
// No compliance screening, unlike some facilitators: opt-in for that reason.
type Direct struct {
	name   string
	key    *secp256k1.PrivateKey
	from   evm.Address
	maxFee *big.Int // wei, per transaction
	chains map[string]Chain
	// factories may be called to deploy an ERC-6492 wallet; nil means
	// DefaultFactories.
	factories []evm.Address

	confirm time.Duration // how long to wait for a receipt
	poll    time.Duration

	mu       sync.Mutex // serialises nonce + send per key
	chainIDs sync.Map   // network -> checked chain id (*big.Int)
}

// DefaultMaxFee is the most one direct settlement may spend on gas: 0.00002
// ETH, about 20 times a Base settlement at 0.01 gwei.
var DefaultMaxFee = big.NewInt(20_000_000_000_000)

func newDirect(bc BackendConfig, chains map[string]Chain) (*Direct, error) {
	if bc.Token == "" {
		return nil, errors.New("direct needs credentials: the gas key (32 bytes, hex)")
	}
	b, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(bc.Token), "0x"))
	if err != nil || len(b) != 32 {
		return nil, errors.New("direct: the gas key must be 32 bytes of hex")
	}
	key := secp256k1.PrivKeyFromBytes(b)
	d := &Direct{key: key, from: evm.PubkeyAddress(key.PubKey()), maxFee: DefaultMaxFee, chains: chains,
		confirm: 8 * time.Second, poll: 500 * time.Millisecond}
	d.name = "direct:" + d.from.String()
	if bc.MaxFee != "" {
		fee, ok := new(big.Rat).SetString(bc.MaxFee)
		if !ok || fee.Sign() <= 0 {
			return nil, fmt.Errorf("direct: maxFee %q is not a positive amount of ETH", bc.MaxFee)
		}
		d.maxFee = new(big.Int).Div(new(big.Int).Mul(fee.Num(), big.NewInt(1e18)), fee.Denom())
	}
	return d, nil
}

func (d *Direct) Name() string { return d.name }

// From is the gas key's address (to fund with ETH).
func (d *Direct) From() evm.Address { return d.from }

// Supports: exact over EIP-3009, x402 v2, on networks the settler has an
// RPC for.
func (d *Direct) Supports(_ context.Context, version int, scheme, network string) (bool, bool) {
	return version == x402.Version && scheme == x402.SchemeExact && d.chains[network] != nil, true
}

func (d *Direct) Settle(ctx context.Context, job *Job) Result {
	network := job.Requirements.Network
	c := d.chains[network]
	if c == nil {
		return Result{Outcome: Unavailable, Reason: "no_rpc"}
	}
	auth, sig, err := authorization(job)
	if err != nil {
		return Result{Outcome: Refused, Reason: x402.ErrInvalidPayload}
	}
	token, err := evm.ParseAddress(job.Requirements.Asset)
	if err != nil {
		return Result{Outcome: Refused, Reason: x402.ErrInvalidRequirements}
	}
	chainID, err := d.chainID(ctx, c, network)
	if err != nil {
		return Result{Outcome: Unavailable, Reason: err.Error()}
	}

	// A smart wallet that is not deployed yet (ERC-6492) is deployed first
	// through its factory, as the x402 reference facilitator does; the gas
	// key pays. A wrapped signature from a wallet that already exists just
	// loses its wrapper.
	if evm.IsERC6492(sig) {
		factory, factoryData, inner, err := evm.ParseERC6492(sig)
		if err != nil {
			return Result{Outcome: Refused, Reason: x402.ErrInvalidPayload}
		}
		raw, err := c.Call(ctx, "eth_getCode", auth.From.Hex(), "latest")
		if err != nil {
			return Result{Outcome: Unavailable, Reason: "rpc_unavailable"}
		}
		var code string
		json.Unmarshal(raw, &code)
		if code == "" || code == "0x" {
			if !d.factoryAllowed(factory) {
				return Result{Outcome: Refused, Reason: "eip6492_factory_not_allowed"}
			}
			if res := d.submit(ctx, c, chainID, factory, factoryData); res.Outcome != Settled {
				if res.Outcome == Refused {
					res.Reason = "smart_wallet_deployment_failed"
				}
				if res.Outcome == Pending {
					// Not mined yet: try the whole settlement again shortly.
					return Result{Outcome: Unavailable, Reason: "smart_wallet_deployment_pending"}
				}
				return res
			}
		}
		sig = inner
	}
	return d.submit(ctx, c, chainID, token, evm.TransferWithAuthorizationCall(auth, sig))
}

// factoryAllowed: only known wallet factories may be called with the
// gas key (Coinbase Smart Wallet v1.1 and v1 unless configured).
func (d *Direct) factoryAllowed(f evm.Address) bool {
	list := d.factories
	if list == nil {
		list = DefaultFactories
	}
	for _, a := range list {
		if a == f {
			return true
		}
	}
	return false
}

// DefaultFactories are the smart-wallet factories direct settlement may
// call to deploy an ERC-6492 wallet: Coinbase Smart Wallet v1.1 and v1, at
// the same address on every chain.
var DefaultFactories = []evm.Address{
	{0xBA, 0x5E, 0xD1, 0x10, 0xeF, 0xDB, 0xa3, 0xD0, 0x05, 0xbf, 0xC8, 0x82, 0xd7, 0x53, 0x58, 0xAC, 0xBb, 0xB8, 0x58, 0x42},
	{0x0B, 0xA5, 0xED, 0x0c, 0x6A, 0xA8, 0xc4, 0x90, 0x38, 0xF8, 0x19, 0xE5, 0x87, 0xE2, 0x63, 0x3c, 0x4A, 0x9F, 0x42, 0x8a},
}

// submit runs one transaction to `to`: simulate, price against maxFee,
// sign and send (one at a time per key), then wait for the receipt.
func (d *Direct) submit(ctx context.Context, c Chain, chainID *big.Int, to evm.Address, data []byte) Result {
	call := map[string]string{"from": d.from.Hex(), "to": to.Hex(), "data": "0x" + hex.EncodeToString(data)}
	gas, res := quantity(c.Call(ctx, "eth_estimateGas", call))
	if res != nil {
		return *res
	}
	tip, base, res := d.fees(ctx, c)
	if res != nil {
		return *res
	}
	feeCap := new(big.Int).Add(new(big.Int).Mul(base, big.NewInt(2)), tip)
	gasLimit := gas.Uint64() + gas.Uint64()/5
	if worst := new(big.Int).Mul(feeCap, new(big.Int).SetUint64(gasLimit)); worst.Cmp(d.maxFee) > 0 {
		return Result{Outcome: Unavailable, Reason: "gas_above_max_fee"}
	}

	d.mu.Lock()
	nonce, res := quantity(c.Call(ctx, "eth_getTransactionCount", d.from.Hex(), "pending"))
	if res != nil {
		d.mu.Unlock()
		return *res
	}
	tx := &evm.DynamicFeeTx{ChainID: chainID, Nonce: nonce.Uint64(), GasTipCap: tip, GasFeeCap: feeCap, Gas: gasLimit, To: to, Data: data}
	raw, hash, err := tx.Sign(d.key)
	if err != nil {
		d.mu.Unlock()
		return Result{Outcome: Unavailable, Reason: "sign_failed"}
	}
	txHash := "0x" + hex.EncodeToString(hash[:])
	_, err = c.Call(ctx, "eth_sendRawTransaction", "0x"+hex.EncodeToString(raw))
	d.mu.Unlock()
	if err != nil {
		var rpcErr *chain.RPCError
		switch {
		case errors.As(err, &rpcErr) && strings.Contains(strings.ToLower(rpcErr.Message), "already known"):
			// The node has it (a repeat on another endpoint): wait for it.
		case errors.As(err, &rpcErr) && strings.Contains(strings.ToLower(rpcErr.Message), "insufficient funds"):
			return Result{Outcome: Unavailable, Reason: "gas_balance_low"}
		case errors.As(err, &rpcErr):
			return Result{Outcome: Unavailable, Reason: "send_rejected"}
		default:
			// Unknown whether it went out: report it pending under its hash,
			// so the settler asks the chain before trying anything else.
			return Result{Outcome: Pending, Transaction: txHash, Reason: errSettlementPending}
		}
	}
	return d.await(ctx, c, txHash)
}

// await polls for the receipt until confirm runs out.
func (d *Direct) await(ctx context.Context, c Chain, txHash string) Result {
	deadline := time.Now().Add(d.confirm)
	for {
		raw, err := c.Call(ctx, "eth_getTransactionReceipt", txHash)
		if err == nil && string(raw) != "null" && len(raw) > 0 {
			var rc struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(raw, &rc) == nil && rc.Status != "" {
				if rc.Status == "0x1" {
					return Result{Outcome: Settled, Transaction: txHash}
				}
				return Result{Outcome: Refused, Transaction: txHash, Reason: "transaction_reverted"}
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return Result{Outcome: Pending, Transaction: txHash, Reason: errSettlementPending}
		}
		select {
		case <-ctx.Done():
		case <-time.After(d.poll):
		}
	}
}

// chainID checks once per network that the RPC serves the chain the
// payment names: signing for the wrong chain would be useless at best.
func (d *Direct) chainID(ctx context.Context, c Chain, network string) (*big.Int, error) {
	if v, ok := d.chainIDs.Load(network); ok {
		return v.(*big.Int), nil
	}
	want, ok := new(big.Int).SetString(strings.TrimPrefix(network, "eip155:"), 10)
	if !ok || !strings.HasPrefix(network, "eip155:") {
		return nil, errors.New("not_evm")
	}
	got, res := quantity(c.Call(ctx, "eth_chainId"))
	if res != nil {
		return nil, errors.New(res.Reason)
	}
	if got.Cmp(want) != 0 {
		return nil, errors.New("rpc_wrong_chain")
	}
	d.chainIDs.Store(network, want)
	return want, nil
}

// fees reads the priority fee and the latest base fee.
func (d *Direct) fees(ctx context.Context, c Chain) (tip, base *big.Int, res *Result) {
	tip, res = quantity(c.Call(ctx, "eth_maxPriorityFeePerGas"))
	if res != nil {
		tip, res = big.NewInt(1_000_000), nil // 0.001 gwei when the node has no estimate
	}
	raw, err := c.Call(ctx, "eth_getBlockByNumber", "latest", false)
	if err != nil {
		return nil, nil, &Result{Outcome: Unavailable, Reason: "rpc_unavailable"}
	}
	var blk struct {
		BaseFee string `json:"baseFeePerGas"`
	}
	if json.Unmarshal(raw, &blk) != nil || blk.BaseFee == "" {
		return nil, nil, &Result{Outcome: Unavailable, Reason: "no_base_fee"}
	}
	base, ok := new(big.Int).SetString(strings.TrimPrefix(blk.BaseFee, "0x"), 16)
	if !ok {
		return nil, nil, &Result{Outcome: Unavailable, Reason: "no_base_fee"}
	}
	return tip, base, nil
}

// quantity decodes a hex quantity answer; a revert becomes a Refused result
// with the token's reason, any other failure Unavailable.
func quantity(raw json.RawMessage, err error) (*big.Int, *Result) {
	if err != nil {
		var rpcErr *chain.RPCError
		if errors.As(err, &rpcErr) {
			if reason, reverted := revertReason(rpcErr); reverted {
				return nil, &Result{Outcome: Refused, Reason: reason}
			}
			if strings.Contains(strings.ToLower(rpcErr.Message), "insufficient funds") {
				return nil, &Result{Outcome: Unavailable, Reason: "gas_balance_low"}
			}
		}
		return nil, &Result{Outcome: Unavailable, Reason: "rpc_unavailable"}
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return nil, &Result{Outcome: Unavailable, Reason: "rpc_malformed"}
	}
	v, ok := new(big.Int).SetString(strings.TrimPrefix(s, "0x"), 16)
	if !ok {
		return nil, &Result{Outcome: Unavailable, Reason: "rpc_malformed"}
	}
	return v, nil
}

// revertReason maps a reverted simulation to an x402-style reason. Only an
// emptied wallet and a bad signature are the payer's doing (see
// payerAttributable); used, expired or not-yet-valid authorizations and
// other reverts are not, so they never block a payer.
func revertReason(e *chain.RPCError) (string, bool) {
	text := strings.ToLower(e.Message + " " + decodeErrorString(e.Data))
	if !strings.Contains(text, "revert") && e.Code != 3 {
		return "", false
	}
	switch {
	case strings.Contains(text, "exceeds balance"):
		return x402.ErrInsufficientFunds, true
	case strings.Contains(text, "invalid signature"):
		return x402.ErrSignature, true
	case strings.Contains(text, "used or canceled"):
		return "authorization_used", true
	case strings.Contains(text, "expired"):
		return "authorization_expired", true
	case strings.Contains(text, "not yet valid"):
		return "authorization_not_yet_valid", true
	case strings.Contains(text, "blacklisted"):
		return "address_blacklisted", true
	}
	return "transaction_reverted", true
}

// decodeErrorString reads Error(string) revert data (0x08c379a0 ...).
func decodeErrorString(data string) string {
	b, err := hex.DecodeString(strings.TrimPrefix(data, "0x"))
	if err != nil || len(b) < 4+64 || hex.EncodeToString(b[:4]) != "08c379a0" {
		return ""
	}
	n := new(big.Int).SetBytes(b[4+32 : 4+64]).Int64()
	if n < 0 || 4+64+int(n) > len(b) {
		return ""
	}
	return string(b[4+64 : 4+64+int(n)])
}

// authorization reads the payer's signed authorization and signature,
// checked against the route's requirements.
func authorization(job *Job) (evm.Authorization, []byte, error) {
	var p x402.ExactEVMPayload
	if err := json.Unmarshal(job.Payload.Payload, &p); err != nil {
		return evm.Authorization{}, nil, err
	}
	a := p.Authorization
	from, err1 := evm.ParseAddress(a.From)
	to, err2 := evm.ParseAddress(a.To)
	value, err3 := evm.ParseUint256(a.Value)
	after, err4 := evm.ParseUint256(a.ValidAfter)
	before, err5 := evm.ParseUint256(a.ValidBefore)
	nonce, err6 := evm.ParseBytes32(a.Nonce)
	sig, err7 := evm.ParseHex(p.Signature, -1)
	if err := errors.Join(err1, err2, err3, err4, err5, err6, err7); err != nil {
		return evm.Authorization{}, nil, err
	}
	payTo, err := evm.ParseAddress(job.Requirements.PayTo)
	if err != nil || to != payTo || value.String() != job.Requirements.Amount {
		return evm.Authorization{}, nil, errors.New("authorization does not match the requirements")
	}
	if len(sig) == 0 || len(sig) > 4096 {
		return evm.Authorization{}, nil, errors.New("signature length")
	}
	return evm.Authorization{From: from, To: to, Value: value, ValidAfter: after, ValidBefore: before, Nonce: nonce}, sig, nil
}

// callData encodes transferWithAuthorization for the job (tests).
func callData(job *Job) ([]byte, error) {
	a, sig, err := authorization(job)
	if err != nil {
		return nil, err
	}
	return evm.TransferWithAuthorizationCall(a, sig), nil
}

// GasBalances reads the gas key's ETH balance (wei) on every network this
// backend settles on; networks that cannot be read are left out.
func (d *Direct) GasBalances(ctx context.Context) map[string]*big.Int {
	out := map[string]*big.Int{}
	for network, c := range d.chains {
		raw, err := c.Call(ctx, "eth_getBalance", d.from.Hex(), "latest")
		if err != nil {
			continue
		}
		var hexBal string
		if json.Unmarshal(raw, &hexBal) != nil {
			continue
		}
		if v, ok := new(big.Int).SetString(strings.TrimPrefix(hexBal, "0x"), 16); ok {
			out[network] = v
		}
	}
	return out
}
