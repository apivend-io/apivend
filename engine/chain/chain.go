// Package chain reads the token state the payer-state engine needs.
//
// The JSON-RPC client has run against public Base and Base Sepolia
// endpoints.
package chain

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"apivend.io/apivend/engine/evm"
)

// Reader is the read-only view of one chain.
type Reader interface {
	// BalanceOf returns owner's balance of the token and the block height the
	// answer is based on.
	BalanceOf(ctx context.Context, token, owner evm.Address) (balance *big.Int, block uint64, err error)
	// AuthorizationState reports whether an EIP-3009 nonce has been used or
	// cancelled on-chain.
	AuthorizationState(ctx context.Context, token, authorizer evm.Address, nonce [32]byte) (used bool, err error)
	// TransactionBlock returns the block a transaction was included in and
	// whether it succeeded. found is false while it is unknown or pending.
	TransactionBlock(ctx context.Context, txHash string) (block uint64, success, found bool, err error)
}

// RPC is a JSON-RPC Reader over one or more HTTP endpoints of the same chain.
// Calls go to the endpoint that last worked and fail over in order.
type RPC struct {
	endpoints []string
	client    *http.Client
	// warm shares client's connection pool with a timeout long enough to set
	// up a connection; see KeepWarm.
	warm      *http.Client
	preferred atomic.Int32
	id        atomic.Int64
}

// NewRPC returns a Reader for the given endpoints. perCall bounds each HTTP
// attempt; the caller's context bounds the whole operation.
func NewRPC(endpoints []string, perCall time.Duration) (*RPC, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("chain: no RPC endpoints")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// Reads sit on the request path: keep enough idle connections for
	// concurrent reads, and keep them longer than Go's 90 s default.
	tr.MaxIdleConnsPerHost = 32
	tr.IdleConnTimeout = 5 * time.Minute
	return &RPC{
		endpoints: endpoints,
		client:    &http.Client{Timeout: perCall, Transport: tr},
		warm:      &http.Client{Timeout: 5 * time.Second, Transport: tr},
	}, nil
}

// KeepWarm calls eth_blockNumber on every endpoint now and then every
// interval until ctx ends, so a connection to each stays open. Without it the
// first read after an idle spell pays DNS, TCP and TLS setup inside the
// caller's budget: 82 ms in-cluster to a public Base Sepolia endpoint against
// a 120 ms read budget, enough to refuse a payer with 503. Failures are
// ignored; the next tick tries again and reads still fail over.
func (r *RPC) KeepWarm(ctx context.Context, every time.Duration) {
	body := []byte(`[{"jsonrpc":"2.0","id":0,"method":"eth_blockNumber","params":[]}]`)
	ping := func() {
		for _, u := range r.endpoints {
			go func() {
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
				if err != nil {
					return
				}
				req.Header.Set("content-type", "application/json")
				resp, err := r.warm.Do(req)
				if err != nil {
					return
				}
				// Drain so the connection goes back to the pool.
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
				resp.Body.Close()
			}()
		}
	}
	ping()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ping()
		}
	}
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	ID     int64           `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	} `json:"error"`
}

// RPCError is a JSON-RPC error answer from a node (as opposed to a transport
// failure): a reverted call, a rejected transaction. Message and Data come
// from the node; Data is the revert data when there is any.
type RPCError struct {
	Method  string
	Code    int
	Message string
	Data    string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("%s: JSON-RPC error %d: %s", e.Method, e.Code, e.Message)
}

// Call makes one JSON-RPC call. Transport failures fail over to the next
// endpoint; an error answer from a node is returned as *RPCError without
// asking the others (it is an answer, e.g. a revert, not an outage).
// Writes such as eth_sendRawTransaction are safe to repeat on another
// endpoint: the same signed transaction has the same hash.
func (r *RPC) Call(ctx context.Context, method string, params ...any) (json.RawMessage, error) {
	if params == nil {
		params = []any{}
	}
	req := rpcRequest{JSONRPC: "2.0", ID: r.id.Add(1), Method: method, Params: params}
	body, err := json.Marshal([]rpcRequest{req})
	if err != nil {
		return nil, err
	}
	start := int(r.preferred.Load())
	var lastErr error
	for n := 0; n < len(r.endpoints); n++ {
		idx := (start + n) % len(r.endpoints)
		res, err := r.post(ctx, r.endpoints[idx], body, []rpcRequest{req})
		var rpcErr *RPCError
		if errors.As(err, &rpcErr) {
			r.preferred.Store(int32(idx))
			return nil, err
		}
		if err == nil {
			r.preferred.Store(int32(idx))
			return res[0], nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("chain: all RPC endpoints failed: %w", lastErr)
}

func (r *RPC) BalanceOf(ctx context.Context, token, owner evm.Address) (*big.Int, uint64, error) {
	// One batch, two answers. The two are not atomic: across a block boundary
	// the height can be off by one from the block the call ran at. Callers
	// use the height only as a watermark for ordering cache writes.
	res, err := r.batch(ctx,
		rpcRequest{Method: "eth_blockNumber", Params: []any{}},
		ethCall(token, evm.CallData("balanceOf(address)", evm.AddressWord(owner))),
	)
	if err != nil {
		return nil, 0, err
	}
	block, err := decodeQuantity(res[0])
	if err != nil {
		return nil, 0, fmt.Errorf("chain: eth_blockNumber: %w", err)
	}
	word, err := decodeWord(res[1])
	if err != nil {
		return nil, 0, fmt.Errorf("chain: balanceOf: %w", err)
	}
	return new(big.Int).SetBytes(word), block, nil
}

func (r *RPC) AuthorizationState(ctx context.Context, token, authorizer evm.Address, nonce [32]byte) (bool, error) {
	return r.AuthorizationStateAt(ctx, token, authorizer, nonce, "")
}

// AuthorizationStateAt is AuthorizationState as of one block, identified by
// hash; an empty hash means the latest block.
func (r *RPC) AuthorizationStateAt(ctx context.Context, token, authorizer evm.Address, nonce [32]byte, blockHash string) (bool, error) {
	req := ethCall(token, evm.CallData("authorizationState(address,bytes32)", evm.AddressWord(authorizer), nonce[:]))
	if blockHash != "" {
		req.Params[1] = map[string]any{"blockHash": blockHash, "requireCanonical": true}
	}
	res, err := r.batch(ctx, req)
	if err != nil {
		return false, err
	}
	word, err := decodeWord(res[0])
	if err != nil {
		return false, fmt.Errorf("chain: authorizationState: %w", err)
	}
	return new(big.Int).SetBytes(word).Sign() != 0, nil
}

func (r *RPC) TransactionBlock(ctx context.Context, txHash string) (uint64, bool, bool, error) {
	res, err := r.batch(ctx, rpcRequest{Method: "eth_getTransactionReceipt", Params: []any{txHash}})
	if err != nil {
		return 0, false, false, err
	}
	if string(res[0]) == "null" {
		return 0, false, false, nil
	}
	var rc struct {
		BlockNumber string `json:"blockNumber"`
		Status      string `json:"status"`
	}
	if err := json.Unmarshal(res[0], &rc); err != nil || rc.BlockNumber == "" {
		return 0, false, false, errors.New("chain: malformed transaction receipt")
	}
	block, err := strconv.ParseUint(strings.TrimPrefix(rc.BlockNumber, "0x"), 16, 64)
	if err != nil {
		return 0, false, false, errors.New("chain: malformed receipt block number")
	}
	return block, rc.Status == "0x1", true, nil
}

func ethCall(to evm.Address, data []byte) rpcRequest {
	return rpcRequest{Method: "eth_call", Params: []any{
		map[string]string{"to": to.Hex(), "data": "0x" + hex.EncodeToString(data)}, "latest",
	}}
}

// batch sends the requests as one JSON-RPC batch and returns the results in
// request order, trying each endpoint until one answers every request.
func (r *RPC) batch(ctx context.Context, reqs ...rpcRequest) ([]json.RawMessage, error) {
	for i := range reqs {
		reqs[i].JSONRPC = "2.0"
		reqs[i].ID = r.id.Add(1)
	}
	body, err := json.Marshal(reqs)
	if err != nil {
		return nil, err
	}
	start := int(r.preferred.Load())
	var lastErr error
	for n := 0; n < len(r.endpoints); n++ {
		idx := (start + n) % len(r.endpoints)
		res, err := r.post(ctx, r.endpoints[idx], body, reqs)
		if err == nil {
			r.preferred.Store(int32(idx))
			return res, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("chain: all RPC endpoints failed: %w", lastErr)
}

func (r *RPC) post(ctx context.Context, url string, body []byte, reqs []rpcRequest) ([]json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		// Do not include the URL: provider URLs usually embed an API key.
		return nil, errors.New("request failed")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, errors.New("read failed")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var out []rpcResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, errors.New("malformed JSON-RPC response")
	}
	byID := make(map[int64]rpcResponse, len(out))
	for _, o := range out {
		byID[o.ID] = o
	}
	results := make([]json.RawMessage, len(reqs))
	for i, q := range reqs {
		o, ok := byID[q.ID]
		if !ok {
			return nil, errors.New("JSON-RPC response missing an answer")
		}
		if o.Error != nil {
			data := ""
			if len(o.Error.Data) > 0 {
				json.Unmarshal(o.Error.Data, &data) // a hex string, when it is one
			}
			return nil, &RPCError{Method: q.Method, Code: o.Error.Code, Message: o.Error.Message, Data: data}
		}
		results[i] = o.Result
	}
	return results, nil
}

func decodeQuantity(raw json.RawMessage) (uint64, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil || !strings.HasPrefix(s, "0x") {
		return 0, errors.New("not a quantity")
	}
	return strconv.ParseUint(s[2:], 16, 64)
}

func decodeWord(raw json.RawMessage) ([]byte, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, errors.New("not a hex string")
	}
	// An empty result means the address has no code: wrong token or chain.
	b, err := evm.ParseHex(s, 32)
	if err != nil {
		return nil, errors.New("result is not one 32-byte word")
	}
	return b, nil
}

// AuthEvent is an EIP-3009 AuthorizationUsed or AuthorizationCanceled log.
type AuthEvent struct {
	Authorizer evm.Address
	Nonce      [32]byte
	Canceled   bool
	Block      uint64
	BlockHash  string
	// TxHash and LogIndex locate the log, so the transfer that used the
	// authorization (the token's next log) can be read: TransferAfter.
	TxHash   string
	LogIndex uint64
}

// TransferLog is an ERC-20 Transfer event.
type TransferLog struct {
	Token    evm.Address
	From, To evm.Address
	Value    *big.Int
}

var topicTransfer = evm.Keccak256([]byte("Transfer(address,address,uint256)"))

var (
	topicAuthUsed     = evm.Keccak256([]byte("AuthorizationUsed(address,bytes32)"))
	topicAuthCanceled = evm.Keccak256([]byte("AuthorizationCanceled(address,bytes32)"))
)

// TransferAfter returns the Transfer event the token logged right after
// the log at logIndex in txHash: in USDC's transferWithAuthorization, the
// transfer that used the authorization. found is false when that log is
// not a Transfer of token.
func (r *RPC) TransferAfter(ctx context.Context, txHash string, token evm.Address, logIndex uint64) (TransferLog, bool, error) {
	res, err := r.batch(ctx, rpcRequest{Method: "eth_getTransactionReceipt", Params: []any{txHash}})
	if err != nil {
		return TransferLog{}, false, err
	}
	var rc struct {
		Logs []struct {
			Address  string   `json:"address"`
			Topics   []string `json:"topics"`
			Data     string   `json:"data"`
			LogIndex string   `json:"logIndex"`
		} `json:"logs"`
	}
	if string(res[0]) == "null" || json.Unmarshal(res[0], &rc) != nil {
		return TransferLog{}, false, errors.New("chain: malformed transaction receipt")
	}
	for _, l := range rc.Logs {
		li, err := strconv.ParseUint(strings.TrimPrefix(l.LogIndex, "0x"), 16, 64)
		if err != nil || li != logIndex+1 {
			continue
		}
		a, err := evm.ParseAddress(l.Address)
		if err != nil || a != token || len(l.Topics) != 3 {
			return TransferLog{}, false, nil
		}
		t0, err0 := evm.ParseBytes32(l.Topics[0])
		from, err1 := evm.ParseBytes32(l.Topics[1])
		to, err2 := evm.ParseBytes32(l.Topics[2])
		data, err3 := hex.DecodeString(strings.TrimPrefix(l.Data, "0x"))
		if err0 != nil || err1 != nil || err2 != nil || err3 != nil || t0 != topicTransfer || len(data) != 32 {
			return TransferLog{}, false, nil
		}
		out := TransferLog{Token: token, Value: new(big.Int).SetBytes(data)}
		copy(out.From[:], from[12:])
		copy(out.To[:], to[12:])
		return out, true, nil
	}
	return TransferLog{}, false, nil
}

// Head returns the latest block's number and hash.
func (r *RPC) Head(ctx context.Context) (uint64, string, error) {
	res, err := r.batch(ctx, rpcRequest{Method: "eth_getBlockByNumber", Params: []any{"latest", false}})
	if err != nil {
		return 0, "", err
	}
	var h struct{ Number, Hash string }
	if err := json.Unmarshal(res[0], &h); err != nil || !strings.HasPrefix(h.Number, "0x") || h.Hash == "" {
		return 0, "", errors.New("chain: malformed block header")
	}
	n, err := strconv.ParseUint(h.Number[2:], 16, 64)
	if err != nil {
		return 0, "", errors.New("chain: malformed block number")
	}
	return n, h.Hash, nil
}

// BalancesAt returns each owner's token balance as of one block, identified
// by hash (so every answer comes from the same, fully processed block) or,
// when blockHash is empty, by number.
func (r *RPC) BalancesAt(ctx context.Context, token evm.Address, owners []evm.Address, block uint64, blockHash string) ([]*big.Int, error) {
	if len(owners) == 0 {
		return nil, nil
	}
	var ref any = "0x" + strconv.FormatUint(block, 16)
	if blockHash != "" {
		ref = map[string]any{"blockHash": blockHash, "requireCanonical": true} // EIP-1898
	}
	reqs := make([]rpcRequest, len(owners))
	for i, o := range owners {
		reqs[i] = ethCall(token, evm.CallData("balanceOf(address)", evm.AddressWord(o)))
		reqs[i].Params[1] = ref
	}
	res, err := r.batch(ctx, reqs...)
	if err != nil {
		return nil, err
	}
	out := make([]*big.Int, len(owners))
	for i := range res {
		word, err := decodeWord(res[i])
		if err != nil {
			return nil, fmt.Errorf("chain: balanceOf: %w", err)
		}
		out[i] = new(big.Int).SetBytes(word)
	}
	return out, nil
}

// AuthEvents returns the authorization events of the given authorizers on
// token in blocks [from, to].
func (r *RPC) AuthEvents(ctx context.Context, token evm.Address, authorizers []evm.Address, from, to uint64) ([]AuthEvent, error) {
	return r.authEvents(ctx, token, authorizers, nil, from, to)
}

// NonceEvents returns the events of one authorizer's nonce in [from, to]
// (at most one Used or Canceled exists on chain).
func (r *RPC) NonceEvents(ctx context.Context, token, authorizer evm.Address, nonce [32]byte, from, to uint64) ([]AuthEvent, error) {
	return r.authEvents(ctx, token, []evm.Address{authorizer}, &nonce, from, to)
}

func (r *RPC) authEvents(ctx context.Context, token evm.Address, authorizers []evm.Address, nonce *[32]byte, from, to uint64) ([]AuthEvent, error) {
	if len(authorizers) == 0 || from > to {
		return nil, nil
	}
	who := make([]string, len(authorizers))
	for i, a := range authorizers {
		who[i] = "0x" + hex.EncodeToString(evm.AddressWord(a))
	}
	filter := map[string]any{
		"address":   token.Hex(),
		"fromBlock": "0x" + strconv.FormatUint(from, 16),
		"toBlock":   "0x" + strconv.FormatUint(to, 16),
		"topics": []any{
			[]string{"0x" + hex.EncodeToString(topicAuthUsed[:]), "0x" + hex.EncodeToString(topicAuthCanceled[:])},
			who,
		},
	}
	if nonce != nil {
		filter["topics"] = append(filter["topics"].([]any), "0x"+hex.EncodeToString(nonce[:]))
	}
	res, err := r.batch(ctx, rpcRequest{Method: "eth_getLogs", Params: []any{filter}})
	if err != nil {
		return nil, err
	}
	var logs []struct {
		Topics      []string `json:"topics"`
		BlockNumber string   `json:"blockNumber"`
		BlockHash   string   `json:"blockHash"`
		TxHash      string   `json:"transactionHash"`
		LogIndex    string   `json:"logIndex"`
	}
	if err := json.Unmarshal(res[0], &logs); err != nil {
		return nil, errors.New("chain: malformed logs")
	}
	out := make([]AuthEvent, 0, len(logs))
	for _, l := range logs {
		if len(l.Topics) != 3 {
			continue
		}
		t0, err0 := evm.ParseBytes32(l.Topics[0])
		who, err1 := evm.ParseBytes32(l.Topics[1])
		nonce, err2 := evm.ParseBytes32(l.Topics[2])
		block, err3 := strconv.ParseUint(strings.TrimPrefix(l.BlockNumber, "0x"), 16, 64)
		if err0 != nil || err1 != nil || err2 != nil || err3 != nil {
			return nil, errors.New("chain: malformed log entry")
		}
		li, _ := strconv.ParseUint(strings.TrimPrefix(l.LogIndex, "0x"), 16, 64)
		ev := AuthEvent{Nonce: nonce, Canceled: t0 == topicAuthCanceled, Block: block, BlockHash: l.BlockHash, TxHash: l.TxHash, LogIndex: li}
		copy(ev.Authorizer[:], who[12:])
		out = append(out, ev)
	}
	return out, nil
}
