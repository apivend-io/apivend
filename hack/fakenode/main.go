// fakenode is a stand-in Ethereum JSON-RPC node for local runs and the
// end-to-end test. Every address has the same fixed token balance, no
// authorization is ever used, and a new block appears every two seconds.
// It moves no money. For batch settlement it keeps a channel ledger that
// the mock facilitator updates (POST /_channel) and answers the settlement
// contract's views from it.
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"apivend.io/apivend/engine/evm"
)

// channel is the ledger entry of one batch-settlement channel.
type channel struct{ balance, claimed, nonce *big.Int }

var (
	mu       sync.Mutex
	channels = map[string]*channel{}
	sel      = func(sig string) string { h := evm.Keccak256([]byte(sig)); return "0x" + hex.EncodeToString(h[:4]) }
	selChan  = sel("channels(bytes32)")
	selWd    = sel("pendingWithdrawals(bytes32)")
	selNonce = sel("refundNonce(bytes32)")
)

func get(id string) *channel {
	id = strings.ToLower(id)
	c := channels[id]
	if c == nil {
		c = &channel{new(big.Int), new(big.Int), new(big.Int)}
		channels[id] = c
	}
	return c
}

func word(v *big.Int) string { return fmt.Sprintf("%064x", v) }

func main() {
	listen := flag.String("listen", "127.0.0.1:8545", "listen address")
	balance := flag.Uint64("balance", 1_000_000, "token balance of every address, in atomic units")
	flag.Parse()

	head := func() (string, string) {
		n := uint64(time.Now().Unix() / 2)
		return fmt.Sprintf("0x%x", n), fmt.Sprintf("0x%064x", n)
	}
	answer := func(method string, params []json.RawMessage) any {
		switch method {
		case "eth_blockNumber":
			n, _ := head()
			return n
		case "eth_getBlockByNumber":
			n, h := head()
			return map[string]string{"number": n, "hash": h}
		case "eth_chainId":
			return "0x14a34" // Base Sepolia
		case "eth_call":
			var call struct{ To, Data, Input string }
			if len(params) > 0 {
				json.Unmarshal(params[0], &call)
			}
			if call.Data == "" {
				call.Data = call.Input // go-ethereum sends "input"
			}
			if strings.EqualFold(call.To, evm.BatchSettlement.Hex()) && len(call.Data) >= 74 {
				mu.Lock()
				defer mu.Unlock()
				c := get("0x" + call.Data[10:74])
				switch call.Data[:10] {
				case selChan:
					return "0x" + word(c.balance) + word(c.claimed)
				case selWd:
					return "0x" + word(new(big.Int)) + word(new(big.Int))
				case selNonce:
					return "0x" + word(c.nonce)
				}
			}
			if strings.HasPrefix(call.Data, "0x70a08231") { // balanceOf(address)
				return fmt.Sprintf("0x%064x", *balance)
			}
			return fmt.Sprintf("0x%064x", 0) // authorizationState: unused
		case "eth_getLogs":
			return []any{}
		default:
			return nil
		}
	}
	// The mock facilitator's ledger updates: a deposit adds to the balance,
	// a claim sets the claimed total, a refund takes from the balance and
	// moves the refund nonce on.
	http.HandleFunc("/_channel", func(w http.ResponseWriter, r *http.Request) {
		var u struct{ ID, Deposit, Claimed, Refund string }
		if json.NewDecoder(r.Body).Decode(&u) != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		num := func(s string) *big.Int { n, _ := new(big.Int).SetString(s, 10); return n }
		mu.Lock()
		defer mu.Unlock()
		c := get(u.ID)
		if n := num(u.Deposit); n != nil {
			c.balance.Add(c.balance, n)
		}
		if n := num(u.Claimed); n != nil && n.Cmp(c.claimed) > 0 {
			c.claimed.Set(n)
		}
		if n := num(u.Refund); n != nil {
			c.balance.Sub(c.balance, n)
			c.nonce.Add(c.nonce, big.NewInt(1))
		}
		log.Printf("channel %s: balance %s claimed %s nonce %s", u.ID, c.balance, c.claimed, c.nonce)
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		type req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		type resp struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Result  any             `json:"result"`
		}
		var batch []req
		if json.Unmarshal(raw, &batch) != nil {
			var one req
			if json.Unmarshal(raw, &one) != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			json.NewEncoder(w).Encode(resp{"2.0", one.ID, answer(one.Method, one.Params)})
			return
		}
		out := make([]resp, len(batch))
		for i, q := range batch {
			out[i] = resp{"2.0", q.ID, answer(q.Method, q.Params)}
		}
		json.NewEncoder(w).Encode(out)
	})
	log.Fatal(http.ListenAndServe(*listen, nil))
}
