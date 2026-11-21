package demo

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	"apivend.io/apivend/engine/evm"
)

// Backend is the demo's own priced service. It is deliberately small:
//
//	GET /v1/balance?network=base-sepolia&address=0x...
//	    USDC balance of any wallet, with the block it was read at. Answered
//	    by the hosted payer-state service, so paying for this route exercises
//	    the whole product.
//	GET /v1/hello
//	    A fixed answer, for the cheapest possible first payment.
//
// /v1/balance is the demo's sample paid route.
type Backend struct {
	// StateURL is the hosted service's HTTP base URL; StateKey its API key.
	StateURL string
	StateKey string
	Client   *http.Client
}

// usdc lists the token the balance route reports, per network.
var usdc = map[string][2]string{
	"base":         {"eip155:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"},
	"base-sepolia": {"eip155:84532", "0x036cbd53842c5426634e7929541ec2318f3dcf7e"},
}

func (b *Backend) Handler() http.Handler {
	if b.Client == nil {
		b.Client = http.DefaultClient
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/hello", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"message": "hello", "paidBy": r.Header.Get("x-x402-payer")})
	})
	mux.HandleFunc("GET /v1/balance", func(w http.ResponseWriter, r *http.Request) {
		net := r.URL.Query().Get("network")
		if net == "" {
			net = "base-sepolia"
		}
		token, ok := usdc[net]
		addr, err := evm.ParseAddress(r.URL.Query().Get("address"))
		if !ok || err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "want ?network=base|base-sepolia&address=0x..."})
			return
		}
		q := url.Values{"chain": {token[0]}, "asset": {token[1]}, "payer": {addr.Hex()}}
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, b.StateURL+"/v1/snapshot?"+q.Encode(), nil)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}
		req.Header.Set("Authorization", "Bearer "+b.StateKey)
		resp, err := b.Client.Do(req)
		if err != nil {
			// A 5xx means the gateway releases the payment: nobody is charged
			// for an answer we could not give.
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "balance source unavailable"})
			return
		}
		defer resp.Body.Close()
		var snap struct {
			Balance     string `json:"balance"`
			Observation struct {
				BlockHeight string `json:"blockHeight"`
				BlockHash   string `json:"blockHash"`
				ObservedAt  string `json:"observedAt"`
			} `json:"observation"`
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		if resp.StatusCode != http.StatusOK || json.Unmarshal(raw, &snap) != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "balance source unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"address": addr.String(), "network": net, "token": "USDC", "balanceAtomic": snap.Balance,
			"blockHeight": snap.Observation.BlockHeight, "blockHash": snap.Observation.BlockHash, "observedAt": snap.Observation.ObservedAt,
		})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
