// mockfacilitator is a stand-in x402 facilitator for local runs and the
// end-to-end test. It accepts every /settle request (except to -refuse-payto)
// and invents a transaction hash. It moves no money and checks nothing.
// Batch-settlement deposits, claims and refunds are reported to the fake
// node's channel ledger (-node), so the gateway sees them on "chain".
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"math/big"
	"net/http"
	"strings"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/internal/x402"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8402", "listen address")
	refuse := flag.String("refuse-payto", "", "refuse settlements to this payTo address with insufficient_funds")
	node := flag.String("node", "", "fake node URL whose batch channel ledger to update")
	flag.Parse()
	http.HandleFunc("/settle", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			PaymentPayload struct {
				Payload json.RawMessage `json:"payload"`
			} `json:"paymentPayload"`
			PaymentRequirements struct {
				Scheme  string `json:"scheme"`
				Network string `json:"network"`
				PayTo   string `json:"payTo"`
			} `json:"paymentRequirements"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if *refuse != "" && strings.EqualFold(req.PaymentRequirements.PayTo, *refuse) {
			w.WriteHeader(http.StatusPaymentRequired)
			json.NewEncoder(w).Encode(map[string]any{
				"success": false, "errorReason": "insufficient_funds", "transaction": "", "network": req.PaymentRequirements.Network,
			})
			log.Printf("refused (pretend) on %s", req.PaymentRequirements.Network)
			return
		}
		if req.PaymentRequirements.Scheme == x402.SchemeBatch && *node != "" {
			ledger(*node, req.PaymentPayload.Payload)
		}
		tx := make([]byte, 32)
		rand.Read(tx)
		json.NewEncoder(w).Encode(map[string]any{
			"success": true, "transaction": "0x" + hex.EncodeToString(tx), "network": req.PaymentRequirements.Network,
		})
		log.Printf("settled (pretend) on %s", req.PaymentRequirements.Network)
	})
	log.Fatal(http.ListenAndServe(*listen, nil))
}

// ledger reports a batch payload's effect to the fake node.
func ledger(node string, raw json.RawMessage) {
	var p struct {
		Type    string                  `json:"type"`
		Voucher x402.BatchVoucher       `json:"voucher"`
		Amount  string                  `json:"amount"`
		Deposit struct{ Amount string } `json:"deposit"`
		Claims  []struct {
			Voucher struct {
				Channel x402.BatchChannelConfig `json:"channel"`
			} `json:"voucher"`
			TotalClaimed string `json:"totalClaimed"`
		} `json:"claims"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return
	}
	post := func(u map[string]string) {
		b, _ := json.Marshal(u)
		if resp, err := http.Post(node+"/_channel", "application/json", bytes.NewReader(b)); err == nil {
			resp.Body.Close()
		}
	}
	for _, c := range p.Claims {
		post(map[string]string{"id": channelID(c.Voucher.Channel), "claimed": c.TotalClaimed})
	}
	switch p.Type {
	case "deposit":
		post(map[string]string{"id": p.Voucher.ChannelID, "deposit": p.Deposit.Amount})
	case "refund":
		post(map[string]string{"id": p.Voucher.ChannelID, "refund": p.Amount})
	}
}

// channelID hashes a channel config on Base Sepolia (the end-to-end chain).
func channelID(cc x402.BatchChannelConfig) string {
	cfg := evm.ChannelConfig{WithdrawDelay: cc.WithdrawDelay}
	cfg.Payer, _ = evm.ParseAddress(cc.Payer)
	cfg.PayerAuthorizer, _ = evm.ParseAddress(cc.PayerAuthorizer)
	cfg.Receiver, _ = evm.ParseAddress(cc.Receiver)
	cfg.ReceiverAuthorizer, _ = evm.ParseAddress(cc.ReceiverAuthorizer)
	cfg.Token, _ = evm.ParseAddress(cc.Token)
	cfg.Salt, _ = evm.ParseBytes32(cc.Salt)
	id := cfg.ChannelID(big.NewInt(84532))
	return "0x" + hex.EncodeToString(id[:])
}
