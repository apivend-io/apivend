// Package settle turns committed reservations into on-chain transfers, off
// the request path.
package settle

import (
	"time"

	"apivend.io/apivend/internal/x402"
)

// Job is the settlement work for one served request. The filter writes it at
// commit; the settler reads it. It carries everything needed to settle and to
// write the receipt, so the settler never has to consult the price table.
type Job struct {
	RequestID  string    `json:"requestId"`
	Route      string    `json:"route"`
	Provider   string    `json:"provider,omitempty"`
	Payer      string    `json:"payer"`
	Amount     string    `json:"amount"` // atomic units
	VerifiedAt time.Time `json:"verifiedAt"`
	ServedAt   time.Time `json:"servedAt"`
	// StateSource and StateBlock record which chain-state source, and which
	// block, the balance check behind this payment rested on. Empty when no
	// balance check was made.
	StateSource string `json:"stateSource,omitempty"`
	StateBlock  uint64 `json:"stateBlock,omitempty"`
	// Payload is the client's signed payment; Requirements are the route's
	// own terms it was verified against. Together they are a facilitator
	// /settle request.
	Payload      x402.PaymentPayload `json:"payload"`
	Requirements x402.Requirements   `json:"requirements"`
	// Kind is empty for a payment; KindBatchDeposit for a batch-settlement
	// deposit (money into the payer's channel escrow, not revenue).
	Kind         string `json:"kind,omitempty"`
	RefundAmount string `json:"refundAmount,omitempty"`
}

// KindBatchDeposit marks a deposit job; its receipts are deposit_served,
// deposit_settled and deposit_unpaid, so revenue queries (status settled)
// never count escrow.
const KindBatchDeposit = "batch-deposit"

// Claim-job work, never queued: a claim of many channels (claimWithSignature)
// and a payout of claimed funds to the seller (settle(receiver, token)).
const (
	KindBatchClaim  = "batch-claim"
	KindBatchPayout = "batch-payout"
)

// KindBatchRefund is a cooperative refund the filter queued: the settler
// completes it (claims and our signatures) before a backend submits it.
// RefundAmount is what the filter resolved (all that is left, or the
// payer's partial amount). Its receipts are refund_*.
const KindBatchRefund = "batch-refund"

// StatusBatchClaimed is the receipt status of a channel's claimed charges:
// revenue moved from the payer's escrow to the seller's side of the
// contract (paid out by the next settle).
const StatusBatchClaimed = "batch_claimed"

// StatusBatchCharged is the receipt of one request paid by voucher.
const StatusBatchCharged = "batch_charged"

// receiptStatus is the status a job's receipt carries.
func receiptStatus(job *Job, status string) string {
	switch job.Kind {
	case KindBatchDeposit:
		return "deposit_" + status
	case KindBatchRefund:
		return "refund_" + status
	}
	return status
}
