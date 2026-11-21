// Package receipts records what happened to each served request.
//
// Receipts are append-only: every status a request reaches is one row, and a
// row is never updated. The latest row per reservation is its current state.
package receipts

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Statuses recorded by the settler. The earlier lifecycle states from the
// design doc (challenged, verified, reserved, released) happen in the filter
// and are not persisted yet.
const (
	StatusServed  = "served"  // backend answered 2xx; queued for settlement
	StatusSettled = "settled" // the transfer landed
	StatusUnpaid  = "unpaid"  // settlement failed for good
)

// Receipt is one row.
type Receipt struct {
	ReservationID string // unique per authorization
	RequestID     string // the gateway's request ID
	Route         string
	Payer         string
	Amount        string // atomic units
	Asset         string
	Chain         string
	Nonce         string
	VerifiedAt    time.Time
	ServedAt      time.Time
	Status        string
	Provider      string // backend that settled, when settled
	TxHash        string
	Block         uint64
	Reason        string // why unpaid, or how settlement was established
	// StateSource and StateBlock: the chain-state source and block the
	// balance check rested on, for disputes about a wrong "sufficient" answer.
	StateSource string
	StateBlock  uint64
}

// Store persists receipts. Record must be idempotent per (ReservationID,
// Status): the settler may repeat a write after a crash.
type Store interface {
	Record(ctx context.Context, r Receipt) error
}

// Log writes receipts to the process log. It is the fallback when no database
// is configured; nothing can be reconciled from it.
type Log struct{ Logger *slog.Logger }

func (l Log) Record(_ context.Context, r Receipt) error {
	l.Logger.Info("receipt", "status", r.Status, "reservation", r.ReservationID, "request_id", r.RequestID,
		"route", r.Route, "payer", r.Payer, "amount", r.Amount, "asset", r.Asset, "chain", r.Chain,
		"provider", r.Provider, "tx", r.TxHash, "block", r.Block, "reason", r.Reason,
		"state_source", r.StateSource, "state_block", r.StateBlock)
	return nil
}

// Memory keeps receipts in process, for tests.
type Memory struct {
	mu   sync.Mutex
	rows []Receipt
	seen map[string]bool
}

func (m *Memory) Record(_ context.Context, r Receipt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = map[string]bool{}
	}
	if k := r.ReservationID + "/" + r.Status; !m.seen[k] {
		m.seen[k] = true
		m.rows = append(m.rows, r)
	}
	return nil
}

// Rows returns a copy of everything recorded.
func (m *Memory) Rows() []Receipt {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Receipt(nil), m.rows...)
}

// Final returns the last status recorded for a reservation, or "".
func (m *Memory) Final(reservationID string) Receipt {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out Receipt
	for _, r := range m.rows {
		if r.ReservationID == reservationID {
			out = r
		}
	}
	return out
}
