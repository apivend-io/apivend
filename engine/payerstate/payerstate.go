// Package payerstate answers "can this payer cover this amount right now, and
// is this authorization unused?" and holds funds while a request is in flight.
//
// Redis is the real implementation (shared across replicas, backed by chain
// reads). Memory is a single-process stand-in for development: it enforces
// nonce uniqueness only.
package payerstate

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"
)

// Reasons a reservation can be denied.
const (
	ReasonNonceUsed           = "payment_already_used"
	ReasonInsufficientBalance = "insufficient_balance"
	ReasonExposureLimit       = "exposure_limit"
	ReasonPayerBlocked        = "payer_blocked"
)

// DeniedError is returned by Reserve when the payment must not be served.
type DeniedError struct{ Reason string }

func (e *DeniedError) Error() string { return "payer state denied: " + e.Reason }

// ErrUnavailable means the state needed to decide could not be reached (store
// down, or chain unreadable for a payer with no fresh balance).
var ErrUnavailable = errors.New("payer state unavailable")

// ErrExpired is returned by Commit when the hold lapsed before the backend
// answered: the authorization is past its validBefore and cannot be settled.
var ErrExpired = errors.New("reservation expired before commit")

// ReservationID identifies a hold. It encodes chain, asset, payer and nonce,
// so it is also unique per authorization.
type ReservationID string

func makeID(chain, asset, payer, nonce string) ReservationID {
	return ReservationID(chain + "|" + asset + "|" + payer + "|" + nonce)
}

// Parts splits a ReservationID into its components.
func (id ReservationID) Parts() (chain, asset, payer, nonce string, err error) {
	p := strings.Split(string(id), "|")
	if len(p) != 4 {
		return "", "", "", "", fmt.Errorf("malformed reservation id")
	}
	return p[0], p[1], p[2], p[3], nil
}

// Policy is the per-route risk configuration.
type Policy struct {
	// MaxUnsettled caps the payer's in-flight plus not-yet-settled amount on
	// this asset, in atomic units. Nil means no cap.
	MaxUnsettled *big.Int
	// Freshness is how old a cached balance may be before it is re-read.
	Freshness time.Duration
	// AllowOnUnavailable serves the request without a balance check when the
	// chain cannot be read. Nonce and exposure checks still apply.
	AllowOnUnavailable bool
}

// Request describes the hold to place.
type Request struct {
	Chain  string // CAIP-2 network
	Asset  string // token contract, lowercase hex
	Payer  string // lowercase hex
	Nonce  string // lowercase hex
	Amount *big.Int
	// Expiry is the authorization's validBefore. The hold and the nonce are
	// remembered until then; afterwards the chain rejects the payment anyway.
	Expiry time.Time
	Policy Policy
	// QueueOnly: work that moves none of the payer's wallet funds (a batch
	// refund out of escrow). Amount may be zero and the balance is not
	// checked; the nonce and the queue apply as usual.
	QueueOnly bool
}

// Job is what Commit hands to settlement.
type Job struct {
	// Body is opaque to the store (see package settle).
	Body []byte
	// Deadline orders the settlement queue: the authorization's validBefore.
	Deadline time.Time
	// Route attributes the unsettled amount for the exposure metric.
	Route string
}

// PayerState is the request-path interface.
type PayerState interface {
	// Reserve atomically marks the nonce used and holds the funds. It returns
	// *DeniedError when the payment must be refused and an error wrapping
	// ErrUnavailable when no decision could be made.
	Reserve(ctx context.Context, req Request) (ReservationID, error)
	// Release frees the hold: the backend failed and nobody is charged.
	Release(ctx context.Context, id ReservationID) error
	// Commit turns the hold into unsettled exposure and enqueues the job,
	// atomically: the backend succeeded.
	Commit(ctx context.Context, id ReservationID, job Job) error
}

// Memory is an in-process PayerState. One replica only, no balance checks, no
// exposure cap, and committed jobs are dropped (nothing settles them).
type Memory struct {
	mu     sync.Mutex
	held   map[ReservationID]time.Time
	now    func() time.Time
	sweeps int
}

func NewMemory() *Memory {
	return &Memory{held: map[ReservationID]time.Time{}, now: time.Now}
}

func (m *Memory) Reserve(_ context.Context, req Request) (ReservationID, error) {
	if req.Amount == nil || req.Amount.Sign() < 0 || (req.Amount.Sign() == 0 && !req.QueueOnly) {
		return "", errors.New("payerstate: amount must be positive")
	}
	id := makeID(req.Chain, req.Asset, req.Payer, req.Nonce)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweep()
	if exp, used := m.held[id]; used && m.now().Before(exp) {
		return "", &DeniedError{Reason: ReasonNonceUsed}
	}
	m.held[id] = req.Expiry
	return id, nil
}

func (m *Memory) Release(_ context.Context, id ReservationID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.held, id)
	return nil
}

func (m *Memory) Commit(_ context.Context, id ReservationID, _ Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.held[id]; !ok {
		return ErrExpired
	}
	return nil
}

// sweep drops expired nonces every so often. Callers hold m.mu.
func (m *Memory) sweep() {
	m.sweeps++
	if m.sweeps%1024 != 0 {
		return
	}
	now := m.now()
	for id, exp := range m.held {
		if now.After(exp) {
			delete(m.held, id)
		}
	}
}
