// Package statesource supplies the chain facts the payer-state engine decides
// on: balances and authorization events. Reservations are not its concern;
// they stay in the gateway's own Redis.
//
// Two implementations: RPC (direct JSON-RPC, the open-source default) and
// Hosted (client for the hosted payer-state service). The engine is given an
// ordered list and uses the first healthy one.
package statesource

import (
	"context"
	"errors"
	"math/big"
	"time"
)

// ErrUnsupported means the source does not serve this chain.
var ErrUnsupported = errors.New("statesource: chain not served by this source")

// AuthState is what is known about an EIP-3009 nonce.
type AuthState int

const (
	AuthUnused AuthState = iota
	// AuthUsed also covers "cancelled" when the source can only ask the
	// contract, which does not distinguish the two.
	AuthUsed
	AuthCanceled
)

// PayerSnapshot is the latest known state of one payer on one asset.
type PayerSnapshot struct {
	Balance     *big.Int
	BlockHeight uint64
	BlockHash   string
	ObservedAt  time.Time
}

type UpdateKind int

const (
	UpdateBalance UpdateKind = iota
	UpdateAuthUsed
	UpdateAuthCanceled
)

// StateUpdate is one change streamed by Watch.
type StateUpdate struct {
	Kind        UpdateKind
	Chain       string
	Asset       string
	Payer       string
	Nonce       string   // for auth events
	Balance     *big.Int // for balance events
	BlockHeight uint64
	BlockHash   string
	ObservedAt  time.Time
}

// Subscription says what to watch. A gateway watches only payers it has seen:
// it sends each on Payers, and sends it again while the payer stays active.
// A source may stop watching a payer it has not been reminded of for a while.
type Subscription struct {
	Chain  string // CAIP-2
	Asset  string // token contract, lowercase hex
	Payers <-chan string
}

// Source supplies chain facts.
type Source interface {
	// Name identifies the source in logs, metrics and receipts.
	Name() string
	// Snapshot returns the latest known state for one payer.
	Snapshot(ctx context.Context, chain, asset, payer string) (PayerSnapshot, error)
	// Watch streams updates for the subscription's payers until ctx ends.
	// The channel is closed when the stream fails; the caller re-subscribes,
	// possibly to another source.
	Watch(ctx context.Context, sub Subscription) (<-chan StateUpdate, error)
	// AuthorizationState reports whether a nonce is unused, used or cancelled.
	AuthorizationState(ctx context.Context, chain, asset, payer, nonce string) (AuthState, error)
}
