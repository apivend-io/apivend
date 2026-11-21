package settle

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/receipts"
	"apivend.io/apivend/internal/x402"
)

// Queue is the part of the payer-state store the settler uses.
type Queue interface {
	Claim(ctx context.Context, lease time.Duration) (*payerstate.Claimed, error)
	Retry(ctx context.Context, id payerstate.ReservationID, delay time.Duration) error
	Settled(ctx context.Context, id payerstate.ReservationID, txBlock uint64) error
	Failed(ctx context.Context, id payerstate.ReservationID, reason string, blockFor time.Duration) error
	SetPendingTx(ctx context.Context, id payerstate.ReservationID, txHash string) error
	PendingTx(ctx context.Context, id payerstate.ReservationID) (string, error)
	PublishResult(ctx context.Context, id payerstate.ReservationID, res payerstate.Result) error
}

// Observer is told about settlement outcomes (metrics).
type Observer interface {
	SettleAttempt(provider string, outcome string, d time.Duration)
	Finished(route, network, asset, status, reason, amount string)
	// BackendUp is called when a backend is marked down or comes back.
	BackendUp(provider string, up bool)
}

type nopObserver struct{}

func (nopObserver) SettleAttempt(string, string, time.Duration)             {}
func (nopObserver) Finished(string, string, string, string, string, string) {}
func (nopObserver) BackendUp(string, bool)                                  {}

// Settler drains the settlement queue.
type Settler struct {
	Queue     Queue
	Providers map[string][]Backend
	// Lookup, when set, replaces Providers. It lets the provider set be
	// swapped while the settler runs.
	Lookup func(provider string) []Backend
	// Chains by CAIP-2 network. Optional, but without a reader the settler
	// cannot tell "already settled" from "refused" and never blocks payers.
	Chains   map[string]chain.Reader
	Receipts receipts.Store
	Log      *slog.Logger
	Observer Observer

	Workers        int           // default 4
	Lease          time.Duration // default 60s; must exceed one job's handling time
	Poll           time.Duration // idle queue poll interval, default 250ms
	BackendTimeout time.Duration // per settle call, default 15s
	MaxAttempts    int           // refusals tolerated before giving up, default 4
	// BlockFor is how long a payer is refused after a settlement failure that
	// is attributable to them. Default 1h; negative means until unblocked.
	BlockFor time.Duration
	Now      func() time.Time

	health *health

	// PrepareRefund completes a batch refund job with claims and our
	// signatures (the Claimer's). Nil: refund jobs cannot be settled.
	PrepareRefund func(ctx context.Context, job *Job) error
}

func (s *Settler) defaults() {
	if s.Workers <= 0 {
		s.Workers = 4
	}
	if s.Lease <= 0 {
		s.Lease = 60 * time.Second
	}
	if s.Poll <= 0 {
		s.Poll = 250 * time.Millisecond
	}
	if s.BackendTimeout <= 0 {
		s.BackendTimeout = 15 * time.Second
	}
	if s.MaxAttempts <= 0 {
		s.MaxAttempts = 4
	}
	if s.BlockFor == 0 {
		s.BlockFor = time.Hour
	}
	if s.Now == nil {
		s.Now = time.Now
	}
	if s.health == nil {
		s.health = &health{}
	}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if s.Observer == nil {
		s.Observer = nopObserver{}
	}
}

// Run settles jobs until ctx is cancelled. In-flight jobs finish first.
func (s *Settler) Run(ctx context.Context) {
	s.defaults()
	var wg sync.WaitGroup
	for i := 0; i < s.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				worked, err := s.ProcessOne(context.WithoutCancel(ctx))
				if err != nil {
					s.Log.Error("settlement queue unavailable", "err", err)
				}
				if !worked {
					select {
					case <-ctx.Done():
					case <-time.After(s.Poll):
					}
				}
			}
		}()
	}
	wg.Wait()
}

// ProcessOne claims and handles a single job. It reports false when there was
// nothing to claim.
func (s *Settler) ProcessOne(ctx context.Context) (bool, error) {
	s.defaults()
	c, err := s.Queue.Claim(ctx, s.Lease)
	if err != nil || c == nil {
		return false, err
	}
	s.handle(ctx, c)
	return true, nil
}

// settledMarker prefixes a recorded transaction once settlement is known to
// have happened but the job could not be finalised (receipt store down).
const settledMarker = "ok:"

func (s *Settler) handle(ctx context.Context, c *payerstate.Claimed) {
	network, asset, payer, nonce, err := c.ID.Parts()
	var job Job
	if err == nil {
		err = json.Unmarshal(c.Body, &job)
	}
	if err != nil {
		s.Log.Error("dropping unreadable settlement job", "job", string(c.ID), "err", err)
		s.Queue.Failed(ctx, c.ID, "malformed_job", 0)
		return
	}
	rc := receipts.Receipt{
		ReservationID: string(c.ID), RequestID: job.RequestID, Route: job.Route, Payer: payer,
		Amount: job.Amount, Asset: asset, Chain: network, Nonce: nonce,
		VerifiedAt: job.VerifiedAt, ServedAt: job.ServedAt,
		StateSource: job.StateSource, StateBlock: job.StateBlock,
	}
	log := s.Log.With("route", job.Route, "payer", payer, "amount", job.Amount, "network", network, "attempt", c.Attempt)

	if c.Attempt == 1 {
		served := rc
		served.Status = receiptStatus(&job, receipts.StatusServed)
		if err := s.Receipts.Record(ctx, served); err != nil {
			log.Warn("could not record served receipt", "err", err)
		}
	}

	known, _ := s.Queue.PendingTx(ctx, c.ID)
	if tx, ok := strings.CutPrefix(known, settledMarker); ok {
		s.finishSettled(ctx, log, c, &job, rc, tx, "", "recovered")
		return
	}

	reader := s.Chains[network]
	expired := !s.Now().Before(c.Deadline)

	// Check before retrying: the on-chain nonce is the source of truth. A
	// previous attempt may have landed even though it looked like a failure.
	if reader != nil && (c.Attempt > 1 || expired) {
		used, err := s.authorizationUsed(ctx, reader, asset, payer, nonce)
		switch {
		case err == nil && used:
			s.finishSettled(ctx, log, c, &job, rc, known, "", "confirmed_on_chain")
			return
		case err != nil && expired:
			if c.Attempt > s.MaxAttempts+6 {
				s.finishUnpaid(ctx, log, c, &job, rc, "authorization_expired_unverified", false)
				return
			}
			log.Warn("chain unreadable for an expired authorization; will retry", "err", err)
			s.Queue.Retry(ctx, c.ID, backoff(c.Attempt))
			return
		}
	}
	if expired {
		// We were too slow or every backend was down. Not the payer's fault.
		s.finishUnpaid(ctx, log, c, &job, rc, "authorization_expired", false)
		return
	}

	if job.Kind == KindBatchRefund {
		if s.PrepareRefund == nil {
			s.finishUnpaid(ctx, log, c, &job, rc, "no_claim_key", false)
			return
		}
		if err := s.PrepareRefund(ctx, &job); err != nil {
			log.Warn("refund not prepared; will retry", "err", err)
			s.Queue.Retry(ctx, c.ID, 5*time.Second)
			return
		}
	}

	backends := s.Providers[job.Provider]
	if s.Lookup != nil {
		backends = s.Lookup(job.Provider)
	}
	if len(backends) == 0 {
		log.Error("no settlement provider configured for job; will retry until the authorization expires", "provider", job.Provider)
		s.Queue.Retry(ctx, c.ID, 15*time.Second)
		return
	}

	capable := s.capable(ctx, backends, &job)
	if len(capable) == 0 {
		log.Error("no settlement backend supports this network; will retry until the authorization expires",
			"provider", job.Provider, "scheme", job.Requirements.Scheme)
		s.Queue.Retry(ctx, c.ID, 15*time.Second)
		return
	}

	// Backends in cool-down are skipped while another can be tried; if every
	// one is cooling down, all are tried anyway rather than none.
	tried := false
	refusedReason, lastReason := "", "no_backend"
	for pass := 0; pass < 2 && !tried; pass++ {
		for _, b := range capable {
			if pass == 0 && !s.health.allow(b.Name(), s.Now()) {
				continue
			}
			tried = true
			start := s.Now()
			bctx, cancel := context.WithTimeout(ctx, s.BackendTimeout)
			res := b.Settle(bctx, &job)
			cancel()
			s.Observer.SettleAttempt(b.Name(), outcomeName(res.Outcome), s.Now().Sub(start))
			if up, changed := s.health.result(b.Name(), res.Outcome != Unavailable, s.Now()); changed {
				s.Observer.BackendUp(b.Name(), up)
				if up {
					log.Info("settlement backend is back", "provider", b.Name())
				} else {
					log.Warn("settlement backend marked down; skipped while another is available", "provider", b.Name())
				}
			}

			switch res.Outcome {
			case Settled:
				s.finishSettled(ctx, log, c, &job, rc, res.Transaction, b.Name(), "")
				return
			case Pending:
				// Broadcast but unconfirmed. Resubmitting elsewhere would race
				// the same nonce; wait and ask the chain.
				log.Info("settlement pending", "provider", b.Name(), "tx", res.Transaction)
				s.Queue.SetPendingTx(ctx, c.ID, res.Transaction)
				s.Queue.Retry(ctx, c.ID, 2*time.Second)
				return
			case Refused:
				log.Warn("settlement refused", "provider", b.Name(), "reason", res.Reason)
				refusedReason = res.Reason
			case Unavailable:
				log.Warn("settlement backend unavailable", "provider", b.Name(), "reason", res.Reason)
				lastReason = res.Reason
			}
		}
	}

	if refusedReason != "" {
		// A refusal can mean "already used" after a transfer that did land.
		if reader != nil {
			if used, err := s.authorizationUsed(ctx, reader, asset, payer, nonce); err == nil && used {
				s.finishSettled(ctx, log, c, &job, rc, known, "", "confirmed_on_chain")
				return
			}
		}
		if c.Attempt >= s.MaxAttempts {
			// Only block when the chain could be consulted: without it an
			// already-settled payment is indistinguishable from a bad one.
			// Batch deposits and refunds never block the payer: only a
			// payment that was served and not paid for does.
			s.finishUnpaid(ctx, log, c, &job, rc, refusedReason, reader != nil && payerAttributable(refusedReason) && job.Kind == "")
			return
		}
		lastReason = refusedReason
	}
	log.Info("settlement will be retried", "reason", lastReason, "in", backoff(c.Attempt).String())
	s.Queue.Retry(ctx, c.ID, backoff(c.Attempt))
}

func (s *Settler) authorizationUsed(ctx context.Context, reader chain.Reader, asset, payer, nonce string) (bool, error) {
	token, err := evm.ParseAddress(asset)
	if err != nil {
		return false, err
	}
	owner, err := evm.ParseAddress(payer)
	if err != nil {
		return false, err
	}
	n, err := evm.ParseBytes32(nonce)
	if err != nil {
		return false, err
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return reader.AuthorizationState(cctx, token, owner, n)
}

func (s *Settler) finishSettled(ctx context.Context, log *slog.Logger, c *payerstate.Claimed, job *Job, rc receipts.Receipt, tx, provider, how string) {
	network := rc.Chain
	var block uint64
	if reader := s.Chains[network]; reader != nil && tx != "" {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if b, ok, found, err := reader.TransactionBlock(cctx, tx); err == nil && found && ok {
			block = b
		}
		cancel()
	}
	rc.Status, rc.TxHash, rc.Block, rc.Provider, rc.Reason = receiptStatus(job, receipts.StatusSettled), tx, block, provider, how
	if err := s.Receipts.Record(ctx, rc); err != nil {
		// The money moved. Keep the job until the receipt exists, and make
		// sure no later attempt mistakes it for unsettled.
		log.Error("settled but receipt not recorded; will retry", "tx", tx, "err", err)
		s.Queue.SetPendingTx(ctx, c.ID, settledMarker+tx)
		s.Queue.Retry(ctx, c.ID, 5*time.Second)
		return
	}
	if err := s.Queue.Settled(ctx, c.ID, block); err != nil {
		log.Error("settled but queue not updated; will retry", "tx", tx, "err", err)
		s.Queue.SetPendingTx(ctx, c.ID, settledMarker+tx)
		return
	}
	// For a filter holding the response in sync mode. Best effort: on error
	// the filter times out and answers as in async mode.
	s.Queue.PublishResult(ctx, c.ID, payerstate.Result{Status: receipts.StatusSettled, Transaction: tx})
	s.Observer.Finished(job.Route, network, rc.Asset, rc.Status, "", job.Amount)
	log.Info("payment settled", "tx", tx, "block", block, "provider", provider, "how", how)
}

func (s *Settler) finishUnpaid(ctx context.Context, log *slog.Logger, c *payerstate.Claimed, job *Job, rc receipts.Receipt, reason string, block bool) {
	rc.Status, rc.Reason = receiptStatus(job, receipts.StatusUnpaid), reason
	if err := s.Receipts.Record(ctx, rc); err != nil {
		log.Error("unpaid but receipt not recorded; will retry", "reason", reason, "err", err)
		s.Queue.Retry(ctx, c.ID, 5*time.Second)
		return
	}
	var blockFor time.Duration
	if block {
		blockFor = s.BlockFor
	}
	if err := s.Queue.Failed(ctx, c.ID, reason, blockFor); err != nil {
		log.Error("unpaid but queue not updated; will retry", "err", err)
		return
	}
	s.Queue.PublishResult(ctx, c.ID, payerstate.Result{Status: receipts.StatusUnpaid, Reason: reason})
	s.Observer.Finished(job.Route, rc.Chain, rc.Asset, rc.Status, reason, job.Amount)
	// This is the line to alert on: a served request that was not paid for.
	log.Error("payment unpaid: served request could not be settled", "reason", reason, "payer_blocked", block)
}

// payerAttributable reports whether a refusal reason is the payer's doing
// (worth blocking them for) rather than ours or the facilitator's.
// Facilitators word an emptied wallet differently: the specification's
// insufficient_funds, or x402.org's invalid_exact_evm_insufficient_balance.
func payerAttributable(reason string) bool {
	return reason == x402.ErrInsufficientFunds ||
		reason == x402.ErrEVMInsufficientBalance ||
		reason == "invalid_transaction_state" ||
		strings.HasPrefix(reason, "invalid_exact_evm_payload")
}

func backoff(attempt int) time.Duration {
	d := 500 * time.Millisecond
	for i := 1; i < attempt && d < 15*time.Second; i++ {
		d *= 2
	}
	if d > 15*time.Second {
		d = 15 * time.Second
	}
	return d
}

func outcomeName(o Outcome) string {
	switch o {
	case Settled:
		return "settled"
	case Pending:
		return "pending"
	case Refused:
		return "refused"
	default:
		return "unavailable"
	}
}

// capable keeps the backends that handle the job's kind (or cannot say); a
// backend that does not is not asked, so its refusal cannot count against
// the payment.
func (s *Settler) capable(ctx context.Context, backends []Backend, job *Job) []Backend {
	var out []Backend
	for _, b := range backends {
		if c, ok := b.(Capable); ok {
			if yes, known := c.Supports(ctx, job.Payload.X402Version, job.Requirements.Scheme, job.Requirements.Network); known && !yes {
				continue
			}
		}
		out = append(out, b)
	}
	return out
}

// submit sends work that is not a queued payment (batch claims and payouts)
// to the job's provider: capable backends in order, skipping those cooling
// down while another can be tried, the next one after a refusal or an
// outage. It returns the outcome and the backend that produced it.
func (s *Settler) submit(ctx context.Context, job *Job, log *slog.Logger) (Result, string) {
	s.defaults()
	backends := s.Providers[job.Provider]
	if s.Lookup != nil {
		backends = s.Lookup(job.Provider)
	}
	capable := s.capable(ctx, backends, job)
	if len(capable) == 0 {
		return Result{Outcome: Unavailable, Reason: "no_backend_supports_" + job.Requirements.Scheme}, ""
	}
	last := Result{Outcome: Unavailable, Reason: "no_backend"}
	tried := false
	for pass := 0; pass < 2 && !tried; pass++ {
		for _, b := range capable {
			if pass == 0 && !s.health.allow(b.Name(), s.Now()) {
				continue
			}
			tried = true
			start := s.Now()
			bctx, cancel := context.WithTimeout(ctx, s.BackendTimeout)
			res := b.Settle(bctx, job)
			cancel()
			s.Observer.SettleAttempt(b.Name(), outcomeName(res.Outcome), s.Now().Sub(start))
			if up, changed := s.health.result(b.Name(), res.Outcome != Unavailable, s.Now()); changed {
				s.Observer.BackendUp(b.Name(), up)
			}
			if res.Outcome == Settled || res.Outcome == Pending {
				return res, b.Name()
			}
			log.Warn("batch work not done by backend", "kind", job.Kind, "provider", b.Name(), "outcome", outcomeName(res.Outcome), "reason", res.Reason)
			last = res
		}
	}
	return last, ""
}
