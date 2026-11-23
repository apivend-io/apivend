package extproc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"math/big"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/metrics"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/settle"
	"apivend.io/apivend/internal/verify"
	"apivend.io/apivend/internal/x402"
)

// Refusal reasons from the channel state, as the batch binding names them.
var channelReasons = map[string]string{
	payerstate.ReasonChannelBusy:      x402.ErrBatchChannelBusy,
	payerstate.ReasonExceedsBalance:   x402.ErrBatchExceedsBalance,
	payerstate.ReasonBelowClaimed:     x402.ErrBatchCumulativeBelow,
	payerstate.ReasonChannelNotFound:  x402.ErrBatchChannelNotFound,
	payerstate.ReasonAmountOutOfRange: x402.ErrBatchVoucherPayload,
	// A refund or a timed withdrawal is under way: no new vouchers. The
	// binding has no code for it; the channel is, in effect, busy for good.
	payerstate.ReasonChannelClosing:  x402.ErrBatchChannelBusy,
	payerstate.ReasonRefundNoBalance: x402.ErrBatchRefundNoBalance,
}

// decideVoucher handles a batch-settlement payload: the voucher is checked
// locally and the channel held for this request; nothing is charged until
// the backend answers 2xx.
func (s *Server) decideVoucher(ctx context.Context, t *txn, hdr map[string]string, route *pricing.Route, resource x402.ResourceInfo, payload *x402.PaymentPayload, missing []string) (*extprocv3.ProcessingResponse, string) {
	if s.channels == nil {
		s.m.Rejection(route.Name, x402.ErrInvalidScheme)
		return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, x402.ErrInvalidScheme, "rejected"), metrics.DecisionRejected
	}
	v, verr := verify.VerifyBatch(ctx, payload, route.Offers, s.now(), s.opts)
	if verr != nil {
		s.log.Info("voucher rejected", "route", route.Name, "reason", verr.Reason, "detail", verr.Message)
		s.m.Rejection(route.Name, verr.Reason)
		switch {
		case verr.Malformed:
			return s.challenge(route, resource, typev3.StatusCode_BadRequest, verr.Reason, "rejected"), metrics.DecisionMalformed
		case verr.Unavailable:
			return immediate(typev3.StatusCode_ServiceUnavailable, nil, []byte(`{"error":"payment verification unavailable"}`), "x402_verify_unavailable"), metrics.DecisionUnavailable
		}
		return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, verr.Reason, "rejected"), metrics.DecisionRejected
	}

	vreq := payerstate.VoucherRequest{
		Network: v.Offer.Wire.Network, ChannelID: v.ChannelID, Config: v.Config,
		MaxClaimable: v.MaxClaimable, Price: v.Offer.Amount, Signature: v.Signature,
		Provider: route.Provider, Route: route.Name, RequestID: hdr["x-request-id"],
	}
	if v.Refund {
		return s.refund(ctx, t, hdr, route, resource, payload, v, vreq)
	}
	// A paying voucher calls the upstream: refused without its required
	// parameters, before anything is reserved (verification changes nothing).
	if len(missing) > 0 {
		return missingParams(missing), "refused"
	}
	if v.Deposit != nil {
		if resp, decision := s.deposit(ctx, t, hdr, route, resource, payload, v, vreq); resp != nil {
			return resp, decision
		}
	}
	hold, err := s.channels.Reserve(ctx, vreq)
	if err != nil {
		var (
			denied   *payerstate.DeniedError
			mismatch *payerstate.MismatchError
		)
		switch {
		case errors.As(err, &mismatch):
			s.log.Info("voucher out of step", "route", route.Name, "payer", v.Config.Payer.Hex(), "charged", mismatch.Charged.String(), "voucher", v.MaxClaimable.String())
			s.m.Rejection(route.Name, x402.ErrBatchCumulativeMismatch)
			return s.corrective(route, resource, v, mismatch), metrics.DecisionDenied
		case errors.As(err, &denied):
			reason := channelReasons[denied.Reason]
			if reason == "" {
				reason = denied.Reason
			}
			s.log.Info("voucher refused", "route", route.Name, "reason", reason, "payer", v.Config.Payer.Hex())
			s.m.Rejection(route.Name, reason)
			return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, reason, "denied"), metrics.DecisionDenied
		}
		// Fail closed: without channel state (Redis or the chain) the
		// request is not served. onStateUnavailable: allow does not apply:
		// a voucher has no other way to be checked.
		s.log.Error("channel state unavailable", "route", route.Name, "err", err)
		return immediate(typev3.StatusCode_ServiceUnavailable, nil, []byte(`{"error":"payment state unavailable"}`), "x402_state_unavailable"), metrics.DecisionUnavailable
	}

	t.requestID, t.verifiedAt = hdr["x-request-id"], s.now()
	t.voucher, t.hold = v, hold
	s.log.Info("voucher reserved", "route", route.Name, "payer", v.Config.Payer.Hex(), "charged", hold.Charged.String(),
		"price", v.Offer.Amount.String(), "network", v.Offer.Wire.Network)
	return t.pass(
		[]*corev3.HeaderValueOption{setHeader(HeaderPayer, v.Config.Payer.String())},
		[]string{x402.HeaderPaymentSignature},
	), metrics.DecisionReserved
}

// commitVoucher charges the held channel after a 2xx and answers with the
// batch payment response: no transaction (nothing moved on chain), the
// charge and the channel snapshot.
func (s *Server) commitVoucher(ctx context.Context, t *txn) *extprocv3.ProcessingResponse {
	t.finished = true
	v := t.voucher
	charged, err := s.channels.Commit(ctx, t.hold)
	if err != nil {
		s.m.Request(t.route.Name, metrics.DecisionCommitLost)
		s.log.Error("voucher commit failed; served request will not be charged", "route", t.route.Name, "payer", v.Config.Payer.Hex(), "err", err)
		return continueResponse(nil)
	}
	s.m.Request(t.route.Name, metrics.DecisionServed)
	s.log.Info("voucher committed", "route", t.route.Name, "payer", v.Config.Payer.Hex(), "amount", v.Offer.Amount.String(), "charged", charged.String())
	// A deposit response reports the deposit's transaction and amount; a
	// voucher response has neither (nothing moved on chain).
	amount := t.depositAmount
	sr := x402.SettlementResponse{
		Success: true, Payer: v.Config.Payer.String(), Transaction: t.depositTx, Network: v.Offer.Wire.Network, Amount: &amount,
		Extra: map[string]any{
			"chargedAmount": v.Offer.Amount.String(),
			"channelState":  channelState(v.ChannelID, t.hold.Chain, charged),
		},
	}
	value, err := x402.EncodeHeader(sr)
	if err != nil {
		return continueResponse(nil)
	}
	return continueResponse([]*corev3.HeaderValueOption{
		setHeader(x402.HeaderPaymentResponse, value),
		appendHeader("access-control-expose-headers", "PAYMENT-RESPONSE"),
	})
}

// corrective is the 402 that lets a client whose cumulative amount is out
// of step resynchronise: the batch offers carry the channel snapshot and
// our last voucher for this channel (the client checks its own signature).
func (s *Server) corrective(route *pricing.Route, resource x402.ResourceInfo, v *verify.Voucher, m *payerstate.MismatchError) *extprocv3.ProcessingResponse {
	shadow := *route
	shadow.Offers = append([]verify.Offer(nil), route.Offers...)
	for i := range shadow.Offers {
		o := &shadow.Offers[i]
		if o.Batch == nil || o.Wire.Network != v.Offer.Wire.Network {
			continue
		}
		extra := maps.Clone(o.Wire.Extra)
		extra["channelState"] = channelState(v.ChannelID, m.Chain, m.Charged)
		if m.Signed != nil {
			extra["voucherState"] = map[string]string{
				"signedMaxClaimable": m.Signed.String(),
				"signature":          "0x" + hex.EncodeToString(m.Signature),
			}
		}
		o.Wire.Extra = extra
	}
	return s.challenge(&shadow, resource, typev3.StatusCode_PaymentRequired, x402.ErrBatchCumulativeMismatch, "denied")
}

func channelState(id [32]byte, c evm.Channel, charged interface{ String() string }) x402.BatchChannelState {
	st := x402.BatchChannelState{
		ChannelID: "0x" + hex.EncodeToString(id[:]), Balance: "0", TotalClaimed: "0", RefundNonce: "0",
		WithdrawRequestedAt: c.WithdrawRequestedAt, ChargedCumulativeAmount: charged.String(),
	}
	if c.Balance != nil {
		st.Balance = c.Balance.String()
	}
	if c.TotalClaimed != nil {
		st.TotalClaimed = c.TotalClaimed.String()
	}
	if c.RefundNonce != nil {
		st.RefundNonce = c.RefundNonce.String()
	}
	return st
}

// deposit executes a deposit payload's transfer into the channel before the
// request is served, the way a sync payment settles: the payer's funds are
// held (balance, replay), the deposit is queued for the settler's backends
// (facilitators that support batch-settlement), and the filter waits for
// the outcome. It returns a response only when the request must stop.
func (s *Server) deposit(ctx context.Context, t *txn, hdr map[string]string, route *pricing.Route, resource x402.ResourceInfo,
	payload *x402.PaymentPayload, v *verify.Voucher, vreq payerstate.VoucherRequest) (*extprocv3.ProcessingResponse, string) {
	waiter, ok := s.state.(resultWaiter)
	if !ok {
		s.m.Rejection(route.Name, x402.ErrBatchPayloadType)
		return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, x402.ErrBatchPayloadType, "rejected"), metrics.DecisionRejected
	}
	d := v.Deposit
	payer := v.Config.Payer.Hex()
	id, basis, err := s.reserve(ctx, payerstate.Request{
		Chain: v.Offer.Wire.Network, Asset: v.Offer.Domain.VerifyingContract.Hex(), Payer: payer,
		Nonce: "0x" + hexEncode(d.Auth.Nonce[:]), Amount: d.Amount, Expiry: d.ValidBefore,
		// No exposure cap: the deposit is executed before anything is served.
		Policy: payerstate.Policy{Freshness: route.Freshness},
	})
	if err != nil {
		var denied *payerstate.DeniedError
		if errors.As(err, &denied) {
			reason := denied.Reason
			if reason == payerstate.ReasonInsufficientBalance {
				reason = x402.ErrInsufficientFunds
			}
			s.log.Info("deposit refused", "route", route.Name, "reason", reason, "payer", payer)
			s.m.Rejection(route.Name, reason)
			return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, reason, "denied"), metrics.DecisionDenied
		}
		s.log.Error("payer state unavailable for deposit", "route", route.Name, "err", err)
		return immediate(typev3.StatusCode_ServiceUnavailable, nil, []byte(`{"error":"payment state unavailable"}`), "x402_state_unavailable"), metrics.DecisionUnavailable
	}
	now := s.now()
	body, err := json.Marshal(settle.Job{
		RequestID: hdr["x-request-id"], Route: route.Name, Provider: route.Provider, Payer: payer,
		Amount: d.Amount.String(), VerifiedAt: now, ServedAt: now,
		StateSource: basis.Source, StateBlock: basis.BlockHeight,
		Payload: *payload, Requirements: v.Offer.Wire, Kind: settle.KindBatchDeposit,
	})
	if err == nil {
		err = s.state.Commit(ctx, id, payerstate.Job{Body: body, Deadline: d.ValidBefore, Route: route.Name})
	}
	if err != nil {
		s.state.Release(context.WithoutCancel(ctx), id)
		s.log.Error("deposit not queued", "route", route.Name, "payer", payer, "err", err)
		return immediate(typev3.StatusCode_ServiceUnavailable, nil, []byte(`{"error":"payment state unavailable"}`), "x402_state_unavailable"), metrics.DecisionUnavailable
	}

	wait := route.SyncTimeout
	if wait == 0 {
		wait = pricing.DefaultSyncTimeout
	}
	t.extend(wait + 4*time.Second)
	res, err := waiter.WaitResult(ctx, id, wait)
	switch {
	case err == nil && res != nil && res.Status == "settled":
	case err == nil && res != nil:
		reason := res.Reason
		if reason == "" {
			reason = x402.ErrBatchDepositFailed
		}
		s.log.Info("deposit failed", "route", route.Name, "payer", payer, "reason", reason)
		s.m.Rejection(route.Name, reason)
		return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, reason, "denied"), metrics.DecisionDenied
	default:
		// Still queued: it may yet land. The client should retry later
		// with a voucher on the same channel.
		s.log.Warn("deposit not confirmed in time", "route", route.Name, "payer", payer, "err", err)
		return immediate(typev3.StatusCode_ServiceUnavailable, []*corev3.HeaderValueOption{setHeader("retry-after", "5")},
			[]byte(`{"error":"deposit pending; retry with a voucher shortly"}`), "x402_deposit_pending"), metrics.DecisionUnavailable
	}
	if err := s.channels.AfterDeposit(ctx, vreq, d.Amount); err != nil {
		s.log.Error("channel not readable after deposit", "route", route.Name, "payer", payer, "tx", res.Transaction, "err", err)
		return immediate(typev3.StatusCode_ServiceUnavailable, []*corev3.HeaderValueOption{setHeader("retry-after", "2")},
			[]byte(`{"error":"deposit landed; retry with a voucher"}`), "x402_state_unavailable"), metrics.DecisionUnavailable
	}
	t.depositTx, t.depositAmount = res.Transaction, d.Amount.String()
	s.log.Info("deposit executed", "route", route.Name, "payer", payer, "amount", d.Amount.String(), "tx", res.Transaction)
	return nil, ""
}

// refund handles a cooperative refund: the payer's zero-charge voucher
// closes the channel to new vouchers at once, the refund is queued for the
// settler (which claims what was charged and signs the refund with our
// key), and the answer carries the refund transaction. The protected API is
// not called. A failed refund reopens the channel.
func (s *Server) refund(ctx context.Context, t *txn, hdr map[string]string, route *pricing.Route, resource x402.ResourceInfo,
	payload *x402.PaymentPayload, v *verify.Voucher, vreq payerstate.VoucherRequest) (*extprocv3.ProcessingResponse, string) {
	waiter, ok := s.state.(resultWaiter)
	if !ok {
		return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, x402.ErrBatchPayloadType, "rejected"), metrics.DecisionRejected
	}
	payer := v.Config.Payer.Hex()
	wait := route.SyncTimeout
	if wait == 0 {
		wait = pricing.DefaultSyncTimeout
	}
	hold, err := s.channels.BeginRefund(ctx, vreq, wait+30*time.Second)
	if err != nil {
		var (
			denied   *payerstate.DeniedError
			mismatch *payerstate.MismatchError
		)
		switch {
		case errors.As(err, &mismatch):
			s.m.Rejection(route.Name, x402.ErrBatchCumulativeMismatch)
			return s.corrective(route, resource, v, mismatch), metrics.DecisionDenied
		case errors.As(err, &denied):
			reason := channelReasons[denied.Reason]
			s.m.Rejection(route.Name, reason)
			return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, reason, "denied"), metrics.DecisionDenied
		}
		s.log.Error("channel state unavailable for refund", "route", route.Name, "err", err)
		return immediate(typev3.StatusCode_ServiceUnavailable, nil, []byte(`{"error":"payment state unavailable"}`), "x402_state_unavailable"), metrics.DecisionUnavailable
	}
	reopen := func() { s.channels.EndRefund(context.WithoutCancel(ctx), hold, vreq) }
	amount := hold.Refundable
	if v.RefundAmount != nil {
		if v.RefundAmount.Cmp(hold.Refundable) > 0 {
			reopen()
			s.m.Rejection(route.Name, x402.ErrBatchRefundExceeds)
			return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, x402.ErrBatchRefundExceeds, "denied"), metrics.DecisionDenied
		}
		amount = v.RefundAmount
	}

	// The queue keys a job by (payer, nonce); a refund's nonce is derived
	// from the channel and its on-chain refund nonce, so each is distinct.
	nonce := evm.Keccak256(v.ChannelID[:], []byte("refund"), hold.Chain.RefundNonce.Bytes())
	id, _, err := s.reserve(ctx, payerstate.Request{
		Chain: v.Offer.Wire.Network, Asset: v.Offer.Domain.VerifyingContract.Hex(), Payer: payer,
		Nonce: "0x" + hexEncode(nonce[:]), Amount: new(big.Int), Expiry: s.now().Add(time.Hour), QueueOnly: true,
	})
	var body []byte
	if err == nil {
		now := s.now()
		body, err = json.Marshal(settle.Job{
			RequestID: hdr["x-request-id"], Route: route.Name, Provider: route.Provider, Payer: payer,
			Amount: amount.String(), VerifiedAt: now, ServedAt: now, Payload: *payload, Requirements: v.Offer.Wire,
			Kind: settle.KindBatchRefund, RefundAmount: amount.String(),
		})
	}
	if err == nil {
		err = s.state.Commit(ctx, id, payerstate.Job{Body: body, Deadline: s.now().Add(time.Hour), Route: route.Name})
	}
	if err != nil {
		reopen()
		s.log.Error("refund not queued", "route", route.Name, "payer", payer, "err", err)
		return immediate(typev3.StatusCode_ServiceUnavailable, nil, []byte(`{"error":"payment state unavailable"}`), "x402_state_unavailable"), metrics.DecisionUnavailable
	}
	s.log.Info("refund requested; channel closed to new vouchers", "route", route.Name, "payer", payer, "amount", amount.String())

	t.extend(wait + 4*time.Second)
	res, err := waiter.WaitResult(ctx, id, wait)
	switch {
	case err == nil && res != nil && res.Status == "settled":
	case err == nil && res != nil:
		reopen()
		reason := res.Reason
		if reason == "" {
			reason = x402.ErrBatchRefundFailed
		}
		s.log.Info("refund failed; channel reopened", "route", route.Name, "payer", payer, "reason", reason)
		s.m.Rejection(route.Name, reason)
		return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, reason, "denied"), metrics.DecisionDenied
	default:
		// Still queued: the channel stays closed until the hold lapses.
		return immediate(typev3.StatusCode_ServiceUnavailable, []*corev3.HeaderValueOption{setHeader("retry-after", "10")},
			[]byte(`{"error":"refund pending"}`), "x402_refund_pending"), metrics.DecisionUnavailable
	}
	ch, err := s.channels.EndRefund(ctx, hold, vreq)
	if err != nil {
		s.log.Warn("refund landed; channel state not refreshed", "route", route.Name, "err", err)
	}
	refunded := amount.String()
	sr := x402.SettlementResponse{
		Success: true, Payer: v.Config.Payer.String(), Transaction: res.Transaction, Network: v.Offer.Wire.Network, Amount: &refunded,
		Extra: map[string]any{"channelState": channelState(v.ChannelID, ch, hold.Charged)},
	}
	value, _ := x402.EncodeHeader(sr)
	s.log.Info("refund executed", "route", route.Name, "payer", payer, "amount", refunded, "tx", res.Transaction)
	return immediate(typev3.StatusCode_OK, []*corev3.HeaderValueOption{
		setHeader(x402.HeaderPaymentResponse, value),
		setHeader("content-type", "application/json"),
		setHeader("cache-control", "no-store"),
		setHeader("access-control-expose-headers", "PAYMENT-RESPONSE"),
	}, []byte("{}"), "x402_refund"), metrics.DecisionServed
}
