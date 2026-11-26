// Package extproc implements the Envoy external processor that enforces x402
// payment on priced routes.
//
// Envoy must send request headers and response headers (bodies and trailers
// are not needed; with the answer check on, the filter asks for a paid
// answer's body through mode override, which the filter config must allow)
// and must fail closed when the processor is unavailable.
// See deploy/envoy/envoy.yaml for a working filter configuration.
package extproc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocfilterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/metrics"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/settle"
	"apivend.io/apivend/internal/verify"
	"apivend.io/apivend/internal/x402"
)

// HeaderPayer tells the backend who paid. Any client-supplied value is
// overwritten on priced routes.
const HeaderPayer = "x-x402-payer"

// Server is the ext_proc gRPC service.
type Server struct {
	refuseUnpriced []string
	extprocv3.UnimplementedExternalProcessorServer

	table atomic.Pointer[pricing.Table]
	state payerstate.PayerState
	log   *slog.Logger
	now   func() time.Time
	opts  verify.Options
	m     *metrics.Filter
	// channels holds batch-settlement channel state; nil refuses vouchers.
	channels *payerstate.Channels
	// answer: what is done with a paid 2xx answer before charging for it.
	answer    AnswerCheck
	answerMax int
	shapes    shapes
}

// Options configures a Server.
type Options struct {
	Logger *slog.Logger
	// MinRemaining: see verify.Options. Defaults to 10s.
	MinRemaining time.Duration
	Now          func() time.Time
	// Metrics is optional.
	Metrics *metrics.Filter
	// Contract, when set, lets contract wallets pay (EIP-1271): a signature
	// that is not the payer's own key is checked with the payer contract.
	Contract verify.ContractChecker
	// Channels, when set, accepts batch-settlement vouchers.
	Channels *payerstate.Channels
	// RefuseUnpriced lists patterns of Envoy route names that are always
	// priced, matched segment by segment ("/"-separated, path.Match globs,
	// a route name may have more segments): "httproute/t-*/p-*" is every
	// route named p-* in a namespace t-*. A request on such a route that is
	// not in the price table (its policy not loaded yet, or rejected) is
	// refused, not passed on for free. Needs the filter attached to the
	// whole listener.
	RefuseUnpriced []string
	// AnswerCheck: off (default), watch or enforce. With watch or enforce,
	// a paid 2xx answer that may be JSON is buffered and checked before it
	// is charged; Envoy's filter config must allow mode override.
	AnswerCheck AnswerCheck
	// AnswerMaxBytes caps the answers checked (larger ones are charged
	// unchecked). Default DefaultAnswerMaxBytes.
	AnswerMaxBytes int
}

func NewServer(table *pricing.Table, state payerstate.PayerState, o Options) *Server {
	s := &Server{state: state, log: o.Logger, now: o.Now, m: o.Metrics, channels: o.Channels, refuseUnpriced: o.RefuseUnpriced}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.now == nil {
		s.now = time.Now
	}
	s.opts.MinRemaining, s.opts.Contract = o.MinRemaining, o.Contract
	if s.opts.MinRemaining == 0 {
		s.opts.MinRemaining = 10 * time.Second
	}
	s.answer, s.answerMax = o.AnswerCheck, o.AnswerMaxBytes
	if s.answer == "" {
		s.answer = AnswerCheckOff
	}
	if s.answerMax <= 0 {
		s.answerMax = DefaultAnswerMaxBytes
	}
	s.table.Store(table)
	return s
}

// SetTable swaps the price table. In-flight requests keep the one they started with.
func (s *Server) SetTable(t *pricing.Table) { s.table.Store(t) }

// Table is the price table in use.
func (s *Server) Table() *pricing.Table { return s.table.Load() }

// txn is the per-request state; one ext_proc stream serves one HTTP request.
type txn struct {
	route *pricing.Route
	// scrubSet and scrubRemove: what every passed request loses (scrub).
	scrubSet    []*corev3.HeaderValueOption
	scrubRemove []string
	requestID   string
	verifiedAt  time.Time
	payload     *x402.PaymentPayload
	payment     *verify.Payment
	basis       payerstate.Reservation // state source and block behind the decision
	reservation payerstate.ReservationID
	finished    bool // reservation committed or released
	// Batch settlement: the voucher and the channel hold.
	voucher *verify.Voucher
	hold    payerstate.VoucherHold
	// deposit is the executed deposit (transaction, amount) on a deposit
	// payload; it is reported in PAYMENT-RESPONSE.
	depositTx, depositAmount string
	// extend asks Envoy for more time on the current message.
	extend func(time.Duration)
	// awaitBody: the answer is being checked; the charge waits for its body.
	awaitBody bool
	heldAt    time.Time // when the answer's headers arrived (awaitBody)
	plan      answerPlan
	head      bool // a HEAD request: its answer has no body
}

func (t *txn) paid() bool { return t.payment != nil || t.voucher != nil }

// Process handles one HTTP request's stream of processing messages.
func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	t := &txn{}
	t.extend = func(d time.Duration) {
		// Honoured only when the filter is configured with
		// max_message_timeout; otherwise the route's message timeout (the
		// controller's) has to cover the wait.
		stream.Send(&extprocv3.ProcessingResponse{OverrideMessageTimeout: durationpb.New(d)})
	}
	// Whatever ends the stream (client reset, backend timeout, Envoy local
	// reply), an uncommitted reservation is released: nobody is charged
	// unless a 2xx was observed.
	defer s.release(t, "stream_closed")
	// The answer's body was asked for but never came (a filter config
	// without allow_mode_override, or the stream ended mid-answer): charge
	// as an unchecked answer would have been, not free.
	defer s.chargeWithoutBody(t)

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil
			}
			return err
		}

		var resp *extprocv3.ProcessingResponse
		switch m := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			resp = s.onRequestHeaders(stream.Context(), t, m.RequestHeaders, routeNameAttribute(req))
		case *extprocv3.ProcessingRequest_ResponseHeaders:
			resp = s.onResponseHeaders(stream.Context(), t, m.ResponseHeaders, func(d time.Duration) {
				// Ask Envoy for more time on this message. Honoured only when
				// the filter is configured with max_message_timeout; otherwise
				// the gateway's message timeout has to cover the wait.
				stream.Send(&extprocv3.ProcessingResponse{OverrideMessageTimeout: durationpb.New(d)})
			})

		// Other bodies and trailers are not needed; answer them so a
		// gateway configured to send them does not stall.
		case *extprocv3.ProcessingRequest_RequestBody:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{RequestBody: &extprocv3.BodyResponse{}}}
		case *extprocv3.ProcessingRequest_ResponseBody:
			resp = s.onResponseBody(stream.Context(), t, m.ResponseBody)
		case *extprocv3.ProcessingRequest_RequestTrailers:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestTrailers{RequestTrailers: &extprocv3.TrailersResponse{}}}
		case *extprocv3.ProcessingRequest_ResponseTrailers:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseTrailers{ResponseTrailers: &extprocv3.TrailersResponse{}}}
		default:
			return status.Error(codes.Unimplemented, "unsupported processing request")
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *Server) onRequestHeaders(ctx context.Context, t *txn, h *extprocv3.HttpHeaders, envoyRoute string) *extprocv3.ProcessingResponse {
	start := time.Now()
	resp, decision := s.decide(ctx, t, h, envoyRoute)
	if t.route != nil {
		s.m.Request(t.route.Name, decision)
		s.m.RequestDuration(decision, time.Since(start))
	}
	return resp
}

// decide handles request headers and names the decision it took.
func (s *Server) decide(ctx context.Context, t *txn, h *extprocv3.HttpHeaders, envoyRoute string) (*extprocv3.ProcessingResponse, string) {
	hdr := indexHeaders(h.GetHeaders())
	t.head = hdr[":method"] == "HEAD"
	t.scrubSet, t.scrubRemove = scrub(h.GetHeaders().GetHeaders())
	route := s.table.Load().Match(hdr[":authority"], hdr[":path"], envoyRoute)
	if route == nil && s.table.Load().Refused(envoyRoute) {
		// Its PaymentPolicy exists but was rejected: fail closed until it
		// is fixed.
		return immediate(typev3.StatusCode_ServiceUnavailable, nil,
			[]byte(`{"error":"not_ready","message":"this endpoint's payment terms are not valid; try again later"}`), "x402_rejected_refused"), "refused"
	}
	if route == nil {
		for _, p := range s.refuseUnpriced {
			if routeMatches(p, envoyRoute) {
				// A route that must be priced, without its prices: fail
				// closed.
				return immediate(typev3.StatusCode_ServiceUnavailable, nil,
					[]byte(`{"error":"not_ready","message":"this endpoint's payment terms are not loaded; try again shortly"}`), "x402_unpriced_refused"), "refused"
			}
		}
		// Not priced: pass it on, but never with a payer the client made up
		// (the backend trusts this header on paid requests).
		return t.pass(nil, []string{HeaderPayer}), ""
	}
	// A CORS preflight carries no payment and must not be charged (HTTP
	// transport spec, "Browser clients"). It is answered here and never
	// forwarded: an upstream that ignores the method would otherwise serve
	// its paid answer to an OPTIONS request for free. Any other OPTIONS is
	// priced like every other method.
	if hdr[":method"] == "OPTIONS" && hdr["origin"] != "" && hdr["access-control-request-method"] != "" {
		return preflight(hdr), "preflight"
	}
	// A priced route is request and response. An upgrade (WebSocket) would
	// answer 101, which releases the payment, and the connection would go
	// on unpaid.
	if hdr["upgrade"] != "" {
		return immediate(typev3.StatusCode_BadRequest, nil, []byte(`{"error":"upgrade_not_supported","message":"connection upgrades are not supported on paid routes"}`), "x402_upgrade_refused"), "refused"
	}
	t.route = route

	resource := x402.ResourceInfo{
		URL:         resourceURL(hdr),
		Description: route.Description,
		MimeType:    route.MimeType,
	}
	if d := route.Discovery; d != nil && d.Publish {
		resource.ServiceName, resource.Tags, resource.IconURL = d.ServiceName, d.Tags, d.IconURL
	}

	raw, present := hdr[x402.HeaderPaymentSignature]
	// A request the upstream cannot answer is refused before its payment is
	// looked at: nothing reserved, nothing charged. Without a payment it
	// still gets its 402 (directories probe the bare URL), naming what is
	// missing.
	missing := missingQuery(hdr[":path"], route.RequiredQuery)
	if !present {
		reason := "PAYMENT-SIGNATURE header is required"
		if len(missing) > 0 {
			reason += "; missing required query parameter: " + strings.Join(missing, ", ") + " (a paid request without it is refused and not charged)"
		}
		return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, reason, "challenge"), metrics.DecisionChallenged
	}
	// A paid HEAD would be charged for headers only. Refused before its
	// payment is looked at: nothing reserved, nothing charged. An unpaid
	// HEAD still gets its 402 above.
	if t.head {
		return immediate(typev3.StatusCode_MethodNotAllowed, []*corev3.HeaderValueOption{setHeader("allow", "GET, POST")},
			[]byte(`{"error":"paid_head_refused","message":"a paid request cannot be HEAD: it would be charged for headers only; nothing was charged"}`), "x402_paid_head"), "refused"
	}
	var payload x402.PaymentPayload
	if err := x402.DecodeHeader(raw, &payload); err != nil {
		s.log.Info("payment rejected", "route", route.Name, "reason", x402.ErrInvalidPayload, "detail", err.Error())
		s.m.Rejection(route.Name, x402.ErrInvalidPayload)
		return s.challenge(route, resource, typev3.StatusCode_BadRequest, x402.ErrInvalidPayload, "malformed"), metrics.DecisionMalformed
	}

	// A batch refund never calls the upstream, so it needs no parameters:
	// vouchers are checked for them once the refund flag is known.
	if payload.Accepted.Scheme == x402.SchemeBatch {
		return s.decideVoucher(ctx, t, hdr, route, resource, &payload, missing)
	}
	if len(missing) > 0 {
		return missingParams(missing), "refused"
	}

	payment, verr := verify.Verify(ctx, &payload, route.Offers, s.now(), s.opts)
	if verr != nil {
		s.log.Info("payment rejected", "route", route.Name, "reason", verr.Reason, "detail", verr.Message)
		s.m.Rejection(route.Name, verr.Reason)
		if verr.Malformed {
			return s.challenge(route, resource, typev3.StatusCode_BadRequest, verr.Reason, "rejected"), metrics.DecisionMalformed
		}
		if verr.Unavailable {
			// Fail closed: the signature could not be checked.
			return immediate(typev3.StatusCode_ServiceUnavailable, []*corev3.HeaderValueOption{setHeader("retry-after", "2")}, []byte(`{"error":"payment verification unavailable"}`), "x402_verify_unavailable"), metrics.DecisionUnavailable
		}
		return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, verr.Reason, "rejected"), metrics.DecisionRejected
	}

	id, basis, err := s.reserve(ctx, payerstate.Request{
		Chain:  payment.Offer.Wire.Network,
		Asset:  payment.Offer.Domain.VerifyingContract.Hex(),
		Payer:  payment.Payer.Hex(),
		Nonce:  "0x" + hexEncode(payment.Auth.Nonce[:]),
		Amount: payment.Auth.Value,
		Expiry: payment.ValidBefore,
		Policy: payerstate.Policy{
			MaxUnsettled:         payment.Offer.MaxUnsettled,
			MaxUnsettledPerRoute: payment.Offer.MaxUnsettledPerRoute,
			Freshness:            route.Freshness,
			AllowOnUnavailable:   route.AllowOnUnavailable,
		},
		Route: route.Name,
	})
	if err != nil {
		var denied *payerstate.DeniedError
		if errors.As(err, &denied) {
			s.log.Info("payment rejected", "route", route.Name, "reason", denied.Reason, "payer", payment.Payer.Hex())
			reason := denied.Reason
			if reason == payerstate.ReasonInsufficientBalance {
				reason = x402.ErrInsufficientFunds
			}
			s.m.Rejection(route.Name, reason)
			if reason == payerstate.ReasonRouteExposureLimit {
				// Not the payer's doing: the route has as much unsettled as
				// it may until settlements catch up. Nothing was held.
				return immediate(typev3.StatusCode_ServiceUnavailable, []*corev3.HeaderValueOption{setHeader("retry-after", "30")},
					[]byte(`{"error":"this route is busy settling payments; try again shortly"}`), "x402_route_exposure_limit"), metrics.DecisionDenied
			}
			return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, reason, "denied"), metrics.DecisionDenied
		}
		if route.AllowOnUnavailable {
			// The route prefers availability: serve without a reservation.
			// Nothing will be settled for this request.
			s.log.Warn("payer state unavailable; serving unpaid per route policy", "route", route.Name, "payer", payment.Payer.Hex(), "err", err)
			return t.pass(
				[]*corev3.HeaderValueOption{setHeader(HeaderPayer, payment.Payer.String())},
				[]string{x402.HeaderPaymentSignature},
			), metrics.DecisionUnpaid
		}
		// Fail closed: without payer state the request is not served.
		s.log.Error("payer state unavailable", "route", route.Name, "err", err)
		return immediate(typev3.StatusCode_ServiceUnavailable, nil, []byte(`{"error":"payment state unavailable"}`), "x402_state_unavailable"), metrics.DecisionUnavailable
	}

	// Catalogues list a route from the settled payment's bazaar extension
	// and resource, both client-supplied: settle with the route's own.
	payload.Extensions = discoveryExtensions(payload.Extensions, route)
	if d := route.Discovery; d != nil && d.Publish {
		payload.Resource = &resource
	}
	t.requestID, t.verifiedAt, t.payload = hdr["x-request-id"], s.now(), &payload
	t.basis = basis
	t.payment, t.reservation = payment, id
	s.log.Info("payment reserved", "route", route.Name, "payer", payment.Payer.Hex(), "amount", payment.Auth.Value.String(),
		"network", payment.Offer.Wire.Network, "state_source", basis.Source, "state_block", basis.BlockHeight)

	// The backend never sees the signed payment, only who paid.
	return t.pass(
		s.answerEncoding(hdr, setHeader(HeaderPayer, payment.Payer.String())),
		[]string{x402.HeaderPaymentSignature},
	), metrics.DecisionReserved
}

// reserve places the hold and, when the payer state can say, reports which
// state source and block the decision rested on.
func (s *Server) reserve(ctx context.Context, req payerstate.Request) (payerstate.ReservationID, payerstate.Reservation, error) {
	if d, ok := s.state.(interface {
		ReserveDetailed(context.Context, payerstate.Request) (payerstate.Reservation, error)
	}); ok {
		res, err := d.ReserveDetailed(ctx, req)
		return res.ID, res, err
	}
	id, err := s.state.Reserve(ctx, req)
	return id, payerstate.Reservation{ID: id}, err
}

// resultWaiter is implemented by payer states that can report settlement
// outcomes (the Redis one). Without it sync routes answer as async ones.
type resultWaiter interface {
	WaitResult(ctx context.Context, id payerstate.ReservationID, timeout time.Duration) (*payerstate.Result, error)
}

func (s *Server) onResponseHeaders(ctx context.Context, t *txn, h *extprocv3.HttpHeaders, extend func(time.Duration)) *extprocv3.ProcessingResponse {
	if !t.paid() || t.finished {
		return continueResponse(nil)
	}
	code, _ := strconv.Atoi(indexHeaders(h.GetHeaders())[":status"])
	if code < 200 || code > 299 {
		s.release(t, "backend_status_"+strconv.Itoa(code))
		return continueResponse(nil)
	}
	if s.answer != AnswerCheckOff {
		p := planAnswer(t.route, code, t.head, indexHeaders(h.GetHeaders()))
		if p.check && h.GetEndOfStream() {
			// No body follows (an empty answer): Envoy will send no body
			// message, so judge it here, while the headers can still be
			// replaced or carry PAYMENT-RESPONSE.
			v := s.checkAnswer(t.route, p, nil, true)
			s.m.AnswerCheck(t.route.Name, v)
			set, imm := s.judgeAnswer(ctx, t, v, extend)
			if imm != nil {
				return imm
			}
			return continueResponse(set)
		}
		if p.check {
			// Charge on the body, once it has been checked. Envoy holds the
			// headers until then.
			t.plan, t.awaitBody, t.heldAt = p, true, time.Now()
			r := continueResponse(nil)
			r.ModeOverride = &extprocfilterv3.ProcessingMode{
				ResponseBodyMode:    extprocfilterv3.ProcessingMode_BUFFERED_PARTIAL,
				ResponseTrailerMode: extprocfilterv3.ProcessingMode_SKIP,
			}
			return r
		}
		s.m.AnswerCheck(t.route.Name, p.verdict)
	}
	set, imm := s.charge(ctx, t, extend, true)
	if imm != nil {
		return imm
	}
	return continueResponse(set)
}

// onResponseBody checks a buffered answer, then charges for it, or (enforce)
// refuses it.
func (s *Server) onResponseBody(ctx context.Context, t *txn, b *extprocv3.HttpBody) *extprocv3.ProcessingResponse {
	if !t.awaitBody || t.finished {
		return bodyResponse(nil)
	}
	t.awaitBody = false
	arrived := time.Now()
	v := s.checkAnswer(t.route, t.plan, b.GetBody(), b.GetEndOfStream())
	s.m.AnswerCheck(t.route.Name, v)
	s.m.AnswerHold(arrived.Sub(t.heldAt), time.Since(arrived))
	set, imm := s.judgeAnswer(ctx, t, v, t.extend)
	if imm != nil {
		return imm
	}
	return bodyResponse(set)
}

// judgeAnswer acts on a checked answer's verdict: in enforce mode an
// unusable answer is released and replaced by an error; otherwise the
// payment is charged (a sync route waits for settlement). It returns the
// headers to add to the answer, or a response that replaces it.
func (s *Server) judgeAnswer(ctx context.Context, t *txn, v string, extend func(time.Duration)) ([]*corev3.HeaderValueOption, *extprocv3.ProcessingResponse) {
	if refusing(v) {
		if s.answer == AnswerCheckEnforce {
			s.log.Info("answer unusable; not charged", "route", t.route.Name, "verdict", v)
			s.release(t, "answer_"+v)
			// 503, not 502: proxies in front of the gateway (Cloudflare
			// among them) replace an origin's 502 and 504 with their own
			// error page, and the buyer would never see the reason. The
			// payment was released, so retrying with it is fine.
			return nil, immediate(typev3.StatusCode_ServiceUnavailable, []*corev3.HeaderValueOption{
				setHeader("content-type", "application/json"),
				setHeader("cache-control", "no-store"),
			}, []byte(`{"error":"unusable_answer","reason":"`+v+`","message":"the API answered, but not with a usable answer; nothing was charged"}`), "x402_answer_"+v)
		}
		s.log.Info("answer unusable (watch mode: charged)", "route", t.route.Name, "verdict", v)
	}
	return s.charge(ctx, t, extend, true)
}

// chargeWithoutBody charges a checked answer whose body never came.
func (s *Server) chargeWithoutBody(t *txn) {
	if !t.awaitBody || t.finished {
		return
	}
	t.awaitBody = false
	s.m.AnswerCheck(t.route.Name, VerdictNoBody)
	// Either the upstream failed mid-answer, or Envoy ignored the mode
	// override (its ext_proc config lacks allow_mode_override): then every
	// checked answer ends up here, charged after it was sent.
	s.log.Warn("paid answer's body never reached the filter; charged unchecked (check allow_mode_override in Envoy's ext_proc config if this repeats)", "route", t.route.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.charge(ctx, t, func(time.Duration) {}, false)
}

// charge commits the payment for a served answer. It returns the headers to
// add to the answer, or a response that replaces it. wait: a sync route
// holds the answer until the payment has settled.
func (s *Server) charge(ctx context.Context, t *txn, extend func(time.Duration), wait bool) ([]*corev3.HeaderValueOption, *extprocv3.ProcessingResponse) {
	if t.voucher != nil {
		return s.commitVoucher(ctx, t), nil
	}

	body, err := json.Marshal(settle.Job{
		RequestID:    t.requestID,
		Route:        t.route.Name,
		Provider:     t.route.Provider,
		Payer:        t.payment.Payer.Hex(),
		Amount:       t.payment.Auth.Value.String(),
		VerifiedAt:   t.verifiedAt,
		ServedAt:     s.now(),
		StateSource:  t.basis.Source,
		StateBlock:   t.basis.BlockHeight,
		Payload:      *t.payload,
		Requirements: t.payment.Offer.Wire,
	})
	if err == nil {
		err = s.state.Commit(ctx, t.reservation, payerstate.Job{Body: body, Deadline: t.payment.ValidBefore, Route: t.route.Name})
	}
	if err != nil {
		s.m.Request(t.route.Name, metrics.DecisionCommitLost)
		// Not committed means not settled: the response is withheld (its
		// body has not left yet). Otherwise an authorization that expires
		// while a slow backend works, or a Redis outage, would serve for
		// free. The reservation stays held until it expires.
		s.log.Error("commit failed; response withheld", "route", t.route.Name, "payer", t.payment.Payer.Hex(), "err", err)
		t.finished = true
		if errors.Is(err, payerstate.ErrExpired) {
			return nil, immediate(typev3.StatusCode_PaymentRequired, nil, []byte(`{"error":"payment_expired","message":"the payment authorization expired before the response was ready; nothing was charged"}`), "x402_commit_expired")
		}
		return nil, immediate(typev3.StatusCode_ServiceUnavailable, nil, []byte(`{"error":"payment_unavailable","message":"the payment could not be recorded; nothing was charged"}`), "x402_commit_failed")
	}
	t.finished = true
	s.m.Request(t.route.Name, metrics.DecisionServed)
	s.log.Info("payment committed", "route", t.route.Name, "payer", t.payment.Payer.Hex(), "amount", t.payment.Auth.Value.String())

	// Async: the transfer has not been broadcast when the response leaves,
	// so there is no transaction hash to report.
	sr := x402.SettlementResponse{
		Success:     true,
		Payer:       t.payment.Payer.String(),
		Transaction: "",
		Network:     t.payment.Offer.Wire.Network,
	}
	if w, ok := s.state.(resultWaiter); ok && t.route.Sync && wait {
		// Sync: hold the response until the payment has settled.
		extend(t.route.SyncTimeout + 2*time.Second)
		start := time.Now()
		res, err := w.WaitResult(ctx, t.reservation, t.route.SyncTimeout)
		switch {
		case err == nil && res != nil && res.Status == "settled":
			s.m.SyncWait("settled", time.Since(start))
			sr.Transaction = res.Transaction
		case err == nil && res != nil:
			// Settlement failed for good: the client does not get the
			// response it did not pay for.
			s.m.SyncWait("unpaid", time.Since(start))
			s.log.Warn("sync settlement failed; withholding response", "route", t.route.Name, "payer", t.payment.Payer.Hex(), "reason", res.Reason)
			sr.Success, sr.ErrorReason = false, res.Reason
			value, _ := x402.EncodeHeader(sr)
			return nil, immediate(typev3.StatusCode_PaymentRequired, []*corev3.HeaderValueOption{
				setHeader(x402.HeaderPaymentResponse, value),
				setHeader("content-type", "application/json"),
				setHeader("cache-control", "no-store"),
				setHeader("access-control-expose-headers", "PAYMENT-RESPONSE"),
			}, []byte("{}"), "x402_settlement_failed")
		default:
			// Still unsettled (or the result could not be read). The job
			// stays queued and will be charged, so the client gets its
			// response, with the async form of the header.
			s.m.SyncWait("timeout", time.Since(start))
			s.log.Warn("sync settlement not confirmed in time; responding as async", "route", t.route.Name, "payer", t.payment.Payer.Hex(), "err", err)
		}
	}
	value, err := x402.EncodeHeader(sr)
	if err != nil {
		return nil, nil
	}
	return []*corev3.HeaderValueOption{
		setHeader(x402.HeaderPaymentResponse, value),
		appendHeader("access-control-expose-headers", "PAYMENT-RESPONSE"),
	}, nil
}

func (s *Server) release(t *txn, why string) {
	if !t.paid() || t.finished {
		return
	}
	t.finished = true
	// The stream context may already be cancelled; releasing must still happen.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if t.voucher != nil {
		if err := s.channels.Release(ctx, t.hold); err != nil {
			s.log.Error("voucher release failed", "route", t.route.Name, "payer", t.voucher.Config.Payer.Hex(), "err", err)
			return
		}
		s.m.Request(t.route.Name, metrics.DecisionReleased)
		s.log.Info("voucher released", "route", t.route.Name, "payer", t.voucher.Config.Payer.Hex(), "why", why)
		return
	}
	if err := s.state.Release(ctx, t.reservation); err != nil {
		s.log.Error("release failed", "route", t.route.Name, "payer", t.payment.Payer.Hex(), "err", err)
		return
	}
	s.m.Request(t.route.Name, metrics.DecisionReleased)
	s.log.Info("payment released", "route", t.route.Name, "payer", t.payment.Payer.Hex(), "why", why)
}

// challenge answers with the route's payment terms. The PAYMENT-REQUIRED
// header is the canonical location; the same object is repeated in the body
// for humans and for clients that read it there.
func (s *Server) challenge(route *pricing.Route, res x402.ResourceInfo, code typev3.StatusCode, reason, details string) *extprocv3.ProcessingResponse {
	pr := x402.PaymentRequired{X402Version: x402.Version, Error: reason, Resource: res}
	for i := range route.Offers {
		pr.Accepts = append(pr.Accepts, route.Offers[i].Wire)
	}
	if d := route.Discovery; d != nil && d.Publish {
		pr.Extensions = map[string]json.RawMessage{x402.ExtensionBazaar: d.Bazaar}
	}
	body, err := json.Marshal(pr)
	if err != nil {
		s.log.Error("encode challenge", "err", err)
		return immediate(typev3.StatusCode_InternalServerError, nil, nil, "x402_internal")
	}
	value, _ := x402.EncodeHeader(pr)
	return immediate(code, []*corev3.HeaderValueOption{
		setHeader(x402.HeaderPaymentRequired, value),
		setHeader("content-type", "application/json"),
		setHeader("cache-control", "no-store"),
		setHeader("access-control-expose-headers", "PAYMENT-REQUIRED"),
	}, body, "x402_"+details)
}

// discoveryExtensions returns a payment's extensions with bazaar set to the
// route's own (published routes) or removed (all others), keeping any other
// extension the client sent. A client must not be able to list, or describe,
// a route through the payments it makes. Extensions are not part of what the
// payer signs, so this does not touch the payment.
func discoveryExtensions(client json.RawMessage, route *pricing.Route) json.RawMessage {
	m := map[string]json.RawMessage{}
	if len(client) > 0 && json.Unmarshal(client, &m) != nil || m == nil { // not an object, or null
		m = map[string]json.RawMessage{}
	}
	delete(m, x402.ExtensionBazaar)
	if d := route.Discovery; d != nil && d.Publish {
		m[x402.ExtensionBazaar] = d.Bazaar
	}
	if len(m) == 0 {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return b
}

// routeNameAttribute extracts xds.route_name from the attributes Envoy sends
// when the filter is configured with request_attributes. It is set by the
// proxy, not the client.
func routeNameAttribute(req *extprocv3.ProcessingRequest) string {
	for _, attrs := range req.GetAttributes() {
		if v, ok := attrs.GetFields()["xds.route_name"]; ok {
			return v.GetStringValue()
		}
	}
	return ""
}

func resourceURL(hdr map[string]string) string {
	scheme := hdr["x-forwarded-proto"]
	if scheme != "http" && scheme != "https" {
		scheme = hdr[":scheme"]
	}
	if scheme != "http" && scheme != "https" {
		scheme = "https"
	}
	path := hdr[":path"]
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	return scheme + "://" + hdr[":authority"] + path
}

func indexHeaders(m *corev3.HeaderMap) map[string]string {
	out := make(map[string]string, len(m.GetHeaders()))
	for _, h := range m.GetHeaders() {
		v := h.GetValue()
		if raw := h.GetRawValue(); len(raw) > 0 {
			v = string(raw)
		}
		out[strings.ToLower(h.GetKey())] = v
	}
	return out
}

func setHeader(k, v string) *corev3.HeaderValueOption {
	return &corev3.HeaderValueOption{
		Header:       &corev3.HeaderValue{Key: k, RawValue: []byte(v)},
		AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
	}
}

func appendHeader(k, v string) *corev3.HeaderValueOption {
	return &corev3.HeaderValueOption{
		Header:       &corev3.HeaderValue{Key: k, RawValue: []byte(v)},
		AppendAction: corev3.HeaderValueOption_APPEND_IF_EXISTS_OR_ADD,
	}
}

// routeMatches matches an Envoy route name against a pattern of
// "/"-separated path.Match globs; the name may have more segments.
func routeMatches(pattern, name string) bool {
	ps, ns := strings.Split(pattern, "/"), strings.Split(name, "/")
	if name == "" || len(ns) < len(ps) {
		return false
	}
	for i, p := range ps {
		if ok, err := path.Match(p, ns[i]); err != nil || !ok {
			return false
		}
	}
	return true
}

// preflight answers a CORS preflight for a priced route: any origin may
// send the method and headers it asked for, and read the x402 headers.
// Whether a browser may read the paid response itself is still the
// backend's to say (its own Access-Control-Allow-Origin).
func preflight(hdr map[string]string) *extprocv3.ProcessingResponse {
	set := []*corev3.HeaderValueOption{
		setHeader("access-control-allow-origin", "*"),
		setHeader("access-control-allow-methods", sanitizeToken(hdr["access-control-request-method"])),
		setHeader("access-control-expose-headers", x402.HeaderPaymentRequired+", "+x402.HeaderPaymentResponse),
		setHeader("access-control-max-age", "600"),
		setHeader("vary", "origin, access-control-request-method, access-control-request-headers"),
	}
	if h := sanitizeList(hdr["access-control-request-headers"]); h != "" {
		set = append(set, setHeader("access-control-allow-headers", h))
	}
	return immediate(typev3.StatusCode_NoContent, set, nil, "x402_preflight")
}

// sanitizeToken keeps an HTTP token (a method name); sanitizeList a
// comma-separated list of header names. Anything else is dropped, so a
// client's bytes never reach a response header unchecked.
func sanitizeToken(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || len(v) > 32 {
		return "GET"
	}
	for _, r := range v {
		if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return "GET"
		}
	}
	return v
}

func sanitizeList(v string) string {
	var out []string
	for _, h := range strings.Split(v, ",") {
		h = strings.TrimSpace(h)
		ok := h != "" && len(h) <= 64
		for _, r := range h {
			if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
				ok = false
				break
			}
		}
		if ok && len(out) < 20 {
			out = append(out, h)
		}
	}
	return strings.Join(out, ", ")
}

// scrub lists what a request must not carry to the upstream, whatever the
// client sent: headers in the gateway's own namespaces (X-Apivend-*,
// X-X402-*), which a backend may trust, except those the gateway sets
// itself (the payer, set or removed by the caller; the origin secret,
// which Envoy's credential injection overwrites); X-Forwarded-Host; and
// Cloudflare Access in front of the gateway (every Cf-Access-* header,
// service-token credentials and the user's email included, and its session
// cookies), which is ours, not the upstream's. Other cookies are kept.
func scrub(hs []*corev3.HeaderValue) (set []*corev3.HeaderValueOption, remove []string) {
	seen := map[string]bool{}
	drop := func(k string) {
		if !seen[k] {
			seen[k] = true
			remove = append(remove, k)
		}
	}
	var cookies []string
	access := false
	for _, h := range hs {
		k := strings.ToLower(h.GetKey())
		v := h.GetValue()
		if raw := h.GetRawValue(); len(raw) > 0 {
			v = string(raw)
		}
		switch {
		case strings.HasPrefix(k, "x-apivend-") && k != "x-apivend-secret",
			strings.HasPrefix(k, "x-x402-") && k != HeaderPayer,
			k == "x-forwarded-host", strings.HasPrefix(k, "cf-access-"):
			drop(k)
		case k == "cookie":
			for _, c := range strings.Split(v, ";") {
				c = strings.TrimSpace(c)
				if name, _, _ := strings.Cut(c, "="); accessCookie(strings.TrimSpace(name)) {
					access = true
				} else if c != "" {
					cookies = append(cookies, c)
				}
			}
		}
	}
	if access {
		drop("cookie")
		if len(cookies) > 0 {
			remove = remove[:len(remove)-1]
			set = append(set, setHeader("cookie", strings.Join(cookies, "; ")))
		}
	}
	return set, remove
}

// accessCookie: the cookies Cloudflare Access keeps its session in.
func accessCookie(name string) bool {
	for _, c := range []string{"CF_Authorization", "CF_AppSession", "CF_Binding"} {
		if strings.EqualFold(name, c) {
			return true
		}
	}
	return false
}

// pass lets the request through with the caller's header changes and the
// scrubbing every request gets.
func (t *txn) pass(set []*corev3.HeaderValueOption, remove []string) *extprocv3.ProcessingResponse {
	return continueRequest(append(set, t.scrubSet...), append(remove, t.scrubRemove...))
}

func continueRequest(set []*corev3.HeaderValueOption, remove []string) *extprocv3.ProcessingResponse {
	hr := &extprocv3.HeadersResponse{}
	if len(set) > 0 || len(remove) > 0 {
		hr.Response = &extprocv3.CommonResponse{
			HeaderMutation: &extprocv3.HeaderMutation{SetHeaders: set, RemoveHeaders: remove},
		}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: hr}}
}

// bodyResponse lets a buffered answer through with the given header changes
// (Envoy has held its headers until now).
func bodyResponse(set []*corev3.HeaderValueOption) *extprocv3.ProcessingResponse {
	br := &extprocv3.BodyResponse{}
	if len(set) > 0 {
		br.Response = &extprocv3.CommonResponse{HeaderMutation: &extprocv3.HeaderMutation{SetHeaders: set}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{ResponseBody: br}}
}

func continueResponse(set []*corev3.HeaderValueOption) *extprocv3.ProcessingResponse {
	hr := &extprocv3.HeadersResponse{}
	if len(set) > 0 {
		hr.Response = &extprocv3.CommonResponse{HeaderMutation: &extprocv3.HeaderMutation{SetHeaders: set}}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{ResponseHeaders: hr}}
}

func immediate(code typev3.StatusCode, set []*corev3.HeaderValueOption, body []byte, details string) *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ImmediateResponse{
		ImmediateResponse: &extprocv3.ImmediateResponse{
			Status:  &typev3.HttpStatus{Code: code},
			Headers: &extprocv3.HeaderMutation{SetHeaders: set},
			Body:    body,
			Details: details,
		},
	}}
}

const hexDigits = "0123456789abcdef"

func hexEncode(b []byte) string {
	out := make([]byte, 2*len(b))
	for i, c := range b {
		out[2*i], out[2*i+1] = hexDigits[c>>4], hexDigits[c&0x0f]
	}
	return string(out)
}

// missingParams is the 400 for a paid request without a required query
// parameter: refused before its payment is verified or reserved.
func missingParams(missing []string) *extprocv3.ProcessingResponse {
	body, _ := json.Marshal(map[string]string{"error": "missing_parameter",
		"message": "missing required query parameter: " + strings.Join(missing, ", ") + "; nothing was charged"})
	return immediate(typev3.StatusCode_BadRequest, nil, body, "x402_missing_parameter")
}

// missingQuery returns the names in required that path's query string does
// not carry with a value. A query that cannot be parsed is left to the
// upstream (nothing is reported missing).
func missingQuery(path string, required []string) []string {
	if len(required) == 0 {
		return nil
	}
	_, raw, _ := strings.Cut(path, "?")
	raw, _, _ = strings.Cut(raw, "#")
	q, err := url.ParseQuery(raw)
	if err != nil {
		return nil
	}
	var out []string
	for _, name := range required {
		if q.Get(name) == "" {
			out = append(out, name)
		}
	}
	return out
}
