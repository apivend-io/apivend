// Package extproc implements the Envoy external processor that enforces x402
// payment on priced routes.
//
// Envoy must send request headers and response headers (bodies and trailers
// are not needed) and must fail closed when the processor is unavailable.
// See deploy/envoy/envoy.yaml for a working filter configuration.
package extproc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
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
	extprocv3.UnimplementedExternalProcessorServer

	table atomic.Pointer[pricing.Table]
	state payerstate.PayerState
	log   *slog.Logger
	now   func() time.Time
	opts  verify.Options
	m     *metrics.Filter
	// channels holds batch-settlement channel state; nil refuses vouchers.
	channels *payerstate.Channels
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
}

func NewServer(table *pricing.Table, state payerstate.PayerState, o Options) *Server {
	s := &Server{state: state, log: o.Logger, now: o.Now, m: o.Metrics, channels: o.Channels}
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
	s.table.Store(table)
	return s
}

// SetTable swaps the price table. In-flight requests keep the one they started with.
func (s *Server) SetTable(t *pricing.Table) { s.table.Store(t) }

// Table is the price table in use.
func (s *Server) Table() *pricing.Table { return s.table.Load() }

// txn is the per-request state; one ext_proc stream serves one HTTP request.
type txn struct {
	route       *pricing.Route
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

		// Bodies and trailers are not needed; answer them so a gateway
		// configured to send them does not stall.
		case *extprocv3.ProcessingRequest_RequestBody:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{RequestBody: &extprocv3.BodyResponse{}}}
		case *extprocv3.ProcessingRequest_ResponseBody:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{ResponseBody: &extprocv3.BodyResponse{}}}
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
	route := s.table.Load().Match(hdr[":authority"], hdr[":path"], envoyRoute)
	// CORS preflights carry no payment and must not be charged (HTTP
	// transport spec, "Browser clients").
	if route == nil || hdr[":method"] == "OPTIONS" {
		return continueRequest(nil, nil), ""
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
	if !present {
		return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, "PAYMENT-SIGNATURE header is required", "challenge"), metrics.DecisionChallenged
	}

	var payload x402.PaymentPayload
	if err := x402.DecodeHeader(raw, &payload); err != nil {
		s.log.Info("payment rejected", "route", route.Name, "reason", x402.ErrInvalidPayload, "detail", err.Error())
		s.m.Rejection(route.Name, x402.ErrInvalidPayload)
		return s.challenge(route, resource, typev3.StatusCode_BadRequest, x402.ErrInvalidPayload, "malformed"), metrics.DecisionMalformed
	}

	if payload.Accepted.Scheme == x402.SchemeBatch {
		return s.decideVoucher(ctx, t, hdr, route, resource, &payload)
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
			return immediate(typev3.StatusCode_ServiceUnavailable, nil, []byte(`{"error":"payment verification unavailable"}`), "x402_verify_unavailable"), metrics.DecisionUnavailable
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
			MaxUnsettled:       payment.Offer.MaxUnsettled,
			Freshness:          route.Freshness,
			AllowOnUnavailable: route.AllowOnUnavailable,
		},
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
			return s.challenge(route, resource, typev3.StatusCode_PaymentRequired, reason, "denied"), metrics.DecisionDenied
		}
		if route.AllowOnUnavailable {
			// The route prefers availability: serve without a reservation.
			// Nothing will be settled for this request.
			s.log.Warn("payer state unavailable; serving unpaid per route policy", "route", route.Name, "payer", payment.Payer.Hex(), "err", err)
			return continueRequest(
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
	return continueRequest(
		[]*corev3.HeaderValueOption{setHeader(HeaderPayer, payment.Payer.String())},
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
	if t.voucher != nil {
		return s.commitVoucher(ctx, t)
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
		// The backend already did the work; the response goes out and the
		// loss is surfaced. The reservation stays held until it expires.
		s.log.Error("commit failed; served request will not be settled", "route", t.route.Name, "payer", t.payment.Payer.Hex(), "err", err)
		t.finished = true
		return continueResponse(nil)
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
	if w, ok := s.state.(resultWaiter); ok && t.route.Sync {
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
			return immediate(typev3.StatusCode_PaymentRequired, []*corev3.HeaderValueOption{
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
		return continueResponse(nil)
	}
	return continueResponse([]*corev3.HeaderValueOption{
		setHeader(x402.HeaderPaymentResponse, value),
		appendHeader("access-control-expose-headers", "PAYMENT-RESPONSE"),
	})
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

func continueRequest(set []*corev3.HeaderValueOption, remove []string) *extprocv3.ProcessingResponse {
	hr := &extprocv3.HeadersResponse{}
	if len(set) > 0 || len(remove) > 0 {
		hr.Response = &extprocv3.CommonResponse{
			HeaderMutation: &extprocv3.HeaderMutation{SetHeaders: set, RemoveHeaders: remove},
		}
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: hr}}
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
