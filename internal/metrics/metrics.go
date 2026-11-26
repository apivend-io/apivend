// Package metrics defines the Prometheus metrics of the filter and settler.
//
// No metric is labelled by payer address: cardinality would be unbounded.
// Per-payer detail lives in the receipts table.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Request decisions, in funnel order.
const (
	DecisionChallenged  = "challenged"    // 402: no payment attached
	DecisionMalformed   = "malformed"     // 400: payment could not be parsed
	DecisionRejected    = "rejected"      // 402: payment failed verification
	DecisionDenied      = "denied"        // 402: payer state refused (funds, nonce, cap, block)
	DecisionUnavailable = "unavailable"   // 503: payer state could not be reached
	DecisionUnpaid      = "served_unpaid" // served without reservation (onStateUnavailable: allow)
	DecisionReserved    = "reserved"      // payment accepted, request forwarded
	DecisionServed      = "served"        // backend 2xx, reservation committed
	DecisionReleased    = "released"      // backend failed or request aborted; nobody charged
	DecisionCommitLost  = "commit_failed" // backend 2xx but the commit failed: revenue lost
)

func newRegistry() *prometheus.Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return r
}

// Filter holds the request-path metrics. A nil *Filter is valid and records nothing.
type Filter struct {
	Registry   *prometheus.Registry
	requests   *prometheus.CounterVec
	rejections *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	chainReads *prometheus.HistogramVec
	syncWait   *prometheus.HistogramVec
	answers    *prometheus.CounterVec
	answerHold *prometheus.HistogramVec
}

func NewFilter() *Filter {
	f := &Filter{
		Registry: newRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "x402_requests_total",
			Help: "Requests to priced routes by decision. The funnel is challenged -> reserved -> served.",
		}, []string{"route", "decision"}),
		rejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "x402_payment_rejections_total",
			Help: "Refused payments by x402 error code or payer-state reason.",
		}, []string{"route", "reason"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "x402_filter_request_seconds",
			Help:    "Time spent handling request headers (decode, verify, reserve). Budget: Envoy's ext_proc message timeout.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .2, .5},
		}, []string{"decision"}),
		chainReads: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "x402_chain_read_seconds",
			Help:    "Synchronous state reads for unknown or stale payers, by source, network and result.",
			Buckets: []float64{.005, .01, .025, .05, .1, .2, .5, 1},
		}, []string{"source", "network", "result"}),
	}
	f.syncWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "x402_sync_settlement_wait_seconds",
		Help:    "How long sync routes held the response for settlement, by result (settled, unpaid, timeout).",
		Buckets: []float64{.1, .25, .5, 1, 2, 5, 10, 20},
	}, []string{"result"})
	f.answers = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "x402_answer_checks_total",
		Help: "Paid 2xx answers by answer-check verdict: ok, a refusal (empty, not_json, html, error_only, missing_fields; charged anyway in watch mode), or why it was not checked.",
	}, []string{"route", "verdict"})
	f.answerHold = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "x402_answer_hold_seconds",
		Help:    "What the answer check adds to a checked answer's first byte: body_wait (from the answer's headers until its body had reached the gateway) and check (the check itself).",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"part"})
	f.Registry.MustRegister(f.requests, f.rejections, f.duration, f.chainReads, f.syncWait, f.answers, f.answerHold)
	return f
}

// Request counts one decision on a priced route.
func (f *Filter) Request(route, decision string) {
	if f != nil {
		f.requests.WithLabelValues(route, decision).Inc()
	}
}

// Rejection counts why a payment was refused.
func (f *Filter) Rejection(route, reason string) {
	if f != nil {
		f.rejections.WithLabelValues(route, reason).Inc()
	}
}

// RequestDuration observes the request-header handling time.
func (f *Filter) RequestDuration(decision string, d time.Duration) {
	if f != nil {
		f.duration.WithLabelValues(decision).Observe(d.Seconds())
	}
}

// SyncWait observes how long a sync route waited for settlement.
func (f *Filter) SyncWait(result string, d time.Duration) {
	if f != nil {
		f.syncWait.WithLabelValues(result).Observe(d.Seconds())
	}
}

// AnswerCheck counts a paid answer's check verdict.
func (f *Filter) AnswerCheck(route, verdict string) {
	if f != nil {
		f.answers.WithLabelValues(route, verdict).Inc()
	}
}

// AnswerHold observes how long a checked answer was held: waiting for its
// body, and checking it.
func (f *Filter) AnswerHold(bodyWait, check time.Duration) {
	if f != nil {
		f.answerHold.WithLabelValues("body_wait").Observe(bodyWait.Seconds())
		f.answerHold.WithLabelValues("check").Observe(check.Seconds())
	}
}

// StateRead observes a synchronous payer-state read from a source.
func (f *Filter) StateRead(source, network string, err error, d time.Duration) {
	if f == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	f.chainReads.WithLabelValues(source, network, result).Observe(d.Seconds())
}

// QueueStats is how the settler metrics read the queue at scrape time.
type QueueStats interface {
	QueueStats(ctx context.Context) (depth int64, earliest time.Time, err error)
	ExposureByRoute(ctx context.Context) (map[string]int64, error)
}

// Settler holds the settlement metrics.
type Settler struct {
	Registry *prometheus.Registry
	attempts *prometheus.HistogramVec
	finished *prometheus.CounterVec
	settled  *prometheus.CounterVec
	backend  *prometheus.GaugeVec
	gas      *prometheus.GaugeVec
}

func NewSettler(q QueueStats) *Settler {
	s := &Settler{
		Registry: newRegistry(),
		attempts: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "x402_settle_attempt_seconds",
			Help:    "Settlement attempts by provider and outcome (settled, pending, refused, unavailable).",
			Buckets: []float64{.05, .1, .25, .5, 1, 2, 5, 10, 15},
		}, []string{"provider", "outcome"}),
		finished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "x402_payments_finished_total",
			Help: "Served requests by final status (settled, unpaid) and, for unpaid, the reason.",
		}, []string{"route", "status", "reason"}),
		settled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "x402_settled_atomic_total",
			Help: "Settled revenue in atomic token units (USDC: divide by 1e6).",
		}, []string{"route", "network", "asset"}),
		backend: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "x402_settle_backend_up",
			Help: "0 while the settler skips a backend after repeated failures (circuit open), 1 once it answers again. Absent until a backend first changes state.",
		}, []string{"provider"}),
		gas: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "x402_settle_gas_balance_wei",
			Help: "ETH balance (wei) of a direct-settlement backend's gas key, per network, read every minute. Alert before it runs dry: direct settlement stops when it does.",
		}, []string{"provider", "network"}),
	}
	s.Registry.MustRegister(s.attempts, s.finished, s.settled, s.backend, s.gas, &queueCollector{q: q,
		depth: prometheus.NewDesc("x402_settlement_queue_depth", "Jobs waiting for settlement.", nil, nil),
		deadline: prometheus.NewDesc("x402_settlement_queue_earliest_deadline_seconds",
			"Seconds until the most urgent queued authorization expires; negative means already expired. Absent when the queue is empty.", nil, nil),
		exposure: prometheus.NewDesc("x402_unsettled_atomic",
			"Served but not yet settled amount, in atomic token units: money at risk.", []string{"route"}, nil),
		up: prometheus.NewDesc("x402_settlement_queue_up", "1 when the queue could be read at scrape time.", nil, nil),
	})
	return s
}

// SettleAttempt implements settle.Observer.
func (s *Settler) SettleAttempt(provider, outcome string, d time.Duration) {
	s.attempts.WithLabelValues(provider, outcome).Observe(d.Seconds())
}

// BackendUp implements settle.Observer.
func (s *Settler) BackendUp(provider string, up bool) {
	v := 0.0
	if up {
		v = 1
	}
	s.backend.WithLabelValues(provider).Set(v)
}

// Finished implements settle.Observer.
func (s *Settler) Finished(route, network, asset, status, reason, amount string) {
	s.finished.WithLabelValues(route, status, reason).Inc()
	if status == "settled" {
		if v, err := strconv.ParseFloat(amount, 64); err == nil {
			s.settled.WithLabelValues(route, network, asset).Add(v)
		}
	}
}

type queueCollector struct {
	q                             QueueStats
	depth, deadline, exposure, up *prometheus.Desc
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.deadline
	ch <- c.exposure
	ch <- c.up
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	depth, earliest, err := c.q.QueueStats(ctx)
	byRoute, err2 := c.q.ExposureByRoute(ctx)
	if err != nil || err2 != nil {
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(depth))
	if depth > 0 {
		ch <- prometheus.MustNewConstMetric(c.deadline, prometheus.GaugeValue, time.Until(earliest).Seconds())
	}
	for route, v := range byRoute {
		ch <- prometheus.MustNewConstMetric(c.exposure, prometheus.GaugeValue, float64(v), route)
	}
}

// Serve exposes /metrics and /healthz on addr until ctx is cancelled.
func Serve(ctx context.Context, addr string, reg *prometheus.Registry, log *slog.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()
	return nil
}

// GasBalance records a direct backend's gas key balance on a network.
func (s *Settler) GasBalance(provider, network string, wei float64) {
	s.gas.WithLabelValues(provider, network).Set(wei)
}
