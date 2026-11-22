// x402-settler takes committed reservations off the settlement queue, settles
// them through the configured providers and writes receipts. It runs off the
// request path; any number of replicas can run side by side.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/cliutil"
	"apivend.io/apivend/internal/metrics"
	"apivend.io/apivend/internal/receipts"
	"apivend.io/apivend/internal/settle"
)

func main() {
	var (
		redisAddr   = flag.String("redis", os.Getenv("X402_REDIS"), "Redis address or URL shared with x402-filter (env X402_REDIS)")
		providers   = flag.String("providers", "/etc/x402/providers.yaml", "settlement providers file")
		postgresDSN = flag.String("postgres", os.Getenv("X402_POSTGRES"), "PostgreSQL DSN for receipts (env X402_POSTGRES); empty = receipts go to the log only")
		workers     = flag.Int("workers", 4, "concurrent settlements")
		maxAttempts = flag.Int("max-attempts", 4, "refusals tolerated before a payment is marked unpaid")
		blockFor    = flag.Duration("block-payer-for", time.Hour, "how long a payer is refused after a failed settlement attributable to them; negative = until unblocked")
		optional    = flag.Bool("providers-optional", false, "start with no providers when the file does not exist yet (the controller writes it); jobs wait")
		provSecret  = flag.String("providers-secret", "", "read the providers from this Secret through the Kubernetes API instead of -providers, so a change applies within about a second (a mounted Secret takes a minute or more); the Secret not existing yet means jobs wait")
		provKey     = flag.String("providers-secret-key", "providers.yaml", "key of the providers document in -providers-secret")
		provNS      = flag.String("providers-namespace", "", "namespace of -providers-secret; default env POD_NAMESPACE, then the pod's own namespace")
		metricsAddr = flag.String("metrics", ":9103", "listen address for /metrics and /healthz; empty disables")
		claimKey    = flag.String("batch-authorizer-key", "", "file holding our batch-settlement claim key (hex); its address is the controller's -batch-receiver-authorizer. Empty: no claims (batch charges accumulate unclaimed)")
		claimEvery  = flag.Duration("claim-interval", 5*time.Minute, "how often batch-settlement channels are claimed; must stay well below the withdrawal delay")
		claimMin    = flag.String("claim-min-amount", "100000", "claim a channel once its unclaimed charges reach this many atomic units (0.10 USDC), a withdrawal is pending, or it waited an hour")
		rpcs        = cliutil.RPCFlag{}
	)
	flag.Var(rpcs, "rpc", "chain RPC endpoints as network=url[,url...]; repeatable. Used to confirm settlements on-chain")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	fatal := func(msg string, err error) {
		log.Error(msg, "err", err)
		os.Exit(1)
	}

	if *redisAddr == "" {
		fatal("configuration", errMissing("-redis"))
	}
	readers, err := rpcs.Readers(5 * time.Second)
	if err != nil {
		fatal("configure rpc", err)
	}
	// Direct backends submit transactions through the same endpoints.
	chains := map[string]settle.Chain{}
	for network, r := range readers {
		if c, ok := r.(settle.Chain); ok {
			chains[network] = c
		}
	}
	withChains := settle.WithChains(chains)

	var backends map[string][]settle.Backend
	if *provSecret == "" {
		var err error
		backends, err = settle.LoadProviders(*providers, 15*time.Second, withChains)
		if err != nil && *optional && errors.Is(err, os.ErrNotExist) {
			log.Warn("providers file does not exist yet; settlements wait until it appears", "path", *providers)
			backends, err = map[string][]settle.Backend{}, nil
		}
		if err != nil {
			fatal("load providers", err)
		}
	}
	rdb, err := cliutil.Redis(context.Background(), *redisAddr)
	if err != nil {
		fatal("connect to redis", err)
	}
	if len(readers) == 0 {
		log.Warn("no -rpc: settlements cannot be confirmed on-chain; a lost facilitator answer may be recorded as unpaid, and payers are never blocked")
	}

	var store receipts.Store = receipts.Log{Logger: log}
	if *postgresDSN == "" {
		log.Warn("no -postgres: receipts are written to the log only")
	} else {
		pg, err := cliutil.OpenWithRetry(log, "postgres", 30*time.Second, func(ctx context.Context) (*receipts.Postgres, error) {
			return receipts.OpenPostgres(ctx, *postgresDSN)
		})
		if err != nil {
			fatal("connect to postgres", err)
		}
		defer pg.Close()
		store = pg
	}

	queue := payerstate.NewRedis(rdb)
	m := metrics.NewSettler(queue)
	var current atomic.Pointer[map[string][]settle.Backend]
	if backends != nil {
		current.Store(&backends)
	}
	s := &settle.Settler{
		Queue:    queue,
		Observer: m,
		Lookup: func(name string) []settle.Backend {
			if p := current.Load(); p != nil {
				return (*p)[name]
			}
			return nil
		},
		Chains:      readers,
		Receipts:    store,
		Log:         log,
		Workers:     *workers,
		MaxAttempts: *maxAttempts,
		BlockFor:    *blockFor,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *metricsAddr != "" {
		if err := metrics.Serve(ctx, *metricsAddr, m.Registry, log); err != nil {
			fatal("metrics listen", err)
		}
	}
	if *provSecret != "" {
		if err := watchProvidersSecret(ctx, log, *provNS, *provSecret, *provKey, &current, withChains); err != nil {
			fatal("watch providers Secret", err)
		}
	} else {
		cliutil.WatchFile(ctx, *providers, 5*time.Second, func() {
			next, err := settle.LoadProviders(*providers, 15*time.Second, withChains)
			if err != nil {
				log.Error("reload providers; keeping current", "err", err)
				return
			}
			current.Store(&next)
			log.Info("providers reloaded", "providers", len(next))
		})
	}
	n := 0
	if p := current.Load(); p != nil {
		n = len(*p)
	}
	log.Info("x402-settler running", "providers", n, "workers", *workers)
	go watchGas(ctx, &current, m)
	if *claimKey != "" {
		c, err := claimer(*claimKey, *claimMin, rdb, readers, s, log)
		if err != nil {
			fatal("batch-settlement claims", err)
		}
		c.Interval = *claimEvery
		s.PrepareRefund = c.PrepareRefund
		// The address only, never the key: it must equal the controller's
		// -batch-receiver-authorizer, or the contract refuses our claims.
		log.Info("claiming batch-settlement channels", "receiver_authorizer", c.Address().Hex(), "interval", claimEvery.String())
		go c.Run(ctx)
	}
	s.Run(ctx)
	log.Info("x402-settler stopped")
}

type errMissing string

func (e errMissing) Error() string { return string(e) + " is required" }
