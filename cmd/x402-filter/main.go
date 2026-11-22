// x402-filter is the Envoy external processor that enforces x402 payment on
// priced routes.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/engine/statesource"
	"apivend.io/apivend/internal/cliutil"
	"apivend.io/apivend/internal/extproc"
	"apivend.io/apivend/internal/metrics"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/verify"
)

func main() {
	var (
		listen          = flag.String("listen", ":9002", "gRPC listen address for Envoy ext_proc")
		configPath      = flag.String("config", "/etc/x402/routes.yaml", "price table file")
		configMap       = flag.String("config-configmap", "", "read the price table from this ConfigMap through the Kubernetes API instead of -config, so every replica sees a change within about a second (a mounted ConfigMap takes up to a minute, at a different moment in each pod)")
		configKey       = flag.String("config-configmap-key", "routes.yaml", "key of the price table in -config-configmap")
		configNS        = flag.String("config-namespace", "", "namespace of -config-configmap; default env POD_NAMESPACE, then the pod's own namespace")
		refuseUnpriced  = flag.String("refuse-unpriced-routes", "", "comma-separated Envoy route-name patterns that are always priced, per-segment globs (e.g. httproute/t-*/p-*): such a route missing from the price table is refused, not served free")
		minRemaining    = flag.Duration("min-validity-remaining", 30*time.Second, "reject authorizations that expire sooner than this")
		redisAddr       = flag.String("redis", os.Getenv("X402_REDIS"), "Redis address or URL for shared payer state (env X402_REDIS); empty = in-memory, single replica")
		readTimeout     = flag.Duration("chain-read-timeout", 300*time.Millisecond, "budget for the synchronous balance read of an unknown payer, across all state sources (hedged)")
		warmEvery       = flag.Duration("rpc-keep-warm", 20*time.Second, "call every -rpc endpoint this often so a connection stays open and a cold read does not spend -chain-read-timeout on connection setup; 0 disables")
		metricsAddr     = flag.String("metrics", ":9102", "listen address for /metrics and /healthz; empty disables")
		optional        = flag.Bool("config-optional", false, "start with no priced routes when the price table file does not exist yet (the controller writes it)")
		reloadEvery     = flag.Duration("reload-interval", 5*time.Second, "how often to check the price table file for changes")
		confirmations   = flag.Uint64("credit-confirmations", 0, "blocks a balance increase must age before it counts (reorg protection); decreases count at once")
		hostedURL       = flag.String("hosted-state", os.Getenv("X402_HOSTED_STATE"), "URL of the hosted payer-state service (env X402_HOSTED_STATE). Preferred over -rpc, which then is the fallback")
		hostedKeyFile   = flag.String("hosted-state-key-file", "", "file holding the hosted service API key (or env X402_HOSTED_STATE_KEY)")
		rpcs            = cliutil.RPCFlag{}
		factories       = flag.String("eip6492-factories", "", "comma-separated smart-wallet factories allowed to deploy an ERC-6492 wallet during verification (simulated only); default: Coinbase Smart Wallet v1.1 and v1")
		contractRate    = flag.Float64("contract-wallet-checks-per-second", 10, "RPC calls a second that contract-wallet signature checks may make in this replica, across all payers (bursts of twice that); past it such payments get 503 and may retry. Payer-key signatures never use it")
		contractTimeout = flag.Duration("contract-wallet-timeout", time.Second, "how long a contract-wallet signature check (EIP-1271 eth_call) may take; only payments whose signature is not the payer's own key wait on it")
	)
	flag.Var(rpcs, "rpc", "chain RPC endpoints as network=url[,url...]; repeatable. Networks without one get no balance checks")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var table *pricing.Table
	var tables *configMapTables
	if *configMap != "" {
		var err error
		if tables, err = startConfigMapTables(ctx, log, *configNS, *configMap, *configKey); err != nil {
			log.Error("watch price table ConfigMap", "err", err)
			os.Exit(1)
		}
		table = tables.initial
	} else {
		var err error
		table, err = pricing.Load(*configPath)
		if err != nil && *optional && errors.Is(err, os.ErrNotExist) {
			log.Warn("price table file does not exist yet; no route is priced until it appears", "path", *configPath)
			table, err = pricing.Empty(), nil
		}
		if err != nil {
			log.Error("load price table", "err", err)
			os.Exit(1)
		}
	}

	m := metrics.NewFilter()
	var state payerstate.PayerState
	var stateRedis redis.UniversalClient // batch-settlement channels live next to payer state
	watchState := func(context.Context) {}
	keepWarm := func(context.Context) {}
	if *redisAddr == "" {
		// Nonce replay protection for a single replica; no balance checks,
		// no exposure cap, and nothing is queued for settlement.
		state = payerstate.NewMemory()
		log.Warn("no -redis: in-memory payer state, single replica only, payments are never settled")
	} else {
		rdb, err := cliutil.Redis(context.Background(), *redisAddr)
		if err != nil {
			log.Error("connect to redis", "err", err)
			os.Exit(1)
		}
		var sources []statesource.Source
		served := map[string]bool{}
		if *hostedURL != "" {
			key := os.Getenv("X402_HOSTED_STATE_KEY")
			if *hostedKeyFile != "" {
				b, err := os.ReadFile(*hostedKeyFile)
				if err != nil {
					log.Error("read hosted state key", "err", err)
					os.Exit(1)
				}
				key = strings.TrimSpace(string(b))
			}
			if key == "" {
				log.Error("-hosted-state needs an API key: -hosted-state-key-file or X402_HOSTED_STATE_KEY")
				os.Exit(1)
			}
			hosted, err := statesource.NewHosted(*hostedURL, key)
			if err != nil {
				log.Error("configure hosted state", "err", err)
				os.Exit(1)
			}
			sources = append(sources, hosted)
			if len(rpcs) == 0 {
				// Allowed, but then the hosted service's uptime is ours.
				log.Warn("hosted state source without an -rpc fallback: if the hosted service is unreachable, priced requests are refused")
			}
			for _, n := range table.Networks() {
				served[n] = true // assumed; an unserved chain is discovered on first use
			}
		}
		rpcSource, err := rpcs.Source(*readTimeout, statesource.RPCOptions{Confirmations: *confirmations})
		if err != nil {
			log.Error("configure rpc", "err", err)
			os.Exit(1)
		}
		if rpcSource != nil {
			sources = append(sources, rpcSource)
			if *warmEvery > 0 {
				keepWarm = func(ctx context.Context) { rpcSource.KeepWarm(ctx, *warmEvery) }
			}
			for _, n := range rpcSource.Chains() {
				served[n] = true
			}
		}
		for _, network := range table.Networks() {
			if !served[network] {
				log.Warn("no state source for network: payer balances are not checked there", "network", network)
			}
		}
		stateRedis = rdb
		r := payerstate.NewRedis(rdb, sources...)
		r.ReadTimeout = *readTimeout
		r.OnStateRead = m.StateRead
		r.Log = log
		watchState = r.Run
		state = r
	}

	// Contract wallets (EIP-1271): a signature that is not the payer's own
	// key is checked with the payer contract over the same RPC endpoints.
	var contract verify.ContractChecker
	var channels *payerstate.Channels
	if len(rpcs) > 0 {
		readers, err := rpcs.Readers(*contractTimeout)
		if err != nil {
			log.Error("configure rpc", "err", err)
			os.Exit(1)
		}
		cc := &verify.ChainChecker{Chains: map[string]verify.Caller{}, Timeout: *contractTimeout,
			PerSecond: *contractRate, Burst: int(2 * *contractRate)}
		for _, f := range strings.Split(*factories, ",") {
			if f = strings.TrimSpace(f); f == "" {
				continue
			}
			a, err := evm.ParseAddress(f)
			if err != nil {
				log.Error("-eip6492-factories", "err", err)
				os.Exit(1)
			}
			cc.AllowedFactories = append(cc.AllowedFactories, a)
		}
		chans := map[string]payerstate.Caller{}
		for network, r := range readers {
			if c, ok := r.(verify.Caller); ok {
				cc.Chains[network] = c
				chans[network] = c
			}
		}
		contract = cc
		// Batch-settlement vouchers: channel state in Redis, mirrored from
		// the chain. Without Redis (memory state) vouchers are refused.
		if stateRedis != nil {
			channels = payerstate.NewChannels(stateRedis, payerstate.ChainChannelReader(chans))
		}
	}
	var refuse []string
	for _, p := range strings.Split(*refuseUnpriced, ",") {
		if p = strings.TrimSpace(p); p != "" {
			refuse = append(refuse, p)
		}
	}
	srv := extproc.NewServer(table, state, extproc.Options{Logger: log, MinRemaining: *minRemaining, Metrics: m, Contract: contract, Channels: channels, RefuseUnpriced: refuse})

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(g, srv)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(g, hs)

	go watchState(ctx)
	keepWarm(ctx)
	if *metricsAddr != "" {
		if err := metrics.Serve(ctx, *metricsAddr, m.Registry, log); err != nil {
			log.Error("metrics listen", "err", err)
			os.Exit(1)
		}
	}

	// The price table is reloaded when the ConfigMap or the file changes (or
	// on SIGHUP for the file). A bad table keeps the current one.
	if tables != nil {
		tables.follow(srv.SetTable)
	} else {
		watchConfigFile(ctx, log, srv, *configPath, *reloadEvery)
	}

	go func() {
		<-ctx.Done()
		hs.Shutdown()
		g.GracefulStop()
	}()

	log.Info("x402-filter listening", "addr", lis.Addr().String(), "routes", srv.Table().Len())
	if err := g.Serve(lis); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

func watchConfigFile(ctx context.Context, log *slog.Logger, srv *extproc.Server, configPath string, every time.Duration) {
	reload := func() {
		t, err := pricing.Load(configPath)
		if err != nil {
			log.Error("reload price table; keeping current", "err", err)
			return
		}
		srv.SetTable(t)
		log.Info("price table reloaded", "routes", t.Len())
	}
	cliutil.WatchFile(ctx, configPath, every, reload)
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			reload()
		}
	}()
}
