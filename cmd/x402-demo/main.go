// x402-demo serves the public demo page: recent payments and 24-hour
// statistics from the receipts database, the live chain head, and the prices
// of the demo routes from the controller's price table. It holds no keys.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/internal/demo"
	"apivend.io/apivend/internal/kubewatch"
	"apivend.io/apivend/internal/pricing"
)

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	var routes listFlag
	listen := flag.String("listen", ":8090", "listen address")
	dsn := flag.String("postgres", os.Getenv("X402_POSTGRES"), "PostgreSQL DSN of the receipts database, ideally a read-only role (env X402_POSTGRES)")
	backend := flag.String("backend", "", "also serve the demo's own priced service (/v1/balance, /v1/hello) on this address")
	stateURL := flag.String("state-url", os.Getenv("X402_HOSTED_STATE"), "hosted payer-state HTTP base URL, for /v1/balance (env X402_HOSTED_STATE)")
	flag.Var(&routes, "route", "demo route as name=url (the URL must not contain '='; use -routes-json for anything else); repeatable")
	routesJSON := flag.String("routes-json", "", `demo routes as a JSON array of {"name","url","note","key"}; key is the HTTPRoute "namespace/name"`)
	headNetwork := flag.String("chain-head", "", "CAIP-2 network (e.g. eip155:8453) whose latest block the page shows; RPC URLs, comma-separated, come from env X402_HEAD_RPC")
	probeVia := flag.String("probe-via", "", "send the route check here (e.g. the gateway's in-cluster Service URL) with the public host as Host, instead of to the public URL")
	ownPayers := flag.String("own-payers", "", "comma-separated addresses of the operator's own test wallets: their receipts are labelled and the page counts payers other than them")
	priceCM := flag.String("price-configmap", "", "read route prices from this ConfigMap (the controller's x402-routes, key routes.yaml) in the pod's namespace, instead of checking each route over HTTP")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	srv := &demo.Server{ProbeVia: *probeVia}
	for _, a := range strings.Split(*ownPayers, ",") {
		if a = strings.TrimSpace(a); a != "" {
			srv.OwnPayers = append(srv.OwnPayers, strings.ToLower(a))
		}
	}
	if *priceCM != "" {
		prices, err := watchPrices(log, *priceCM)
		if err != nil {
			log.Error("price table", "err", err)
			os.Exit(1)
		}
		srv.Prices = prices
	}
	if *routesJSON != "" {
		if err := json.Unmarshal([]byte(*routesJSON), &srv.Routes); err != nil {
			log.Error("bad -routes-json", "err", err)
			os.Exit(2)
		}
	}
	for _, r := range routes {
		name, u, ok := strings.Cut(r, "=")
		if !ok || !strings.HasPrefix(u, "http") || strings.Contains(u, "=") {
			log.Error("bad -route; want name=url, or use -routes-json", "value", r)
			os.Exit(2)
		}
		srv.Routes = append(srv.Routes, demo.Route{Name: name, URL: u})
	}
	if *headNetwork != "" {
		var urls []string
		for _, u := range strings.Split(os.Getenv("X402_HEAD_RPC"), ",") {
			if u = strings.TrimSpace(u); u != "" {
				urls = append(urls, u)
			}
		}
		rpc, err := chain.NewRPC(urls, 2*time.Second)
		if err != nil {
			// Never log the URLs: they can embed provider keys.
			log.Error("-chain-head needs X402_HEAD_RPC with at least one valid RPC URL")
			os.Exit(2)
		}
		srv.Heads = map[string]demo.HeadReader{*headNetwork: rpc}
	}
	for _, r := range srv.Routes {
		if r.Name == "" || !strings.HasPrefix(r.URL, "http") {
			log.Error("a demo route needs a name and an http(s) URL", "name", r.Name)
			os.Exit(2)
		}
	}
	if *dsn != "" {
		// Opened directly, without creating the schema: this process should
		// run as a read-only database role.
		db, err := sql.Open("pgx", *dsn)
		if err != nil {
			log.Error("invalid postgres DSN")
			os.Exit(1)
		}
		defer db.Close()
		db.SetMaxOpenConns(4)
		srv.DB = db
	} else {
		log.Warn("no -postgres: the payment feed will be empty")
	}

	if *backend != "" {
		b := &demo.Backend{StateURL: strings.TrimRight(*stateURL, "/"), StateKey: os.Getenv("X402_HOSTED_STATE_KEY"), Client: &http.Client{Timeout: 5 * time.Second}}
		go func() {
			bs := &http.Server{Addr: *backend, Handler: b.Handler(), ReadHeaderTimeout: 5 * time.Second}
			if err := bs.ListenAndServe(); err != nil {
				log.Error("demo backend", "err", err)
				os.Exit(1)
			}
		}()
	}
	hs := &http.Server{Addr: *listen, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Info("x402-demo listening", "addr", *listen, "routes", len(srv.Routes), "price_table", *priceCM != "")
	if err := hs.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// watchPrices follows the price table through the Kubernetes API. A missing
// or invalid table shows every route as free (unpriced) or keeps the last good
// one: the page never shows a price the gateway would not charge.
func watchPrices(log *slog.Logger, name string) (func() *pricing.Table, error) {
	ns := kubewatch.PodNamespace()
	if ns == "" {
		return nil, fmt.Errorf("-price-configmap needs POD_NAMESPACE (run in a pod)")
	}
	client, err := kubewatch.Client()
	if err != nil {
		return nil, err
	}
	var current atomic.Pointer[pricing.Table]
	current.Store(pricing.Empty())
	w := &kubewatch.ConfigMap{Client: client, Namespace: ns, Name: name, Key: "routes.yaml",
		OnChange: func(value string, ok bool) {
			if !ok {
				current.Store(pricing.Empty())
				return
			}
			t, err := pricing.Parse([]byte(value))
			if err != nil {
				log.Warn("price table invalid; keeping the previous one", "configmap", ns+"/"+name, "err", err)
				return
			}
			current.Store(t)
		}}
	if err := w.Start(context.Background()); err != nil {
		return nil, err
	}
	return current.Load, nil
}
