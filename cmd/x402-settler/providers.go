package main

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"sync/atomic"
	"time"

	"apivend.io/apivend/internal/kubewatch"
	"apivend.io/apivend/internal/metrics"
	"apivend.io/apivend/internal/settle"
)

// watchProvidersSecret keeps current in step with the providers Secret,
// read through the Kubernetes API. An invalid update keeps the last good
// providers; a missing Secret (or key) means no providers, so jobs wait.
func watchProvidersSecret(ctx context.Context, log *slog.Logger, namespace, name, key string, current *atomic.Pointer[map[string][]settle.Backend], opts ...settle.Option) error {
	if namespace == "" {
		namespace = kubewatch.PodNamespace()
	}
	if namespace == "" {
		return errors.New("-providers-secret needs -providers-namespace or POD_NAMESPACE outside a pod")
	}
	client, err := kubewatch.Client()
	if err != nil {
		return err
	}
	w := &kubewatch.Secret{Client: client, Namespace: namespace, Name: name, Key: key,
		OnChange: func(value string, ok bool) {
			if !ok {
				empty := map[string][]settle.Backend{}
				current.Store(&empty)
				log.Warn("providers Secret or key missing; settlements wait until it appears", "secret", namespace+"/"+name, "key", key)
				return
			}
			next, err := settle.ParseProviders([]byte(value), 15*time.Second, opts...)
			if err != nil {
				log.Error("providers Secret is invalid; keeping current", "secret", namespace+"/"+name, "err", err)
				if current.Load() == nil {
					empty := map[string][]settle.Backend{}
					current.Store(&empty)
				}
				return
			}
			current.Store(&next)
			log.Info("providers loaded", "source", namespace+"/"+name, "providers", len(next))
			logDirect(log, next)
		}}
	log.Info("reading providers from the Kubernetes API", "secret", namespace+"/"+name, "key", key)
	return w.Start(ctx)
}

// logDirect names the gas address of each direct backend: it needs a little
// ETH on every network it settles.
func logDirect(log *slog.Logger, providers map[string][]settle.Backend) {
	for name, bs := range providers {
		for _, b := range bs {
			if d, ok := b.(*settle.Direct); ok {
				log.Info("direct settlement enabled; keep the gas address funded", "provider", name, "gas_address", d.From().String())
			}
		}
	}
}

// watchGas reads every direct backend's gas balance once a minute into
// x402_settle_gas_balance_wei, so an alert fires before settlement stops.
func watchGas(ctx context.Context, current *atomic.Pointer[map[string][]settle.Backend], m *metrics.Settler) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		if p := current.Load(); p != nil {
			for _, backends := range *p {
				for _, b := range backends {
					if d, ok := b.(*settle.Direct); ok {
						cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
						for network, wei := range d.GasBalances(cctx) {
							f, _ := new(big.Float).SetInt(wei).Float64()
							m.GasBalance(d.Name(), network, f)
						}
						cancel()
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
