package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"apivend.io/apivend/internal/kubewatch"
	"apivend.io/apivend/internal/pricing"
)

// configMapTables turns the controller's ConfigMap into price tables.
//
// At startup a missing ConfigMap means no priced route yet (the controller
// has not written it, so no route has ext_proc either), and an invalid one
// is fatal. Once running, a deleted ConfigMap or an invalid table keeps the
// current table: routes stay priced rather than become free.
type configMapTables struct {
	log     *slog.Logger
	source  string
	initial *pricing.Table

	mu      sync.Mutex
	started bool
	set     func(*pricing.Table)
	pending *pricing.Table
}

func startConfigMapTables(ctx context.Context, log *slog.Logger, namespace, name, key string) (*configMapTables, error) {
	if namespace == "" {
		namespace = kubewatch.PodNamespace()
	}
	if namespace == "" {
		return nil, errors.New("-config-configmap needs -config-namespace or POD_NAMESPACE outside a pod")
	}
	client, err := kubewatch.Client()
	if err != nil {
		return nil, err
	}
	t := &configMapTables{log: log, source: namespace + "/" + name}
	var initErr error
	w := &kubewatch.ConfigMap{Client: client, Namespace: namespace, Name: name, Key: key,
		OnChange: func(value string, ok bool) {
			t.mu.Lock()
			first := !t.started
			t.started = true
			t.mu.Unlock()
			if first {
				t.initial, initErr = t.first(value, ok)
				return
			}
			t.update(value, ok)
		}}
	log.Info("reading price table from the Kubernetes API", "configmap", t.source, "key", key)
	if err := w.Start(ctx); err != nil {
		return nil, err
	}
	if initErr != nil {
		return nil, fmt.Errorf("%s: %w", t.source, initErr)
	}
	return t, nil
}

func (t *configMapTables) first(value string, ok bool) (*pricing.Table, error) {
	if !ok {
		t.log.Warn("price table ConfigMap does not exist yet; no route is priced until it appears", "configmap", t.source)
		return pricing.Empty(), nil
	}
	table, err := pricing.Parse([]byte(value))
	if err != nil {
		return nil, err
	}
	t.log.Info("price table loaded", "configmap", t.source, "routes", table.Len())
	return table, nil
}

func (t *configMapTables) update(value string, ok bool) {
	if !ok {
		t.log.Error("price table ConfigMap or its key is gone; keeping the current table", "configmap", t.source)
		return
	}
	table, err := pricing.Parse([]byte(value))
	if err != nil {
		t.log.Error("reload price table; keeping current", "configmap", t.source, "err", err)
		return
	}
	t.mu.Lock()
	if t.set == nil {
		t.pending = table
	} else {
		t.set(table)
	}
	t.mu.Unlock()
	t.log.Info("price table reloaded", "configmap", t.source, "routes", table.Len())
}

// follow sends every later table to set, starting with any that arrived
// before the server existed.
func (t *configMapTables) follow(set func(*pricing.Table)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.set = set
	if t.pending != nil {
		set(t.pending)
		t.pending = nil
	}
}
