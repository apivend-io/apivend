// Package cliutil holds flag handling shared by the binaries.
package cliutil

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/statesource"
)

// networkAliases lets operators write the short names used in policies.
var networkAliases = map[string]string{
	"base":         "eip155:8453",
	"base-sepolia": "eip155:84532",
}

// RPCFlag collects repeated -rpc network=url[,url...] flags.
type RPCFlag map[string][]string

func (f RPCFlag) String() string {
	// Never print URLs: they usually embed provider API keys.
	names := make([]string, 0, len(f))
	for n := range f {
		names = append(names, n)
	}
	return strings.Join(names, ",")
}

func (f RPCFlag) Set(v string) error {
	name, urls, ok := strings.Cut(v, "=")
	if !ok || name == "" || urls == "" {
		return fmt.Errorf("want network=url[,url...]")
	}
	if alias, ok := networkAliases[strings.ToLower(name)]; ok {
		name = alias
	}
	if !strings.Contains(name, ":") {
		return fmt.Errorf("unknown network %q (use base, base-sepolia or a CAIP-2 id)", name)
	}
	for _, u := range strings.Split(urls, ",") {
		if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
			return fmt.Errorf("RPC endpoint for %s must be an http(s) URL", name)
		}
		f[name] = append(f[name], u)
	}
	return nil
}

// Readers builds one chain reader per configured network.
func (f RPCFlag) Readers(perCall time.Duration) (map[string]chain.Reader, error) {
	out := map[string]chain.Reader{}
	for name, urls := range f {
		r, err := chain.NewRPC(urls, perCall)
		if err != nil {
			return nil, err
		}
		out[name] = r
	}
	return out, nil
}

// Source builds the direct-RPC state source over the configured networks, or
// nil when none is configured.
func (f RPCFlag) Source(perCall time.Duration, opt statesource.RPCOptions) (*statesource.RPC, error) {
	if len(f) == 0 {
		return nil, nil
	}
	clients := map[string]statesource.ChainClient{}
	for name, urls := range f {
		r, err := chain.NewRPC(urls, perCall)
		if err != nil {
			return nil, err
		}
		clients[name] = r
	}
	return statesource.NewRPC("rpc", clients, opt), nil
}

// Redis connects to a Redis URL (redis://[:password@]host:port/db) or a bare
// host:port, and checks that it answers.
func Redis(ctx context.Context, addr string) (*redis.Client, error) {
	opts := &redis.Options{Addr: addr}
	if strings.Contains(addr, "://") {
		var err error
		if opts, err = redis.ParseURL(addr); err != nil {
			return nil, fmt.Errorf("redis: invalid URL")
		}
	}
	// The request path has a 200 ms budget; never wait longer than that.
	opts.DialTimeout = time.Second
	opts.ReadTimeout = 150 * time.Millisecond
	opts.WriteTimeout = 150 * time.Millisecond
	rdb := redis.NewClient(opts)
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := rdb.Ping(pctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("redis: %w", err)
	}
	return rdb, nil
}

// WatchFile calls onChange whenever the content of path changes, checking
// every interval. Polling (not inotify) because a mounted ConfigMap or Secret
// is updated by swapping a symlink, which file watches miss.
func WatchFile(ctx context.Context, path string, interval time.Duration, onChange func()) {
	sum := func() ([32]byte, bool) {
		b, err := os.ReadFile(path)
		if err != nil {
			return [32]byte{}, false
		}
		return sha256.Sum256(b), true
	}
	last, _ := sum()
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if cur, ok := sum(); ok && cur != last {
					last = cur
					onChange()
				}
			}
		}
	}()
}
