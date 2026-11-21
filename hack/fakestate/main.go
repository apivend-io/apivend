// fakestate is a stand-in for the hosted payer-state service, built only from
// the open engine: it serves the hosted service's gRPC API (api/statepb) from
// a direct-RPC state source. It has no tenants, metering, tracking store or
// top-up. It exists so the gateway's hosted-source path can be developed and
// tested without the hosted service itself.
//
// It accepts one API key (-api-key) and prints it at startup in the same
// "api key: ..." form x402-state prints a demo key, so hack/e2e.sh can use
// either.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"apivend.io/apivend/api/statepb"
	"apivend.io/apivend/internal/cliutil"
)

func main() {
	var (
		listen = flag.String("grpc", "127.0.0.1:9443", "gRPC listen address")
		apiKey = flag.String("api-key", "x402s_fakestate", "the one API key accepted")
		poll   = flag.Duration("poll-interval", 2*time.Second, "how often watched payers are re-read")
		rpcs   = cliutil.RPCFlag{}
	)
	flag.Var(rpcs, "rpc", "chain RPC endpoints as network=url[,url...]; repeatable (e.g. hack/fakenode)")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	src, err := rpcs.Source(5*time.Second, sourceOptions(*poll))
	if err != nil || src == nil {
		log.Error("at least one valid -rpc network=url is required", "err", err)
		os.Exit(1)
	}
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer()
	statepb.RegisterPayerStateServer(g, &server{src: src, key: *apiKey})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		// Watch streams never end on their own, so a graceful stop alone
		// would wait for ever: give in-flight calls a moment, then cut.
		done := make(chan struct{})
		go func() { g.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			g.Stop()
		}
	}()

	log.Info("fakestate running", "grpc", lis.Addr().String(), "networks", src.Chains())
	// Same shape as x402-state's demo key line; this key is not a secret.
	os.Stderr.WriteString("api key: " + *apiKey + "\n")
	if err := g.Serve(lis); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
