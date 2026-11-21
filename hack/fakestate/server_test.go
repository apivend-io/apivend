package main

import (
	"context"
	"errors"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"apivend.io/apivend/api/statepb"
	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/statesource"
)

const (
	network = "eip155:84532"
	usdc    = "0x036cbd53842c5426634e7929541ec2318f3dcf7e"
	alice   = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"
)

// The gateway's real hosted-source client, talking gRPC to fakestate, which
// reads a fake chain: the stand-in must answer exactly as the client expects.
func TestHostedClientAgainstFakestate(t *testing.T) {
	aliceAddr, _ := evm.ParseAddress(alice)
	fake := chain.NewFake()
	fake.SetBalance(aliceAddr, 5_000)
	src := statesource.NewRPC("rpc", map[string]statesource.ChainClient{network: fake}, statesource.RPCOptions{PollInterval: 20 * time.Millisecond})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	statepb.RegisterPayerStateServer(g, &server{src: src, key: "x402s_test"})
	go g.Serve(lis)
	defer g.Stop()
	url := "http://" + lis.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Wrong key: refused, never answered.
	bad, _ := statesource.NewHosted(url, "wrong")
	if _, err := bad.Snapshot(ctx, network, usdc, alice); err == nil {
		t.Fatal("snapshot with a wrong API key succeeded")
	}

	h, err := statesource.NewHosted(url, "x402s_test")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	snap, err := h.Snapshot(ctx, network, usdc, alice)
	if err != nil || snap.Balance.Int64() != 5_000 || snap.BlockHeight == 0 || snap.BlockHash == "" || snap.ObservedAt.IsZero() {
		t.Fatalf("snapshot = %+v, %v", snap, err)
	}
	if _, err := h.Snapshot(ctx, "eip155:1", usdc, alice); !errors.Is(err, statesource.ErrUnsupported) {
		t.Fatalf("unserved chain: err = %v, want ErrUnsupported (so the gateway tries its next source)", err)
	}

	nonce := "0x07" + strings.Repeat("0", 62)
	if st, err := h.AuthorizationState(ctx, network, usdc, alice, nonce); err != nil || st != statesource.AuthUnused {
		t.Fatalf("fresh nonce = %v, %v", st, err)
	}

	payers := make(chan string, 1)
	updates, err := h.Watch(ctx, statesource.Subscription{Chain: network, Asset: usdc, Payers: payers})
	if err != nil {
		t.Fatal(err)
	}
	payers <- alice
	waitFor := func(kind statesource.UpdateKind, ok func(statesource.StateUpdate) bool) {
		t.Helper()
		for {
			select {
			case u, open := <-updates:
				if !open {
					t.Fatalf("watch ended while waiting for kind %d", kind)
				}
				if u.Kind == kind && ok(u) {
					return
				}
			case <-ctx.Done():
				t.Fatalf("no update of kind %d", kind)
			}
		}
	}
	waitFor(statesource.UpdateBalance, func(u statesource.StateUpdate) bool { return u.Balance.Int64() == 5_000 })

	// Spent elsewhere: the watch reports the nonce, then the lower balance;
	// a direct query agrees.
	if err := fake.Transfer(aliceAddr, [32]byte{7}, big.NewInt(1_500), "0xtx"); err != nil {
		t.Fatal(err)
	}
	waitFor(statesource.UpdateAuthUsed, func(u statesource.StateUpdate) bool { return u.Nonce == nonce })
	waitFor(statesource.UpdateBalance, func(u statesource.StateUpdate) bool { return u.Balance.Int64() == 3_500 })
	if st, err := h.AuthorizationState(ctx, network, usdc, alice, nonce); err != nil || st == statesource.AuthUnused {
		t.Fatalf("used nonce = %v, %v", st, err)
	}
}
