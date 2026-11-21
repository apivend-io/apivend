package main

import (
	"context"
	"errors"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"apivend.io/apivend/api/statepb"
	"apivend.io/apivend/engine/statesource"
)

func sourceOptions(poll time.Duration) statesource.RPCOptions {
	return statesource.RPCOptions{PollInterval: poll}
}

// server answers the hosted service's API straight from a direct-RPC source.
type server struct {
	statepb.UnimplementedPayerStateServer
	src *statesource.RPC
	key string
}

func (s *server) auth(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		if strings.TrimPrefix(v, "Bearer ") == s.key {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "unknown API key")
}

func toStatus(err error) error {
	if errors.Is(err, statesource.ErrUnsupported) {
		// The client reads InvalidArgument as "chain not served here".
		return status.Error(codes.InvalidArgument, "chain is not served")
	}
	return status.Error(codes.Unavailable, "chain state unavailable")
}

func observation(height uint64, hash string, at time.Time, provider string) *statepb.Observation {
	return &statepb.Observation{BlockHeight: height, BlockHash: hash, ObservedAt: timestamppb.New(at), Provider: provider}
}

func (s *server) Snapshot(ctx context.Context, req *statepb.SnapshotRequest) (*statepb.SnapshotResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	snap, err := s.src.Snapshot(ctx, req.GetChain(), strings.ToLower(req.GetAsset()), req.GetPayer())
	if err != nil {
		return nil, toStatus(err)
	}
	return &statepb.SnapshotResponse{Balance: snap.Balance.String(),
		Observation: observation(snap.BlockHeight, snap.BlockHash, snap.ObservedAt, s.src.Name())}, nil
}

func (s *server) AuthorizationState(ctx context.Context, req *statepb.AuthorizationStateRequest) (*statepb.AuthorizationStateResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	st, obs, err := s.src.AuthorizationStateObserved(ctx, req.GetChain(), strings.ToLower(req.GetAsset()), req.GetPayer(), req.GetNonce())
	if err != nil {
		return nil, toStatus(err)
	}
	out := statepb.AuthState_AUTH_STATE_UNUSED
	switch st {
	case statesource.AuthUsed:
		out = statepb.AuthState_AUTH_STATE_USED
	case statesource.AuthCanceled:
		out = statepb.AuthState_AUTH_STATE_CANCELED
	}
	return &statepb.AuthorizationStateResponse{State: out,
		Observation: observation(obs.BlockHeight, obs.BlockHash, obs.ObservedAt, s.src.Name())}, nil
}

// Watch relays the direct-RPC source's watch: the first request names chain
// and asset, every request adds payers.
func (s *server) Watch(stream statepb.PayerState_WatchServer) error {
	ctx := stream.Context()
	if err := s.auth(ctx); err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	payers := make(chan string, 64)
	updates, err := s.src.Watch(ctx, statesource.Subscription{
		Chain: first.GetChain(), Asset: strings.ToLower(first.GetAsset()), Payers: payers})
	if err != nil {
		return toStatus(err)
	}
	add := func(req *statepb.WatchRequest) {
		for _, p := range req.GetPayers() {
			select {
			case payers <- p:
			case <-ctx.Done():
			}
		}
	}
	add(first)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				return
			}
			add(req)
		}
	}()
	for u := range updates {
		out := &statepb.WatchResponse{Chain: first.GetChain(), Asset: first.GetAsset(), Payer: u.Payer, Nonce: u.Nonce,
			Observation: observation(u.BlockHeight, u.BlockHash, u.ObservedAt, s.src.Name())}
		switch u.Kind {
		case statesource.UpdateBalance:
			out.Kind, out.Balance = statepb.WatchResponse_KIND_BALANCE, u.Balance.String()
		case statesource.UpdateAuthUsed:
			out.Kind = statepb.WatchResponse_KIND_AUTH_USED
		case statesource.UpdateAuthCanceled:
			out.Kind = statepb.WatchResponse_KIND_AUTH_CANCELED
		}
		if err := stream.Send(out); err != nil {
			return err
		}
	}
	// The upstream watch ended (chain unreadable): end the stream so the
	// gateway moves to its next source rather than trusting silence.
	return status.Error(codes.Unavailable, "upstream watch ended")
}
