package statesource

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/big"
	"net/url"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"apivend.io/apivend/api/statepb"
)

// Hosted is the client for the hosted payer-state service. The service only
// supplies chain facts; if it is unreachable the engine moves on to the next
// source in its list.
type Hosted struct {
	conn   *grpc.ClientConn
	client statepb.PayerStateClient
	key    string
}

// NewHosted connects to the service at endpoint, an https:// URL (TLS) or an
// http:// URL (plaintext, for in-cluster or local use). The connection is
// opened at once and kept open while idle: a first call that had to connect
// spent the filter's whole read budget on the dev cluster.
func NewHosted(endpoint, apiKey string, extra ...grpc.DialOption) (*Hosted, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return nil, errors.New("statesource: hosted endpoint must be an http(s) URL")
	}
	var creds credentials.TransportCredentials
	host := u.Host
	switch u.Scheme {
	case "https":
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
		if u.Port() == "" {
			host += ":443"
		}
	case "http":
		creds = insecure.NewCredentials()
		if u.Port() == "" {
			host += ":80"
		}
	default:
		return nil, errors.New("statesource: hosted endpoint must be an http(s) URL")
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		// Never drop to idle (gRPC's default after 30 min), which would put
		// connection setup back on the request path.
		grpc.WithIdleTimeout(0),
	}
	conn, err := grpc.NewClient(host, append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("statesource: hosted: %w", err)
	}
	conn.Connect()
	return &Hosted{conn: conn, client: statepb.NewPayerStateClient(conn), key: apiKey}, nil
}

func (h *Hosted) Name() string { return "hosted" }

func (h *Hosted) Close() error { return h.conn.Close() }

func (h *Hosted) auth(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+h.key)
}

// mapErr turns "you asked for a chain we do not serve" into ErrUnsupported
// so the engine tries the next source without counting this one as broken.
func mapErr(err error) error {
	if status.Code(err) == codes.InvalidArgument {
		return ErrUnsupported
	}
	return err
}

func snapshotFromProto(balance string, o *statepb.Observation) (PayerSnapshot, error) {
	out := PayerSnapshot{BlockHeight: o.GetBlockHeight(), BlockHash: o.GetBlockHash()}
	if o.GetObservedAt() != nil {
		out.ObservedAt = o.GetObservedAt().AsTime()
	}
	if balance != "" {
		v, ok := new(big.Int).SetString(balance, 10)
		if !ok || v.Sign() < 0 {
			return out, errors.New("statesource: hosted service returned a malformed balance")
		}
		out.Balance = v
	}
	return out, nil
}

func (h *Hosted) Snapshot(ctx context.Context, chain, asset, payer string) (PayerSnapshot, error) {
	resp, err := h.client.Snapshot(h.auth(ctx), &statepb.SnapshotRequest{Chain: chain, Asset: asset, Payer: payer})
	if err != nil {
		return PayerSnapshot{}, mapErr(err)
	}
	snap, err := snapshotFromProto(resp.GetBalance(), resp.GetObservation())
	if err == nil && snap.Balance == nil {
		err = errors.New("statesource: hosted service returned no balance")
	}
	return snap, err
}

func (h *Hosted) AuthorizationState(ctx context.Context, chain, asset, payer, nonce string) (AuthState, error) {
	resp, err := h.client.AuthorizationState(h.auth(ctx), &statepb.AuthorizationStateRequest{Chain: chain, Asset: asset, Payer: payer, Nonce: nonce})
	if err != nil {
		return AuthUnused, mapErr(err)
	}
	switch resp.GetState() {
	case statepb.AuthState_AUTH_STATE_UNUSED:
		return AuthUnused, nil
	case statepb.AuthState_AUTH_STATE_CANCELED:
		return AuthCanceled, nil
	case statepb.AuthState_AUTH_STATE_USED:
		return AuthUsed, nil
	default:
		// An answer we do not understand must not read as "unused".
		return AuthUnused, errors.New("statesource: hosted service returned an unknown authorization state")
	}
}

func (h *Hosted) Watch(ctx context.Context, sub Subscription) (<-chan StateUpdate, error) {
	stream, err := h.client.Watch(h.auth(ctx))
	if err != nil {
		return nil, mapErr(err)
	}
	if err := stream.Send(&statepb.WatchRequest{Chain: sub.Chain, Asset: sub.Asset}); err != nil {
		return nil, mapErr(err)
	}
	out := make(chan StateUpdate, 256)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case p, ok := <-sub.Payers:
				if !ok {
					stream.CloseSend()
					return
				}
				if stream.Send(&statepb.WatchRequest{Payers: []string{p}}) != nil {
					return
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			snap, err := snapshotFromProto(msg.GetBalance(), msg.GetObservation())
			if err != nil {
				return
			}
			u := StateUpdate{Chain: msg.GetChain(), Asset: msg.GetAsset(), Payer: msg.GetPayer(), Nonce: msg.GetNonce(),
				Balance: snap.Balance, BlockHeight: snap.BlockHeight, BlockHash: snap.BlockHash, ObservedAt: snap.ObservedAt}
			switch msg.GetKind() {
			case statepb.WatchResponse_KIND_BALANCE:
				if u.Balance == nil {
					return
				}
				u.Kind = UpdateBalance
			case statepb.WatchResponse_KIND_AUTH_USED:
				u.Kind = UpdateAuthUsed
			case statepb.WatchResponse_KIND_AUTH_CANCELED:
				u.Kind = UpdateAuthCanceled
			default:
				continue // a kind added later; ignore
			}
			select {
			case out <- u:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
