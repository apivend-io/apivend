package settle

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"

	"apivend.io/apivend/engine/evm"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/receipts"
	"apivend.io/apivend/internal/x402"
)

// Claimer turns batch-settlement charges into money for the seller
// (docs/BATCH-SETTLEMENT.md, B3). On every tick it lists the channels with
// unclaimed charges, signs one ClaimBatch per (network, provider, receiver,
// token) with our receiver authorizer key, has a backend submit
// claimWithSignature, then settle(receiver, token), which transfers the
// claimed funds to the seller. Any backend that lists batch-settlement can
// relay our signature, so the settler's failover applies.
//
// Vouchers left unclaimed when a payer's timed withdrawal completes are
// lost; Interval must stay well below the withdrawal delay (at least 15
// minutes on chain), and a channel with a withdrawal pending is claimed on
// the next tick whatever its amount.
type Claimer struct {
	Settler  *Settler // backends, health, observer
	Channels *payerstate.Channels
	// Key signs claims; its address must be the receiverAuthorizer the
	// controller advertises.
	Key *secp256k1.PrivateKey
	// Interval between claim rounds. Default 5 minutes.
	Interval time.Duration
	// MinAmount: a channel is claimed once its unclaimed charges reach this
	// (atomic units), or a withdrawal is pending, or it waited MaxWait.
	MinAmount *big.Int
	MaxWait   time.Duration // default 1 hour
	// MaxRows per claim transaction. Default 50.
	MaxRows int
	Log     *slog.Logger

	waiting map[string]time.Time // channel -> first seen unclaimed
}

// Address is the receiver authorizer address for the key.
func (c *Claimer) Address() evm.Address { return evm.PubkeyAddress(c.Key.PubKey()) }

// Run claims until ctx ends.
func (c *Claimer) Run(ctx context.Context) {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Minute
	}
	t := time.NewTicker(c.Interval)
	defer t.Stop()
	receipts := time.NewTicker(5 * time.Second)
	defer receipts.Stop()
	for {
		if err := c.Round(ctx); err != nil {
			c.Log.Warn("claim round failed; will retry", "err", err)
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-receipts.C:
				c.RecordCharges(ctx)
				continue
			case <-t.C:
			}
			break
		}
	}
}

// RecordCharges writes a batch_charged receipt for each request paid by
// voucher since the last call: the per-request record, as settled is for
// exact payments. Revenue earned and backed by escrow; it moves on chain
// with the next claim (batch_claimed).
func (c *Claimer) RecordCharges(ctx context.Context) {
	for {
		charges, err := c.Channels.DrainCharges(ctx, 100)
		if err != nil || len(charges) == 0 {
			return
		}
		for i, ch := range charges {
			rc := receipts.Receipt{
				ReservationID: "charge:" + ch.Network + ":" + ch.ChannelID + ":" + ch.Cumulative, Status: StatusBatchCharged,
				RequestID: ch.RequestID, Route: ch.Route, Payer: ch.Payer, Amount: ch.Amount, Asset: ch.Token,
				Chain: ch.Network, Nonce: ch.ChannelID, VerifiedAt: ch.At, ServedAt: ch.At,
			}
			if err := c.Settler.Receipts.Record(ctx, rc); err != nil {
				c.Log.Warn("charge receipts not recorded; will retry", "err", err)
				c.Channels.Requeue(context.WithoutCancel(ctx), charges[i:])
				return
			}
		}
	}
}

// group is one claim transaction's worth of channels.
type group struct {
	network, provider string
	receiver, token   evm.Address
	rows              []payerstate.Claimable
}

// Round runs one claim round.
func (c *Claimer) Round(ctx context.Context) error {
	c.Settler.defaults()
	if c.Log == nil {
		c.Log = c.Settler.Log
	}
	if c.MaxRows <= 0 {
		c.MaxRows = 50
	}
	if c.MaxWait <= 0 {
		c.MaxWait = time.Hour
	}
	if c.waiting == nil {
		c.waiting = map[string]time.Time{}
	}
	list, err := c.Channels.Unclaimed(ctx)
	if err != nil {
		return err
	}
	now := c.Settler.Now()
	me := c.Address()
	groups := map[string]*group{}
	var order []string
	for _, ch := range list {
		key := ch.Network + "|" + hex.EncodeToString(ch.ChannelID[:])
		if _, ok := c.waiting[key]; !ok {
			c.waiting[key] = now
		}
		if ch.Config.ReceiverAuthorizer != me {
			c.Log.Error("channel's receiver authorizer is not our claim key; cannot claim", "network", ch.Network,
				"channel", "0x"+hex.EncodeToString(ch.ChannelID[:]), "authorizer", ch.Config.ReceiverAuthorizer.Hex(), "ours", me.Hex())
			continue
		}
		unclaimed := new(big.Int).Sub(ch.Charged, ch.Chain.TotalClaimed)
		due := ch.Chain.WithdrawRequestedAt > 0 || now.Sub(c.waiting[key]) >= c.MaxWait ||
			c.MinAmount == nil || unclaimed.Cmp(c.MinAmount) >= 0
		if !due {
			continue
		}
		gk := strings.Join([]string{ch.Network, ch.Provider, ch.Config.Receiver.Hex(), ch.Config.Token.Hex()}, "|")
		g := groups[gk]
		if g == nil {
			g = &group{network: ch.Network, provider: ch.Provider, receiver: ch.Config.Receiver, token: ch.Config.Token}
			groups[gk] = g
			order = append(order, gk)
		}
		g.rows = append(g.rows, ch)
	}
	sort.Strings(order)
	for _, gk := range order {
		g := groups[gk]
		for start := 0; start < len(g.rows); start += c.MaxRows {
			part := *g
			part.rows = g.rows[start:min(start+c.MaxRows, len(g.rows))]
			if err := c.claim(ctx, &part); err != nil {
				c.Log.Warn("claim failed; retried next round", "network", g.network, "receiver", g.receiver.Hex(), "channels", len(part.rows), "err", err)
				continue
			}
			for _, ch := range part.rows {
				delete(c.waiting, ch.Network+"|"+hex.EncodeToString(ch.ChannelID[:]))
			}
		}
		c.payOut(ctx, g)
	}
	return nil
}

// requirements is the minimal PaymentRequirements the SDK's channel manager
// sends with claim and settle calls.
func batchRequirements(network string, receiver, token evm.Address) x402.Requirements {
	return x402.Requirements{Scheme: x402.SchemeBatch, Network: network, Asset: token.String(), Amount: "0",
		PayTo: receiver.String(), Extra: map[string]any{}}
}

type wireClaim struct {
	Voucher struct {
		Channel            map[string]any `json:"channel"`
		MaxClaimableAmount string         `json:"maxClaimableAmount"`
	} `json:"voucher"`
	Signature    string `json:"signature"`
	TotalClaimed string `json:"totalClaimed"`
}

func (c *Claimer) claim(ctx context.Context, g *group) error {
	chain, err := chainID(g.network)
	if err != nil {
		return err
	}
	var entries []evm.ClaimEntry
	var claims []wireClaim
	for _, ch := range g.rows {
		entries = append(entries, evm.ClaimEntry{ChannelID: ch.ChannelID, MaxClaimable: ch.Charged, TotalClaimed: ch.Charged})
		var w wireClaim
		w.Voucher.Channel = channelWire(ch.Config)
		w.Voucher.MaxClaimableAmount = ch.Charged.String()
		w.Signature = "0x" + hex.EncodeToString(ch.Signature)
		w.TotalClaimed = ch.Charged.String()
		claims = append(claims, w)
	}
	sig := evm.Sign(evm.ClaimBatchDigest(chain, entries), c.Key)
	inner, _ := json.Marshal(map[string]any{"type": "claim", "claims": claims, "claimAuthorizerSignature": "0x" + hex.EncodeToString(sig)})
	req := batchRequirements(g.network, g.receiver, g.token)
	job := &Job{Route: "batch-settlement", Provider: g.provider, Payload: x402.PaymentPayload{X402Version: x402.Version, Accepted: req, Payload: inner},
		Requirements: req, Kind: KindBatchClaim}
	res, backend := c.Settler.submit(ctx, job, c.Log)
	if res.Outcome != Settled {
		return fmt.Errorf("%s: %s", outcomeName(res.Outcome), res.Reason)
	}
	for _, ch := range g.rows {
		delta := new(big.Int).Sub(ch.Charged, ch.Chain.TotalClaimed)
		if err := c.Channels.MarkClaimed(ctx, ch.Network, ch.ChannelID, ch.Charged); err != nil {
			c.Log.Error("claimed but not recorded; the next round reads the chain", "tx", res.Transaction, "err", err)
		}
		id := "0x" + hex.EncodeToString(ch.ChannelID[:])
		rc := receipts.Receipt{
			ReservationID: "claim:" + ch.Network + ":" + id + ":" + ch.Charged.String(), Status: StatusBatchClaimed,
			Route: ch.Route, Payer: ch.Config.Payer.Hex(), Amount: delta.String(), Asset: ch.Config.Token.Hex(),
			Chain: ch.Network, Nonce: id, Provider: backend, TxHash: res.Transaction,
		}
		if err := c.Settler.Receipts.Record(ctx, rc); err != nil {
			c.Log.Warn("claim receipt not recorded", "tx", res.Transaction, "err", err)
		}
		c.Settler.Observer.Finished(ch.Route, ch.Network, ch.Config.Token.Hex(), StatusBatchClaimed, "", delta.String())
	}
	c.Log.Info("channels claimed", "network", g.network, "receiver", g.receiver.Hex(), "channels", len(g.rows), "tx", res.Transaction, "provider", backend)
	return nil
}

// payOut transfers claimed funds to the seller: settle(receiver, token) is
// permissionless and sweeps everything claimed and not yet paid out.
func (c *Claimer) payOut(ctx context.Context, g *group) {
	inner, _ := json.Marshal(map[string]any{"type": "settle", "receiver": g.receiver.String(), "token": g.token.String()})
	req := batchRequirements(g.network, g.receiver, g.token)
	job := &Job{Route: "batch-settlement", Provider: g.provider, Payload: x402.PaymentPayload{X402Version: x402.Version, Accepted: req, Payload: inner},
		Requirements: req, Kind: KindBatchPayout}
	res, backend := c.Settler.submit(ctx, job, c.Log)
	switch res.Outcome {
	case Settled:
		c.Log.Info("claimed funds paid out to the seller", "network", g.network, "receiver", g.receiver.Hex(), "tx", res.Transaction, "provider", backend)
	default:
		// Nothing lost: claimed funds stay in the contract for the receiver;
		// the next round (or anyone) can sweep them.
		c.Log.Warn("payout not done; claimed funds wait in the contract", "network", g.network, "receiver", g.receiver.Hex(), "reason", res.Reason)
	}
}

func channelWire(cfg evm.ChannelConfig) map[string]any {
	return map[string]any{
		"payer": cfg.Payer.String(), "payerAuthorizer": cfg.PayerAuthorizer.String(), "receiver": cfg.Receiver.String(),
		"receiverAuthorizer": cfg.ReceiverAuthorizer.String(), "token": cfg.Token.String(),
		"withdrawDelay": cfg.WithdrawDelay, "salt": "0x" + hex.EncodeToString(cfg.Salt[:]),
	}
}

func chainID(network string) (*big.Int, error) {
	n, ok := strings.CutPrefix(network, "eip155:")
	if !ok {
		return nil, fmt.Errorf("not an EVM network: %s", network)
	}
	id, err := strconv.ParseInt(n, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("bad chain id in %s", network)
	}
	return big.NewInt(id), nil
}

// PrepareRefund completes a refund job (scheme_batch_settlement_evm.md,
// cooperative refund): the client's zero-charge voucher becomes the claim
// of everything charged, then our ClaimBatch and Refund signatures are
// added, with the refund nonce read from the chain. The facilitator submits
// claimWithSignature then refundWithSignature.
func (c *Claimer) PrepareRefund(ctx context.Context, job *Job) error {
	var bp x402.BatchEVMPayload
	if err := json.Unmarshal(job.Payload.Payload, &bp); err != nil || bp.Type != "refund" {
		return fmt.Errorf("not a refund payload")
	}
	cfg, err := wireConfig(bp.ChannelConfig)
	if err != nil {
		return err
	}
	chain, err := chainID(job.Requirements.Network)
	if err != nil {
		return err
	}
	id := cfg.ChannelID(chain)
	ch, err := c.Channels.Read(ctx, job.Requirements.Network, id)
	if err != nil {
		return fmt.Errorf("reading channel: %w", err)
	}
	max, ok := new(big.Int).SetString(bp.Voucher.MaxClaimableAmount, 10)
	amount, ok2 := new(big.Int).SetString(job.RefundAmount, 10)
	if !ok || !ok2 {
		return fmt.Errorf("bad refund amounts")
	}
	out := map[string]any{
		"type": "refund", "channelConfig": channelWire(cfg), "voucher": bp.Voucher,
		"amount": amount.String(), "refundNonce": ch.RefundNonce.String(),
	}
	claims := []wireClaim{}
	if max.Cmp(ch.TotalClaimed) > 0 {
		var w wireClaim
		w.Voucher.Channel = channelWire(cfg)
		w.Voucher.MaxClaimableAmount = max.String()
		w.Signature = bp.Voucher.Signature
		w.TotalClaimed = max.String()
		claims = append(claims, w)
		sig := evm.Sign(evm.ClaimBatchDigest(chain, []evm.ClaimEntry{{ChannelID: id, MaxClaimable: max, TotalClaimed: max}}), c.Key)
		out["claimAuthorizerSignature"] = "0x" + hex.EncodeToString(sig)
	}
	out["claims"] = claims
	sig := evm.Sign(evm.RefundDigest(chain, id, ch.RefundNonce, amount), c.Key)
	out["refundAuthorizerSignature"] = "0x" + hex.EncodeToString(sig)
	raw, err := json.Marshal(out)
	if err != nil {
		return err
	}
	job.Payload.Payload = raw
	return nil
}

func wireConfig(cc x402.BatchChannelConfig) (evm.ChannelConfig, error) {
	cfg := evm.ChannelConfig{WithdrawDelay: cc.WithdrawDelay}
	var err error
	for _, f := range []struct {
		dst *evm.Address
		src string
	}{{&cfg.Payer, cc.Payer}, {&cfg.PayerAuthorizer, cc.PayerAuthorizer}, {&cfg.Receiver, cc.Receiver},
		{&cfg.ReceiverAuthorizer, cc.ReceiverAuthorizer}, {&cfg.Token, cc.Token}} {
		if *f.dst, err = evm.ParseAddress(f.src); err != nil {
			return cfg, err
		}
	}
	cfg.Salt, err = evm.ParseBytes32(cc.Salt)
	return cfg, err
}
