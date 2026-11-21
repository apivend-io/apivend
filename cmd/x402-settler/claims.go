package main

import (
	"encoding/hex"
	"errors"
	"log/slog"
	"math/big"
	"os"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/redis/go-redis/v9"

	"apivend.io/apivend/engine/chain"
	"apivend.io/apivend/engine/payerstate"
	"apivend.io/apivend/internal/settle"
)

// claimer builds the batch-settlement claim job: our claim key from a file
// (a mounted Secret), channel state in the shared Redis, channels read
// through the same RPC endpoints.
func claimer(keyFile, minAmount string, rdb redis.UniversalClient, readers map[string]chain.Reader, s *settle.Settler, log *slog.Logger) (*settle.Claimer, error) {
	raw, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	b, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(string(raw)), "0x"))
	if err != nil || len(b) != 32 {
		return nil, errors.New("claim key file does not hold a 32-byte hex key")
	}
	min, ok := new(big.Int).SetString(minAmount, 10)
	if !ok || min.Sign() < 0 {
		return nil, errors.New("-claim-min-amount is not a non-negative integer")
	}
	callers := map[string]payerstate.Caller{}
	for network, r := range readers {
		if c, ok := r.(payerstate.Caller); ok {
			callers[network] = c
		}
	}
	return &settle.Claimer{
		Settler: s, Channels: payerstate.NewChannels(rdb, payerstate.ChainChannelReader(callers)),
		Key: secp256k1.PrivKeyFromBytes(b), MinAmount: min, Log: log,
	}, nil
}
