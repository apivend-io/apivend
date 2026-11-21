package pricing

import (
	"fmt"
	"strings"

	"apivend.io/apivend/engine/evm"
)

type asset struct {
	caip2    string
	chainID  int64
	contract evm.Address
	decimals int
	// EIP-712 domain of the token contract. A wrong value here makes every
	// signature fail to verify, so these must match the deployed contract.
	domainName    string
	domainVersion string
	usdPegged     bool
}

// Known networks and assets.
//
// Both domains are pinned to the contracts' on-chain DOMAIN_SEPARATOR() by
// TestDomainSeparatorsMatchChain; Base Sepolia also by the x402
// specification's example signature (evm.TestSpecExampleVector).
var networks = map[string]map[string]asset{
	"base": {
		"USDC": {
			caip2:         "eip155:8453",
			chainID:       8453,
			contract:      mustAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
			decimals:      6,
			domainName:    "USD Coin",
			domainVersion: "2",
			usdPegged:     true,
		},
	},
	"base-sepolia": {
		"USDC": {
			caip2:         "eip155:84532",
			chainID:       84532,
			contract:      mustAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e"),
			decimals:      6,
			domainName:    "USDC",
			domainVersion: "2",
			usdPegged:     true,
		},
	},
}

func lookupAsset(network, symbol string) (asset, error) {
	assets, ok := networks[strings.ToLower(network)]
	if !ok {
		return asset{}, fmt.Errorf("unknown network %q", network)
	}
	a, ok := assets[strings.ToUpper(symbol)]
	if !ok {
		return asset{}, fmt.Errorf("unknown asset %q on %s", symbol, network)
	}
	return a, nil
}

func mustAddress(s string) evm.Address {
	a, err := evm.ParseAddress(s)
	if err != nil {
		panic(err)
	}
	// Catch transcription errors in the table above: literals must carry a
	// valid EIP-55 checksum.
	if a.String() != s {
		panic("address literal fails EIP-55 checksum: " + s)
	}
	return a
}
