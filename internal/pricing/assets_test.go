package pricing

import (
	"encoding/hex"
	"math/big"
	"testing"

	"apivend.io/apivend/engine/evm"
)

// TestDomainSeparatorsMatchChain pins each asset's EIP-712 domain to the
// DOMAIN_SEPARATOR() its contract returned on chain. If a domain field in the
// asset table is wrong, every signature for that asset fails to verify.
//
// Values read with eth_call on 2026-11-17 from https://mainnet.base.org and
// https://sepolia.base.org (selector 0x3644e515). The same calls returned
// name() "USD Coin" / "USDC" and version() "2".
func TestDomainSeparatorsMatchChain(t *testing.T) {
	onChain := map[string]string{
		"base":         "02fa7265e7c5d81118673727957699e4d68f74cd74b7db77da710fe8a2c7834f",
		"base-sepolia": "71f17a3b2ff373b803d70a5a07c046c1a2bc8e89c09ef722fcb047abe94c9818",
	}
	for network, want := range onChain {
		a, err := lookupAsset(network, "USDC")
		if err != nil {
			t.Fatal(err)
		}
		d := evm.Domain{
			Name:              a.domainName,
			Version:           a.domainVersion,
			ChainID:           big.NewInt(a.chainID),
			VerifyingContract: a.contract,
		}
		got := d.Separator()
		if hex.EncodeToString(got[:]) != want {
			t.Errorf("%s USDC: separator %x, contract returns %s", network, got, want)
		}
	}
}
