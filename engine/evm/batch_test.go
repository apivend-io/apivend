package evm

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"testing"
)

// Reference values from the x402 Foundation Go SDK (v2.31.0,
// mechanisms/evm/batch-settlement): testdata/batch_sdk.json.
func TestBatchDigestsMatchSDK(t *testing.T) {
	raw, err := os.ReadFile("testdata/batch_sdk.json")
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]string
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	addr := func(s string) Address { return mustAddress(s) }
	var salt [32]byte
	salt[31] = 0xa7
	cfg := ChannelConfig{
		Payer: addr("0x11d0dcB977c973731215D7005665159D21faF0C5"), PayerAuthorizer: addr("0x11d0dcB977c973731215D7005665159D21faF0C5"),
		Receiver: addr("0x707522232dF81025b7A1893c104aCB0973863c7A"), ReceiverAuthorizer: addr("0x01d4000000000000000000000000000000006aE6"),
		Token: addr("0x036CbD53842c5426634e7929541eC2318f3dCF7e"), WithdrawDelay: 900, Salt: salt,
	}
	chain := big.NewInt(84532)
	h := func(b [32]byte) string { return "0x" + hex.EncodeToString(b[:]) }
	id := cfg.ChannelID(chain)
	var other [32]byte
	other[31] = 77
	nonce := DepositNonce(id, big.NewInt(0x2a))
	recv := Authorization{From: cfg.Payer, To: ERC3009DepositCollector, Value: big.NewInt(100000), ValidAfter: big.NewInt(0), ValidBefore: big.NewInt(1770000000), Nonce: nonce}
	got := map[string]string{
		"channelId":    h(id),
		"voucher":      h(VoucherDigest(chain, id, big.NewInt(5000))),
		"refund":       h(RefundDigest(chain, id, big.NewInt(3), big.NewInt(1500))),
		"claimBatch":   h(ClaimBatchDigest(chain, []ClaimEntry{{id, big.NewInt(5000), big.NewInt(4000)}, {other, big.NewInt(9), big.NewInt(9)}})),
		"depositNonce": h(nonce),
		"receive":      h(recv.ReceiveDigest(Domain{Name: "USDC", Version: "2", ChainID: chain, VerifyingContract: cfg.Token})),
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: got %s, SDK %s", k, got[k], w)
		}
	}
	// Another chain, another channel.
	if cfg.ChannelID(big.NewInt(8453)) == id {
		t.Error("channel id does not depend on the chain")
	}
}

func TestParseChannel(t *testing.T) {
	w := func(v int64) []byte { return word(big.NewInt(v)) }
	c, err := ParseChannel(append(w(100000), w(3200)...), append(w(0), w(1795000000)...), w(2))
	if err != nil {
		t.Fatal(err)
	}
	if c.Balance.Int64() != 100000 || c.TotalClaimed.Int64() != 3200 || c.WithdrawRequestedAt != 1795000000 || c.RefundNonce.Int64() != 2 {
		t.Fatalf("%+v", c)
	}
	if _, err := ParseChannel(w(1), w(1), w(1)); err == nil {
		t.Error("short return data accepted")
	}
}
