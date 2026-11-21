package evm

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
)

// Vectors from the RLP specification (ethereum.org, "Recursive-length
// prefix").
func TestRLPVectors(t *testing.T) {
	lorem := []byte("Lorem ipsum dolor sit amet, consectetur adipisicing elit")
	for _, tc := range []struct {
		in   any
		want string
	}{
		{[]byte("dog"), "83646f67"},
		{[]any{[]byte("cat"), []byte("dog")}, "c88363617483646f67"},
		{[]byte{}, "80"},
		{[]any{}, "c0"},
		{uint64(0), "80"},
		{[]byte{0x00}, "00"},
		{[]byte{0x0f}, "0f"},
		{uint64(15), "0f"},
		{uint64(1024), "820400"},
		{[]any{[]any{}, []any{[]any{}}, []any{[]any{}, []any{[]any{}}}}, "c7c0c1c0c3c0c1c0"},
		{lorem, "b838" + hex.EncodeToString(lorem)},
	} {
		got, err := rlpEncode(tc.in)
		if err != nil || hex.EncodeToString(got) != tc.want {
			t.Errorf("rlp(%v) = %x, %v; want %s", tc.in, got, err, tc.want)
		}
	}
}

func testTx() *DynamicFeeTx {
	data, _ := hex.DecodeString("e3ee160e" + strings.Repeat("ab", 9*32))
	to, _ := ParseAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e")
	return &DynamicFeeTx{
		ChainID: big.NewInt(84532), Nonce: 7, GasTipCap: big.NewInt(1_000_000), GasFeeCap: big.NewInt(12_000_000),
		Gas: 120_000, To: to, Data: data,
	}
}

func TestDynamicFeeTxSignsAndRecovers(t *testing.T) {
	keyBytes, _ := hex.DecodeString(strings.Repeat("11", 32))
	key := secp256k1.PrivKeyFromBytes(keyBytes)
	tx := testTx()
	raw, hash, err := tx.Sign(key)
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != 0x02 || Keccak256(raw) != hash {
		t.Fatalf("raw %x hash %x", raw[:4], hash)
	}
	// The last three list items are yParity, r, s: recover the sender.
	h, _ := tx.SigningHash()
	sig := Sign(h, key)
	from, err := TxSender(tx, sig[64]-27, sig[:32], sig[32:64])
	if err != nil || from != PubkeyAddress(key.PubKey()) {
		t.Fatalf("sender %s, %v", from, err)
	}
	// Deterministic (RFC 6979): the same transaction signs identically.
	again, _, _ := tx.Sign(key)
	if hex.EncodeToString(again) != hex.EncodeToString(raw) {
		t.Fatal("signing is not deterministic")
	}
	t.Logf("raw %x", raw)
}
