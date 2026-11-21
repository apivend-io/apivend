package evm

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
)

func TestKeccakEmpty(t *testing.T) {
	got := Keccak256()
	want := "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
	if hex.EncodeToString(got[:]) != want {
		t.Fatalf("keccak256(\"\") = %x", got)
	}
}

func TestAddressFromKeyAndChecksum(t *testing.T) {
	// Private key 1 is a widely published vector.
	key := secp256k1.PrivKeyFromBytes(word(big.NewInt(1)))
	got := PubkeyAddress(key.PubKey()).String()
	if want := "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"; got != want {
		t.Fatalf("address = %s, want %s", got, want)
	}
	// EIP-55 specification examples.
	for _, s := range []string{
		"0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
		"0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
		"0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
		"0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
	} {
		a, err := ParseAddress(s)
		if err != nil {
			t.Fatal(err)
		}
		if a.String() != s {
			t.Errorf("checksum(%s) = %s", s, a)
		}
	}
}

func TestParseRejects(t *testing.T) {
	for _, s := range []string{"", "0x", "7E5F4552091A69125d5DfCb7b8C2659029395Bdf", "0x7E5F", "0xZZ5F4552091A69125d5DfCb7b8C2659029395Bdf"} {
		if _, err := ParseAddress(s); err == nil {
			t.Errorf("ParseAddress(%q) accepted", s)
		}
	}
	for _, s := range []string{"", "-1", "+1", " 1", "1e3", "0x10", "1.0",
		"115792089237316195423570985008687907853269984665640564039457584007913129639936"} { // 2^256
		if _, err := ParseUint256(s); err == nil {
			t.Errorf("ParseUint256(%q) accepted", s)
		}
	}
	if v, err := ParseUint256("115792089237316195423570985008687907853269984665640564039457584007913129639935"); err != nil || v.Cmp(MaxUint256) != 0 {
		t.Errorf("max uint256 rejected: %v", err)
	}
}

func testAuth() (Authorization, Domain) {
	asset, _ := ParseAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e")
	to, _ := ParseAddress("0x209693Bc6afc0C5328bA36FaF03C514EF312287C")
	d := Domain{Name: "USDC", Version: "2", ChainID: big.NewInt(84532), VerifyingContract: asset}
	a := Authorization{To: to, Value: big.NewInt(10000), ValidAfter: big.NewInt(1740672089), ValidBefore: big.NewInt(1740672154)}
	a.Nonce, _ = ParseBytes32("0xf3746613c2d920b5fdabc0856f2aeb2d4f88ee6037b8cc5d04a71a4462f13480")
	return a, d
}

func TestSignRecoverRoundTrip(t *testing.T) {
	key := secp256k1.PrivKeyFromBytes(word(big.NewInt(0xbeef)))
	a, d := testAuth()
	a.From = PubkeyAddress(key.PubKey())
	digest := a.Digest(d)
	sig := Sign(digest, key)

	got, err := RecoverSigner(digest, sig)
	if err != nil || got != a.From {
		t.Fatalf("recover = %s, %v; want %s", got, err, a.From)
	}

	// Any change to the signed fields must change the recovered signer.
	b := a
	b.Value = big.NewInt(10001)
	if got, err := RecoverSigner(b.Digest(d), sig); err == nil && got == a.From {
		t.Fatal("signature still valid after changing value")
	}
	d2 := d
	d2.ChainID = big.NewInt(8453)
	if got, err := RecoverSigner(a.Digest(d2), sig); err == nil && got == a.From {
		t.Fatal("signature valid on another chain")
	}
}

func TestRecoverStrictness(t *testing.T) {
	key := secp256k1.PrivKeyFromBytes(word(big.NewInt(0xbeef)))
	a, d := testAuth()
	digest := a.Digest(d)
	sig := Sign(digest, key)

	if _, err := RecoverSigner(digest, sig[:64]); err == nil {
		t.Error("64-byte signature accepted")
	}

	// v as 0/1 instead of 27/28: USDC's ECRecover rejects it, so must we.
	v01 := bytes.Clone(sig)
	v01[64] -= 27
	if _, err := RecoverSigner(digest, v01); err == nil {
		t.Error("v in {0,1} accepted")
	}

	// The malleable twin (n - s, flipped v) recovers to the same key but is
	// rejected on-chain; accepting it would serve a request that cannot settle.
	var s secp256k1.ModNScalar
	s.SetByteSlice(sig[32:64])
	s.Negate()
	twin := bytes.Clone(sig)
	sb := s.Bytes()
	copy(twin[32:64], sb[:])
	twin[64] = 27 + 28 - sig[64]
	if _, err := RecoverSigner(digest, twin); err == nil {
		t.Error("high-s signature accepted")
	}
}

// The x402 v2 specification's example payload carries a real signature, which
// makes it an independent check of the whole EIP-712 pipeline and of the Base
// Sepolia USDC domain (name "USDC", version "2").
func TestSpecExampleVector(t *testing.T) {
	a, d := testAuth()
	a.From, _ = ParseAddress("0x857b06519E91e3A54538791bDbb0E22373e36b66")
	sig, _ := ParseHex("0x2d6a7588d6acca505cbf0d9a4a227e0c52c6c34008c8e8986a1283259764173608a2ce6496642e377d6da8dbbf5836e9bd15092f9ecab05ded3d6293af148b571c", 65)
	got, err := RecoverSigner(a.Digest(d), sig)
	if err != nil {
		t.Fatal(err)
	}
	if got != a.From {
		t.Fatalf("recovered %s, want %s", got, a.From)
	}
}
