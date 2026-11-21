package evm

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"testing"
)

// Vectors generated with viem 2.58.2 (serializeErc6492Signature,
// encodeFunctionData); see testdata/erc6492_viem.json.
func vectors(t *testing.T) map[string]string {
	raw, err := os.ReadFile("testdata/erc6492_viem.json")
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]string
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func unhex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseERC6492MatchesViem(t *testing.T) {
	v := vectors(t)
	factory, data, inner, err := ParseERC6492(unhex(t, v["wrapped"]))
	if err != nil {
		t.Fatal(err)
	}
	if factory.String() != v["factory"] || !bytes.Equal(data, unhex(t, v["factoryData"])) || !bytes.Equal(inner, unhex(t, v["inner"])) {
		t.Fatalf("parsed %s %x %x", factory, data[:8], inner[:8])
	}
	// Malformed wrappers are refused, never read out of range.
	w := unhex(t, v["wrapped"])
	bad := map[string][]byte{
		"no magic":     w[:len(w)-1],
		"short":        append(make([]byte, 64), ERC6492Magic...),
		"huge offset":  append(append(append(make([]byte, 32), bytes.Repeat([]byte{0xff}, 32)...), make([]byte, 32)...), ERC6492Magic...),
		"dirty addr":   append(append(bytes.Repeat([]byte{1}, 32), w[32:len(w)-32]...), ERC6492Magic...),
		"length > end": func() []byte { c := append([]byte{}, w...); c[32*4-1] = 0xff; c[32*4-2] = 0xff; return c }(),
	}
	for name, b := range bad {
		if _, _, _, err := ParseERC6492(b); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func TestCallEncodingsMatchViem(t *testing.T) {
	v := vectors(t)
	from, _ := ParseAddress("0x8F52B124a85dc3E515260658Cf60Bb74A9c678a8")
	to, _ := ParseAddress("0x4C5d78BebbF28853c28eB385b936875e68107C60")
	var nonce [32]byte
	for i := range nonce {
		nonce[i] = 7
	}
	a := Authorization{From: from, To: to, Value: big.NewInt(10000), ValidAfter: big.NewInt(1), ValidBefore: big.NewInt(2000000000), Nonce: nonce}
	twa := TransferWithAuthorizationCall(a, unhex(t, v["inner"]))
	if hex.EncodeToString(twa) != strings.TrimPrefix(v["twaBytes"], "0x") {
		t.Fatalf("transferWithAuthorization(bytes) differs from viem")
	}
	factory, _ := ParseAddress(v["factory"])
	usdc, _ := ParseAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e")
	agg := Aggregate3([]Call{{Target: factory, Data: unhex(t, v["factoryData"])}, {Target: usdc, Data: twa}})
	if hex.EncodeToString(agg) != strings.TrimPrefix(v["agg"], "0x") {
		t.Fatalf("aggregate3 differs from viem")
	}
	if Multicall3.String() != "0xcA11bde05977b3631167028862bE2a173976CA11" {
		t.Fatalf("Multicall3 %s", Multicall3)
	}
}
