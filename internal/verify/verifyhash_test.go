package verify

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"testing"

	"apivend.io/apivend/engine/evm"
)

// aggregate3Answer encodes (bool,bytes)[] as Multicall3 returns it.
func aggregate3Answer(results ...[]byte) []byte {
	w := func(n int) []byte { return new(big.Int).SetInt64(int64(n)).FillBytes(make([]byte, 32)) }
	var tuples [][]byte
	for _, r := range results {
		t := append(w(1), w(64)...)
		t = append(t, w(len(r))...)
		t = append(t, r...)
		t = append(t, make([]byte, (32-len(r)%32)%32)...)
		tuples = append(tuples, t)
	}
	out := append(w(32), w(len(tuples))...)
	off := 32 * len(tuples)
	for _, t := range tuples {
		out = append(out, w(off)...)
		off += len(t)
	}
	for _, t := range tuples {
		out = append(out, t...)
	}
	return out
}

type fakeCaller struct {
	code    string
	answer  []byte
	methods []string
	lastTo  string
}

func (f *fakeCaller) Call(_ context.Context, method string, params ...any) (json.RawMessage, error) {
	f.methods = append(f.methods, method)
	if method == "eth_getCode" {
		return json.Marshal(f.code)
	}
	f.lastTo = params[0].(map[string]string)["to"]
	return json.Marshal("0x" + hex.EncodeToString(f.answer))
}

func wrap6492(factory evm.Address, data, inner []byte) []byte {
	w := func(n int) []byte { return new(big.Int).SetInt64(int64(n)).FillBytes(make([]byte, 32)) }
	pad := func(b []byte) []byte { return append(append([]byte{}, b...), make([]byte, (32-len(b)%32)%32)...) }
	out := append(evm.AddressWord(factory), w(96)...)
	out = append(out, w(96+32+len(pad(data)))...)
	out = append(out, w(len(data))...)
	out = append(out, pad(data)...)
	out = append(out, w(len(inner))...)
	out = append(out, pad(inner)...)
	return append(out, evm.ERC6492Magic...)
}

func TestVerifyHashUndeployed(t *testing.T) {
	account, _ := evm.ParseAddress("0xC1f5BF53c9560C232c391EC444990CB25b960bbF")
	var digest [32]byte
	inner := []byte{1, 2, 3}
	sig := wrap6492(DefaultFactories[0], []byte{0xaa, 0xbb}, inner)
	if f, _, in, err := evm.ParseERC6492(sig); err != nil || f != DefaultFactories[0] || string(in) != string(inner) {
		t.Fatalf("test wrapper: %v", err)
	}
	accept := append(append([]byte{}, magic1271...), make([]byte, 28)...)

	for name, tc := range map[string]struct {
		answer []byte
		want   bool
	}{
		"wallet accepts":  {aggregate3Answer(nil, accept), true},
		"wallet declines": {aggregate3Answer(nil, make([]byte, 32)), false},
		"empty answer":    {aggregate3Answer(), false},
		"garbage answer":  {[]byte{1, 2, 3}, false},
		"huge offset":     {append(new(big.Int).Lsh(big.NewInt(1), 200).FillBytes(make([]byte, 32)), make([]byte, 64)...), false},
	} {
		fc := &fakeCaller{code: "0x", answer: tc.answer}
		c := &ChainChecker{Chains: map[string]Caller{"eip155:8453": fc}}
		got, err := c.VerifyHash(context.Background(), "eip155:8453", account, digest, sig)
		if err != nil || got != tc.want {
			t.Errorf("%s: %v %v", name, got, err)
		}
		if fc.lastTo != evm.Multicall3.Hex() {
			t.Errorf("%s: simulated through %s", name, fc.lastTo)
		}
	}

	// A factory that is not allowed is never simulated.
	other, _ := evm.ParseAddress("0x1111111111111111111111111111111111111111")
	fc := &fakeCaller{code: "0x", answer: aggregate3Answer(nil, accept)}
	c := &ChainChecker{Chains: map[string]Caller{"eip155:8453": fc}}
	if ok, _ := c.VerifyHash(context.Background(), "eip155:8453", account, digest, wrap6492(other, nil, inner)); ok || len(fc.methods) != 1 {
		t.Fatalf("other factory: %v, calls %v", ok, fc.methods)
	}

	// A deployed wallet is asked directly with the unwrapped signature.
	fc = &fakeCaller{code: "0x6080", answer: accept}
	c = &ChainChecker{Chains: map[string]Caller{"eip155:8453": fc}}
	if ok, err := c.VerifyHash(context.Background(), "eip155:8453", account, digest, sig); !ok || err != nil || fc.lastTo != account.Hex() {
		t.Fatalf("deployed: %v %v to %s", ok, err, fc.lastTo)
	}
}
