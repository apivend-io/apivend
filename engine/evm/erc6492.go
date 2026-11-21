package evm

import (
	"bytes"
	"errors"
	"math/big"
)

// ERC6492Magic ends a signature from a contract wallet that is not deployed
// yet (ERC-6492): abi.encode(factory, factoryCalldata, innerSignature) ||
// magic. Checking it means deploying the wallet first (simulated).
var ERC6492Magic = func() []byte {
	b := make([]byte, 32)
	for i := 0; i < 32; i += 2 {
		b[i], b[i+1] = 0x64, 0x92
	}
	return b
}()

// IsERC6492 reports whether sig carries the ERC-6492 suffix.
func IsERC6492(sig []byte) bool {
	return len(sig) >= 32 && bytes.Equal(sig[len(sig)-32:], ERC6492Magic)
}

// ParseERC6492 unwraps an ERC-6492 signature into the factory that deploys
// the wallet, the call that deploys it and the wallet's own signature. The
// input is attacker-controlled: every offset and length is bounds-checked.
func ParseERC6492(sig []byte) (factory Address, factoryData, inner []byte, err error) {
	if !IsERC6492(sig) {
		return factory, nil, nil, errors.New("evm: not an ERC-6492 signature")
	}
	body := sig[:len(sig)-32]
	if len(body) < 3*32 {
		return factory, nil, nil, errors.New("evm: ERC-6492 wrapper too short")
	}
	if !allZero(body[:12]) {
		return factory, nil, nil, errors.New("evm: ERC-6492 factory is not an address")
	}
	copy(factory[:], body[12:32])
	if factoryData, err = abiBytesAt(body, body[32:64]); err != nil {
		return factory, nil, nil, err
	}
	if inner, err = abiBytesAt(body, body[64:96]); err != nil {
		return factory, nil, nil, err
	}
	return factory, factoryData, inner, nil
}

// abiBytesAt reads a dynamic bytes value whose offset word is off.
func abiBytesAt(data, off []byte) ([]byte, error) {
	o, ok := smallWord(off)
	if !ok || o+32 > len(data) {
		return nil, errors.New("evm: ABI offset out of range")
	}
	n, ok := smallWord(data[o : o+32])
	if !ok || o+32+n > len(data) {
		return nil, errors.New("evm: ABI length out of range")
	}
	return data[o+32 : o+32+n], nil
}

// smallWord reads a 32-byte word that must fit comfortably in an int.
func smallWord(w []byte) (int, bool) {
	if len(w) != 32 || !allZero(w[:28]) {
		return 0, false
	}
	v := int(new(big.Int).SetBytes(w[28:]).Int64())
	return v, v >= 0 && v < 1<<20
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// The two overloads of USDC (FiatToken v2.2) transferWithAuthorization: v,
// r, s for 65-byte signatures, bytes for others (contract wallets). Both
// accept EIP-1271 contract signers.
const (
	transferWithAuthorizationVRS   = "transferWithAuthorization(address,address,uint256,uint256,uint256,bytes32,uint8,bytes32,bytes32)"
	transferWithAuthorizationBytes = "transferWithAuthorization(address,address,uint256,uint256,uint256,bytes32,bytes)"
)

// TransferWithAuthorizationCall encodes the token call that settles auth
// with sig: the v, r, s overload for a 65-byte signature, the bytes
// overload otherwise.
func TransferWithAuthorizationCall(a Authorization, sig []byte) []byte {
	head := [][]byte{AddressWord(a.From), AddressWord(a.To), word(a.Value), word(a.ValidAfter), word(a.ValidBefore), a.Nonce[:]}
	if len(sig) == 65 {
		v := sig[64]
		if v < 27 {
			v += 27
		}
		return CallData(transferWithAuthorizationVRS, append(head, word(big.NewInt(int64(v))), sig[:32], sig[32:64])...)
	}
	// bytes: an offset to the tail (7 head words), then length and data.
	return CallData(transferWithAuthorizationBytes, append(head, word(big.NewInt(7*32)), word(big.NewInt(int64(len(sig)))), padded(sig))...)
}

// Multicall3 is deployed at the same address on every EVM chain.
var Multicall3 = Address{0xca, 0x11, 0xbd, 0xe0, 0x59, 0x77, 0xb3, 0x63, 0x11, 0x67, 0x02, 0x88, 0x62, 0xbE, 0x2a, 0x17, 0x39, 0x76, 0xCA, 0x11}

// Call is one call in a Multicall3 batch.
type Call struct {
	Target Address
	Data   []byte
}

// Aggregate3 encodes Multicall3.aggregate3 with allowFailure false for every
// call, so the batch reverts if any call does.
func Aggregate3(calls []Call) []byte {
	sel := Keccak256([]byte("aggregate3((address,bool,bytes)[])"))
	var tuples [][]byte
	for _, c := range calls {
		t := append(AddressWord(c.Target), word(big.NewInt(0))...) // allowFailure false
		t = append(t, word(big.NewInt(3*32))...)                   // offset of callData in the tuple
		t = append(t, word(big.NewInt(int64(len(c.Data))))...)
		t = append(t, padded(c.Data)...)
		tuples = append(tuples, t)
	}
	out := append([]byte{}, sel[:4]...)
	out = append(out, word(big.NewInt(32))...) // offset of the array
	out = append(out, word(big.NewInt(int64(len(tuples))))...)
	off := 32 * len(tuples)
	for _, t := range tuples {
		out = append(out, word(big.NewInt(int64(off)))...)
		off += len(t)
	}
	for _, t := range tuples {
		out = append(out, t...)
	}
	return out
}

func padded(b []byte) []byte {
	return append(append([]byte{}, b...), make([]byte, (32-len(b)%32)%32)...)
}
