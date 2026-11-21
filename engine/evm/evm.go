// Package evm holds the small amount of Ethereum machinery the filter needs:
// addresses, Keccak-256, EIP-712 hashing for EIP-3009 and secp256k1 recovery.
// It deliberately avoids go-ethereum to keep the request-path binary small.
package evm

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"golang.org/x/crypto/sha3"
)

// Address is a 20-byte EVM account address.
type Address [20]byte

// MaxUint256 is the largest value an EVM uint256 can hold.
var MaxUint256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

// Keccak256 hashes the concatenation of its arguments.
func Keccak256(data ...[]byte) [32]byte {
	h := sha3.NewLegacyKeccak256()
	for _, d := range data {
		h.Write(d)
	}
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// ParseAddress accepts "0x" followed by exactly 40 hex digits in any case.
// The EIP-55 checksum is not enforced: clients commonly send lowercase.
func ParseAddress(s string) (Address, error) {
	var a Address
	b, err := parseHex(s, 20)
	if err != nil {
		return a, fmt.Errorf("address: %w", err)
	}
	copy(a[:], b)
	return a, nil
}

// ParseBytes32 accepts "0x" followed by exactly 64 hex digits.
func ParseBytes32(s string) ([32]byte, error) {
	var out [32]byte
	b, err := parseHex(s, 32)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

// ParseHex accepts "0x" followed by exactly 2*n hex digits, or any even
// number of them when n < 0.
func ParseHex(s string, n int) ([]byte, error) { return parseHex(s, n) }

func parseHex(s string, n int) ([]byte, error) {
	if !strings.HasPrefix(s, "0x") && !strings.HasPrefix(s, "0X") {
		return nil, errors.New("missing 0x prefix")
	}
	s = s[2:]
	if n < 0 && len(s)%2 != 0 {
		return nil, errors.New("odd number of hex digits")
	}
	if n >= 0 && len(s) != 2*n {
		return nil, fmt.Errorf("want %d hex digits, got %d", 2*n, len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, errors.New("invalid hex")
	}
	return b, nil
}

// ParseUint256 accepts a non-empty string of ASCII decimal digits that fits in
// a uint256. No sign, no whitespace, no exponent.
func ParseUint256(s string) (*big.Int, error) {
	if s == "" || len(s) > 78 {
		return nil, errors.New("not a uint256")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return nil, errors.New("not a uint256")
		}
	}
	v, _ := new(big.Int).SetString(s, 10)
	if v.Cmp(MaxUint256) > 0 {
		return nil, errors.New("not a uint256")
	}
	return v, nil
}

// String renders the address with its EIP-55 mixed-case checksum.
func (a Address) String() string {
	lower := hex.EncodeToString(a[:])
	hash := Keccak256([]byte(lower))
	out := []byte("0x" + lower)
	for i := 0; i < 40; i++ {
		c := out[2+i]
		if c < 'a' || c > 'f' {
			continue
		}
		nibble := hash[i/2]
		if i%2 == 0 {
			nibble >>= 4
		}
		if nibble&0x0f >= 8 {
			out[2+i] = c - 'a' + 'A'
		}
	}
	return string(out)
}

// Hex renders the address in lowercase, the form used for store keys.
func (a Address) Hex() string { return "0x" + hex.EncodeToString(a[:]) }

func word(v *big.Int) []byte {
	var w [32]byte
	v.FillBytes(w[:])
	return w[:]
}

func addressWord(a Address) []byte {
	var w [32]byte
	copy(w[12:], a[:])
	return w[:]
}

// CallData ABI-encodes a call to a function whose arguments are all static
// 32-byte words, e.g. CallData("balanceOf(address)", AddressWord(a)).
func CallData(signature string, words ...[]byte) []byte {
	sel := Keccak256([]byte(signature))
	out := append([]byte{}, sel[:4]...)
	for _, w := range words {
		out = append(out, w...)
	}
	return out
}

// AddressWord left-pads an address to a 32-byte ABI word.
func AddressWord(a Address) []byte { return addressWord(a) }
