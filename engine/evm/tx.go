package evm

import (
	"errors"
	"math/big"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

// DynamicFeeTx is an EIP-1559 (type 2) transaction with an empty access
// list. Direct settlement uses it to submit transferWithAuthorization; the
// key that signs it pays gas only.
type DynamicFeeTx struct {
	ChainID   *big.Int
	Nonce     uint64
	GasTipCap *big.Int // maxPriorityFeePerGas
	GasFeeCap *big.Int // maxFeePerGas
	Gas       uint64
	To        Address
	Value     *big.Int
	Data      []byte
}

func (tx *DynamicFeeTx) fields() []any {
	value := tx.Value
	if value == nil {
		value = new(big.Int)
	}
	return []any{tx.ChainID, tx.Nonce, tx.GasTipCap, tx.GasFeeCap, tx.Gas, tx.To[:], value, tx.Data, []any{}}
}

// SigningHash is keccak256(0x02 || rlp([chainId, nonce, tip, feeCap, gas,
// to, value, data, accessList])).
func (tx *DynamicFeeTx) SigningHash() ([32]byte, error) {
	enc, err := rlpEncode(tx.fields())
	if err != nil {
		return [32]byte{}, err
	}
	return Keccak256([]byte{0x02}, enc), nil
}

// Sign returns the raw signed transaction (0x02 || rlp([..., yParity, r,
// s])), ready for eth_sendRawTransaction, and its hash.
func (tx *DynamicFeeTx) Sign(key *secp256k1.PrivateKey) (raw []byte, hash [32]byte, err error) {
	if tx.ChainID == nil || tx.GasTipCap == nil || tx.GasFeeCap == nil {
		return nil, hash, errors.New("evm: transaction is missing chain id or fees")
	}
	h, err := tx.SigningHash()
	if err != nil {
		return nil, hash, err
	}
	compact := ecdsa.SignCompact(key, h[:], false) // [27+recid, r, s]
	yParity := uint64(compact[0] - 27)
	r := new(big.Int).SetBytes(compact[1:33])
	s := new(big.Int).SetBytes(compact[33:65])
	enc, err := rlpEncode(append(tx.fields(), yParity, r, s))
	if err != nil {
		return nil, hash, err
	}
	raw = append([]byte{0x02}, enc...)
	return raw, Keccak256(raw), nil
}

// TxSender recovers the address that signed a raw type-2 transaction (for
// tests and checks; the settler knows its own address).
func TxSender(tx *DynamicFeeTx, yParity byte, r, s []byte) (Address, error) {
	h, err := tx.SigningHash()
	if err != nil {
		return Address{}, err
	}
	sig := make([]byte, 65)
	copy(sig[32-len(r):32], r)
	copy(sig[64-len(s):64], s)
	sig[64] = 27 + yParity
	return RecoverSigner(h, sig)
}

// rlpEncode encodes byte strings, unsigned integers (uint64, *big.Int) and
// lists of those, as Ethereum's RLP.
func rlpEncode(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		if len(x) == 1 && x[0] < 0x80 {
			return []byte{x[0]}, nil
		}
		return append(rlpHeader(0x80, len(x)), x...), nil
	case uint64:
		return rlpEncode(new(big.Int).SetUint64(x))
	case *big.Int:
		if x.Sign() < 0 {
			return nil, errors.New("evm: rlp cannot encode a negative integer")
		}
		return rlpEncode(x.Bytes()) // big-endian, no leading zeros; zero is the empty string
	case []any:
		var body []byte
		for _, item := range x {
			enc, err := rlpEncode(item)
			if err != nil {
				return nil, err
			}
			body = append(body, enc...)
		}
		return append(rlpHeader(0xc0, len(body)), body...), nil
	}
	return nil, errors.New("evm: rlp cannot encode this type")
}

func rlpHeader(base byte, n int) []byte {
	if n < 56 {
		return []byte{base + byte(n)}
	}
	l := new(big.Int).SetInt64(int64(n)).Bytes()
	return append([]byte{base + 55 + byte(len(l))}, l...)
}
