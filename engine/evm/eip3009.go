package evm

import (
	"errors"
	"math/big"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

var (
	domainTypeHash = Keccak256([]byte(
		"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	transferTypeHash = Keccak256([]byte(
		"TransferWithAuthorization(address from,address to,uint256 value,uint256 validAfter,uint256 validBefore,bytes32 nonce)"))
)

// Domain is the EIP-712 domain of a token contract.
type Domain struct {
	Name              string
	Version           string
	ChainID           *big.Int
	VerifyingContract Address
}

// Separator returns the EIP-712 domain separator.
func (d Domain) Separator() [32]byte {
	name := Keccak256([]byte(d.Name))
	version := Keccak256([]byte(d.Version))
	return Keccak256(domainTypeHash[:], name[:], version[:], word(d.ChainID), addressWord(d.VerifyingContract))
}

// Authorization is an EIP-3009 transferWithAuthorization message.
type Authorization struct {
	From        Address
	To          Address
	Value       *big.Int
	ValidAfter  *big.Int
	ValidBefore *big.Int
	Nonce       [32]byte
}

// Digest returns the EIP-712 hash the payer signs.
func (a Authorization) Digest(d Domain) [32]byte {
	structHash := Keccak256(transferTypeHash[:], addressWord(a.From), addressWord(a.To),
		word(a.Value), word(a.ValidAfter), word(a.ValidBefore), a.Nonce[:])
	sep := d.Separator()
	return Keccak256([]byte{0x19, 0x01}, sep[:], structHash[:])
}

// RecoverSigner returns the address that produced sig (r || s || v) over digest.
//
// It applies the same strictness as USDC's on-chain ECRecover so that a
// signature accepted here is not rejected at settlement: v must be 27 or 28
// and s must be in the lower half of the curve order.
func RecoverSigner(digest [32]byte, sig []byte) (Address, error) {
	var addr Address
	if len(sig) != 65 {
		return addr, errors.New("signature must be 65 bytes")
	}
	v := sig[64]
	if v != 27 && v != 28 {
		return addr, errors.New("signature v must be 27 or 28")
	}
	var s secp256k1.ModNScalar
	if overflow := s.SetByteSlice(sig[32:64]); overflow || s.IsOverHalfOrder() {
		return addr, errors.New("signature s is not canonical")
	}
	compact := make([]byte, 65)
	compact[0] = v
	copy(compact[1:], sig[:64])
	pub, _, err := ecdsa.RecoverCompact(compact, digest[:])
	if err != nil {
		return addr, errors.New("signature does not recover")
	}
	return PubkeyAddress(pub), nil
}

// Sign produces an Ethereum-style signature (r || s || v, v in {27, 28}).
// The gateway never signs payments; this exists for clients and tests.
func Sign(digest [32]byte, key *secp256k1.PrivateKey) []byte {
	compact := ecdsa.SignCompact(key, digest[:], false)
	sig := make([]byte, 65)
	copy(sig, compact[1:])
	sig[64] = compact[0]
	return sig
}

// PubkeyAddress derives the account address of a public key.
func PubkeyAddress(pub *secp256k1.PublicKey) Address {
	h := Keccak256(pub.SerializeUncompressed()[1:])
	var a Address
	copy(a[:], h[12:])
	return a
}
