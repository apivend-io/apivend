package evm

import (
	"errors"
	"math/big"
)

// The x402 batch-settlement scheme on EVM (x402 specs/schemes/
// batch-settlement/scheme_batch_settlement_evm.md, v1.1): payers deposit
// into a channel of the x402BatchSettlement contract once, then sign
// cumulative vouchers per request; the receiver side claims many channels in
// one transaction and sweeps them with settle. Same addresses on every chain
// (CREATE2).
var (
	BatchSettlement         = mustAddress("0x4020074e9dF2ce1deE5A9C1b5c3f541D02a10003")
	ERC3009DepositCollector = mustAddress("0x4020806089470a89826cB9fB1f4059150b550004")
)

// Withdrawal delay bounds the contract enforces (15 minutes to 30 days).
const (
	MinWithdrawDelay = 15 * 60
	MaxWithdrawDelay = 30 * 24 * 3600
)

var (
	channelConfigTypeHash = Keccak256([]byte("ChannelConfig(address payer,address payerAuthorizer,address receiver,address receiverAuthorizer,address token,uint40 withdrawDelay,bytes32 salt)"))
	voucherTypeHash       = Keccak256([]byte("Voucher(bytes32 channelId,uint128 maxClaimableAmount)"))
	refundTypeHash        = Keccak256([]byte("Refund(bytes32 channelId,uint256 nonce,uint128 amount)"))
	claimEntryTypeHash    = Keccak256([]byte("ClaimEntry(bytes32 channelId,uint128 maxClaimableAmount,uint128 totalClaimed)"))
	claimBatchTypeHash    = Keccak256([]byte("ClaimBatch(ClaimEntry[] claims)ClaimEntry(bytes32 channelId,uint128 maxClaimableAmount,uint128 totalClaimed)"))
	receiveTypeHash       = Keccak256([]byte("ReceiveWithAuthorization(address from,address to,uint256 value,uint256 validAfter,uint256 validBefore,bytes32 nonce)"))
)

func mustAddress(s string) Address {
	a, err := ParseAddress(s)
	if err != nil {
		panic(err)
	}
	return a
}

// BatchDomain is the contract's EIP-712 domain on a chain.
func BatchDomain(chainID *big.Int) Domain {
	return Domain{Name: "x402 Batch Settlement", Version: "1", ChainID: chainID, VerifyingContract: BatchSettlement}
}

// ChannelConfig is the immutable configuration that identifies a channel.
type ChannelConfig struct {
	Payer Address
	// PayerAuthorizer signs vouchers (an EOA); zero means the payer itself,
	// checked through EIP-1271 when it is a contract.
	PayerAuthorizer Address
	// Receiver gets the claimed funds: the seller's payTo.
	Receiver Address
	// ReceiverAuthorizer signs claims and refunds (ours).
	ReceiverAuthorizer Address
	Token              Address
	WithdrawDelay      uint64 // seconds, uint40 on chain
	Salt               [32]byte
}

func (c ChannelConfig) structHash() [32]byte {
	return Keccak256(channelConfigTypeHash[:], addressWord(c.Payer), addressWord(c.PayerAuthorizer),
		addressWord(c.Receiver), addressWord(c.ReceiverAuthorizer), addressWord(c.Token),
		word(new(big.Int).SetUint64(c.WithdrawDelay)), c.Salt[:])
}

// ChannelID is the EIP-712 hash of the config under the chain's batch
// domain: the same config has a different id on another chain.
func (c ChannelConfig) ChannelID(chainID *big.Int) [32]byte {
	return typedHash(BatchDomain(chainID), c.structHash())
}

// VoucherDigest is what the payer (authorizer) signs: the channel may be
// claimed up to maxClaimable, cumulatively.
func VoucherDigest(chainID *big.Int, channelID [32]byte, maxClaimable *big.Int) [32]byte {
	return typedHash(BatchDomain(chainID), Keccak256(voucherTypeHash[:], channelID[:], word(maxClaimable)))
}

// RefundDigest is what the receiver authorizer signs to return unclaimed
// escrow to the payer before the withdrawal delay.
func RefundDigest(chainID *big.Int, channelID [32]byte, nonce, amount *big.Int) [32]byte {
	return typedHash(BatchDomain(chainID), Keccak256(refundTypeHash[:], channelID[:], word(nonce), word(amount)))
}

// ClaimEntry is one channel in a claim batch.
type ClaimEntry struct {
	ChannelID    [32]byte
	MaxClaimable *big.Int // the voucher's ceiling
	TotalClaimed *big.Int // the channel's new claimed total
}

// ClaimBatchDigest is what the receiver authorizer signs so that anyone (a
// facilitator, our own settler) may submit claimWithSignature.
func ClaimBatchDigest(chainID *big.Int, claims []ClaimEntry) [32]byte {
	var entries []byte
	for _, c := range claims {
		h := Keccak256(claimEntryTypeHash[:], c.ChannelID[:], word(c.MaxClaimable), word(c.TotalClaimed))
		entries = append(entries, h[:]...)
	}
	array := Keccak256(entries)
	return typedHash(BatchDomain(chainID), Keccak256(claimBatchTypeHash[:], array[:]))
}

// DepositNonce is the ERC-3009 nonce the deposit collector expects:
// keccak256(abi.encode(channelId, salt)).
func DepositNonce(channelID [32]byte, salt *big.Int) [32]byte {
	return Keccak256(channelID[:], word(salt))
}

// ReceiveDigest is the EIP-3009 receiveWithAuthorization digest a deposit
// carries (to: the deposit collector).
func (a Authorization) ReceiveDigest(d Domain) [32]byte {
	structHash := Keccak256(receiveTypeHash[:], addressWord(a.From), addressWord(a.To),
		word(a.Value), word(a.ValidAfter), word(a.ValidBefore), a.Nonce[:])
	return typedHash(d, structHash)
}

func typedHash(d Domain, structHash [32]byte) [32]byte {
	sep := d.Separator()
	return Keccak256([]byte{0x19, 0x01}, sep[:], structHash[:])
}

// Channel is a channel's on-chain state, from three views of the contract.
type Channel struct {
	Balance             *big.Int // deposited minus withdrawn and refunded
	TotalClaimed        *big.Int // claimed by the receiver side so far
	WithdrawRequestedAt uint64   // unix seconds; 0 when no timed withdrawal is pending
	RefundNonce         *big.Int
}

// The views: channels(id) -> (uint128 balance, uint128 totalClaimed);
// pendingWithdrawals(id) -> (uint128 amount, uint40 initiatedAt);
// refundNonce(id) -> uint256.
func ChannelsCall(id [32]byte) []byte { return CallData("channels(bytes32)", id[:]) }
func PendingWithdrawalsCall(id [32]byte) []byte {
	return CallData("pendingWithdrawals(bytes32)", id[:])
}
func RefundNonceCall(id [32]byte) []byte { return CallData("refundNonce(bytes32)", id[:]) }

// ParseChannel decodes the three views' return data.
func ParseChannel(channels, withdrawals, nonce []byte) (Channel, error) {
	if len(channels) != 64 || len(withdrawals) != 64 || len(nonce) != 32 {
		return Channel{}, errShortReturn
	}
	at := new(big.Int).SetBytes(withdrawals[32:64])
	if !at.IsUint64() {
		return Channel{}, errShortReturn
	}
	return Channel{
		Balance:             new(big.Int).SetBytes(channels[:32]),
		TotalClaimed:        new(big.Int).SetBytes(channels[32:64]),
		WithdrawRequestedAt: at.Uint64(),
		RefundNonce:         new(big.Int).SetBytes(nonce),
	}, nil
}

var errShortReturn = errors.New("evm: unexpected return data from the batch settlement contract")
