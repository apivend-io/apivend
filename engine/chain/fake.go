package chain

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"apivend.io/apivend/engine/evm"
)

// Fake is an in-memory chain for tests and local demos. It keeps balance
// history, so reads at an earlier block see the earlier balance.
type Fake struct {
	mu      sync.Mutex
	history map[evm.Address][]balanceAt
	used    map[[32]byte]bool
	events  []AuthEvent
	txs     map[string]uint64
	// transfers: each transaction's Transfer logs, by log index.
	transfers map[string]map[uint64]TransferLog
	logIndex  uint64
	Block     uint64
	Err       error // returned by every call when set
	Reads     int   // number of balance read calls served
	// MaxLogRange, when set, makes AuthEvents refuse wider block ranges, as
	// RPC providers do (the public Base endpoints allow 200).
	MaxLogRange uint64
}

type balanceAt struct {
	block   uint64
	balance *big.Int
}

func NewFake() *Fake {
	return &Fake{history: map[evm.Address][]balanceAt{}, used: map[[32]byte]bool{}, txs: map[string]uint64{}, Block: 100}
}

func (f *Fake) setLocked(owner evm.Address, v *big.Int) {
	f.Block++
	f.history[owner] = append(f.history[owner], balanceAt{f.Block, v})
}

func (f *Fake) balanceLocked(owner evm.Address, block uint64) *big.Int {
	out := new(big.Int)
	for _, h := range f.history[owner] {
		if h.block <= block {
			out = h.balance
		}
	}
	return new(big.Int).Set(out)
}

// SetBalance changes a balance in a new block.
func (f *Fake) SetBalance(owner evm.Address, v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setLocked(owner, big.NewInt(v))
}

// Mine advances the chain by n empty blocks.
func (f *Fake) Mine(n uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Block += n
}

func (f *Fake) SetErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Err = err
}

// Transfer applies an EIP-3009 transfer to no one in particular (the zero
// address); TransferTo names the recipient.
func (f *Fake) Transfer(from evm.Address, nonce [32]byte, amount *big.Int, txHash string) error {
	return f.TransferTo(from, evm.Address{}, nonce, amount, txHash)
}

// TransferTo applies an EIP-3009 transfer: marks the nonce used, debits
// the payer, and records the transaction with its AuthorizationUsed and
// Transfer logs. It fails as the token contract would.
func (f *Fake) TransferTo(from, to evm.Address, nonce [32]byte, amount *big.Int, txHash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.used[nonce] {
		return errors.New("authorization is used or canceled")
	}
	bal := f.balanceLocked(from, f.Block)
	if bal.Cmp(amount) < 0 {
		return errors.New("transfer amount exceeds balance")
	}
	f.setLocked(from, new(big.Int).Sub(bal, amount))
	f.used[nonce] = true
	f.logIndex++
	f.events = append(f.events, AuthEvent{Authorizer: from, Nonce: nonce, Block: f.Block, BlockHash: fakeHash(f.Block), TxHash: txHash, LogIndex: f.logIndex})
	if f.transfers == nil {
		f.transfers = map[string]map[uint64]TransferLog{}
	}
	if f.transfers[txHash] == nil {
		f.transfers[txHash] = map[uint64]TransferLog{}
	}
	f.logIndex++
	f.transfers[txHash][f.logIndex] = TransferLog{From: from, To: to, Value: new(big.Int).Set(amount)}
	f.txs[txHash] = f.Block
	return nil
}

// TransferAfter implements the RPC method of the same name.
func (f *Fake) TransferAfter(_ context.Context, txHash string, token evm.Address, logIndex uint64) (TransferLog, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return TransferLog{}, false, f.Err
	}
	t, ok := f.transfers[txHash][logIndex+1]
	t.Token = token
	return t, ok, nil
}

// Cancel marks an authorization cancelled by its signer, in a new block.
func (f *Fake) Cancel(authorizer evm.Address, nonce [32]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Block++
	f.used[nonce] = true
	f.events = append(f.events, AuthEvent{Authorizer: authorizer, Nonce: nonce, Canceled: true, Block: f.Block, BlockHash: fakeHash(f.Block)})
}

func (f *Fake) ReadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Reads
}

func fakeHash(block uint64) string { return fmt.Sprintf("0x%064x", block) }

func (f *Fake) BalanceOf(_ context.Context, _, owner evm.Address) (*big.Int, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Reads++
	if f.Err != nil {
		return nil, 0, f.Err
	}
	return f.balanceLocked(owner, f.Block), f.Block, nil
}

func (f *Fake) Head(context.Context) (uint64, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return 0, "", f.Err
	}
	return f.Block, fakeHash(f.Block), nil
}

func (f *Fake) BalancesAt(_ context.Context, _ evm.Address, owners []evm.Address, block uint64, blockHash string) ([]*big.Int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Reads++
	if f.Err != nil {
		return nil, f.Err
	}
	if blockHash != "" {
		if _, err := fmt.Sscanf(blockHash, "0x%x", &block); err != nil {
			return nil, errors.New("unknown block hash")
		}
	}
	out := make([]*big.Int, len(owners))
	for i, o := range owners {
		out[i] = f.balanceLocked(o, block)
	}
	return out, nil
}

func (f *Fake) AuthEvents(_ context.Context, _ evm.Address, authorizers []evm.Address, from, to uint64) ([]AuthEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return nil, f.Err
	}
	if f.MaxLogRange > 0 && to-from+1 > f.MaxLogRange {
		return nil, fmt.Errorf("eth_getLogs is limited to a %d range", f.MaxLogRange)
	}
	want := map[evm.Address]bool{}
	for _, a := range authorizers {
		want[a] = true
	}
	var out []AuthEvent
	for _, e := range f.events {
		if want[e.Authorizer] && e.Block >= from && e.Block <= to {
			out = append(out, e)
		}
	}
	return out, nil
}

// NonceEvents is AuthEvents for one authorizer's nonce.
func (f *Fake) NonceEvents(ctx context.Context, token, authorizer evm.Address, nonce [32]byte, from, to uint64) ([]AuthEvent, error) {
	evs, err := f.AuthEvents(ctx, token, []evm.Address{authorizer}, from, to)
	var out []AuthEvent
	for _, e := range evs {
		if e.Nonce == nonce {
			out = append(out, e)
		}
	}
	return out, err
}

func (f *Fake) AuthorizationState(_ context.Context, _, _ evm.Address, nonce [32]byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return false, f.Err
	}
	return f.used[nonce], nil
}

func (f *Fake) AuthorizationStateAt(ctx context.Context, token, authorizer evm.Address, nonce [32]byte, _ string) (bool, error) {
	return f.AuthorizationState(ctx, token, authorizer, nonce)
}

func (f *Fake) TransactionBlock(_ context.Context, txHash string) (uint64, bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return 0, false, false, f.Err
	}
	b, ok := f.txs[txHash]
	return b, ok, ok, nil
}
