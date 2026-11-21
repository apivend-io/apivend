package receipts_test

import (
	"context"
	"testing"
	"time"

	"apivend.io/apivend/internal/pgtest"
	"apivend.io/apivend/internal/receipts"
)

func TestPostgresAppendOnlyAndIdempotent(t *testing.T) {
	dsn := pgtest.Start(t)
	ctx := context.Background()
	var store *receipts.Postgres
	var err error
	// The port accepts connections slightly before the server is ready.
	for i := 0; i < 50; i++ {
		if store, err = receipts.OpenPostgres(ctx, dsn); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Opening again must not fail on the existing schema.
	if again, err := receipts.OpenPostgres(ctx, dsn); err != nil {
		t.Fatal(err)
	} else {
		again.Close()
	}

	r := receipts.Receipt{
		ReservationID: "eip155:84532|0xasset|0xpayer|0xnonce", RequestID: "req-1", Route: "search",
		Payer: "0xpayer", Asset: "0xasset", Chain: "eip155:84532", Nonce: "0xnonce",
		// Larger than int64: amounts are uint256.
		Amount:     "115792089237316195423570985008687907853269984665640564039457584007913129639935",
		VerifiedAt: time.Unix(1_800_000_000, 0), ServedAt: time.Unix(1_800_000_001, 0),
		Status: receipts.StatusServed,
	}
	settled := r
	settled.Status, settled.TxHash, settled.Block, settled.Provider = receipts.StatusSettled, "0xabc", 42, "fac.example"

	for _, rc := range []receipts.Receipt{r, settled, settled, r} { // repeats are no-ops
		if err := store.Record(ctx, rc); err != nil {
			t.Fatal(err)
		}
	}

	var rows int
	store.DB().QueryRow(`SELECT count(*) FROM x402_receipts`).Scan(&rows)
	if rows != 2 {
		t.Fatalf("%d rows; want 2 (one per status)", rows)
	}
	var status, tx, amount string
	var block int64
	if err := store.DB().QueryRow(`SELECT status, tx_hash, block, amount::text FROM x402_receipts_current`).
		Scan(&status, &tx, &block, &amount); err != nil {
		t.Fatal(err)
	}
	if status != "settled" || tx != "0xabc" || block != 42 || amount != r.Amount {
		t.Fatalf("current = %s %s %d %s", status, tx, block, amount)
	}
}
