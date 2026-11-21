package receipts

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"
)

const schema = `
CREATE TABLE IF NOT EXISTS x402_receipts (
    id             bigserial   PRIMARY KEY,
    recorded_at    timestamptz NOT NULL DEFAULT now(),
    reservation_id text        NOT NULL,
    status         text        NOT NULL,
    request_id     text        NOT NULL DEFAULT '',
    route          text        NOT NULL,
    payer          text        NOT NULL,
    amount         numeric(78,0) NOT NULL,
    asset          text        NOT NULL,
    chain          text        NOT NULL,
    nonce          text        NOT NULL,
    verified_at    timestamptz,
    served_at      timestamptz,
    provider       text        NOT NULL DEFAULT '',
    tx_hash        text        NOT NULL DEFAULT '',
    block          bigint      NOT NULL DEFAULT 0,
    reason         text        NOT NULL DEFAULT '',
    UNIQUE (reservation_id, status)
);
ALTER TABLE x402_receipts ADD COLUMN IF NOT EXISTS state_source text   NOT NULL DEFAULT '';
ALTER TABLE x402_receipts ADD COLUMN IF NOT EXISTS state_block  bigint NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS x402_receipts_payer   ON x402_receipts (payer, recorded_at);
CREATE INDEX IF NOT EXISTS x402_receipts_route   ON x402_receipts (route, recorded_at);
CREATE INDEX IF NOT EXISTS x402_receipts_tx_hash ON x402_receipts (tx_hash) WHERE tx_hash <> '';
-- One request's rows (the seller console matches credit purchases by it).
CREATE INDEX IF NOT EXISTS x402_receipts_request ON x402_receipts (request_id) WHERE request_id <> '';
-- Time windows (the demo's 24-hour statistics and totals).
CREATE INDEX IF NOT EXISTS x402_receipts_recorded ON x402_receipts (recorded_at);

-- Current state of each reservation: its most recent row.
DROP VIEW IF EXISTS x402_receipts_current;
CREATE VIEW x402_receipts_current AS
SELECT DISTINCT ON (reservation_id) *
FROM x402_receipts
ORDER BY reservation_id, id DESC;
`

// Postgres is the durable receipt store.
type Postgres struct{ db *sql.DB }

// OpenPostgres connects to dsn (a postgres:// URL or key=value string) and
// creates the schema if needed.
func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("receipts: invalid postgres DSN")
	}
	db.SetMaxOpenConns(8)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("receipts: create schema: %w", err)
	}
	return &Postgres{db: db}, nil
}

func (p *Postgres) Close() error { return p.db.Close() }

// DB exposes the handle for queries (reconciliation, exports, tests).
func (p *Postgres) DB() *sql.DB { return p.db }

func (p *Postgres) Record(ctx context.Context, r Receipt) error {
	_, err := p.db.ExecContext(ctx, `
INSERT INTO x402_receipts
    (reservation_id, status, request_id, route, payer, amount, asset, chain, nonce,
     verified_at, served_at, provider, tx_hash, block, reason, state_source, state_block)
VALUES ($1, $2, $3, $4, $5, $6::numeric, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
ON CONFLICT (reservation_id, status) DO NOTHING`,
		r.ReservationID, r.Status, r.RequestID, r.Route, r.Payer, r.Amount, r.Asset, r.Chain, r.Nonce,
		nullTime(r.VerifiedAt), nullTime(r.ServedAt), r.Provider, r.TxHash, int64(r.Block), r.Reason, r.StateSource, int64(r.StateBlock))
	return err
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
