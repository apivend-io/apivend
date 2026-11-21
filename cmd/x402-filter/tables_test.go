package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"apivend.io/apivend/internal/pricing"
)

const oneRoute = `
routes:
  - name: search
    match: {pathPrefix: /search}
    price: {amount: "0.01", currency: USD}
    accepts:
      - {network: base-sepolia, asset: USDC, payTo: "0x209693Bc6afc0C5328bA36FaF03C514EF312287C"}
`

func newTables() *configMapTables {
	return &configMapTables{log: slog.New(slog.NewTextHandler(io.Discard, nil)), source: "ns/routes"}
}

func TestFirstTable(t *testing.T) {
	tb := newTables()
	if table, err := tb.first("", false); err != nil || table.Len() != 0 {
		t.Fatalf("missing ConfigMap: %d routes, %v; want an empty table", table.Len(), err)
	}
	if table, err := tb.first(oneRoute, true); err != nil || table.Len() != 1 {
		t.Fatalf("valid: %v", err)
	}
	if _, err := tb.first(strings.Replace(oneRoute, "price:", "prize:", 1), true); err == nil {
		t.Fatal("an invalid table at startup must be fatal")
	}
}

func TestUpdatesNeverDropPricing(t *testing.T) {
	tb := newTables()
	var current *pricing.Table
	tb.follow(func(t *pricing.Table) { current = t })

	tb.update(oneRoute, true)
	if current == nil || current.Len() != 1 {
		t.Fatal("valid update not applied")
	}
	priced := current

	tb.update("", false) // ConfigMap deleted
	if current != priced {
		t.Fatal("a deleted ConfigMap replaced the table")
	}
	tb.update("routes: [{name: x}]", true) // invalid
	if current != priced {
		t.Fatal("an invalid table replaced the current one")
	}
	tb.update("routes: []", true) // the controller removed every policy
	if current.Len() != 0 {
		t.Fatal("an empty table from the controller must apply")
	}
}

func TestUpdateBeforeFollowIsKept(t *testing.T) {
	tb := newTables()
	tb.update(oneRoute, true)
	var current *pricing.Table
	tb.follow(func(t *pricing.Table) { current = t })
	if current == nil || current.Len() != 1 {
		t.Fatal("a table that arrived before the server existed was lost")
	}
}
